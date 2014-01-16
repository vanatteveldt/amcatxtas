####################################################################################
#                          The MIT License (MIT)                                   #
#                                                                                  #
# Copyright (c) 2014 Wouter van Atteveldt                                          #
#                                                                                  #
# Permission is hereby granted, free of charge, to any person obtaining a copy of  #
# this software and associated documentation files (the "Software"), to deal in    #
# the Software without restriction, including without limitation the rights to     #
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of #
# the Software, and to permit persons to whom the Software is furnished to do so,  #
# subject to the following conditions:                                             #
#                                                                                  #
# The above copyright notice and this permission notice shall be included in all   #
# copies or substantial portions of the Software.                                  #
#                                                                                  #
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR       #
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS #
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR   #
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER   #
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN          #
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.       #
####################################################################################

"""
Process a set of articles and cache the results. Will filter on unprocessed articles
and work in random order, so can be run in parallel with minimal inefficiency.

It assumes that plugins have .xtas_key, .process(text), and .serialize() properties,
and that plugin.serialize(plugin.process(text)) returns the value to be cached

It also assumes (for now) that the 'source' index contains 'headline' and 'text' fields
"""

import re
import logging
log = logging.getLogger(__name__)

from elasticsearch import Elasticsearch
from elasticsearch.client import indices

# global settings
import os
ES_HOST=os.environ.get("ES_HOST", 'localhost')
ES_PORT=os.environ.get("ES_PORT", 9200)
ES_INDEX =os.environ.get('ES_INDEX', 'amcat')
ES_ARTICLE_DOCTYPE=os.environ.get('ES_ARICLE_DOCTYPE', 'article')


class NLPRunner(object):
    def __init__(self, plugin):
        """Create an NLPRunner with the given plugin, which should have .xtas_key and .process(text) properties"""
        self.es = Elasticsearch(hosts=[{"host":ES_HOST, "port":ES_PORT}])
        self.plugin = plugin
        self.check_mapping()

    def check_mapping(self):
        """Check that the mapping for cached results of this plugin exists and create it otherwise"""
        if not indices.IndicesClient(self.es).exists_type(ES_INDEX, self.doctype):
            body = {self.doctype : {"_parent" : {"type" : "article"}}}
            indices.IndicesClient(self.es).put_mapping(ES_INDEX, self.doctype, body=body)

    @property
    def doctype(self):
        return "_".join(self.plugin.xtas_key)

    def process_article(self, article):
        """Process one article and cache (store) the result"""
        aid = article["_id"]
        log.info("Parsing {aid}".format(**locals()))
        headline, text = article['fields']['headline'], article['fields']['text']
        pars = [headline] + text.split("\n\n")
        text = "\n\n".join(re.sub(r"\s+", " ", t) for t in pars)
        
        p = self.plugin.process(text)
        body = self.plugin.serialize(p)
        self.es.index(index=ES_INDEX, doc_type=self.doctype, id=aid, body=body, parent=aid)

    def get_filter(self, setid):
        """Create a DSL filter dict to filter on set and no existing parser"""
        noparse =  {"not" : {"has_child" : { "type": self.doctype,
                                             "query" : {"match_all" : {}}}}}
        return {"bool" : {"must" : [{"term" : {"sets" : setid}}, noparse]}}
        
    def get_articles(self, setid, size=1):
        """Return one or more ranbom uncached articles from the set"""
        body = {"query" : {"function_score" : {"filter" : self.get_filter(setid), "random_score" : {}}}}
        result = self.es.search(index=ES_INDEX, doc_type=ES_ARTICLE_DOCTYPE, body=body, fields=["headline", "text"], size=size)
        return result['hits']['hits']

    def process_articles(self, setid, size=1):
        """Process one or more random uncached articles from the given set"""
        for a in self.get_articles(setid, size=size):
            self.process_article(a)

    def progress(self, setid):
        """Return a todo, total pair to indicate how many articles exist without result / in total"""
        body = {"filter" : self.get_filter(setid)}
        result = self.es.search(index=ES_INDEX, doc_type=ES_ARTICLE_DOCTYPE, body=body, size=0)
        todo = result['hits']['total']
        print body
        body = {"filter" : {"term" : {"sets" : setid}}}
        result = self.es.search(index=ES_INDEX, doc_type=ES_ARTICLE_DOCTYPE, body=body, size=0)
        total = result['hits']['total']
        return todo, total
            
def import_attribute(module, attribute=None):
    """
    Import and return the attribute from the module
    If attribute is None, assume module is of form mo.du.le.attribute
    """
    if attribute is None:
        if "." in module:
            module, attribute = module.rsplit(".", 1)
        else:
            return __import__(module)
    mod = __import__(module, fromlist=[str(attribute)])
    try:
        return getattr(mod, attribute)
    except AttributeError:
        raise ImportError("Module %r has no attribute %r" % (module, attribute))

    
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--progress', action='store_const', const=True)
    parser.add_argument('plugin')
    parser.add_argument('articleset', type=int)
    args = parser.parse_args()

    plugin = import_attribute(args.plugin)
    
    n = NLPRunner(plugin)
    if args.progress:
        print n.progress(args.articleset)
    else:    
        n.process_articles(args.articleset)
