"""
Parsel lets you extract text from XML/HTML documents using XPath
or CSS selectors
"""

__author__ = "Scrapy project"
__email__ = "info@scrapy.org"
__version__ = "1.11.0"
__all__ = [
    "Selector",
    "SelectorList",
    "css2xpath",
    "xpathfuncs",
]

from parsel import xpathfuncs
from parsel.csstranslator import css2xpath
from parsel.selector import Selector, SelectorList

xpathfuncs.setup()
