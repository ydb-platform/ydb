from __future__ import unicode_literals
import os
import sys

try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse

import csv
import cssutils

import requests
from lxml import etree
from inlinestyler.cssselect import CSSSelector, ExpressionError


class Conversion(object):
    def __init__(self):
        self.CSSErrors = []
        self.CSSUnsupportErrors = dict()
        self.supportPercentage = 100
        self.convertedHTML = ""

    def perform(self, document, sourceHTML, sourceURL, encoding='unicode'):
        aggregate_css = ""

        # Retrieve CSS rel links from html pasted and aggregate into one string
        CSSRelSelector = CSSSelector("link[rel=stylesheet],link[rel=StyleSheet],link[rel=STYLESHEET]")
        matching = CSSRelSelector.evaluate(document)
        for element in matching:
            try:
                csspath = element.get("href")
                if len(sourceURL):
                    if element.get("href").lower().find("http://", 0) < 0:
                        parsed_url = urlparse.urlparse(sourceURL)
                        csspath = urlparse.urljoin(parsed_url.scheme + "://" + parsed_url.hostname, csspath)

                css_content = requests.get(csspath).text
                aggregate_css += ''.join(css_content)

                element.getparent().remove(element)
            except:
                raise IOError('The stylesheet ' + element.get("href") + ' could not be found')

        # Include inline style elements
        CSSStyleSelector = CSSSelector("style,Style")
        matching = CSSStyleSelector.evaluate(document)
        for element in matching:
            aggregate_css += element.text
            element.getparent().remove(element)

        # Convert document to a style dictionary compatible with etree
        styledict = self.get_view(document, aggregate_css)

        # Set inline style attribute if not one of the elements not worth styling
        ignore_list = ['html', 'head', 'title', 'meta', 'link', 'script']
        for element, style in styledict.items():
            if element.tag not in ignore_list:
                v = style.getCssText(separator='')
                element.set('style', v)

        self.convertedHTML = etree.tostring(document, method="html", pretty_print=True, encoding=encoding)
        return self

    def styleattribute(self, element):
        """
          returns css.CSSStyleDeclaration of inline styles, for html: @style
          """
        css_text = element.get('style')
        if css_text:
            return cssutils.css.CSSStyleDeclaration(cssText=css_text)
        else:
            return None

    def get_view(self, document, css):

        view = {}
        specificities = {}
        supportratios = {}
        support_failrate = 0
        support_totalrate = 0
        compliance = dict()

        from importlib.resources import files
        with (files(__package__) / "css_compliance.csv").open() as csv_file:
            compat_list = csv_file.readlines()

        mycsv = csv.DictReader(compat_list, delimiter=str(','))

        for row in mycsv:
            # Count clients so we can calculate an overall support percentage later
            client_count = len(row)
            compliance[row['property'].strip()] = dict(row)

        # Decrement client count to account for first col which is property name
        client_count -= 1

        sheet = cssutils.parseString(css)

        rules = (rule for rule in sheet if rule.type == rule.STYLE_RULE)
        for rule in rules:

            for selector in rule.selectorList:
                try:
                    cssselector = CSSSelector(selector.selectorText)
                    matching = cssselector.evaluate(document)

                    for element in matching:
                        # add styles for all matching DOM elements
                        if element not in view:
                            # add initial
                            view[element] = cssutils.css.CSSStyleDeclaration()
                            specificities[element] = {}

                            # add inline style if present
                            inlinestyletext = element.get('style')
                            if inlinestyletext:
                                inlinestyle = cssutils.css.CSSStyleDeclaration(cssText=inlinestyletext)
                            else:
                                inlinestyle = None
                            if inlinestyle:
                                for p in inlinestyle:
                                    # set inline style specificity
                                    view[element].setProperty(p)
                                    specificities[element][p.name] = (1, 0, 0, 0)

                        for p in rule.style:
                            if p.name not in supportratios:
                                supportratios[p.name] = {'usage': 0, 'failedClients': 0}

                            supportratios[p.name]['usage'] += 1

                            try:
                                if p.name not in self.CSSUnsupportErrors:
                                    for client, support in compliance[p.name].items():
                                        if support == "N" or support == "P":
                                            # Increment client failure count for this property
                                            supportratios[p.name]['failedClients'] += 1
                                            if p.name not in self.CSSUnsupportErrors:
                                                if support == "P":
                                                    self.CSSUnsupportErrors[p.name] = [client + ' (partial support)']
                                                else:
                                                    self.CSSUnsupportErrors[p.name] = [client]
                                            else:
                                                if support == "P":
                                                    self.CSSUnsupportErrors[p.name].append(client + ' (partial support)')
                                                else:
                                                    self.CSSUnsupportErrors[p.name].append(client)

                            except KeyError:
                                pass

                            # update styles
                            if p not in view[element]:
                                view[element].setProperty(p.name, p.value, p.priority)
                                specificities[element][p.name] = selector.specificity
                            else:
                                sameprio = (p.priority == view[element].getPropertyPriority(p.name))
                                if not sameprio and bool(p.priority) or (sameprio and selector.specificity >= specificities[element][p.name]):
                                    # later, more specific or higher prio
                                    view[element].setProperty(p.name, p.value, p.priority)

                except ExpressionError:
                    if str(sys.exc_info()[1]) not in self.CSSErrors:
                        self.CSSErrors.append(str(sys.exc_info()[1]))
                    pass

        for props, propvals in supportratios.items():
            support_failrate += (propvals['usage']) * int(propvals['failedClients'])
            support_totalrate += int(propvals['usage']) * client_count

        if support_failrate and support_totalrate:
            self.supportPercentage = 100 - ((float(support_failrate) / float(support_totalrate)) * 100)
        return view
