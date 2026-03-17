# -*- coding: utf-8 -*-

"""
JSON 2 HTML Converter
=====================

(c) Varun Malhotra 2013
Source Code: https://github.com/softvar/json2html


Contributors:
-------------
1. Michel Müller (@muellermichel), https://github.com/softvar/json2html/pull/2
2. Daniel Lekic (@lekic), https://github.com/softvar/json2html/pull/17

LICENSE: MIT
--------
"""

import sys

if sys.version_info[:2] < (2, 7):
    from ordereddict import OrderedDict
    import simplejson as json_parser
else:
    from collections import OrderedDict
    import json as json_parser

if sys.version_info[:2] < (3, 0):
    from cgi import escape as html_escape
    text = unicode
    text_types = (unicode, str)
else:
    from html import escape as html_escape
    text = str
    text_types = (str,)


class Json2Html:
    def convert(self, json="", table_attributes='border="1"', clubbing=True, encode=False, escape=True):
        """
            Convert JSON to HTML Table format
        """
        # table attributes such as class, id, data-attr-*, etc.
        # eg: table_attributes = 'class = "table table-bordered sortable"'
        self.table_init_markup = "<table %s>" % table_attributes
        self.clubbing = clubbing
        self.escape = escape
        json_input = None
        if not json:
            json_input = {}
        elif type(json) in text_types:
            try:
                json_input = json_parser.loads(json, object_pairs_hook=OrderedDict)
            except ValueError as e:
                #so the string passed here is actually not a json string
                # - let's analyze whether we want to pass on the error or use the string as-is as a text node
                if u"Expecting property name" in text(e):
                    #if this specific json loads error is raised, then the user probably actually wanted to pass json, but made a mistake
                    raise e
                json_input = json
        else:
            json_input = json
        converted = self.convert_json_node(json_input)
        if encode:
            return converted.encode('ascii', 'xmlcharrefreplace')
        return converted

    def column_headers_from_list_of_dicts(self, json_input):
        """
            This method is required to implement clubbing.
            It tries to come up with column headers for your input
        """
        if not json_input \
        or not hasattr(json_input, '__getitem__') \
        or not hasattr(json_input[0], 'keys'):
            return None
        column_headers = json_input[0].keys()
        for entry in json_input:
            if not hasattr(entry, 'keys') \
            or not hasattr(entry, '__iter__') \
            or len(entry.keys()) != len(column_headers):
                return None
            for header in column_headers:
                if header not in entry:
                    return None
        return column_headers

    def convert_json_node(self, json_input):
        """
            Dispatch JSON input according to the outermost type and process it
            to generate the super awesome HTML format.
            We try to adhere to duck typing such that users can just pass all kinds
            of funky objects to json2html that *behave* like dicts and lists and other
            basic JSON types.
        """
        if type(json_input) in text_types:
            if self.escape:
                return html_escape(text(json_input))
            else:
                return text(json_input)
        if hasattr(json_input, 'items'):
            return self.convert_object(json_input)
        if hasattr(json_input, '__iter__') and hasattr(json_input, '__getitem__'):
            return self.convert_list(json_input)
        return text(json_input)

    def convert_list(self, list_input):
        """
            Iterate over the JSON list and process it
            to generate either an HTML table or a HTML list, depending on what's inside.
            If suppose some key has array of objects and all the keys are same,
            instead of creating a new row for each such entry,
            club such values, thus it makes more sense and more readable table.

            @example:
                jsonObject = {
                    "sampleData": [
                        {"a":1, "b":2, "c":3},
                        {"a":5, "b":6, "c":7}
                    ]
                }
                OUTPUT:
                _____________________________
                |               |   |   |   |
                |               | a | c | b |
                |   sampleData  |---|---|---|
                |               | 1 | 3 | 2 |
                |               | 5 | 7 | 6 |
                -----------------------------

            @contributed by: @muellermichel
        """
        if not list_input:
            return ""
        converted_output = ""
        column_headers = None
        if self.clubbing:
            column_headers = self.column_headers_from_list_of_dicts(list_input)
        if column_headers is not None:
            converted_output += self.table_init_markup
            converted_output += '<thead>'
            converted_output += '<tr><th>' + '</th><th>'.join(column_headers) + '</th></tr>'
            converted_output += '</thead>'
            converted_output += '<tbody>'
            for list_entry in list_input:
                converted_output += '<tr><td>'
                converted_output += '</td><td>'.join([self.convert_json_node(list_entry[column_header]) for column_header in
                                                     column_headers])
                converted_output += '</td></tr>'
            converted_output += '</tbody>'
            converted_output += '</table>'
            return converted_output

        #so you don't want or need clubbing eh? This makes @muellermichel very sad... ;(
        #alright, let's fall back to a basic list here...
        converted_output = '<ul><li>'
        converted_output += '</li><li>'.join([self.convert_json_node(child) for child in list_input])
        converted_output += '</li></ul>'
        return converted_output

    def convert_object(self, json_input):
        """
            Iterate over the JSON object and process it
            to generate the super awesome HTML Table format
        """
        if not json_input:
            return "" #avoid empty tables
        converted_output = self.table_init_markup + "<tr>"
        converted_output += "</tr><tr>".join([
            "<th>%s</th><td>%s</td>" %(
                self.convert_json_node(k),
                self.convert_json_node(v)
            )
            for k, v in json_input.items()
        ])
        converted_output += '</tr></table>'
        return converted_output

json2html = Json2Html()
