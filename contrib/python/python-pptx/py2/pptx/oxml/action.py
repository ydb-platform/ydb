# encoding: utf-8

"""
lxml custom element classes for text-related XML elements.
"""

from __future__ import absolute_import

from .simpletypes import XsdString
from .xmlchemy import BaseOxmlElement, OptionalAttribute


class CT_Hyperlink(BaseOxmlElement):
    """
    Custom element class for <a:hlinkClick> elements.
    """

    rId = OptionalAttribute("r:id", XsdString)
    action = OptionalAttribute("action", XsdString)

    @property
    def action_fields(self):
        """
        A dictionary containing any key-value pairs present in the query
        portion of the `ppaction://` URL in the action attribute. For example
        `{'id':'0', 'return':'true'}` in
        'ppaction://customshow?id=0&return=true'. Returns an empty dictionary
        if the URL contains no query string or if no action attribute is
        present.
        """
        url = self.action

        if url is None:
            return {}

        halves = url.split("?")
        if len(halves) == 1:
            return {}

        key_value_pairs = halves[1].split("&")
        return dict([pair.split("=") for pair in key_value_pairs])

    @property
    def action_verb(self):
        """
        The host portion of the `ppaction://` URL contained in the action
        attribute. For example 'customshow' in
        'ppaction://customshow?id=0&return=true'. Returns |None| if no action
        attribute is present.
        """
        url = self.action

        if url is None:
            return None

        protocol_and_host = url.split("?")[0]
        host = protocol_and_host[11:]

        return host
