# -*- coding: utf-8 -*-
"""
Created : 2021-07-30

@author: Eric Lapouyade
"""
try:
    from html import escape
except ImportError:
    # cgi.escape is deprecated in python 3.7
    from cgi import escape


class RichText(object):
    """class to generate Rich Text when using templates variables

    This is much faster than using Subdoc class,
    but this only for texts INSIDE an existing paragraph.
    """

    def __init__(self, text=None, **text_prop):
        self.xml = ""
        if text:
            self.add(text, **text_prop)

    def add(
        self,
        text,
        style=None,
        color=None,
        highlight=None,
        size=None,
        subscript=None,
        superscript=None,
        bold=False,
        italic=False,
        underline=False,
        strike=False,
        font=None,
        url_id=None,
    ):

        # If a RichText is added
        if isinstance(text, RichText):
            self.xml += text.xml
            return

        # If not a string : cast to string (ex: int, dict etc...)
        if not isinstance(text, (str, bytes)):
            text = str(text)
        if not isinstance(text, str):
            text = text.decode("utf-8", errors="ignore")
        text = escape(text)

        prop = ""

        if style:
            prop += '<w:rStyle w:val="%s"/>' % style
        if color:
            if color[0] == "#":
                color = color[1:]
            prop += '<w:color w:val="%s"/>' % color
        if highlight:
            if highlight[0] == "#":
                highlight = highlight[1:]
            prop += '<w:shd w:fill="%s"/>' % highlight
        if size:
            prop += '<w:sz w:val="%s"/>' % size
            prop += '<w:szCs w:val="%s"/>' % size
        if subscript:
            prop += '<w:vertAlign w:val="subscript"/>'
        if superscript:
            prop += '<w:vertAlign w:val="superscript"/>'
        if bold:
            prop += "<w:b/>"
        if italic:
            prop += "<w:i/>"
        if underline:
            if underline not in [
                "single",
                "double",
                "thick",
                "dotted",
                "dash",
                "dotDash",
                "dotDotDash",
                "wave",
            ]:
                underline = "single"
            prop += '<w:u w:val="%s"/>' % underline
        if strike:
            prop += "<w:strike/>"
        if font:
            regional_font = ""
            if ":" in font:
                region, font = font.split(":", 1)
                regional_font = ' w:{region}="{font}"'.format(font=font, region=region)
            prop += '<w:rFonts w:ascii="{font}" w:hAnsi="{font}" w:cs="{font}"{regional_font}/>'.format(
                font=font, regional_font=regional_font
            )

        xml = "<w:r>"
        if prop:
            xml += "<w:rPr>%s</w:rPr>" % prop
        xml += '<w:t xml:space="preserve">%s</w:t></w:r>' % text
        if url_id:
            xml = '<w:hyperlink r:id="%s" w:tgtFrame="_blank">%s</w:hyperlink>' % (
                url_id,
                xml,
            )
        self.xml += xml

    def __unicode__(self):
        return self.xml

    def __str__(self):
        return self.xml

    def __html__(self):
        return self.xml


R = RichText
