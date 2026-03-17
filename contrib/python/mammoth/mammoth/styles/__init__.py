import collections


def style(document_matcher, html_path):
    return Style(document_matcher, html_path)


Style = collections.namedtuple("Style", ["document_matcher", "html_path"])
