from markdown2 import markdown as orig_markdown


def markdown(*args, **kwargs):
    return orig_markdown(*args, **kwargs)
