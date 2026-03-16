import re
from urllib.parse import urljoin

import bs4
from bs4.element import Comment, NavigableString, Tag

_whitespace_to_space_regex = re.compile(r"[\n\t\r]+")
_reduce_spaces_regex = re.compile(r" {2,}")


def try_urljoin(base, url, allow_fragments=True):
    """attempts urljoin, on ValueError passes through url. Shortcuts http(s):// urls"""
    if url.startswith(("https://", "http://")):
        return url
    try:
        url = urljoin(base, url, allow_fragments=allow_fragments)
    except ValueError:
        pass
    return url


def get_attr(el, attr, check_name=None):
    """Get the attribute of an element if it exists.

    Args:
      el (bs4.element.Tag): a DOM element
      attr (string): the attribute to get
      check_name (string or list, optional): a list/tuple of strings or single
        string, that must match the element's tag name

    Returns:
      string: the attribute's value
    """
    if check_name is None:
        return el.get(attr)
    if isinstance(check_name, str) and el.name == check_name:
        return el.get(attr)
    if isinstance(check_name, (tuple, list)) and el.name in check_name:
        return el.get(attr)


def parse_srcset(srcset, base_url):
    """Return a dictionary of sources found in srcset."""
    sources = {}
    for url, descriptor in re.findall(
        r"(\S+)\s*([\d.]+[xw])?\s*,?\s*",
        srcset,
        re.MULTILINE,
    ):
        if not descriptor:
            descriptor = "1x"
        if descriptor not in sources:
            sources[descriptor] = try_urljoin(base_url, url.strip(","))
    return sources


def get_img(img, base_url):
    """Return a dictionary with src and alt/srcset if present, else just string src."""
    src = get_attr(img, "src", check_name="img")
    if src is None:
        return
    src = try_urljoin(base_url, src)
    alt = get_attr(img, "alt", check_name="img")
    srcset = get_attr(img, "srcset", check_name="img")
    if alt is not None or srcset:
        prop_value = {"value": src}
        if alt is not None:
            prop_value["alt"] = alt
        if srcset:
            prop_value["srcset"] = parse_srcset(srcset, base_url)
        return prop_value
    else:
        return src


def get_children(node):
    """An iterator over the immediate children tags of this tag"""
    for child in node.children:
        if isinstance(child, bs4.Tag) and child.name != "template":
            yield child


def get_descendents(node):
    """An iterator over the all children tags (descendants) of this tag"""
    for desc in node.descendants:
        if isinstance(desc, bs4.Tag) and desc.name != "template":
            yield desc


def get_textContent(el, replace_img=False, img_to_src=True, base_url=""):
    """Get the text content of an element, replacing images by alt or src"""

    DROP_TAGS = ("script", "style", "template")
    PRE_TAGS = ("pre",)
    P_BREAK_BEFORE = 1
    P_BREAK_AFTER = 0
    PRE_BEFORE = 2
    PRE_AFTER = 3

    def text_collection(el, replace_img=False, img_to_src=True, base_url=""):
        # returns array of strings or integers

        items = []

        # drops the tags defined above and comments
        if el.name in DROP_TAGS or isinstance(el, Comment):
            items = []

        elif isinstance(el, NavigableString):
            value = el
            # replace \t \n \r by space
            value = _whitespace_to_space_regex.sub(" ", value)
            # replace multiple spaces with one space
            items = [_reduce_spaces_regex.sub(" ", value)]

        # don't do anything special for PRE-formatted tags defined above
        elif el.name in PRE_TAGS:
            items = [PRE_BEFORE, el.get_text(), PRE_AFTER]

        elif el.name == "img" and replace_img:
            value = el.get("alt")
            if value is None and img_to_src:
                value = el.get("src")
                if value is not None:
                    value = try_urljoin(base_url, value)

            if value is not None:
                items = [" ", value, " "]

        elif el.name == "br":
            items = ["\n"]

        else:
            for child in el.children:
                child_items = text_collection(child, replace_img, img_to_src, base_url)
                items.extend(child_items)

            if el.name == "p":
                items = [P_BREAK_BEFORE] + items + [P_BREAK_AFTER, "\n"]

        return items

    results = [
        t for t in text_collection(el, replace_img, img_to_src, base_url) if t != ""
    ]

    if results:
        # remove <space> if it is first and last or if it is preceded by a <space> or <p> open/close
        length = len(results)
        for i in range(0, length):
            if results[i] == " " and (
                i == 0
                or i == length - 1
                or results[i - 1] == " "
                or results[i - 1] in (P_BREAK_BEFORE, P_BREAK_AFTER)
                or results[i + 1] == " "
                or results[i + 1] in (P_BREAK_BEFORE, P_BREAK_AFTER)
            ):
                results[i] = ""

    if results:
        # remove leading whitespace and <int> i.e. next lines
        while (
            isinstance(results[0], str) and (results[0] == "" or results[0].isspace())
        ) or results[0] in (P_BREAK_BEFORE, P_BREAK_AFTER):
            results.pop(0)
            if not results:
                break

    if results:
        # remove trailing whitespace and <int> i.e. next lines
        while (
            isinstance(results[-1], str)
            and (results[-1] == "" or results[-1].isspace())
        ) or results[-1] in (P_BREAK_BEFORE, P_BREAK_AFTER):
            results.pop(-1)
            if not results:
                break

    # trim leading and trailing non-<pre> whitespace
    if results:
        if isinstance(results[0], str):
            results[0] = results[0].lstrip()
        if isinstance(results[-1], str):
            results[-1] = results[-1].rstrip()

    # create final string by concatenating replacing consecutive sequence of <int> by largest value number of \n
    text = ""
    count = 0
    last = None
    for t in results:
        if t in (P_BREAK_BEFORE, P_BREAK_AFTER):
            count = max(t, count)
        elif t == PRE_BEFORE:
            text = text.rstrip(" ")
        elif not isinstance(t, int):
            if count or last == "\n":
                t = t.lstrip(" ")
            text = "".join([text, "\n" * count, t])
            count = 0
        last = t

    return text
