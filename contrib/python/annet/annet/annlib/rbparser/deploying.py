import functools
import re
from collections import OrderedDict as odict
from collections import namedtuple


Answer = namedtuple("Answer", ("text", "send_nl"))


def compile_messages(tree):
    ignore = []
    dialogs = odict()
    for attrs in tree.values():
        if attrs["type"] == "normal":
            match = re.match(r"^ignore:(.+)$", attrs["row"])
            if match:
                ignore.append(MakeMessageMatcher(match.group(1)))
                continue

            match = re.match(r"^dialog:(.+):::(.+)$", attrs["row"])
            if match:
                dialogs[MakeMessageMatcher(match.group(1))] = Answer(
                    text=match.group(2).strip(),
                    send_nl=attrs["params"]["send_nl"],
                )
    return (ignore, dialogs)


class MakeMessageMatcher:
    def __init__(self, text):
        text = text.strip()
        self._text = text
        if text.startswith("/") and text.endswith("/"):
            regexp = re.compile(text[1:-1].strip(), flags=re.I)
            self._fn = regexp.match
        else:
            self._fn = lambda arg: _simplify_text(text) in _simplify_text(arg)

    def __str__(self):
        return "%s(%r)" % (self.__class__.__name__, self._text)

    __repr__ = __str__

    def __call__(self, intext):
        return self._fn(intext)

    def __eq__(self, other):
        return type(other) is type(self) and self._text == other._text  # pylint: disable=protected-access

    def __hash__(self):
        return hash("%s_%s" % (self.__class__.__name__, self._text))


@functools.lru_cache()
def _simplify_text(text):
    return re.sub(r"\s", "", text).lower()
