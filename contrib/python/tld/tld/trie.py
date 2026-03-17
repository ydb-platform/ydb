__author__ = "Artur Barseghyan"
__copyright__ = "2013-2025 Artur Barseghyan"
__license__ = "MPL-1.1 OR GPL-2.0-only OR LGPL-2.1-or-later"
__all__ = (
    "Trie",
    "TrieNode",
)


class TrieNode(object):
    """Class representing a single Trie node."""

    __slots__ = ("children", "exception", "leaf", "private")

    def __init__(self):
        self.children = None
        self.exception = None
        self.leaf = False
        self.private = False


class Trie(object):
    """An adhoc Trie data structure to store tlds in reverse notation order."""

    def __init__(self):
        self.root = TrieNode()
        self.__nodes = 0

    def __len__(self):
        return self.__nodes

    def add(self, tld: str, private: bool = False) -> None:
        node = self.root

        # Iterating over the tld parts in reverse order
        # for part in reversed(tld.split('.')):
        tld_split = tld.split(".")
        tld_split.reverse()
        for part in tld_split:

            if part.startswith("!"):
                node.exception = part[1:]
                break

            # To save up some RAM, we initialize the children dict only
            # when strictly necessary
            if node.children is None:
                node.children = {}
                child = TrieNode()
            else:
                child = node.children.get(part)
                if child is None:
                    child = TrieNode()

            node.children[part] = child

            node = child

        node.leaf = True

        if private:
            node.private = True

        self.__nodes += 1
