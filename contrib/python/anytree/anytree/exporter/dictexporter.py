class DictExporter:
    """
    Tree to dictionary exporter.

    Every node is converted to a dictionary with all instance
    attributes as key-value pairs.
    Child nodes are exported to the children attribute.
    A list of dictionaries.

    Keyword Args:
        dictcls: class used as dictionary. :any:`dict` by default.
        attriter: attribute iterator for sorting and/or filtering.
        childiter: child iterator for sorting and/or filtering.
        maxlevel (int): Limit export to this number of levels.

    >>> from pprint import pprint  # just for nice printing
    >>> from anytree import AnyNode
    >>> from anytree.exporter import DictExporter
    >>> root = AnyNode(a="root")
    >>> s0 = AnyNode(a="sub0", parent=root)
    >>> s0a = AnyNode(a="sub0A", b="foo", parent=s0)
    >>> s0b = AnyNode(a="sub0B", parent=s0)
    >>> s1 = AnyNode(a="sub1", parent=root)

    >>> exporter = DictExporter()
    >>> pprint(exporter.export(root))
    {'a': 'root',
     'children': [{'a': 'sub0',
                   'children': [{'a': 'sub0A', 'b': 'foo'}, {'a': 'sub0B'}]},
                  {'a': 'sub1'}]}

    The attribute iterator `attriter` may be used for filtering too.
    For example, just dump attributes named `a`:

    >>> exporter = DictExporter(attriter=lambda attrs: [(k, v) for k, v in attrs if k == "a"])
    >>> pprint(exporter.export(root))
    {'a': 'root',
     'children': [{'a': 'sub0', 'children': [{'a': 'sub0A'}, {'a': 'sub0B'}]},
                  {'a': 'sub1'}]}

    The child iterator `childiter` can be used for sorting and filtering likewise:

    >>> exporter = DictExporter(childiter=lambda children: [child for child in children if "0" in child.a])
    >>> pprint(exporter.export(root))
    {'a': 'root',
     'children': [{'a': 'sub0',
                   'children': [{'a': 'sub0A', 'b': 'foo'}, {'a': 'sub0B'}]}]}
    """

    def __init__(self, dictcls=dict, attriter=None, childiter=list, maxlevel=None):
        self.dictcls = dictcls
        self.attriter = attriter
        self.childiter = childiter
        self.maxlevel = maxlevel

    def export(self, node):
        """Export tree starting at `node`."""
        attriter = self.attriter or (lambda attr_values: attr_values)
        return self.__export(node, self.dictcls, attriter, self.childiter)

    def __export(self, node, dictcls, attriter, childiter, level=1):
        attr_values = attriter(self._iter_attr_values(node))
        data = dictcls(attr_values)
        maxlevel = self.maxlevel
        if maxlevel is None or level < maxlevel:
            children = [
                self.__export(child, dictcls, attriter, childiter, level=level + 1)
                for child in childiter(node.children)
            ]
            if children:
                data["children"] = children
        return data

    @staticmethod
    def _iter_attr_values(node):
        # pylint: disable=C0103
        for k, v in node.__dict__.items():
            if k in ("_NodeMixin__children", "_NodeMixin__parent"):
                continue
            yield k, v
