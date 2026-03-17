import json

from .dictexporter import DictExporter


class JsonExporter:
    """
    Tree to JSON exporter.

    The tree is converted to a dictionary via `dictexporter` and exported to JSON.

    Keyword Arguments:
        dictexporter: Dictionary Exporter used (see :any:`DictExporter`).
        maxlevel (int): Limit export to this number of levels.
        kwargs: All other arguments are passed to
                :any:`json.dump`/:any:`json.dumps`.
                See documentation for reference.

    >>> from anytree import AnyNode
    >>> from anytree.exporter import JsonExporter
    >>> root = AnyNode(a="root")
    >>> s0 = AnyNode(a="sub0", parent=root)
    >>> s0a = AnyNode(a="sub0A", b="foo", parent=s0)
    >>> s0b = AnyNode(a="sub0B", parent=s0)
    >>> s1 = AnyNode(a="sub1", parent=root)

    >>> exporter = JsonExporter(indent=2, sort_keys=True)
    >>> print(exporter.export(root))
    {
      "a": "root",
      "children": [
        {
          "a": "sub0",
          "children": [
            {
              "a": "sub0A",
              "b": "foo"
            },
            {
              "a": "sub0B"
            }
          ]
        },
        {
          "a": "sub1"
        }
      ]
    }

    .. note:: Whenever the json output does not meet your expectations, see the :any:`json` documentation.
              For instance, if you have unicode/ascii issues, please try `JsonExporter(..., ensure_ascii=False)`.
    """

    def __init__(self, dictexporter=None, maxlevel=None, **kwargs):
        self.dictexporter = dictexporter
        self.maxlevel = maxlevel
        self.kwargs = kwargs

    def _export(self, node):
        dictexporter = self.dictexporter or DictExporter()
        if self.maxlevel is not None:
            dictexporter.maxlevel = self.maxlevel
        return dictexporter.export(node)

    def export(self, node):
        """Return JSON for tree starting at `node`."""
        data = self._export(node)
        return json.dumps(data, **self.kwargs)

    def write(self, node, filehandle):
        """Write JSON to `filehandle` starting at `node`."""
        data = self._export(node)
        return json.dump(data, filehandle, **self.kwargs)
