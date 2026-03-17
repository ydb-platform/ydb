import warnings

from anytree.exporter.dotexporter import DotExporter


class RenderTreeGraph(DotExporter):
    def __init__(self, *args, **kwargs):
        """Legacy. Use :any:`anytree.exporter.DotExporter` instead."""
        warnings.warn(
            ("anytree.RenderTreeGraph has moved. Use anytree.exporter.DotExporter instead"),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
