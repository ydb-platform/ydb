from __future__ import annotations

from typing import Iterable, Union

from annet.lib import add_annotation, flatten

from .annotate import AbstractAnnotateFormatter, annotate_formatter_connector
from .base import NONE_SEARCHER, TreeGenerator, _filter_str
from .exceptions import InvalidValueFromGenerator


class PartialGenerator(TreeGenerator):
    TYPE = "PARTIAL"

    def __init__(self, storage):
        super().__init__()
        self.storage = storage
        self._running_gen = None
        self._annotate: AbstractAnnotateFormatter = annotate_formatter_connector.get(self)
        self._annotations = []

    def supports_device(self, device) -> bool:
        if self.__class__.run is PartialGenerator.run:
            return bool(self._get_vendor_func(device.hw.vendor, "run"))
        else:
            return True

    def acl(self, device):
        if acl_func := self._get_vendor_func(device.hw.vendor, "acl"):
            return acl_func(device)
        return None

    def acl_safe(self, device):
        if acl_func := self._get_vendor_func(device.hw.vendor, "acl_safe"):
            return acl_func(device)
        return None

    def run(self, device) -> Iterable[Union[str, tuple]] | None:
        if run_func := self._get_vendor_func(device.hw.vendor, "run"):
            return run_func(device)
        return None

    def get_user_runner(self, device):
        if self.__class__.run is not PartialGenerator.run:
            return self.run
        return self._get_vendor_func(device.hw.vendor, "run")

    def _get_vendor_func(self, vendor: str, func_name: str):
        attr_name = f"{func_name}_{vendor}"
        return getattr(self, attr_name, None)

    # =====

    def __call__(self, device, annotate: bool = False):
        self._indents = []
        self._rows = []

        self._running_gen = self.run(device)
        for text in self._running_gen:
            if isinstance(text, tuple):
                text = " ".join(map(_filter_str, flatten(text)))
            else:
                text = _filter_str(text)
            self._append_text(text)

        for row, annotation in zip(self._rows, self._annotations):
            if NONE_SEARCHER.search(row):
                raise InvalidValueFromGenerator("Found 'None' in yield result: %s" % add_annotation(row, annotation))

        if annotate:
            generated_rows = (add_annotation(x, y) for (x, y) in zip(self._rows, self._annotations))
        else:
            generated_rows = self._rows

        return "\n".join((*generated_rows, ""))

    def _append_text(self, text):
        def annotation_cb(row):
            self._annotations.append(self._annotate.make_annotation(self._running_gen))
            return row

        self._append_text_cb(text, row_cb=annotation_cb)

    def get_running_line(self) -> tuple[str, int]:
        return self._annotate.get_running_line(self._running_gen)

    @classmethod
    def literal(cls, item):
        return '"{}"'.format(item)

    def __repr__(self):
        return "<%s>" % self.__class__.__name__
