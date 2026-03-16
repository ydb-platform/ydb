import abc
from types import GeneratorType

from annet.connectors import Connector

from .base import BaseGenerator


class AbstractAnnotateFormatter(abc.ABC):
    def __init__(self, gen: BaseGenerator):
        self.gen = gen
        self._annotation_module = ".".join(gen.__class__.__module__.split(".")[-2:])

    @abc.abstractmethod
    def make_annotation(self, running_gen: GeneratorType) -> str:
        raise NotImplementedError

    def get_running_line(self, running_gen: GeneratorType) -> tuple[str, int]:
        if not running_gen or not running_gen.gi_frame:
            return repr(running_gen), -1
        return self._annotation_module, running_gen.gi_frame.f_lineno


class DefaultAnnotateFormatter(AbstractAnnotateFormatter):
    def make_annotation(self, running_gen: GeneratorType) -> str:
        return "%s:%d" % self.get_running_line(running_gen)


class _AnnotateFormatterConnector(Connector[AbstractAnnotateFormatter]):
    name = "AnnotateFormatterConnector"
    ep_name = "annotate_formatter"

    def _get_default(self) -> type[AbstractAnnotateFormatter]:
        return DefaultAnnotateFormatter


annotate_formatter_connector = _AnnotateFormatterConnector()
