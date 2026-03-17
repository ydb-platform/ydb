import re
from abc import ABC
from os import path
from typing import Any, Dict, Iterable, List, OrderedDict, TypedDict, Union

from annet.annlib.lib import mako_render
from annet.annlib.rbparser.ordering import CompiledTree, compile_ordering_text
from annet.annlib.rbparser.platform import VENDOR_ALIASES
from annet.connectors import CachedConnector
from annet.rulebook.deploying import compile_deploying_text
from annet.rulebook.patching import compile_patching_text
from annet.vendors import registry_connector


class RulebookTexts(TypedDict):
    patching: str
    ordering: str
    deploying: str


class Rulebook(TypedDict):
    patching: Dict[str, OrderedDict[str, Any]]
    ordering: CompiledTree
    deploying: OrderedDict[str, Any]
    texts: RulebookTexts


class RulebookProvider(ABC):
    def get_rulebook(self, hw) -> Rulebook:
        raise NotImplementedError

    def get_root_modules(self) -> Iterable[str]:
        raise NotImplementedError


class _RulebookProviderConnector(CachedConnector[RulebookProvider]):
    name = "Rulebook provider"
    ep_name = "rulebook"


rulebook_provider_connector = _RulebookProviderConnector()


def get_rulebook(hw) -> Rulebook:
    return rulebook_provider_connector.get().get_rulebook(hw)


class DefaultRulebookProvider(RulebookProvider):
    root_dir = (path.dirname(__file__),)
    root_modules = ("annet.rulebook",)

    def __init__(
        self, root_dir: Union[str, Iterable[str], None] = None, root_modules: Union[str, Iterable[str], None] = None
    ):
        self._rulebook_cache = {}
        self._render_rul_cache = {}
        self._escaped_rul_cache = {}

        if root_dir is None:
            pass
        elif isinstance(root_dir, str):
            self.root_dir = (root_dir,)
        else:
            self.root_dir = tuple(root_dir)

        if root_modules is None:
            pass
        elif isinstance(root_modules, str):
            self.root_modules = (root_modules,)
        else:
            self.root_modules = tuple(root_modules)

    def get_root_modules(self):
        return self.root_modules

    def get_rulebook(self, hw) -> Rulebook:
        if hw in self._rulebook_cache:
            return self._rulebook_cache[hw]

        assert hw.vendor in registry_connector.get(), "Unknown vendor: %s" % (hw.vendor)
        rul_vendor_name = VENDOR_ALIASES.get(hw.vendor, hw.vendor)

        patching_text = self._render_rul(rul_vendor_name + ".rul", hw)
        patching = compile_patching_text(patching_text, rul_vendor_name)

        try:
            ordering_text = self._render_rul(hw.vendor + ".order", hw)
        except FileNotFoundError:
            ordering_text = ""
        ordering = compile_ordering_text(ordering_text, hw.vendor)

        try:
            deploying_text = self._render_rul(hw.vendor + ".deploy", hw)
        except FileNotFoundError:
            deploying_text = ""

        deploying = compile_deploying_text(deploying_text, hw.vendor)

        self._rulebook_cache[hw] = {
            "patching": patching,
            "ordering": ordering,
            "deploying": deploying,
            "texts": {
                "patching": patching_text,
                "ordering": ordering_text,
                "deploying": deploying_text,
            },
        }
        return self._rulebook_cache[hw]

    def _render_rul(self, name, hw):
        key = (name, hw)
        if key not in self._render_rul_cache:
            self._render_rul_cache[key] = mako_render(self._read_escaped_rul(name), hw=hw)
        return self._render_rul_cache[key]

    def _read_escaped_rul(self, name):
        if name in self._escaped_rul_cache:
            return self._escaped_rul_cache[name]
        for root_dir in self.root_dir:
            try:
                with open(path.join(root_dir, "texts", name), "r", encoding="utf-8") as f:
                    self._escaped_rul_cache[name] = self._escape_mako(f.read())
                    return self._escaped_rul_cache[name]
            except FileNotFoundError:
                pass

        raise FileNotFoundError(f"Unable to find rul: {name}")

    @staticmethod
    def _escape_mako(text):
        # Экранирование всего, что начинается на %, например %comment -> %%comment, чтобы он не интерпретировался
        # как mako-оператор
        text = re.sub(r"(?:^|\n)%((?!if\s*|elif\s*|else\s*|endif\s*|for\s*|endfor\s*))", "\n%%\\1", text)
        text = re.sub(r"(?:^|\n)\s*#.*", "", text)
        return text
