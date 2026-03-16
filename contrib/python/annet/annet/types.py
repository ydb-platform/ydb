from collections import OrderedDict
from typing import Any, Dict, List, MutableMapping, NamedTuple, Optional, Tuple, Union

from annet.annlib.types import Op  # pylint: disable=unused-import
from annet.storage import Device, Storage


class PCDiffFile(NamedTuple):
    label: str
    diff_lines: List[str]


class PCDiff(NamedTuple):
    hostname: str
    diff_files: List[PCDiffFile]


Diff = List[Tuple]
ExitCode = int


class GeneratorPerf:
    """
    Рантайм статистика времени выполнения генератора
    """

    def __init__(
        self, total: float, rt: Optional[Dict[str, List[Dict[str, Any]]]], meta: Optional[Dict[str, Any]] = None
    ):
        self.total = total
        self.rt = rt
        self._meta = meta

    @property
    def meta(self) -> Dict[str, Any]:
        return self._meta or {}


class GeneratorPartialRunArgs:
    """
    Параметры и модификаторы для запуска run_partial_generators
    """

    def __init__(
        self,
        device: Device,
        use_acl: bool = False,
        use_acl_safe: bool = False,
        annotate: bool = False,
        generators_context: Optional[str] = None,
        no_new: bool = False,
    ):
        self.device = device
        self.use_acl = use_acl  # фильтруем по acl ввыод генератора (--no-acl для дебага)
        self.use_acl_safe = use_acl_safe  # [NOCDEV-6190] используем более строгий генераторный acl
        self.annotate = annotate  # добавляем в каждую строку вывода информацию откуда она была заyield'ена
        self.generators_context = generators_context  # строка с именем контекста генераторов
        self.no_new = no_new  # для опции --clear, не пытаемся запустить генераторы, выдаем только acl


class GeneratorPartialResult:
    """
    Результат запуска Partial-генератора
    """

    def __init__(
        self,
        name: str,
        tags: List[str],
        acl: str,
        acl_rules: Dict[str, Any],  # OrderedDict
        acl_safe: str,
        acl_safe_rules: Dict[str, Any],  # OrderedDict
        output: str,
        config: Dict[str, Any],  # OrderedDict
        safe_config: Dict[str, Any],  # OrderedDict
        perf: GeneratorPerf,
    ):
        self.name = name
        self.tags = tags
        self.acl = acl
        self.acl_rules = acl_rules
        self.acl_safe = acl_safe
        self.acl_safe_rules = acl_safe_rules
        self.output = output
        self.config = config
        self.safe_config = safe_config
        self.perf = perf


class GeneratorEntireResult:
    """
    Результат запуска Entire-генератора
    """

    def __init__(
        self,
        name: str,
        tags: List[str],
        path: Optional[str],
        output: str,
        reload: str,
        prio: int,
        perf: GeneratorPerf,
        is_safe: bool,
    ):
        self.name = name
        self.tags = tags
        self.path = path
        self.output = output
        self.reload = reload
        self.prio = prio
        self.perf = perf
        self.is_safe = is_safe


class GeneratorJSONFragmentResult:
    """
    Результат запуска JSONFragment-генератора
    """

    def __init__(
        self,
        name: str,
        tags: List[str],
        path: str,
        acl: List[str],
        acl_safe: List[str],
        config: Dict[str, Any],
        reload: str,
        perf: GeneratorPerf,
        reload_prio: int,
    ):
        self.name = name
        self.tags = tags
        self.path = path
        self.acl = acl
        self.acl_safe = acl_safe
        self.config = config
        self.reload = reload
        self.perf = perf
        self.reload_prio = reload_prio


GeneratorResult = Union[GeneratorEntireResult, GeneratorPartialResult, GeneratorJSONFragmentResult]


class OldNewResult:
    """Результат запуска old_new"""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        device=None,
        old=None,
        new=None,
        acl_rules=None,
        new_files=None,
        old_files=None,
        err=None,
        partial_result=None,
        entire_result=None,
        old_json_fragment_files=None,
        new_json_fragment_files=None,
        json_fragment_result=None,
        implicit_rules=None,
        perf=None,
        acl_safe_rules=None,
        safe_old=None,
        safe_new=None,
        safe_new_files=None,
        safe_new_json_fragment_files=None,
        filter_acl_rules=None,
    ):
        self.device: Device = device
        self.old: MutableMapping = old if old else OrderedDict()
        self.new: MutableMapping = new if new else OrderedDict()
        self.acl_rules: MutableMapping = acl_rules
        self.new_files: MutableMapping = new_files if new_files else {}
        self.old_files: MutableMapping = old_files if old_files else {}
        self.err: Optional[Exception] = err
        self.partial_results: Dict[str, GeneratorPartialResult] = partial_result or {}
        self.entire_results: Dict[str, GeneratorEntireResult] = entire_result or {}
        self.old_json_fragment_files: Dict[str, Any] = old_json_fragment_files or {}
        self.new_json_fragment_files: Dict[str, Tuple[Any, Optional[str]]] = new_json_fragment_files or {}
        self.json_fragment_results: Dict[str, GeneratorJSONFragmentResult] = json_fragment_result or {}
        self.implicit_rules: Dict[str, Any] = implicit_rules or OrderedDict()
        self.perf: Dict[str, Dict[str, float]] = perf or {}

        # safe acl and configs with it applied
        self.acl_safe_rules: MutableMapping = acl_safe_rules or {}
        self.safe_old: MutableMapping = safe_old if safe_old else OrderedDict()
        self.safe_new: MutableMapping = safe_new if safe_new else OrderedDict()
        self.safe_new_files: MutableMapping = safe_new_files if safe_new_files else {}
        self.safe_new_json_fragment_files: MutableMapping = safe_new_json_fragment_files or {}

        self.filter_acl_rules: Optional[MutableMapping] = filter_acl_rules

    def get_old(self, safe: bool = False) -> MutableMapping:
        if safe:
            return self.safe_old

        return self.old

    def get_new(self, safe: bool = False) -> MutableMapping:
        if safe:
            return self.safe_new

        return self.new

    def get_acl_rules(self, safe: bool = False) -> MutableMapping:
        if safe:
            return self.acl_safe_rules

        return self.acl_rules

    def get_new_files(self, safe: bool = False) -> MutableMapping:
        if safe:
            return self.safe_new_files

        return self.new_files

    def get_new_file_fragments(self, safe: bool = False) -> Dict[str, Tuple[Any, Optional[str]]]:
        if safe:
            return self.safe_new_json_fragment_files

        return self.new_json_fragment_files
