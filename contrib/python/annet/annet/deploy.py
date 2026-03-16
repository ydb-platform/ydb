# pylint: disable=unused-argument
import abc
import itertools
from collections import namedtuple
from typing import Any, Dict, List, Optional, OrderedDict, Tuple, Type

from contextlog import get_logger

from annet.annlib.command import Command, CommandList, Question
from annet.annlib.netdev.views.hardware import HardwareView
from annet.annlib.rbparser.deploying import Answer, MakeMessageMatcher
from annet.cli_args import DeployOptions
from annet.connectors import Connector, get_connector_from_config
from annet.rulebook import deploying, get_rulebook
from annet.storage import Device


_DeployResultBase = namedtuple("_DeployResultBase", ("hostnames", "results", "durations", "original_states"))


class ProgressBar(abc.ABC):
    @abc.abstractmethod
    def set_content(self, tile_name: str, content: str):
        pass

    @abc.abstractmethod
    def add_content(self, tile_name: str, content: str):
        pass

    @abc.abstractmethod
    def reset_content(self, tile_name: str):
        pass

    @abc.abstractmethod
    def set_progress(
        self,
        tile_name: str,
        iteration: int,
        total: int,
        prefix: str = "",
        suffix: str = "",
        fill: str = "",
        error: bool = False,
    ):
        pass

    @abc.abstractmethod
    def set_exception(self, tile_name: str, cmd_exc: str, last_cmd: str, progress_max: int, content: str = "") -> None:
        pass


class DeployResult(_DeployResultBase):  # noqa: E302
    def add_results(self, results: dict[str, Exception]) -> None:
        for hostname, excs in results.items():
            self.hostnames.append(hostname)
            self.results[hostname] = excs
            self.durations[hostname] = 0.0
            self.original_states[hostname] = None


class _FetcherConnector(Connector["Fetcher"]):
    name = "Fetcher"
    ep_name = "deploy_fetcher"
    ep_by_group_only = "annet.connectors.fetcher"

    def _get_default(self) -> Type["Fetcher"]:
        # if entry points are broken, try to use direct import
        import annet.adapters.fetchers.stub.fetcher as stub_fetcher

        return stub_fetcher.StubFetcher


class _DriverConnector(Connector["DeployDriver"]):
    name = "DeployDriver"
    ep_name = "deploy_driver"
    ep_by_group_only = "annet.connectors.deployer"

    def _get_default(self) -> Type["DeployDriver"]:
        # if entry points are broken, try to use direct import
        import annet.adapters.deployers.stub.deployer as stub_deployer

        return stub_deployer.StubDeployDriver


fetcher_connector = _FetcherConnector()
driver_connector = _DriverConnector()


class Fetcher(abc.ABC):
    @abc.abstractmethod
    async def fetch_packages(
        self,
        devices: list[Device],
        processes: int = 1,
        max_slots: int = 0,
    ) -> tuple[dict[Device, str], dict[Device, Any]]:
        pass

    @abc.abstractmethod
    async def fetch(
        self,
        devices: list[Device],
        files_to_download: dict[str, list[str]] | None = None,
        processes: int = 1,
        max_slots: int = 0,
    ):
        pass


def get_fetcher() -> Fetcher:
    connectors = fetcher_connector.get_all()
    fetcher, _ = get_connector_from_config("fetcher", connectors)
    return fetcher


class DeployDriver(abc.ABC):
    @abc.abstractmethod
    async def bulk_deploy(
        self, deploy_cmds: dict, args: DeployOptions, progress_bar: ProgressBar | None = None
    ) -> DeployResult:
        pass

    @abc.abstractmethod
    def apply_deploy_rulebook(self, hw: HardwareView, cmd_paths, do_finalize=True, do_commit=True):
        pass

    @abc.abstractmethod
    def build_configuration_cmdlist(self, hw: HardwareView, do_finalize=True, do_commit=True):
        pass

    @abc.abstractmethod
    def build_exit_cmdlist(self, hw):
        pass


def get_deployer() -> DeployDriver:
    connectors = driver_connector.get_all()
    deployer, _ = get_connector_from_config("deployer", connectors)
    return deployer


# ===
def scrub_config(text: str, breed: str) -> str:
    return text


def show_bulk_report(hostnames, results, durations, log_dir):
    pass


class RulebookQuestionHandler:
    def __init__(self, dialogs):
        self._dialogs = dialogs

    def __call__(self, dev: Connector, cmd: Command, match_content: bytes):
        content = match_content.strip()
        content = content.decode()
        for matcher, answer in self._dialogs.items():
            if matcher(content):
                return Command(answer.text)

        get_logger().info("no answer in rulebook. dialogs=%s match_content=%s", self._dialogs, match_content)
        return None


def rb_question_to_question(q: MakeMessageMatcher, a: Answer) -> Question:  # TODO: drop MakeMessageMatcher
    text: str = q._text  # pylint: disable=protected-access
    answer: str = a.text
    is_regexp = False
    if text.startswith("/") and text.endswith("/"):
        is_regexp = True
        text = text[1:-1]
    res = Question(question=text, answer=answer, is_regexp=is_regexp, not_send_nl=not a.send_nl)
    return res


def make_cmd_params(rule: Dict[str, Any]) -> Dict[str, Any]:
    if rule:
        qa_handler = RulebookQuestionHandler(rule["attrs"]["dialogs"])
        qa_list: List[Question] = []
        for matcher, answer in qa_handler._dialogs.items():  # pylint: disable=protected-access
            qa_list.append(rb_question_to_question(matcher, answer))
        return {
            "questions": qa_list,
            "timeout": rule["attrs"]["timeout"],
        }
    return {
        "timeout": 30,
    }


def make_apply_commands(rule: dict, hw: HardwareView, do_commit: bool, do_finalize: bool, path: Optional[str] = None):
    apply_logic = rule["attrs"]["apply_logic"]
    before, after = apply_logic(hw, do_commit=do_commit, do_finalize=do_finalize, path=path)
    return before, after


def fill_cmd_params(rules: OrderedDict, cmd: Command):
    rule = deploying.match_deploy_rule(rules, (cmd.cmd,), {})
    if rule:
        cmd_params = make_cmd_params(rule)
        cmd.questions = cmd_params.get("questions", None)
        if cmd.timeout is None:
            cmd.timeout = cmd_params["timeout"]
        if cmd.read_timeout is None:
            cmd.read_timeout = cmd.timeout


def apply_deploy_rulebook(hw: HardwareView, cmd_paths, do_finalize=True, do_commit=True):
    rules = get_rulebook(hw)["deploying"]
    cmds_with_apply = []
    for cmd_path, context in cmd_paths.items():
        rule = deploying.match_deploy_rule(rules, cmd_path, context)
        cmd_params = make_cmd_params(rule)
        before, after = make_apply_commands(rule, hw, do_commit, do_finalize)

        cmd = Command(cmd_path[-1], **cmd_params)
        # XXX более чистый способ передавать-мета инфу о команде
        cmd.level = len(cmd_path) - 1
        cmds_with_apply.append((cmd, before, after))

    def _key(item):
        _cmd, before, after = item
        return (tuple(cmd.cmd for cmd in before), tuple(cmd.cmd for cmd in after))

    cmdlist = CommandList()
    for _k, cmd_before_after in itertools.groupby(cmds_with_apply, key=_key):
        cmd_before_after = list(cmd_before_after)
        _, before, after = cmd_before_after[0]
        for c in before:
            c.level = 0
            fill_cmd_params(rules, c)
            cmdlist.add_cmd(c)
        for cmd, _before, _after in cmd_before_after:
            cmdlist.add_cmd(cmd)
        for c in after:
            c.level = 0
            fill_cmd_params(rules, c)
            cmdlist.add_cmd(c)
    return cmdlist
