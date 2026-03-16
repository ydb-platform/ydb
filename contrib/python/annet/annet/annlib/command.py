from dataclasses import dataclass, field
from typing import List, Optional


FIRST_EXCEPTION = 1
ALL_COMPLETED = 2


@dataclass
class Question:
    question: str  # frame it using / if it is a regular expression
    answer: str
    is_regexp: Optional[bool] = False
    not_send_nl: bool = False


@dataclass
class Command:
    cmd: str | bytes
    questions: Optional[List[Question]] = None
    exc_handler: Optional[List[Question]] = None
    timeout: Optional[int] = None  # total timeout
    read_timeout: Optional[int] = None  # timeout between consecutive reads
    suppress_nonzero: bool = False
    suppress_eof: bool = False

    def __str__(self) -> str:
        if isinstance(self.cmd, bytes):
            return self.cmd.decode("utf-8")
        return self.cmd


@dataclass
class CommandList:
    cmss: List[Command] = field(default_factory=list)

    def __post_init__(self):
        if not self.cmss:
            self.cmss = []

    def __iter__(self):
        return iter(self.cmss)

    def __len__(self) -> int:
        return len(self.cmss)

    def add_cmd(self, cmd: Command) -> None:
        assert isinstance(cmd, Command)
        self.cmss.append(cmd)

    def as_list(self) -> List[Command]:  # TODO: delete
        return self.cmss
