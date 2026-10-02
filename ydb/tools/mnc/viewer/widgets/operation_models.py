from dataclasses import dataclass, field
from typing import Optional


@dataclass
class OperationRequest:
    operation_id: str
    waiting: int = 15
    bin_path: Optional[str] = None
    do_not_init: bool = False
    ignore_failed_stop: bool = False


@dataclass
class OperationBacktraceFrame:
    path: str
    line: int
    function: str
    source: str = ""


@dataclass
class OperationState:
    status: str = "IDLE"
    operation_id: str = ""
    message: str = "No operation has been started."
    result_renderable: object = None
    error_type: str = ""
    error_message: str = ""
    backtrace: list[OperationBacktraceFrame] = field(default_factory=list)
    progress_backend: object = None
