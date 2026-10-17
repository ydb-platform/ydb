from __future__ import annotations
from typing import Any, Optional
import time
import traceback
from enum import IntEnum
from types import TracebackType


class ErrorPriority(IntEnum):
    WARNING = 1
    # Максимальный приоритет. Код опирается на это: success и get_error_stats
    # фильтруют через '>= ERROR', а get_errors/get_integrated_error — через '== ERROR'.
    # Если появится уровень выше ERROR, эти места нужно синхронизировать.
    ERROR = 2


class ErrorArea(IntEnum):
    OTHER = 0
    YDB_INFRA = 1
    TEST_INFRA = 2
    REQUEST = 3
    TIMEOUT = 4
    DIFF = 5
    NODE_FAIL = 6
    PERFORMANCE = 7


class WorkloadError(RuntimeError):
    def __init__(self, message: str, priority: ErrorPriority = ErrorPriority.ERROR, area: Optional[ErrorArea] = ErrorArea.OTHER, tb: Optional[TracebackType] = None):
        super().__init__(message)
        self.__priority = priority
        self.__area = area if area is not None else ErrorArea.OTHER
        self.__traceback__ = tb

    @property
    def priority(self):
        return self.__priority

    @property
    def area(self):
        return self.__area

    def serialize(self) -> dict:
        result = {
            'priority': self.__priority.name,
            'area': self.__area.name,
            'message': str(self)
        }
        if self.__traceback__ is not None:
            result['traceback'] = [t.rstrip() for t in traceback.extract_tb(self.__traceback__).format()]
        return result


class QueryPlan:
    def __init__(self) -> None:
        self.plan: dict = None
        self.table: str = None
        self.ast: str = None
        self.svg: str = None
        self.stats: str = None


class Iteration:
    def __init__(self):
        self.final_plan: Optional[QueryPlan] = None
        self.in_progress_plan: Optional[QueryPlan] = None
        self.error_message: Optional[str] = None
        self.time: Optional[float] = None

    def get_error_class(self) -> Optional[ErrorArea]:
        msg_to_class = {
            'Deadline Exceeded': ErrorArea.TIMEOUT,
            'Request timeout': ErrorArea.TIMEOUT,
            'Query did not complete within specified timeout': ErrorArea.TIMEOUT,
            'There is diff': ErrorArea.DIFF
        }
        for msg, cl in msg_to_class.items():
            if self.error_message and self.error_message.find(msg) >= 0:
                return cl
        if self.error_message:
            return ErrorArea.OTHER
        return None


class WorkloadRunResult:
    def __init__(self):
        self._stats: dict[str, dict[str, Any]] = {}
        self.query_out: Optional[str] = None
        self.stdout: str = ''
        self.stderr: str = ''
        self.__errors: list[WorkloadError] = []
        self.explain = Iteration()
        self.iterations: dict[int, Iteration] = {}
        self.start_time = time.time()

    def merge(self, *others: list[WorkloadRunResult]) -> WorkloadRunResult:
        def not_empty(x):
            return bool(x)

        results = [r for r in filter(lambda x: x is not None, others)]
        self.start_time = min([r.start_time for r in results])
        self.query_out = '\n'.join(filter(not_empty, [r.query_out for r in results]))
        self.stdout = '\n'.join(filter(not_empty, [r.stdout for r in results]))
        self.stderr = '\n'.join(filter(not_empty, [r.stderr for r in results]))
        for r in results:
            self._stats.update(r._stats)
            self.__errors.extend(r.get_errors())
            self.explain = r.explain
            for num, iter in r.iterations.items():
                while num in self.iterations:
                    num = max(num + 1, len(self.iterations))
                self.iterations[num] = iter
        return self

    @property
    def success(self) -> bool:
        return not any(e.priority >= ErrorPriority.ERROR for e in self.__errors)

    def get_stats(self, test: str) -> dict[str, dict[str, Any]]:
        result = self._stats.get(test, {})
        result.update({
            f'with_{x.name.lower()}s': any(e.priority == x for e in self.__errors)
            for x in ErrorPriority
        })
        result['errors'] = self.get_error_stats()
        return result

    def add_stat(self, test: str, signal: str, value: Any) -> None:
        self._stats.setdefault(test, {})
        self._stats[test][signal] = value

    def get_error_stats(self):
        result = {}
        for iter in self.iterations.values():
            cl = iter.get_error_class()
            if cl is not None:
                result[cl.name.lower()] = True
        for e in self.__errors:
            if e.priority >= ErrorPriority.ERROR:
                result[e.area.name.lower()] = True
        if any(e.priority == ErrorPriority.WARNING for e in self.__errors):
            result['warning'] = True
        return result

    def __add_error(self, msg: Optional[str], priority: ErrorPriority, area: ErrorArea) -> bool:
        if msg:
            self.__errors.append(WorkloadError(msg, priority=priority, area=area))
            return True
        return False

    def add_error(self, msg: Optional[str], area) -> bool:
        return self.__add_error(msg, area=area, priority=ErrorPriority.ERROR)

    def add_warning(self, msg: Optional[str], area):
        return self.__add_error(msg, area=area, priority=ErrorPriority.WARNING)

    def add_custom_error(self, error: WorkloadError) -> None:
        self.__errors.append(error)

    def get_errors(self, priority: Optional[ErrorPriority] = None, area: Optional[ErrorArea] = None) -> list[WorkloadError]:
        return [
            e for e in self.__errors
            if (priority is None or e.priority == priority) and (area is None or e.area == area)
        ]

    def get_integrated_error(self, priority: Optional[ErrorPriority] = None) -> Optional[WorkloadError]:
        errors = self.get_errors(priority)
        if len(errors) == 0:
            return None
        main_error = max(errors, key=lambda e: e.priority)
        return WorkloadError(
            '\n'.join([f'{e.area.name}: {e}' for e in errors]),
            priority=main_error.priority,
            area=main_error.area,
            tb=next((e.__traceback__ for e in errors if e.__traceback__ is not None), None),
        )
