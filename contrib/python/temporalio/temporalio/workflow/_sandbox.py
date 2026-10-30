from __future__ import annotations

import logging
import sys
import threading
from collections.abc import Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from enum import Flag, auto
from typing import Any

from ._context import Info, _Runtime, current_update_info

__all__ = [
    "LoggerAdapter",
    "SandboxImportNotificationPolicy",
    "logger",
    "unsafe",
]

_sandbox_unrestricted = threading.local()
_in_sandbox = threading.local()
_imports_passed_through = threading.local()
_sandbox_import_notification_policy_override = threading.local()


class SandboxImportNotificationPolicy(Flag):
    """Defines the behavior taken when modules are imported into the sandbox after the workflow is initially loaded or unintentionally missing from the passthrough list."""

    SILENT = auto()
    """Allow imports that do not violate sandbox restrictions and no warnings are generated."""
    WARN_ON_DYNAMIC_IMPORT = auto()
    """Allows dynamic imports that do not violate sandbox restrictions but issues a warning when an import is triggered in the sandbox after initial workflow load."""
    WARN_ON_UNINTENTIONAL_PASSTHROUGH = auto()
    """Allows imports that do not violate sandbox restrictions but issues a warning when an import is triggered in the sandbox that was unintentionally passed through."""
    RAISE_ON_UNINTENTIONAL_PASSTHROUGH = auto()
    """Raise an error when an import is triggered in the sandbox that was unintentionally passed through."""


class unsafe:
    """Contains static methods that should not normally be called during
    workflow execution except in advanced cases.
    """

    def __init__(self) -> None:  # noqa: D107
        raise NotImplementedError

    @staticmethod
    def in_sandbox() -> bool:
        """Whether the code is executing on a sandboxed thread.

        Returns:
            True if the code is executing in the sandbox thread.
        """
        return getattr(_in_sandbox, "value", False)

    @staticmethod
    def _set_in_sandbox(v: bool) -> None:
        _in_sandbox.value = v

    @staticmethod
    def is_replaying() -> bool:
        """Whether the workflow is currently replaying.

        This includes queries and update validators that occur during replay.

        Returns:
            True if the workflow is currently replaying
        """
        return _Runtime.current().workflow_is_replaying()

    @staticmethod
    def is_replaying_history_events() -> bool:
        """Whether the workflow is replaying history events.

        This excludes queries and update validators, which are live operations.

        Returns:
            True if replaying history events, False otherwise.
        """
        return _Runtime.current().workflow_is_replaying_history_events()

    @staticmethod
    def is_read_only() -> bool:
        """Whether the workflow is currently in read-only mode.

        Read-only mode occurs during queries and update validators where
        side effects are not allowed.

        Returns:
            True if the workflow is in read-only mode, False otherwise.
        """
        return _Runtime.current().workflow_is_read_only()

    @staticmethod
    def is_sandbox_unrestricted() -> bool:
        """Whether the current block of code is not restricted via sandbox.

        Returns:
            True if the current code is not restricted in the sandbox.
        """
        # Activations happen in different threads than init and possibly the
        # local hasn't been initialized in _that_ thread, so we allow unset here
        # instead of just setting value = False globally.
        return getattr(_sandbox_unrestricted, "value", False)

    @staticmethod
    @contextmanager
    def sandbox_unrestricted() -> Iterator[None]:
        """A context manager to run code without sandbox restrictions."""
        # Only apply if not already applied. Nested calls just continue
        # unrestricted.
        if unsafe.is_sandbox_unrestricted():
            yield None
            return
        _sandbox_unrestricted.value = True
        try:
            yield None
        finally:
            _sandbox_unrestricted.value = False

    @staticmethod
    def is_imports_passed_through() -> bool:
        """Whether the current block of code is in
        :py:meth:imports_passed_through.

        Returns:
            True if the current code's imports will be passed through
        """
        # See comment in is_sandbox_unrestricted for why we allow unset instead
        # of just global false.
        return getattr(_imports_passed_through, "value", False)

    @staticmethod
    @contextmanager
    def imports_passed_through() -> Iterator[None]:
        """Context manager to mark all imports that occur within it as passed
        through (meaning not reloaded by the sandbox).
        """
        # Only apply if not already applied. Nested calls just continue
        # passed through.
        if unsafe.is_imports_passed_through():
            yield None
            return
        _imports_passed_through.value = True
        try:
            yield None
        finally:
            _imports_passed_through.value = False

    @staticmethod
    def current_import_notification_policy_override() -> (
        SandboxImportNotificationPolicy | None
    ):
        """Gets the current import notification policy override if one is set."""
        applied_policy = getattr(
            _sandbox_import_notification_policy_override,
            "value",
            None,
        )
        return applied_policy

    @staticmethod
    @contextmanager
    def sandbox_import_notification_policy(
        policy: SandboxImportNotificationPolicy,
    ) -> Iterator[None]:
        """Context manager to apply the given import notification policy."""
        original_policy = _sandbox_import_notification_policy_override.value = getattr(
            _sandbox_import_notification_policy_override,
            "value",
            None,
        )
        _sandbox_import_notification_policy_override.value = policy
        try:
            yield None
        finally:
            _sandbox_import_notification_policy_override.value = original_policy


def _build_log_context(
    workflow_details: Mapping[str, Any] | None,
    update_details: Mapping[str, Any] | None = None,
    *,
    workflow_info_on_message: bool = True,
    workflow_info_on_extra: bool = True,
    full_workflow_info: Info | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build the msg_extra suffix and extra dict entries for a temporal log record.

    Returns:
        (msg_extra, extra) where msg_extra should be appended to the log message
        and extra should be merged into the log record's extra dict.
    """
    msg_extra: dict[str, Any] = {}
    extra: dict[str, Any] = {}

    if workflow_details is not None:
        if workflow_info_on_message:
            msg_extra.update(workflow_details)
        if workflow_info_on_extra:
            extra["temporal_workflow"] = dict(workflow_details)

    if update_details is not None:
        if workflow_info_on_message:
            msg_extra.update(update_details)
        if workflow_info_on_extra:
            extra.setdefault("temporal_workflow", {}).update(update_details)

    if full_workflow_info is not None:
        extra["workflow_info"] = full_workflow_info

    return msg_extra, extra


class LoggerAdapter(logging.LoggerAdapter):
    """Adapter that adds details to the log about the running workflow.

    Attributes:
        workflow_info_on_message: Boolean for whether a string representation of
            a dict of some workflow info will be appended to each message.
            Default is True.
        workflow_info_on_extra: Boolean for whether a ``temporal_workflow``
            dictionary value will be added to the ``extra`` dictionary with some
            workflow info, making it present on the ``LogRecord.__dict__`` for
            use by others. Default is True.
        full_workflow_info_on_extra: Boolean for whether a ``workflow_info``
            value will be added to the ``extra`` dictionary with the entire
            workflow info, making it present on the ``LogRecord.__dict__`` for
            use by others. Default is False.
        log_during_replay: Boolean for whether logs should occur during replay.
            Default is False.

    Values added to ``extra`` are merged with the ``extra`` dictionary from a
    logging call, with values from the logging call taking precedence. I.e. the
    behavior is that of ``merge_extra=True`` in Python >= 3.13.
    """

    def __init__(self, logger: logging.Logger, extra: Mapping[str, Any] | None) -> None:
        """Create the logger adapter."""
        super().__init__(logger, extra or {})
        self.workflow_info_on_message = True
        self.workflow_info_on_extra = True
        self.full_workflow_info_on_extra = False
        self.log_during_replay = False
        self.disable_sandbox = False

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        """Override to add workflow details."""
        msg_extra: dict[str, Any] = {}
        extra: dict[str, Any] = {}

        if (
            self.workflow_info_on_message
            or self.workflow_info_on_extra
            or self.full_workflow_info_on_extra
        ):
            runtime = _Runtime.maybe_current()
            update_info = current_update_info()
            msg_extra, extra = _build_log_context(
                runtime.logger_details if runtime else None,
                update_info._logger_details if update_info else None,
                workflow_info_on_message=self.workflow_info_on_message,
                workflow_info_on_extra=self.workflow_info_on_extra,
                full_workflow_info=runtime.workflow_info()
                if runtime and self.full_workflow_info_on_extra
                else None,
            )

            kwargs["extra"] = {**extra, **(kwargs.get("extra") or {})}
            if msg_extra:
                msg = f"{msg} ({msg_extra})"
        return msg, kwargs

    def log(
        self,
        level: int,
        msg: object,
        *args: Any,
        stacklevel: int = 1,
        **kwargs: Any,
    ):
        """Override to potentially disable the sandbox."""
        if sys.version_info < (3, 11) and stacklevel == 1:
            # An additional stacklevel is needed on 3.10 because it doesn't skip internal frames until after stacklevel
            # is decremented, so it needs an additional stacklevel to skip the internal frame.
            stacklevel += 1  # type: ignore[reportUnreachable]
        stacklevel += 1
        if self.disable_sandbox:
            with unsafe.sandbox_unrestricted():
                with unsafe.imports_passed_through():
                    super().log(level, msg, *args, stacklevel=stacklevel, **kwargs)
        else:
            super().log(level, msg, *args, stacklevel=stacklevel, **kwargs)

    def isEnabledFor(self, level: int) -> bool:
        """Override to ignore replay logs."""
        if not self.log_during_replay and unsafe.is_replaying_history_events():
            return False
        return super().isEnabledFor(level)

    @property
    def base_logger(self) -> logging.Logger:
        """Underlying logger usable for actions such as adding
        handlers/formatters.
        """
        return self.logger

    def unsafe_disable_sandbox(self, value: bool = True):
        """Disable the sandbox during log processing.
        Can be turned back on with unsafe_disable_sandbox(False).
        """
        self.disable_sandbox = value


logger = LoggerAdapter(logging.getLogger("temporalio.workflow"), None)
"""Logger that will have contextual workflow details embedded.

Logs are skipped during replay by default.
"""
