from __future__ import annotations

import pickle
from dataclasses import asdict
from functools import partial
from typing import Any, Callable

from logfire.propagate import ContextCarrier, attach_context, get_context

from ..stack_info import warn_at_user_stacklevel

try:
    # concurrent.futures does not work in pyodide

    from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

    submit_t_orig = ThreadPoolExecutor.submit
    submit_p_orig = ProcessPoolExecutor.submit

    def instrument_executors() -> None:
        """Monkey-patch `submit()` methods of `ThreadPoolExecutor` and `ProcessPoolExecutor`
        to carry over OTEL context across threads and processes.
        """  # noqa: D205
        global submit_t_orig, submit_p_orig
        if ThreadPoolExecutor.submit is submit_t_orig:
            ThreadPoolExecutor.submit = submit_t
        if ProcessPoolExecutor.submit is submit_p_orig:
            ProcessPoolExecutor.submit = submit_p

    def submit_t(s: ThreadPoolExecutor, fn: Callable[..., Any], /, *args: Any, **kwargs: Any):
        """A wrapper around ThreadPoolExecutor.submit() that carries over OTEL context across threads."""
        fn = partial(fn, *args, **kwargs)
        carrier = get_context()
        return submit_t_orig(s, _run_with_context, carrier=carrier, func=fn, parent_config=None)

    def submit_p(s: ProcessPoolExecutor, fn: Callable[..., Any], /, *args: Any, **kwargs: Any):
        """A wrapper around ProcessPoolExecutor.submit() that carries over OTEL context across processes."""
        fn = partial(fn, *args, **kwargs)
        carrier = get_context()
        return submit_p_orig(s, _run_with_context, carrier=carrier, func=fn, parent_config=serialize_config())

    def _run_with_context(
        carrier: ContextCarrier, func: Callable[[], Any], parent_config: dict[str, Any] | None
    ) -> Any:
        """A wrapper around a function that restores OTEL context from a carrier and then calls the function.

        This gets run from within a process / thread.
        """
        if parent_config is not None:
            deserialize_config(parent_config)  # pragma: no cover

        with attach_context(carrier):
            return func()

except ImportError:  # pragma: no cover

    def instrument_executors() -> None:
        pass


def serialize_config() -> dict[str, Any] | None:
    """Serialize the global config for transmission to child processes.

    Returns None if the config cannot be pickled, in which case a warning is emitted.
    See: https://github.com/pydantic/logfire/issues/1556
    """
    from ..config import GLOBAL_CONFIG

    # note: since `logfire.config._LogfireConfigData` is a dataclass
    # but `LogfireConfig` is not we only get the attributes from `_LogfireConfigData`
    # which is what we want here!
    config_dict = asdict(GLOBAL_CONFIG)

    try:
        pickle.dumps(config_dict)
    except Exception:
        warn_at_user_stacklevel(
            'The Logfire configuration cannot be pickled and will not be automatically '
            'sent to child processes. You will need to manually call logfire.configure() '
            'in each child process. This typically happens when using local functions '
            'as callbacks (e.g., exception_callback).',
            UserWarning,
        )
        return None

    return config_dict


def deserialize_config(config: dict[str, Any] | None) -> None:
    """Deserialize a config dict and apply it to the global config."""
    from ..config import GLOBAL_CONFIG, configure

    if config is None:
        return

    if not GLOBAL_CONFIG._initialized:  # type: ignore
        configure(**config)
