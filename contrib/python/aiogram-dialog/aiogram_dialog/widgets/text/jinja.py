import warnings
from collections.abc import Callable, Iterable, Mapping
from typing import (
    Any,
)

from aiogram import Bot, Dispatcher
from jinja2 import BaseLoader, Environment

from aiogram_dialog.api.protocols import DialogManager
from aiogram_dialog.widgets.common import WhenCondition
from .base import Text

JINJA_ENV_FIELD = "DialogsJinjaEnvironment"

Filter = Callable[..., str]
Filters = (
    Iterable[tuple[str, Filter]]
    | Mapping[str, Filter]
)


class Jinja(Text):
    def __init__(self, text: str, when: WhenCondition = None):
        super().__init__(when=when)
        self.template_text = text

    async def _render_text(
            self, data: dict, manager: DialogManager,
    ) -> str:
        if JINJA_ENV_FIELD in manager.middleware_data:
            env = manager.middleware_data[JINJA_ENV_FIELD]
        else:
            bot: Bot = manager.middleware_data.get("bot")
            env: Environment = getattr(bot, JINJA_ENV_FIELD, default_env)
        template = env.get_template(self.template_text)

        if env.is_async:
            return await template.render_async(data)
        else:
            return template.render(data)


class StubLoader(BaseLoader):
    def get_source(self, environment, template):
        del environment  # unused
        return template, template, lambda: True


def _create_env(
        *args: Any, filters: Filters | None = None, **kwargs: Any,
) -> Environment:
    kwargs.setdefault("autoescape", True)
    kwargs.setdefault("lstrip_blocks", True)
    kwargs.setdefault("trim_blocks", True)
    if "loader" not in kwargs:
        kwargs["loader"] = StubLoader()
    env = Environment(*args, **kwargs)  # noqa: S701
    if filters is not None:
        env.filters.update(filters)
    return env


def setup_jinja(
        dp: Bot | Dispatcher,
        *args: Any,
        filters: Filters | None = None,
        **kwargs: Any,
) -> Environment:
    env = _create_env(*args, filters=filters, **kwargs)
    if isinstance(dp, Bot):
        warnings.warn(
            "Passing `Bot` to setup_jinja is deprecated, use `Dispatcher`",
            DeprecationWarning,
            stacklevel=2,
        )
        setattr(dp, JINJA_ENV_FIELD, env)
    else:
        dp[JINJA_ENV_FIELD] = env
    return env


default_env = _create_env()
