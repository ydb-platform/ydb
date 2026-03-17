from __future__ import annotations

from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, TYPE_CHECKING, Union

from jinja2 import BaseLoader, Environment as BaseEnvironment, Template, TemplateNotFound

from .ctx import has_app_context, has_request_context
from .globals import _app_ctx_stack, _request_ctx_stack, current_app
from .signals import before_render_template, template_rendered

if TYPE_CHECKING:
    from .app import Quart  # noqa


class Environment(BaseEnvironment):
    """Quart specific Jinja2 Environment.

    This changes the default Jinja2 loader to use the
    DispatchingJinjaLoader, and enables async Jinja by default.
    """

    def __init__(self, app: "Quart", **options: Any) -> None:
        """Create a Quart specific Jinja2 Environment.

        Arguments:
            app: The Quart app to bind to.
            options: The standard Jinja2 Environment options.
        """
        if "loader" not in options:
            options["loader"] = app.create_global_jinja_loader()
        options["enable_async"] = True
        super().__init__(**options)


class DispatchingJinjaLoader(BaseLoader):
    """Quart specific Jinja2 Loader.

    This changes the default sourcing to consider the app
    and blueprints.
    """

    def __init__(self, app: "Quart") -> None:
        self.app = app

    def get_source(
        self, environment: BaseEnvironment, template: str
    ) -> Tuple[str, Optional[str], Optional[Callable[[], bool]]]:
        """Returns the template source from the environment.

        This considers the loaders on the :attr:`app` and blueprints.
        """
        for loader in self._loaders():
            try:
                return loader.get_source(environment, template)
            except TemplateNotFound:
                continue
        raise TemplateNotFound(template)

    def _loaders(self) -> Generator[BaseLoader, None, None]:
        loader = self.app.jinja_loader
        if loader is not None:
            yield loader

        for blueprint in self.app.iter_blueprints():
            loader = blueprint.jinja_loader
            if loader is not None:
                yield loader

    def list_templates(self) -> List[str]:
        """Returns a list of all available templates in environment.

        This considers the loaders on the :attr:`app` and blueprints.
        """
        result = set()
        for loader in self._loaders():
            for template in loader.list_templates():
                result.add(str(template))
        return list(result)


async def render_template(template_name_or_list: Union[str, List[str]], **context: Any) -> str:
    """Render the template with the context given.

    Arguments:
        template_name_or_list: Template name to render of a list of
            possible template names.
        context: The variables to pass to the template.
    """
    await current_app.update_template_context(context)
    template = current_app.jinja_env.get_or_select_template(template_name_or_list)  # type: ignore
    return await _render(template, context, current_app._get_current_object())  # type: ignore


async def render_template_string(source: str, **context: Any) -> str:
    """Render the template source with the context given.

    Arguments:
        source: The template source code.
        context: The variables to pass to the template.
    """
    await current_app.update_template_context(context)
    template = current_app.jinja_env.from_string(source)
    return await _render(template, context, current_app._get_current_object())  # type: ignore


async def _render(template: Template, context: dict, app: "Quart") -> str:
    await before_render_template.send(app, template=template, context=context)
    rendered_template = await template.render_async(context)
    await template_rendered.send(app, template=template, context=context)
    return rendered_template


async def _default_template_context_processor() -> Dict[str, Any]:
    context = {}
    if has_app_context():
        context["g"] = _app_ctx_stack.top.g
    if has_request_context():
        context["request"] = _request_ctx_stack.top.request
        context["session"] = _request_ctx_stack.top.session
    return context
