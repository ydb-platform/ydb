"""
Since django 1.8.x, django comes with native multiple template engine support.
It also comes with jinja2 backend, but it is slightly unflexible, and it does
not support by default all django filters and related stuff.

This is an implementation of django backend inteface for use
django_jinja easy with django 1.8.
"""

import sys
import os
import os.path as path
import functools
from importlib import import_module

import jinja2
from django.conf import settings
from django.core import signals
from django.core.exceptions import ImproperlyConfigured
from django.dispatch import receiver
from django.middleware import csrf
from django.template import TemplateDoesNotExist
from django.template import TemplateSyntaxError
from django.template.backends.base import BaseEngine
from django.template.context import BaseContext
from django.utils.encoding import smart_str
from django.utils.functional import SimpleLazyObject
from django.utils.functional import cached_property
from django.utils.module_loading import import_string
from django.utils.safestring import mark_safe

from . import base
from . import builtins
from . import library
from . import utils


class Origin:
    """
    A container to hold debug information as described in the template API
    documentation.
    """
    def __init__(self, name, template_name):
        self.name = name
        self.template_name = template_name


class Template:
    def __init__(self, template, backend):
        self.template = template
        self.backend = backend

        self.name = template.name
        self.origin = Origin(
            name=template.filename, template_name=template.name
        )

    def render(self, context=None, request=None):
        return mark_safe(self._process_template(self.template.render, context, request))

    def stream(self, context=None, request=None):
        return self._process_template(self.template.stream, context, request)

    def _process_template(self, handler, context=None, request=None):
        if context is None:
            context = {}

        context = base.dict_from_context(context)

        if request is not None:
            def _get_val():
                token = csrf.get_token(request)
                if token is None:
                    return 'NOTPROVIDED'
                else:
                    return smart_str(token)

            context["request"] = request
            context["csrf_token"] = SimpleLazyObject(_get_val)

            # Support for django context processors
            for processor in self.backend.context_processors:
                context.update(processor(request))

        if self.backend._tmpl_debug:
            from django.test import signals

            # Define a "django" like context for emitatet the multi
            # layered context object. This is mainly for apps like
            # django-debug-toolbar that are very coupled to django's
            # internal implementation of context.

            if not isinstance(context, BaseContext):
                class CompatibilityContext(dict):
                    @property
                    def dicts(self):
                        return [self]

                context = CompatibilityContext(context)

            signals.template_rendered.send(sender=self,
                                           template=self,
                                           context=context)

        return handler(context)


class Jinja2(BaseEngine):
    app_dirname = "templates"

    @staticmethod
    @functools.lru_cache()
    def get_default():
        """
        When only one django-jinja backend is configured, returns it.
        Raises ImproperlyConfigured otherwise.

        This is required for finding the match extension where the
        developer does not specify a template_engine on a
        TemplateResponseMixin subclass.
        """
        from django.template import engines

        jinja_engines = [engine for engine in engines.all()
                         if isinstance(engine, Jinja2)]
        if len(jinja_engines) == 1:
            # Unwrap the Jinja2 engine instance.
            return jinja_engines[0]
        elif len(jinja_engines) == 0:
            raise ImproperlyConfigured(
                "No Jinja2 backend is configured.")
        else:
            raise ImproperlyConfigured(
                "Several Jinja2 backends are configured. "
                "You must select one explicitly.")

    def __init__(self, params):
        params = params.copy()
        options = params.pop("OPTIONS", {}).copy()

        self.app_dirname = options.pop("app_dirname", "templates")
        super().__init__(params)

        newstyle_gettext = options.pop("newstyle_gettext", True)
        context_processors = options.pop("context_processors", [])
        match_extension = options.pop("match_extension", ".jinja")
        match_regex = options.pop("match_regex", None)
        environment_clspath = options.pop("environment", "jinja2.Environment")
        extra_filters = options.pop("filters", {})
        extra_tests = options.pop("tests", {})
        extra_globals = options.pop("globals", {})
        extra_constants = options.pop("constants", {})
        translation_engine = options.pop("translation_engine", "django.utils.translation")
        policies = options.pop("policies", {})

        tmpl_debug = options.pop("debug", settings.DEBUG)
        bytecode_cache = options.pop("bytecode_cache", {})
        bytecode_cache.setdefault("name", "default")
        bytecode_cache.setdefault("enabled", False)
        bytecode_cache.setdefault("backend", "django_jinja.cache.BytecodeCache")

        undefined = options.pop("undefined", None)
        if undefined is not None:
            if isinstance(undefined, str):
                options["undefined"] = utils.load_class(undefined)
            else:
                options["undefined"] = undefined

        if settings.DEBUG:
            options.setdefault("undefined", jinja2.DebugUndefined)
        else:
            options.setdefault("undefined", jinja2.Undefined)

        environment_cls = import_string(environment_clspath)

        if isinstance(options.get("loader"), str):
            # Allow to specify a loader as string
            loader_cls = import_string(options.pop("loader"))
        else:
            # Backward compatible default
            loader_cls = jinja2.FileSystemLoader

        options.setdefault("loader", loader_cls(self.template_dirs))
        options.setdefault("extensions", builtins.DEFAULT_EXTENSIONS)
        options.setdefault("auto_reload", settings.DEBUG)
        options.setdefault("autoescape", True)

        self.env = environment_cls(**options)

        # Initialize i18n support
        if settings.USE_I18N:
            translation = import_module(translation_engine)
            self.env.install_gettext_translations(translation, newstyle=newstyle_gettext)
        else:
            self.env.install_null_translations(newstyle=newstyle_gettext)

        self._context_processors = context_processors
        self._match_regex = match_regex
        self._match_extension = match_extension
        self._tmpl_debug = tmpl_debug
        self._bytecode_cache = bytecode_cache

        self._initialize_builtins(filters=extra_filters,
                                  tests=extra_tests,
                                  globals=extra_globals,
                                  constants=extra_constants)
        self._initialize_policies(policies)

        self._initialize_thirdparty()
        self._initialize_bytecode_cache()

    def _initialize_bytecode_cache(self):
        if self._bytecode_cache["enabled"]:
            cls = utils.load_class(self._bytecode_cache["backend"])
            self.env.bytecode_cache = cls(self._bytecode_cache["name"])

    def _initialize_thirdparty(self):
        """
        Iterate over all available apps in searching and preloading
        available template filters or functions for jinja2.
        """
        for app_path, mod_path in base._iter_templatetags_modules_list():
            if not path.isdir(mod_path):
                continue

            for filename in filter(lambda x: x.endswith(".py") or x.endswith(".pyc"), os.listdir(mod_path)):
                # Exclude __init__.py files
                if filename == "__init__.py" or filename == "__init__.pyc":
                    continue

                file_mod_path = f"{app_path}.templatetags.{filename.rsplit('.', 1)[0]}"
                try:
                    import_module(file_mod_path)
                except ImportError:
                    pass

            library._update_env(self.env)

    def _initialize_builtins(self, filters=None, tests=None, globals=None, constants=None):
        def insert(data, name, value):
            if isinstance(value, str):
                data[name] = import_string(value)
            else:
                data[name] = value

        if filters:
            for name, value in filters.items():
                insert(self.env.filters, name, value)

        if tests:
            for name, value in tests.items():
                insert(self.env.tests, name, value)

        if globals:
            for name, value in globals.items():
                insert(self.env.globals, name, value)

        if constants:
            for name, value in constants.items():
                self.env.globals[name] = value

    def _initialize_policies(self, policies):
        # Set policies like those in jinja2.defaults.DEFAULT_POLICIES
        for name, value in policies.items():
            self.env.policies[name] = value

    @cached_property
    def context_processors(self):
        return tuple(import_string(path) for path in self._context_processors)

    @property
    def match_extension(self):
        return self._match_extension

    def from_string(self, template_code):
        return Template(self.env.from_string(template_code), self)

    def match_template(self, template_name):
        return base.match_template(template_name,
                                   self._match_extension,
                                   self._match_regex)

    def get_template(self, template_name):
        if not self.match_template(template_name):
            message = f"Template {template_name} does not exists"
            raise TemplateDoesNotExist(message)

        try:
            return Template(self.env.get_template(template_name), self)
        except jinja2.TemplateNotFound as exc:
            # Unlike django's template engine, jinja2 doesn't like windows-style path separators.
            # But because django does, its docs encourage the usage of os.path.join().
            # Rather than insisting that our users switch to posixpath.join(), this try block
            # will attempt to retrieve the template path again with forward slashes on windows:
            if os.name == 'nt' and '\\' in template_name:
                try:
                    return self.get_template(template_name.replace("\\", "/"))
                except jinja2.TemplateNotFound:
                    pass

            exc = TemplateDoesNotExist(exc.name, backend=self)

            utils.reraise(
                TemplateDoesNotExist,
                exc,
                sys.exc_info()[2],
            )
        except jinja2.TemplateSyntaxError as exc:
            new = TemplateSyntaxError(exc.args)
            new.template_debug = get_exception_info(exc)
            utils.reraise(TemplateSyntaxError, new, sys.exc_info()[2])


@receiver(signals.setting_changed)
def _setting_changed(sender, setting, *args, **kwargs):
    """ Reset the Jinja2.get_default() cached when TEMPLATES changes. """
    if setting == "TEMPLATES":
        Jinja2.get_default.cache_clear()


def get_exception_info(exception):
    """
    Formats exception information for display on the debug page using the
    structure described in the template API documentation.
    """
    context_lines = 10
    lineno = exception.lineno
    if exception.source is None:
        if os.path.exists(exception.filename):
            with open(exception.filename) as f:
                source = f.read()
    else:
        source = exception.source
    lines = list(enumerate(source.strip().split("\n"), start=1))
    during = lines[lineno - 1][1]
    total = len(lines)
    top = max(0, lineno - context_lines - 1)
    bottom = min(total, lineno + context_lines)

    return {
        'name': exception.filename,
        'message': exception.message,
        'source_lines': lines[top:bottom],
        'line': lineno,
        'before': '',
        'during': during,
        'after': '',
        'total': total,
        'top': top,
        'bottom': bottom,
    }
