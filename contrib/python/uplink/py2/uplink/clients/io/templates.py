# Standard library imports
import operator

# Local imports
from uplink.clients.io import RequestTemplate, transitions


class DefaultRequestTemplate(RequestTemplate):
    """The fallback behaviors for all hooks."""

    def before_request(self, request):
        return transitions.send(request)

    def after_response(self, request, response):
        return transitions.finish(response)

    def after_exception(self, request, exc_type, exc_val, exc_tb):
        return transitions.fail(exc_type, exc_val, exc_tb)


class CompositeRequestTemplate(RequestTemplate):
    """A chain of many templates with fallback behaviors."""

    __FALLBACK = DefaultRequestTemplate()

    def _get_transition(self, method, *args, **kwargs):
        caller = operator.methodcaller(method, *args, **kwargs)
        for template in self._templates:
            transition = caller(template)
            if transition is not None:
                return transition
        else:
            return caller(self._fallback)

    def __init__(self, templates, fallback=__FALLBACK):
        self._templates = list(templates)
        self._fallback = fallback

    def before_request(self, request):
        return self._get_transition(
            RequestTemplate.before_request.__name__, request
        )

    def after_response(self, request, response):
        return self._get_transition(
            RequestTemplate.after_response.__name__, request, response
        )

    def after_exception(self, request, exc_type, exc_val, exc_tb):
        return self._get_transition(
            RequestTemplate.after_exception.__name__,
            request,
            exc_type,
            exc_val,
            exc_tb,
        )
