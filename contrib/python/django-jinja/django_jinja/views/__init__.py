from django import http
from django.template import loader
from django.views import View

from ..base import get_match_extension


class GenericView(View):
    response_cls = http.HttpResponse
    content_type = "text/html"
    tmpl_name = None

    def get_context_data(self):
        return {"view": self}

    def get(self, request, *args, **kwargs):
        context = self.get_context_data()
        template_name = callable(self.tmpl_name) and self.tmpl_name() or self.tmpl_name
        output = loader.render_to_string(template_name, context, request=request)
        return self.response_cls(output, content_type=self.content_type)


class ErrorView(GenericView):
    def head(self, request, *args, **kwargs):
        return self.get(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.get(request, *args, **kwargs)

    def options(self, request, *args, **kwargs):
        return self.get(request, *args, **kwargs)

    def delete(self, request, *args, **kwargs):
        return self.get(request, *args, **kwargs)

    def put(self, request, *args, **kwargs):
        return self.get(request, *args, **kwargs)

    def patch(self, request, *args, **kwargs):
        return self.get(request, *args, **kwargs)


class PageNotFound(ErrorView):
    response_cls = http.HttpResponseNotFound

    def tmpl_name(self):
        return "404" + (get_match_extension() or ".jinja")


class PermissionDenied(ErrorView):
    response_cls = http.HttpResponseForbidden

    def tmpl_name(self):
        return "403" + (get_match_extension() or ".jinja")


class BadRequest(ErrorView):
    response_cls = http.HttpResponseBadRequest

    def tmpl_name(self):
        return "400" + (get_match_extension() or ".jinja")


class ServerError(ErrorView):
    response_cls = http.HttpResponseServerError

    def tmpl_name(self):
        return "500" + (get_match_extension() or ".jinja")
