import json
from django.core import serializers
from django.core.exceptions import ImproperlyConfigured
from django.core.serializers.json import DjangoJSONEncoder
from django.http import HttpResponse, HttpResponseBadRequest, JsonResponse


class JSONResponseMixin:
    """
    Basic serialized responses.

    For anything more complicated than basic Python types or Django
    models, please use something like django-rest-framework.
    """

    content_type = None
    json_dumps_kwargs = None
    json_encoder_class = None

    def get_content_type(self):
        """Get the appropriate content type for the response"""
        if self.content_type is not None and not isinstance(
            self.content_type, str
        ):
            class_name = self.__class__.__name__
            raise ImproperlyConfigured(
                f"{class_name} is missing a content type. Define {class_name}"
                ".content_type or override {class_name}.get_content_type()."
            )
        return self.content_type or "application/json"

    def get_json_dumps_kwargs(self):
        """Get kwargs for custom JSON compilation"""
        dumps_kwargs = getattr(self, "json_dumps_kwargs", None) or {}
        dumps_kwargs.setdefault("ensure_ascii", False)
        return dumps_kwargs

    def get_json_encoder_class(self):
        """Get the encoder class to use"""
        if self.json_encoder_class is None:
            self.json_encoder_class = DjangoJSONEncoder
        return self.json_encoder_class

    def render_json_response(self, context_dict, status=200):
        """
        Limited serialization for shipping plain data.
        Do not use for models or other complex objects.
        """
        response = JsonResponse(
            data=context_dict,
            safe=False,
            encoder=self.get_json_encoder_class(),
            json_dumps_params=self.get_json_dumps_kwargs(),
            content_type=self.get_content_type(),
            status=status
        )
        return response

    def render_json_object_response(self, objects, **kwargs):
        """
        Serializes objects using Django's builtin JSON serializer. Additional
        kwargs can be used the same way for django.core.serializers.serialize.
        """
        try:
            response = self.render_json_response(objects, **kwargs)
        except TypeError:
            json_data = serializers.serialize("json", objects, **kwargs)
            response = HttpResponse(json_data, content_type=self.get_content_type())
        return response


class AjaxResponseMixin:
    """
    Mixin allows you to define alternative methods for ajax requests. Similar
    to the normal get, post, and put methods, you can use get_ajax, post_ajax,
    and put_ajax.
    """

    def dispatch(self, request, *args, **kwargs):
        """Call the appropriate handler method"""
        if all([
            request.headers.get("x-requested-with") == "XMLHttpRequest",
            request.method.lower() in self.http_method_names
        ]):
            handler = getattr(
                self,
                f"{request.method.lower()}_ajax",
                self.http_method_not_allowed,
            )
            self.request = request
            self.args = args
            self.kwargs = kwargs
            return handler(request, *args, **kwargs)

        return super().dispatch(request, *args, **kwargs)

    def get_ajax(self, request, *args, **kwargs):
        """Handle a GET request made with AJAX"""
        return self.get(request, *args, **kwargs)

    def post_ajax(self, request, *args, **kwargs):
        """Handle a POST request made with AJAX"""
        return self.post(request, *args, **kwargs)

    def put_ajax(self, request, *args, **kwargs):
        """Handle a PUT request made with AJAX"""
        return self.get(request, *args, **kwargs)

    def delete_ajax(self, request, *args, **kwargs):
        """Handle a DELETE request made with AJAX"""
        return self.get(request, *args, **kwargs)


class JsonRequestResponseMixin(JSONResponseMixin):
    """
    Attempt to parse the request body as JSON.

    If successful, self.request_json will contain the deserialized object.
    Otherwise, self.request_json will be None.

    Set the attribute require_json to True to return a 400 "Bad Request" error
    for requests that don't contain JSON.

    Note: To allow public access to your view, you'll need to use the
    csrf_exempt decorator or CsrfExemptMixin.

    Example Usage:

        class SomeView(CsrfExemptMixin, JsonRequestResponseMixin):
            def post(self, request, *args, **kwargs):
                do_stuff_with_contents_of_request_json()
                return self.render_json_response(
                    {'message': 'Thanks!'})
    """

    require_json = False
    error_response_dict = {"errors": ["Improperly formatted request"]}

    def render_bad_request_response(self, error_dict=None):
        """Generate errors for bad content"""
        if error_dict is None:
            error_dict = self.error_response_dict
        json_context = json.dumps(
            error_dict,
            cls=self.get_json_encoder_class(),
            **self.get_json_dumps_kwargs()
        ).encode("utf-8")
        return HttpResponseBadRequest(
            json_context, content_type=self.get_content_type()
        )

    def get_request_json(self):
        """Get the JSON included in the body"""
        try:
            return json.loads(self.request.body.decode("utf-8"))
        except (json.JSONDecodeError, ValueError):
            return None

    def dispatch(self, request, *args, **kwargs):
        """Trigger the appropriate method"""
        self.request = request
        self.args = args
        self.kwargs = kwargs

        self.request_json = self.get_request_json()
        if all(
            [
                request.method != "OPTIONS",
                self.require_json,
                self.request_json is None,
            ]
        ):
            return self.render_bad_request_response()
        return super().dispatch(request, *args, **kwargs)


class JSONRequestResponseMixin(JsonRequestResponseMixin):
    """Convenience alias"""
