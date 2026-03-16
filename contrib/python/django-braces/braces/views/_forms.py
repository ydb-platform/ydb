from functools import partial

from django.contrib import messages
from django.core.exceptions import ImproperlyConfigured
from django.utils.decorators import method_decorator
from django.utils.encoding import force_str
from django.utils.functional import Promise
from django.views.decorators.csrf import csrf_exempt
from django.urls import reverse


class CsrfExemptMixin:
    """
    Exempts the view from CSRF requirements.

    NOTE:
        This should be the left-most mixin of a view.
    """

    @method_decorator(csrf_exempt)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)


class UserFormKwargsMixin:
    """
    Automatically include `request.user` in form kwargs.

    ## Note
    You will need to handle the `user` kwarg in your form. Usually
    this means `user = kwargs.pop("user")` in your form's `__init__`.
    """

    def get_form_kwargs(self):
        kwargs = super().get_form_kwargs()
        # Update the existing form kwargs dict with the request's user.
        kwargs.update({"user": self.request.user})
        return kwargs


class SuccessURLRedirectListMixin:
    """
    Automatically reverses `success_list_url` and returns that as
    the `success_url` for a form view.

    This is meant to redirect to a view without arguments. If you need
    to include arguments to `reverse`, you can omit this mixin.
    """

    success_list_url = None  # Default the success url to none

    def get_success_url(self):
        # Return the reversed success url.
        if self.success_list_url is None:
            class_name = self.__class__.__name__
            raise ImproperlyConfigured(
                f"{class_name} is missing a success_list_url attribute. "
                f"Define {class_name}.success_list_url or override "
                f"{class_name}.get_success_url()."
            )
        return reverse(self.success_list_url)


class _MessageAPIWrapper:
    """
    Wrapper for the django.contrib.messages.api module.
    Automatically pass a request object as the first parameter of
    message function calls.
    """

    API = set(
        [
            "add_message",
            "get_messages",
            "get_level",
            "set_level",
            "debug",
            "info",
            "success",
            "warning",
            "error",
        ]
    )

    def __init__(self, request):
        for name in self.API:
            api_fn = getattr(messages.api, name)
            setattr(self, name, partial(api_fn, request))


class _MessageDescriptor:
    """
    A descriptor that binds the _MessageAPIWrapper to the view's
    request.
    """

    def __get__(self, instance, *args, **kwargs):
        return _MessageAPIWrapper(instance.request)


class MessageMixin:
    """
    Add a `messages` attribute on the view instance that wraps
    `django.contrib.messages`, automatically passing the current
    request object.
    """

    messages = _MessageDescriptor()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._class_name = self.__class__.__name__


class FormValidMessageMixin(MessageMixin):
    """
    Set a string to be sent via Django's messages framework when a form
    passes validation.
    """

    form_valid_message = None  # Default to None

    def get_form_valid_message(self):
        """
        Validate that form_valid_message is set correctly
        """
        if self.form_valid_message is None:
            raise ImproperlyConfigured(
                f"{self._class_name}.form_valid_message is not set. Define "
                f"{self._class_name}.form_valid_message, or override "
                f"{self._class_name}.get_form_valid_message()."
            )

        if not isinstance(self.form_valid_message, (str, Promise)):
            raise ImproperlyConfigured(
                f"{self._class_name}.form_valid_message must be a str or Promise."
            )

        return force_str(self.form_valid_message)

    def form_valid(self, form):
        """
        Set the "form valid" message for standard form validation
        """
        response = super().form_valid(form)
        self.messages.success(
            self.get_form_valid_message(), fail_silently=True
        )
        return response

    def delete(self, *args, **kwargs):
        """
        Set the "form valid" message for delete form validation
        """
        response = super().delete(*args, **kwargs)
        self.messages.success(
            self.get_form_valid_message(), fail_silently=True
        )
        return response


class FormInvalidMessageMixin(MessageMixin):
    """
    Set a string to be sent via Django's messages framework when a form
    fails validation.
    """

    form_invalid_message = None

    def get_form_invalid_message(self):
        """
        Validate that form_invalid_message is set correctly.
        """
        if self.form_invalid_message is None:
            raise ImproperlyConfigured(
                f"{self._class_name}.form_invalid_message is not set. Define "
                f"{self._class_name}.form_invalid_message, or override "
                f"{self._class_name}.get_form_invalid_message()."
            )

        if not isinstance(self.form_invalid_message, (str, Promise)):
            raise ImproperlyConfigured(
                f"{self._class_name}.form_invalid_message must be a str or Promise."
            )

        return force_str(self.form_invalid_message)

    def form_invalid(self, form):
        """
        Set the "form invalid" message for standard form validation
        """
        response = super().form_invalid(form)
        self.messages.error(
            self.get_form_invalid_message(), fail_silently=True
        )
        return response


class FormMessagesMixin(FormValidMessageMixin, FormInvalidMessageMixin):
    """
    Set messages to be sent whether a form is valid or invalid.
    """
