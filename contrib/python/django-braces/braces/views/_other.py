from django.core.exceptions import ImproperlyConfigured
from django.shortcuts import redirect
from django.views.decorators.cache import cache_control, never_cache
from django.urls import resolve
from django.utils.encoding import force_str
from django.urls import resolve


class SetHeadlineMixin:
    """
    Define a `headline` context item as a view attribute
    """

    headline = None  # Default the headline to none

    def get_context_data(self, **kwargs):
        """Add the headline to the context"""
        kwargs = super().get_context_data(**kwargs)
        kwargs.update({"headline": self.get_headline()})
        return kwargs

    def get_headline(self):
        """Fetch the headline from the instance"""
        if self.headline is None:
            class_name = self.__class__.__name__
            raise ImproperlyConfigured(
                f"{class_name} is missing the headline attribute. "
                f"Define {class_name}.headline, or override {class_name}.get_headline()."
            )
        return force_str(self.headline)


class StaticContextMixin:
    """
    Set static context items via an attribute on the view.
    """

    static_context = None

    def get_context_data(self, **kwargs):
        """Update the context to include the static content"""
        kwargs = super().get_context_data(**kwargs)

        try:
            kwargs.update(self.get_static_context())
        except (TypeError, ValueError):
            raise ImproperlyConfigured(
                f"{self.__class__.__name__}.static_context must be a "
                "dictionary or a series of two-tuples."
            )
        else:
            return kwargs

    def get_static_context(self):
        """Fetch the static content from the view"""
        if self.static_context is None:
            class_name = self.__class__.__name__
            raise ImproperlyConfigured(
                f"{class_name} is missing the static_context attribute. Define "
                f"{class_name}.static_context, or override {class_name}.get_static_context()"
            )
        return self.static_context


class CanonicalSlugDetailMixin:
    """
    Enforce a canonical slug in the URL.

    If a URL takes a object's pk and slug as arguments and the slug URL
    argument does not equal the object's canonical slug, this mixin will
    redirect to the URL containing the canonical slug.
    """

    def dispatch(self, request, *args, **kwargs):
        """
        Redirect to the appropriate URL if necessary.
        Otherwise, trigger HTTP-method-appropriate handler.
        """
        self.request = request
        self.args = args
        self.kwargs = kwargs

        # Get the current object, url slug, and url name.
        obj = self.get_object()
        slug = self.kwargs.get(self.slug_url_kwarg, None)
        match = resolve(request.path_info)
        url_parts = match.namespaces
        url_parts.append(match.url_name)
        current_urlpattern = ":".join(url_parts)

        # Find the canonical slug for the object
        if hasattr(obj, "get_canonical_slug"):
            canonical_slug = obj.get_canonical_slug()
        else:
            canonical_slug = self.get_canonical_slug()

        # Redirect if current slug is not the canonical one
        if canonical_slug != slug:
            params = {
                self.pk_url_kwarg: obj.pk,
                self.slug_url_kwarg: canonical_slug,
                "permanent": True,
            }
            return redirect(current_urlpattern, **params)

        return super().dispatch(request, *args, **kwargs)

    def get_canonical_slug(self):
        """
        Provide a method to return the correct slug for this object.
        """
        return self.get_object().slug


class AllVerbsMixin:
    """
    Call a single method for all HTTP verbs.

    The name of the method should be specified using the class attribute
    `all_handler`. The default value of this attribute is 'all'.
    """

    all_handler = "all"

    def dispatch(self, request, *args, **kwargs):
        """Call the all handler"""
        if not self.all_handler:
            raise ImproperlyConfigured(
                f"{self.__class__.__name__} requires the all_handler attribute to be set."
            )

        handler = getattr(self, self.all_handler, self.http_method_not_allowed)
        return handler(request, *args, **kwargs)


class HeaderMixin:
    """
    Add extra HTTP headers to a response by specifying them in the
    ``headers`` attribute or by overriding the ``get_headers()`` method.
    """

    headers = {}

    def get_headers(self, request):
        return self.headers

    def dispatch(self, request, *args, **kwargs):
        """
        Override this method to customize the way additional headers are
        retrieved.  It is mandatory that the returned value supports the
        ``.items()`` method.
        """
        response = super().dispatch(request, *args, **kwargs)
        for key, value in self.get_headers(request).items():
            if key not in response:
                response[key] = value
        return response


class CacheControlMixin:
    """
    Mixin that allows setting Cache-Control options.

    Specify Cache-Control options as class attributes on the view class.

    Cache-Control directive explanations:
    http://condor.depaul.edu/dmumaugh/readings/handouts/SE435/HTTP/node24.html

    Django's ``django.views.decorators.cache.cache_control`` options:
    https://docs.djangoproject.com/en/dev/topics/cache/#controlling-cache-using-other-headers
    """
    # These are all ``None``, which indicates unset.
    cachecontrol_public = None
    cachecontrol_private = None
    cachecontrol_no_cache = None
    cachecontrol_no_transform = None
    cachecontrol_must_revalidate = None
    cachecontrol_proxy_revalidate = None
    cachecontrol_max_age = None
    cachecontrol_s_maxage = None

    @classmethod
    def get_cachecontrol_options(cls):
        """Compile a dictionary of selected cache options"""
        opts = (
            'public', 'private', 'no_cache', 'no_transform',
            'must_revalidate', 'proxy_revalidate', 'max_age',
            's_maxage'
        )
        options = {}
        for opt in opts:
            value = getattr(cls, f'cachecontrol_{opt}', None)
            if value is not None:
                options[opt] = value
        return options

    @classmethod
    def as_view(cls, *args, **kwargs):
        """Wrap the view with appropriate cache controls"""
        view_func = super().as_view(*args, **kwargs)
        options = cls.get_cachecontrol_options()
        return cache_control(**options)(view_func)


class NeverCacheMixin:
    """
    Mixin that applies Django's `never_cache` view decorator to prevent
    upstream HTTP-based caching.
    """
    @classmethod
    def as_view(cls, *args, **kwargs):
        """
        Wrap the view with the `never_cache` decorator.
        """
        view_func = super().as_view(*args, **kwargs)
        return never_cache(view_func)
