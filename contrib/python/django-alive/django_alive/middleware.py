import logging

from django.urls import NoReverseMatch, reverse

log = logging.getLogger(__name__)


def healthcheck_bypass_host_check(get_response):
    try:
        healthcheck_urls = [reverse("alive_alive"), reverse("alive_health")]
    except NoReverseMatch:
        log.warning("django-alive URLs have not been added to urlconf")
        healthcheck_urls = []

    def middleware(request):
        if request.path in healthcheck_urls:
            request.get_host = request._get_raw_host

        return get_response(request)

    return middleware
