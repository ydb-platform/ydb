import logging

from django.http import HttpResponse, JsonResponse

from .utils import perform_healthchecks

log = logging.getLogger(__name__)


def alive(request):
    return HttpResponse("ok")


def healthcheck(request):
    # Verify DB is connected
    healthy, errors = perform_healthchecks()
    response = {"healthy": healthy}
    if healthy:
        status = 200
    else:
        status = 503
        response.update({"errors": errors})

    return JsonResponse(response, status=status)
