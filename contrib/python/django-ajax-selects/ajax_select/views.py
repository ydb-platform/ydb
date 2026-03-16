import json

from django.http import HttpResponse
from django.utils.encoding import force_str

from ajax_select import registry


def ajax_lookup(request, channel):
    """
    Load the named lookup channel and lookup matching models.

    GET or POST should contain 'term'

    Returns:
        HttpResponse - JSON: `[{pk: value: match: repr:}, ...]`
    Raises:
        PermissionDenied - depending on the LookupChannel's implementation of check_auth

    """
    # it should come in as GET unless global $.ajaxSetup({type:"POST"}) has been set
    # in which case we'll support POST
    if request.method == "GET":
        # we could also insist on an ajax request
        if "term" not in request.GET:
            return HttpResponse("")
        query = request.GET["term"]
    else:
        if "term" not in request.POST:
            return HttpResponse("")  # suspicious
        query = request.POST["term"]

    lookup = registry.get(channel)
    if hasattr(lookup, "check_auth"):
        lookup.check_auth(request)

    instances = lookup.get_query(query, request) if len(query) >= getattr(lookup, "min_length", 1) else []

    results = json.dumps(
        [
            {
                "pk": force_str(getattr(item, "pk", None)),
                "value": lookup.get_result(item),
                "match": lookup.format_match(item),
                "repr": lookup.format_item_display(item),
            }
            for item in instances
        ]
    )

    response = HttpResponse(results, content_type="application/json")
    response["Cache-Control"] = "max-age=0, must-revalidate, no-store, no-cache;"
    return response
