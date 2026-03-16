def getfixturevalue(request, value):
    if hasattr(request, "getfixturevalue"):
        return request.getfixturevalue(value)

    return request.getfuncargvalue(value)  # pragma: no cover
