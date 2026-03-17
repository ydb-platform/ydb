from js_asset.js import importmap as _importmap


def importmap(request):
    return {"importmap": _importmap}
