RESOURCES_LIBRARY()

# After repository-owner approval, populate bundles.json with the immutable
# platform sbr: IDs from registration.json and enable this declaration:
# DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON(SKIPPER_BROWSER bundles.json)
# It exposes SKIPPER_BROWSER_RESOURCE_GLOBAL to the PY3TEST. Until registration,
# browser/test_browser.py fails explicitly; no browser is substituted.

END()
