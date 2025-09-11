RESOURCES_LIBRARY()

SUBSCRIBER(g:contrib)

# Note: the json below is also referred from build/ya.conf.json,
# please change these references consistently
DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON(CLANG_TIDY clang-tidy18.json)

END()
