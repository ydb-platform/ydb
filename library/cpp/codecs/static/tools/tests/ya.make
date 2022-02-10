PY2TEST()

OWNER(velavokr)

TEST_SRCS(static_codec_tools.py)

DATA(sbr://143310406)

TIMEOUT(4200)

TAG(ya:not_autocheck)

DEPENDS(
    library/cpp/codecs/static/tools/static_codec_checker 
    library/cpp/codecs/static/tools/static_codec_generator 
)



END()
