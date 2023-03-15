LIBRARY()

SRCS(
    xml-document.cpp
    xml-textreader.cpp
    xml-options.cpp
)

PEERDIR(
    library/cpp/xml/init
    contrib/libs/libxml
    library/cpp/string_utils/ztstrbuf
)

END()

RECURSE_FOR_TESTS(
    ut
)
