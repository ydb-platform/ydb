LIBRARY()

SRCS(
    prefix_tree.cpp
    prefix_tree_rules_handler.cpp
    robots_txt_parser.cpp
    rules_handler.cpp
)

PEERDIR(
    library/cpp/robots_txt/robotstxtcfg
    library/cpp/case_insensitive_string
    library/cpp/charset
    library/cpp/string_utils/url
    library/cpp/uri
)

END()
