
Y_BENCHMARK()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/string_utils/quote
)

FROM_SANDBOX(FILE 11167534572 RENAME RESOURCE OUT_NOAUTO cgi_huge_array.txt)
FROM_SANDBOX(FILE 11167537234 RENAME RESOURCE OUT_NOAUTO cgi_array.txt)
FROM_SANDBOX(FILE 11167578180 RENAME RESOURCE OUT_NOAUTO long_cgi.txt)

RESOURCE(
    cgi_huge_array.txt /test_files/cgi_huge_array.txt
    cgi_array.txt /test_files/cgi_array.txt
    long_cgi.txt /test_files/long_cgi.txt
)

END()
