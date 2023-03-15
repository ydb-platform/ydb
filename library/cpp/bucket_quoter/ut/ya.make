UNITTEST()

FORK_SUBTESTS()
SRCS(
	main.cpp
	test_namespace.cpp
)
PEERDIR(
    library/cpp/bucket_quoter
    library/cpp/getopt
)

END()
