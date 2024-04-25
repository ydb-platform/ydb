LIBRARY()

SRCS(
    tablet.cpp
)

PEERDIR(
	library/cpp/lwtrace/protos
	ydb/core/tablet_flat
	ydb/core/protos
	ydb/core/scheme/protos
)

END()
