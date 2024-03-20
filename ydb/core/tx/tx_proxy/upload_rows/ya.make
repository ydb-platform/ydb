LIBRARY()

SRCS(
    upload_rows_common_impl.cpp
    upload_rows.cpp
)


PEERDIR(
    ydb/core/tx/tx_proxy
)

END()
