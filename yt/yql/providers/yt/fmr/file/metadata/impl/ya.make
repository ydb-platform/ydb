LIBRARY()

SRCS(
    yql_yt_file_metadata_impl.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/file/metadata/interface
    yt/yql/providers/yt/fmr/request_options
    yt/yql/providers/yt/fmr/utils
)

YQL_LAST_ABI_VERSION()

END()
