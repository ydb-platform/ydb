LIBRARY()

SRCS(
    yql_yt_file_upload_impl.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/file/upload/interface
    yt/yql/providers/yt/fmr/utils
    yt/yql/providers/yt/fmr/request_options
    yt/yql/providers/yt/gateway/lib

)

YQL_LAST_ABI_VERSION()

END()
