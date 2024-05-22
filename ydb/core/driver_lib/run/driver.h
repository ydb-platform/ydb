#pragma once

#include <ydb/core/driver_lib/cli_config_base/config_base.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <util/stream/file.h>
#include <util/stream/format.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/string_utils/parse_size/parse_size.h>
#include <library/cpp/svnversion/svnversion.h>


namespace NKikimr {

#define MODE_MAP(XX) \
    XX(EDM_RUN, "run", "run kikimr node") \
    XX(EDM_SERVER, "server", "run kikimr node") \
    XX(EDM_CONFIG, "config", "config utils") \
    XX(EDM_ADMIN, "admin", "admin running kikimr") \
    XX(EDM_DB, "db", "admin running kikimr") \
    XX(EDM_TABLET, "tablet", "admin running kikimr") \
    XX(EDM_DEBUG, "debug", "admin running kikimr") \
    XX(EDM_BS, "bs", "admin running kikimr") \
    XX(EDM_BLOBSTORAGE, "blobstorage", "admin running kikimr") \
    XX(EDM_CMS, "cms", "admin running kikimr") \
    XX(EDM_DISCOVERY, "discovery", "discover endpoints") \
    XX(EDM_WHOAMI, "whoami", "admin running kikimr") \
    XX(EDM_FORMAT_INFO, "format-info", "read pdisk format info") \
    XX(EDM_FORMAT_UTIL, "format-util", "query blob storage format configuration file") \
    XX(EDM_NODE_BY_HOST, "node-by-host", "get node id by hostname") \
    XX(EDM_SCHEME_INITROOT, "scheme-initroot", "init scheme root") \
    XX(EDM_COMPILE_AND_EXEC_MINIKQL, "minikql-exec", "compile and execute MiniKQL program") \
    XX(EDM_KEYVALUE_REQUEST, "keyvalue-request", "send protobuf request to a keyvalue tablet") \
    XX(EDM_PERSQUEUE_REQUEST, "persqueue-request", "send protobuf request to a persqueue tablet") \
    XX(EDM_PERSQUEUE_STRESS, "persqueue-stress", "stress read or write to a persqueue tablet") \
    XX(EDM_PERSQUEUE_DISCOVER_CLUSTERS, "persqueue-discover-clusters", "persqueue session clusters discovery") \
    XX(EDM_ACTORSYS_PERFTEST, "actorsys-perf-test", "make actorsystem performance test") \


CLI_MODES_IMPL(EDriverMode, EDM_NO, MODE_MAP);

}
