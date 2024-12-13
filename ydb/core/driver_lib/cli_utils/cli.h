#pragma once

#include "cli_cmd_config.h"

#include <ydb/core/driver_lib/cli_base/cli.h>
#include <ydb/core/driver_lib/run/factories.h>

#include <ydb/library/actors/interconnect/poller_tcp.h>
#include <ydb/public/lib/deprecated/client/msgbus_client.h>

#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/system/hostname.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/string_utils/parse_size/parse_size.h>
#include <library/cpp/svnversion/svnversion.h>

#include <google/protobuf/text_format.h>

namespace NKikimr {

namespace NDriverClient {

    void DumpProxyErrorCodes(IOutputStream &o, const NKikimrClient::TResponse &response);
    void DumpSchemeErrorCode(IOutputStream &o, const NKikimrClient::TResponse &response);

    int SchemeInitRoot(TCommandConfig &cmdConf, int argc, char** argv);
    int CompileAndExecMiniKQL(TCommandConfig &cmdConf, int argc, char **argv);
    int KeyValueRequest(TCommandConfig &cmdConf, int argc, char **argv);
    int PersQueueRequest(TCommandConfig &cmdConf, int argc, char **argv);
    int PersQueueStress(TCommandConfig &cmdConf, int argc, char **argv);
    int PersQueueDiscoverClustersRequest(TCommandConfig &cmdConf, int argc, char **argv);
    int ActorsysPerfTest(TCommandConfig &cmdConf, int argc, char **argv);
    void HideOptions(NLastGetopt::TOpts& opts, const TString& prefix);
    void HideOptions(NLastGetopt::TOpts& opts);
    int NewClient(int argc, char** argv, std::shared_ptr<TModuleFactories> factories);
    TString NewClientCommandsDescription(const TString& name, std::shared_ptr<TModuleFactories> factories);
}
}
