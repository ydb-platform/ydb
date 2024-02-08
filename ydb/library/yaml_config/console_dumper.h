#pragma once

#include <ydb/core/protos/console_config.pb.h>

#include <util/generic/string.h>

namespace NKikimr::NYamlConfig {

TString DumpConsoleConfigs(const ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> &configItems);

struct TDumpConsoleConfigItemResult {
    bool Domain;
    TString Config;
};

TDumpConsoleConfigItemResult DumpConsoleConfigItem(const NKikimrConsole::TConfigItem &item);

NKikimrConsole::TConfigureRequest DumpYamlConfigRequest(const TString &config, const TString &domain);

} // namespace NKikimr::NYamlConfig
