#pragma once

#include <ydb/core/protos/console_config.pb.h>

#include <util/generic/string.h>

namespace NYamlConfig {

TString DumpConsoleConfigs(const ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> &configItems);

struct TDumpConsoleConfigItemResult {
    bool Domain;
    TString Config;
};

TDumpConsoleConfigItemResult DumpConsoleConfigItem(const NKikimrConsole::TConfigItem &item);

NKikimrConsole::TConfigItem DumpYamlConfigItem(const TString &config, const TString &domain);

} // namespace NYamlConfig
