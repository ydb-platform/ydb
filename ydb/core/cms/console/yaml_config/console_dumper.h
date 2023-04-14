#pragma once

#include <ydb/core/protos/console_config.pb.h>

#include <util/generic/string.h>

namespace NYamlConfig {

TString DumpConsoleConfigs(const ::google::protobuf::RepeatedPtrField<NKikimrConsole::TConfigItem> &configItems);

} // namespace NYamlConfig
