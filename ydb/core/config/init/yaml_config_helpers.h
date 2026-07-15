#pragma once

#include <ydb/core/protos/config.pb.h>

#include <library/cpp/json/json_value.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NKikimr::NConfig {

NJson::TJsonValue LoadYamlAsJsonOrThrow(const TString& config, TStringBuf source);

void ParseJsonConfigOrThrow(const NJson::TJsonValue& json, TStringBuf source, NKikimrConfig::TAppConfig& config);

} // namespace NKikimr::NConfig
