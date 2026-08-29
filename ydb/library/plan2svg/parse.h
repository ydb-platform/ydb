#pragma once

#include <library/cpp/json/json_reader.h>

#include <util/generic/string.h>

namespace NPlan2Svg {

TString GetEstimation(const NJson::TJsonValue& node);
const NJson::TJsonValue* GetOutputStatNode(const NJson::TJsonValue& node);
const NJson::TJsonValue* GetInputStatNode(const NJson::TJsonValue& node);
TString ParseTableOrIndexName(const TString& table);
TString ParseColumns(const NJson::TJsonValue* node);

} // namespace NPlan2Svg
