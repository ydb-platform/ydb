#pragma once

#include "yaml_config_parser.h"

#include <util/generic/string.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/erasure/erasure.h>

#include <library/cpp/json/writer/json.h>

#include <ydb/library/yaml_config/protos/config.pb.h>
#include <library/cpp/protobuf/json/util.h>

#include <span>

namespace NKikimr::NYaml {

NJson::TJsonValue* Traverse(NJson::TJsonValue& json, const TStringBuf& path, TString* lastName = nullptr);

const NJson::TJsonValue* Traverse(const NJson::TJsonValue& json, const TStringBuf& path, TString* lastName = nullptr);

void Iterate(
    const NJson::TJsonValue& json,
    const std::span<TString>& pathPieces,
    std::function<void(const std::vector<ui32>&, const NJson::TJsonValue&)> onElem,
    std::vector<ui32>& offsets,
    size_t offsetId = 0);

void Iterate(const NJson::TJsonValue& json, const TStringBuf& path, std::function<void(const std::vector<ui32>&, const NJson::TJsonValue&)> onElem);

void IterateMut(
    NJson::TJsonValue& json,
    const std::span<TString>& pathPieces,
    std::function<void(const std::vector<ui32>&, NJson::TJsonValue&)> onElem,
    std::vector<ui32>& offsets,
    size_t offsetId = 0);

void IterateMut(NJson::TJsonValue& json, const TStringBuf& path, std::function<void(const std::vector<ui32>&, NJson::TJsonValue&)> onElem);

}
