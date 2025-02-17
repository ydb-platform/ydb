#pragma once

#include "format.h"

#include <yql/essentials/minikql/dom/node.h>

#include <util/generic/maybe.h>

#include <variant>

namespace NKikimr::NBinaryJson {

/**
 * @brief Translates textual JSON into BinaryJson
 */
std::variant<TBinaryJson, TString> SerializeToBinaryJson(const TStringBuf json);

/**
 * @brief Translates DOM layout from `yql/library/dom` library into BinaryJson
 */
TBinaryJson SerializeToBinaryJson(const NUdf::TUnboxedValue& value);

}

