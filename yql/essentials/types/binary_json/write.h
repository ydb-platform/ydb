#pragma once

#include "format.h"

#include <util/generic/maybe.h>

#include <variant>

namespace NYql::NUdf {
class TUnboxedValue;
};

namespace NKikimr::NBinaryJson {

/**
 * @brief Translates textual JSON into BinaryJson
 */
std::variant<TBinaryJson, TString> SerializeToBinaryJson(const TStringBuf json, bool allowInf = false);

/**
 * @brief Translates DOM layout from `yql/library/dom` library into BinaryJson
 */
TBinaryJson SerializeToBinaryJson(const NYql::NUdf::TUnboxedValue& value);
}

