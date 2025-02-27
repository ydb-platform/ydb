#pragma once

#include "format.h"

#include <util/generic/maybe.h>

#include <variant>

namespace NYql::NUdf {
class TUnboxedValue;
};

namespace NKikimr::NBinaryJson {

enum class EOutOfBoundsHandlingPolicy {
    REJECT = 1,
    CLIP = 2,
};

/**
 * @brief Translates textual JSON into BinaryJson
 */
std::variant<TBinaryJson, TString> SerializeToBinaryJson(
    const TStringBuf json, const EOutOfBoundsHandlingPolicy outOfBoundsHandling = EOutOfBoundsHandlingPolicy::REJECT);

/**
 * @brief Translates DOM layout from `yql/library/dom` library into BinaryJson
 */
TBinaryJson SerializeToBinaryJson(const NYql::NUdf::TUnboxedValue& value);
}

