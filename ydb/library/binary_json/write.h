#pragma once

#include "format.h"

#include <ydb/library/conclusion/result.h>
#include <ydb/library/yql/minikql/dom/node.h>

#include <util/generic/maybe.h>

namespace NKikimr::NBinaryJson {

/**
 * @brief Translates textual JSON into BinaryJson
 */
TConclusion<TBinaryJson> SerializeToBinaryJson(const TStringBuf json);

/**
 * @brief Translates DOM layout from `yql/library/dom` library into BinaryJson
 */
TBinaryJson SerializeToBinaryJson(const NUdf::TUnboxedValue& value);

}

