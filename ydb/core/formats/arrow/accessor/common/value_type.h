#pragma once

#include <util/system/types.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

// Logical (as seen by external consumers) value type of data stored in a subcolumn.
// PERSISTED DO NOT REMOVE/REORDER: these numeric codes are written to disk as the `value_type` column of TDictStats.
enum class EValueType : ui8 {
    BinaryJson = 0,
    Double = 1,
    Bool = 2,
    String = 3,
};

// The Others store holds and reads every value as BinaryJson, whatever its logical type.
inline constexpr EValueType OthersExplicitBinaryJson = EValueType::BinaryJson;

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
