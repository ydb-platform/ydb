#pragma once
#include "stats.h"

#include <deque>

#include <library/cpp/json/writer/json_value.h>

#include <yql/essentials/types/binary_json/format.h>

// Codec between the per-value BinaryJson blobs used on the write path and the raw scalars stored in
// native-typed sub-columns. Phase 2 only handles String; Double/Bool are added later.
//
// Definitions live in native_scalars.cpp so that binary_json read/write (which drags UDF headers) is
// not pulled into every translation unit that includes the iterators/builders.
namespace NKikimr::NArrow::NAccessor::NSubColumns {

// Native scalar type of a single BinaryJson blob, or nullopt if it is not a native scalar
// (a container, a JSON null, or empty).
std::optional<EValueType> ClassifyBinaryJsonScalar(const TStringBuf blob);

// Common native value type of a whole column: the shared scalar type if every value has one,
// otherwise BinaryJson (which also covers nulls and containers).
EValueType DetectNativeValueType(const std::deque<NBinaryJson::TBinaryJson>& values);

// Raw bytes to store in the native array for a BinaryJson blob of the given (already detected) type.
// For String the view points into `blob`; the caller must copy before `blob` is released.
TStringBuf ExtractNativeScalar(const TStringBuf blob, const EValueType valueType);

// Re-encode a raw native scalar back into a BinaryJson blob (used to revert native columns to
// BinaryJson, e.g. during compaction).
NBinaryJson::TBinaryJson NativeScalarToBinaryJson(const TStringBuf rawValue, const EValueType valueType);

// Reconstruct the JSON value of a stored value for document assembly, given how it is stored.
NJson::TJsonValue NativeScalarToJsonValue(const TStringBuf rawValue, const EValueType valueType);

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
