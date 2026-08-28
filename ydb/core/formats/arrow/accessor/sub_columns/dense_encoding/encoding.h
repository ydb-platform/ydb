#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/util/compression.h>

#include <util/generic/array_ref.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

// On-disk dense encoding of string/BinaryJson sub-columns, for both plain and dictionary columns.
// Null records have no value length or dictionary index; a validity bitmap maps the dense streams
// back to records. Integer streams use byte-stream split and every section is compressed separately.
namespace NKikimr::NArrow::NAccessor::NSubColumns {

// [width][byte-stream-split lengths], where width is 1, 2, or 4 bytes.
TString EncodeLengths(TConstArrayRef<ui32> values);
TVector<ui32> DecodeLengths(TStringBuf data, ui32 count);

// Serialize a binary sub-column (the values of a plain column or a dictionary) as
//   [has-nulls][validity section][lengths section][value bytes section].
// Each section is individually compressed with codec.
TString SerializeBinaryArray(const arrow::BinaryArray& array, const std::shared_ptr<arrow::util::Codec>& codec);
std::shared_ptr<arrow::BinaryArray> DeserializeBinaryArray(
    TStringBuf blob, ui32 recordsCount, const std::shared_ptr<arrow::util::Codec>& codec);

// Serialize a dictionary index stream (one index per record, nulls for absent records) as
//   [has-nulls][validity bitmap][indices, packed at indexType's width]
// Absent records have no index slot. Index values use the caller's narrowed index type (see
// NDictionary::TConstructor::GetTypeByVariantsCount); the reader rebuilds the same type with nulls
// back in place.
TString SerializeIndices(const std::shared_ptr<arrow::Array>& positions, const std::shared_ptr<arrow::FixedWidthType>& indexType,
    const std::shared_ptr<arrow::util::Codec>& codec);
std::shared_ptr<arrow::Array> DeserializeIndices(TStringBuf blob, ui32 recordsCount, const std::shared_ptr<arrow::FixedWidthType>& indexType,
    const std::shared_ptr<arrow::util::Codec>& codec);

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
