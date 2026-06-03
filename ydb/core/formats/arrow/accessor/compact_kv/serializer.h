#pragma once

#include "settings.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/array.h>

#include <util/generic/string.h>

#include <memory>

namespace NKikimr::NArrow::NSerialization {
class ISerializer;
}

namespace NKikimr::NArrow::NAccessor::NCompactKV {

// Encodes/decodes a binary-JSON Arrow array to/from the compact_kv on-disk
// format (3 sections: keys, values, rows-as-Avro-container + a null bitmap).
//
// The logic is ported from the standalone builder/reader tools.
class TSerializer {
public:
    // 4-byte magic at the start of every compact_kv blob.
    // Allows distinguishing compact_kv data from Arrow IPC (which starts with "ARROW1").
    static constexpr ui32 Magic = 0x434B5601;   // "CKV\x01"

    // Encode a plain binary-JSON arrow::BinaryArray into the compact_kv blob.
    // Compression is controlled by the column's serializer (COMPRESSION setting).
    static TString SerializeArray(const std::shared_ptr<arrow::Array>& binaryJsonArray, const TSettings& settings,
        const std::shared_ptr<NSerialization::ISerializer>& columnSerializer);

    // Decode a compact_kv blob back into a plain binary-JSON arrow::BinaryArray.
    // The columnSerializer must match the one used during serialization (same COMPRESSION setting).
    static std::shared_ptr<arrow::Array> DeserializeArray(const TString& blob, const ui32 recordsCount,
        const std::shared_ptr<NSerialization::ISerializer>& columnSerializer);

    // Check whether a blob starts with the compact_kv magic.
    static bool IsCompactKV(const TString& blob);
};

}   // namespace NKikimr::NArrow::NAccessor::NCompactKV
