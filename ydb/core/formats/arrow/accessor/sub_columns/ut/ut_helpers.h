#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>

#include <ydb/library/actors/core/log.h>

#include <yql/essentials/types/binary_json/read.h>

#include <util/generic/string.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns::NTesting {

// Reconstruct the JSON documents of a sub-columns array as text, for round-trip assertions.
inline TString PrintBinaryJsons(const std::shared_ptr<arrow::ChunkedArray>& array) {
    TStringBuilder sb;
    sb << "[";
    for (auto&& i : array->chunks()) {
        sb << "[";
        AFL_VERIFY(i->type()->id() == arrow::binary()->id());
        auto views = std::static_pointer_cast<arrow::BinaryArray>(i);
        for (ui32 r = 0; r < views->length(); ++r) {
            if (views->IsNull(r)) {
                sb << "null";
            } else {
                sb << NKikimr::NBinaryJson::SerializeToJson(TStringBuf(views->GetView(r).data(), views->GetView(r).size()));
            }
            if (r + 1 != views->length()) {
                sb << ",";
            }
        }
        sb << "]";
    }
    sb << "]";
    return sb;
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns::NTesting
