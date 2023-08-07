#include "arrow_util.h"
#include "mkql_bit_utils.h"

#include <arrow/array/array_base.h>
#include <arrow/chunked_array.h>

#include <ydb/library/yql/minikql/mkql_node_builder.h>

#include <util/system/yassert.h>

namespace NKikimr::NMiniKQL {

std::shared_ptr<arrow::ArrayData> Unwrap(const arrow::ArrayData& data, TType* itemType) {
    bool nested;
    if (itemType->IsPg()) {
        nested = false;
    } else {
        bool isOptional;
        auto unpacked = UnpackOptional(itemType, isOptional);
        MKQL_ENSURE(isOptional, "Expected optional");
        if (unpacked->IsOptional() || unpacked->IsVariant() || unpacked->IsPg()) {
            nested = true;
        } else {
            nested = false;
        }
    }

    if (nested) {
        MKQL_ENSURE(data.child_data.size() == 1, "Expected struct with one element");
        return data.child_data[0];
    } else {
        auto buffers = data.buffers;
        MKQL_ENSURE(buffers.size() >= 1, "Missing nullable bitmap");
        buffers[0] = nullptr;
        return arrow::ArrayData::Make(data.type, data.length, buffers, data.child_data, data.dictionary, 0, data.offset);
    }
}

}
