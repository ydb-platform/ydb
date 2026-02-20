#include "arrow_util.h"
#include "mkql_bit_utils.h"

#include <arrow/array/array_base.h>
#include <arrow/chunked_array.h>

#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_type_helper.h>

#include <util/system/yassert.h>

namespace NKikimr::NMiniKQL {

std::shared_ptr<arrow::ArrayData> Unwrap(const arrow::ArrayData& data, TType* itemType) {
    if (NeedWrapWithExternalOptional(itemType)) {
        MKQL_ENSURE(data.child_data.size() == 1, "Expected struct with one element");
        return data.child_data[0];
    } else {
        auto buffers = data.buffers;
        MKQL_ENSURE(buffers.size() >= 1, "Missing nullable bitmap");
        buffers[0] = nullptr;
        return arrow::ArrayData::Make(data.type, data.length, buffers, data.child_data, data.dictionary, 0, data.offset);
    }
}

std::shared_ptr<arrow::Scalar> UnwrapScalar(std::shared_ptr<arrow::Scalar> scalar, TType* itemType) {
    if (NeedWrapWithExternalOptional(itemType)) {
        return dynamic_cast<arrow::StructScalar&>(*scalar).value.at(0);
    }
    return scalar;
}

std::shared_ptr<arrow::Buffer> MakeEmptyBuffer() {
    // NOLINTNEXTLINE(modernize-avoid-c-arrays)
    static constexpr ui8 Data alignas(ArrowAlignment)[1]{};
    return std::make_shared<arrow::Buffer>(Data, 0);
}

} // namespace NKikimr::NMiniKQL
