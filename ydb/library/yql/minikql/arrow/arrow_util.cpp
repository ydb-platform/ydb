#include "arrow_util.h"

#include <util/system/yassert.h>

namespace NKikimr::NMiniKQL {

std::shared_ptr<arrow::ArrayData> DeepSlice(const std::shared_ptr<arrow::ArrayData>& data, size_t offset, size_t len) {
    Y_VERIFY(data->length >= 0);
    Y_VERIFY(offset + len <= (size_t)data->length);
    if (offset == 0 && len == (size_t)data->length) {
        return data;
    }

    std::shared_ptr<arrow::ArrayData> result = data->Copy();
    result->offset = data->offset + offset;
    result->length = len;

    if (data->null_count == data->length) {
        result->null_count = len;
    } else if (len == 0) {
        result->null_count = 0;
    } else {
        result->null_count = data->null_count != 0 ? arrow::kUnknownNullCount : 0;
    }

    for (size_t i = 0; i < data->child_data.size(); ++i) {
        result->child_data[i] = DeepSlice(data->child_data[i], offset, len);
    }

    return result;
}

std::shared_ptr<arrow::ArrayData> Chop(std::shared_ptr<arrow::ArrayData>& data, size_t len) {
    auto first = DeepSlice(data, 0, len);
    data = DeepSlice(data, len, data->length - len);
    return first;
}


}
