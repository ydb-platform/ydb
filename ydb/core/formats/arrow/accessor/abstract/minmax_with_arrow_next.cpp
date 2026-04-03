#include "minmax_with_arrow_next.h"
#include <arrow/array/array_base.h>
#include <arrow/chunked_array.h>
#include <arrow/c/bridge.h> //order matters
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/c/bridge.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api_aggregate.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/util.h> // Для MakeArrayFromScalar
#include <ydb/library/actors/core/log.h>


NKikimr::NArrow::NAccessor::TMinMax NKikimr::NArrow::NAccessor::ComputeMinMaxWithArrowNext(std::shared_ptr<arrow::Array> arr) {
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;
    namespace arrow5 = arrow;

    arrow5::Status export_status = arrow5::ExportArray(*arr, &c_array, &c_schema);
    AFL_VERIFY(export_status.ok());

    auto import_result = arrow20::ImportArray(&c_array, &c_schema).ValueOrDie();


    auto scalar20 = arrow20::compute::MinMax(import_result).ValueOrDie().scalar();


    auto new_array = arrow20::MakeArrayFromScalar(*scalar20, 1).ValueOrDie();


    auto export_status20 = arrow20::ExportArray(*new_array, &c_array, &c_schema);
    AFL_VERIFY(export_status.ok());

    auto import_result5 = arrow5::ImportArray(&c_array, &c_schema).ValueOrDie();

    // --- ШАГ 3: Извлекаем скаляр из массива (старая версия) ---
    return *dynamic_cast<arrow5::StructScalar*>(import_result5->GetScalar(0).ValueOrDie().get());

}
NKikimr::NArrow::NAccessor::TMinMax NKikimr::NArrow::NAccessor::ComputeMinMaxWithArrowNext(std::shared_ptr<arrow::ChunkedArray> arr) {
    auto res = TMinMax::MakeNull(arr->type());
    for(auto& chunk: arr->chunks()) {
        res.UniteWith(ComputeMinMaxWithArrowNext(chunk));
    }
    return res;
}
