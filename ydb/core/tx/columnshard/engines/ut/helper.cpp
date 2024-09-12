#include "helper.h"
#include <ydb/core/formats/arrow/simple_arrays_cache.h>

namespace  NKikimr::NOlap::NEngines::NTest {

NKikimrTxColumnShard::TLogicalMetadata TLocalHelper::GetMetaProto() {
    NKikimrTxColumnShard::TLogicalMetadata result;
    result.SetDirtyWriteTimeSeconds(TInstant::Now().Seconds());

    std::vector<std::shared_ptr<arrow::Array>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("1", arrow::uint64()) };
    for (auto&& i : fields) {
        columns.emplace_back(NArrow::TThreadSimpleArraysCache::Get(i->type(), NArrow::DefaultScalar(i->type()), 1));
    }
    auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), 1, columns);

    NArrow::TFirstLastSpecialKeys flKeys = NArrow::TFirstLastSpecialKeys(batch);
    result.SetSpecialKeysPayloadData(flKeys.SerializePayloadToString());
    return result;
}

}