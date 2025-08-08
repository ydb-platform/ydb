#include "helper.h"
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace  NKikimr::NOlap::NEngines::NTest {

std::shared_ptr<arrow::Schema> TLocalHelper::GetMetaSchema() {
    return std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("1", arrow::uint64()) }));
}

NKikimrTxColumnShard::TLogicalMetadata TLocalHelper::GetMetaProto() {
    NKikimrTxColumnShard::TLogicalMetadata result;
    result.SetDirtyWriteTimeSeconds(TInstant::Now().Seconds());

    std::vector<std::shared_ptr<arrow::Array>> columns;
    auto schema = GetMetaSchema();
    for (auto&& i : schema->fields()) {
        columns.emplace_back(NArrow::TThreadSimpleArraysCache::Get(i->type(), NArrow::DefaultScalar(i->type()), 1));
    }
    auto batch = arrow::RecordBatch::Make(schema, 1, columns);

    NArrow::TFirstLastSpecialKeys flKeys = NArrow::TFirstLastSpecialKeys(batch);
    result.SetSpecialKeysPayloadData(flKeys.SerializePayloadToString());
    return result;
}

}