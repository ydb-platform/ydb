#pragma once
#include <ydb/core/tx/columnshard/engines/insert_table/meta.h>

namespace NKikimr::NOlap::NEngines::NTest {

class TLocalHelper {
public:
    static NKikimrTxColumnShard::TLogicalMetadata GetMetaProto();
    static std::shared_ptr<arrow::Schema> GetMetaSchema();
};

};