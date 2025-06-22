#pragma once
#include <ydb/core/protos/tx_columnshard.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap::NEngines::NTest {

class TLocalHelper {
public:
    static NKikimrTxColumnShard::TLogicalMetadata GetMetaProto();
    static std::shared_ptr<arrow::Schema> GetMetaSchema();
};

};   // namespace NKikimr::NOlap::NEngines::NTest
