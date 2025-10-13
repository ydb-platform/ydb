#include "local.h"

namespace NKikimr::NKqp {

void TTableWithNullsHelper::CreateTableWithNulls(TString tableName /*= "tableWithNulls"*/, ui32 shardsCount /*= 4*/) {
    TBase::CreateTestOlapTable("", Sprintf(R"(
            Name: "%s"
            ColumnShardCount: %d
            Schema {
                %s
            }
            Sharding {
                HashSharding {
                    Function: HASH_FUNCTION_CONSISTENCY_64
                    Columns: "id"
                }
            })", tableName.c_str(), shardsCount, PROTO_SCHEMA));
}

}