#include "read_table_scan.h"
#include "read_table_add_cell.h"

#include <ydb/public/api/protos/ydb_value.pb.h>

namespace NKikimr {
namespace NDataShard {

bool AddRowToYdbResultSet(
    Ydb::ResultSet& resultSet,
    TConstArrayRef<NScheme::TTypeInfo> types,
    TConstArrayRef<TCell> cells,
    TString& error)
{
    if (types.size() != cells.size()) {
        error = TStringBuilder()
            << "Types count " << types.size()
            << " does not match cells count " << cells.size();
        return false;
    }

    auto* protoRow = resultSet.add_rows();
    protoRow->mutable_items()->Reserve(cells.size());
    for (size_t i = 0; i < cells.size(); ++i) {
        if (!AddCell(*protoRow, types[i], cells[i], error)) {
            resultSet.mutable_rows()->RemoveLast();
            return false;
        }
    }

    return true;
}

} // namespace NDataShard
} // namespace NKikimr
