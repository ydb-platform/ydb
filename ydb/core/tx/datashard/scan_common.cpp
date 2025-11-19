#include "scan_common.h"
#include <ydb/core/tx/datashard/datashard_user_table.h>

namespace NKikimr::NDataShard {

TColumnsTags GetAllTags(const TUserTable& tableInfo) {
    TColumnsTags result;

    for (const auto& it : tableInfo.Columns) {
        result[it.second.Name] = it.first;
    }

    return result;
}

void AddTags(TTags& tags, const TColumnsTags& allTags, TProtoColumnsCRef columns) {
    for (const auto& colName : columns) {
        tags.push_back(allTags.at(colName));
    }
}

TColumnsTypes GetAllTypes(const TUserTable& tableInfo) {
    TColumnsTypes result;

    for (const auto& it : tableInfo.Columns) {
        result[it.second.Name] = it.second.Type;
    }

    return result;
}

// amount of read data is calculated based only on row cells as it is done in KQP part
// complex scan read bytes metric would be too hard to be explained for users
ui64 CountRowCellBytes(TConstArrayRef<TCell> key, TConstArrayRef<TCell> value) {
    ui64 bytes = 0;
    for (auto& cell : key) {
        bytes += cell.Size();
    }
    for (auto& cell : value) {
        bytes += cell.Size();
    }
    return bytes;
}

}
