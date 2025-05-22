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

ui64 CountBytes(TArrayRef<const TCell> key, const NTable::TRowState& row) {
    ui64 bytes = 0;
    for (auto& cell : key) {
        bytes += cell.Size();
    }
    for (auto& cell : *row) {
        bytes += cell.Size();
    }
    return bytes;
}

}
