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

}
