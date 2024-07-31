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

TColumnsTypes GetAllTypes(const TUserTable& tableInfo) {
    TColumnsTypes result;
    result.reserve(tableInfo.Columns.size());

    for (const auto& it : tableInfo.Columns) {
        result[it.second.Name] = it.second.Type;
    }

    return result;
}

}
