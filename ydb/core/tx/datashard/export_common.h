#pragma once

#include "datashard_user_table.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/map.h>
#include <util/generic/maybe.h>

namespace NKikimr::NDataShard {

TMaybe<Ydb::Table::CreateTableRequest> GenYdbScheme(
    const TMap<ui32, TUserTable::TUserColumn>& columns,
    const NKikimrSchemeOp::TPathDescription& pathDesc);

TMaybe<Ydb::Scheme::ModifyPermissionsRequest> GenYdbPermissions(
    const NKikimrSchemeOp::TPathDescription& pathDesc);

} // NKikimr::NDataShard
