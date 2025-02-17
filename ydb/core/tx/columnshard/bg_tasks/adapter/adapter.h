#pragma once
#include <ydb/core/tx/columnshard/bg_tasks/templates/adapter.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NColumnShard::NBackground {

using TAdapter = NTx::NBackground::TAdapterTemplate<NColumnShard::Schema>;

}