#pragma once

#include <ydb/library/yql/dq/runtime/dq_compute.h>

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>

namespace NKikimr {
namespace NMiniKQL {

class TKqpComputeContextBase : public NYql::NDq::TDqComputeContextBase {
public:
    struct TColumn {
        NTable::TTag Tag;
        NScheme::TTypeInfo Type;
        TString TypeMod;
        TPgType* PgType = nullptr;
    };

    // used only at then building of a computation graph, to inject taskId in runtime nodes
    void SetCurrentTaskId(ui64 taskId) { CurrentTaskId = taskId; }
    ui64 GetCurrentTaskId() const { return CurrentTaskId; }

private:
    ui64 CurrentTaskId = 0;
};

} // namespace NMiniKQL
} // namespace NKikimr
