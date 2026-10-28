#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.h>
#include <ydb/library/yql/dq/runtime/dq_compute.h>

#include <functional>

namespace NKikimr::NMiniKQL {

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

    void SetWakeupCallback(std::function<void()> wakeupCallback);
    const std::function<void()>& GetWakeupCallback() const;

    void SetCheckpointContext(TIntrusiveConstPtr<NYql::NDq::TCheckpointContext> checkpointContext);
    TIntrusiveConstPtr<NYql::NDq::TCheckpointContext> GetCheckpointContext() const;

    void SetQueryContext(const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken);
    const TString& GetDatabase() const;
    const TIntrusiveConstPtr<NACLib::TUserToken>& GetUserToken() const;

private:
    ui64 CurrentTaskId = 0;
    std::function<void()> WakeupCallback;
    TIntrusiveConstPtr<NYql::NDq::TCheckpointContext> CheckpointContext;
    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

} // namespace NKikimr::NMiniKQL
