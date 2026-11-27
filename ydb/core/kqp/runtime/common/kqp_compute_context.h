#pragma once

#include <ydb/core/kqp/common/simple/kqp_compute_column.h>

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_checkpoints.h>
#include <ydb/library/yql/dq/runtime/dq_compute.h>

#include <functional>

namespace NYql::NDq {
class IDqOutputConsumer;
}

namespace NKikimr::NMiniKQL {

class TKqpComputeContextBase : public NYql::NDq::TDqComputeContextBase {
public:
    using TColumn = TKqpComputeColumn;

    // used only at then building of a computation graph, to inject taskId in runtime nodes
    void SetCurrentTaskId(ui64 taskId) { CurrentTaskId = taskId; }
    ui64 GetCurrentTaskId() const { return CurrentTaskId; }

    void SetWakeupCallback(std::function<void()> wakeupCallback);
    const std::function<void()>& GetWakeupCallback() const;

    void SetRemoteEmbedderWakeupActor(NActors::TActorId actor) { RemoteEmbedderWakeupActor = actor; }
    NActors::TActorId GetRemoteEmbedderWakeupActor() const { return RemoteEmbedderWakeupActor; }
    void EnableRemoteEmbedderStage() { RemoteEmbedderStage = true; }
    void RegisterRemoteEmbedderOutput(NYql::NDq::IDqOutputConsumer* output);
    void FlushRemoteEmbedderOutputs() const;

    void SetCheckpointContext(TIntrusiveConstPtr<NYql::NDq::TCheckpointContext> checkpointContext);
    TIntrusiveConstPtr<NYql::NDq::TCheckpointContext> GetCheckpointContext() const;

    void SetQueryContext(const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken);
    const TString& GetDatabase() const;
    const TIntrusiveConstPtr<NACLib::TUserToken>& GetUserToken() const;

private:
    ui64 CurrentTaskId = 0;
    std::function<void()> WakeupCallback;
    NActors::TActorId RemoteEmbedderWakeupActor;
    bool RemoteEmbedderStage = false;
    TVector<NYql::NDq::IDqOutputConsumer*> RemoteEmbedderOutputs;
    TIntrusiveConstPtr<NYql::NDq::TCheckpointContext> CheckpointContext;
    TString Database;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

} // namespace NKikimr::NMiniKQL
