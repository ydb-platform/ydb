#include "kqp_compute_context.h"

#include <ydb/library/yql/dq/runtime/dq_output_consumer.h>

namespace NKikimr::NMiniKQL {

void TKqpComputeContextBase::SetWakeupCallback(std::function<void()> wakeupCallback) {
    WakeupCallback = std::move(wakeupCallback);
}

const std::function<void()>& TKqpComputeContextBase::GetWakeupCallback() const {
    return WakeupCallback;
}

void TKqpComputeContextBase::RegisterRemoteEmbedderOutput(NYql::NDq::IDqOutputConsumer* output) {
    if (RemoteEmbedderStage) {
        // The task runner owns these consumers for the life of the stage.
        RemoteEmbedderOutputs.push_back(output);
    }
}

void TKqpComputeContextBase::FlushRemoteEmbedderOutputs() const {
    for (auto* output : RemoteEmbedderOutputs) {
        output->Flush();
    }
}

void TKqpComputeContextBase::SetCheckpointContext(TIntrusiveConstPtr<NYql::NDq::TCheckpointContext> checkpointContext) {
    CheckpointContext = std::move(checkpointContext);
}

TIntrusiveConstPtr<NYql::NDq::TCheckpointContext> TKqpComputeContextBase::GetCheckpointContext() const {
    return CheckpointContext;
}

void TKqpComputeContextBase::SetQueryContext(const TString& database, TIntrusiveConstPtr<NACLib::TUserToken> userToken) {
    Database = database;
    UserToken = std::move(userToken);
}

const TString& TKqpComputeContextBase::GetDatabase() const {
    return Database;
}

const TIntrusiveConstPtr<NACLib::TUserToken>& TKqpComputeContextBase::GetUserToken() const {
    return UserToken;
}

} // namespace NKikimr::NMiniKQL
