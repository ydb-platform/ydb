#pragma once

#include "schema_int.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NPQ::NSchema {

class TTopicAlterer : public TBaseActor<TTopicAlterer>
                    , public TPipeCacheClient
                    , public TConstantLogPrefix {
    static constexpr size_t MaxWaitTxCompletionRetries = 3;
public:
    TTopicAlterer(NKikimrServices::EServiceKikimr service, TTopicAltererSettings&& settings);
    ~TTopicAlterer() = default;

    void Bootstrap();
    void PassAway() override;

    TString BuildLogPrefix() const override;
    void OnException(const std::exception& exc) override;

private:
    void DoDescribe();
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev);
    STFUNC(DescribeState);

private:
    void DoAlter();
    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev);
    void HandleOnAlter(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);
    STFUNC(AlterState);

private:
    void DoWaitTxCompletion();
    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev);
    void HandleOnWaitTxCompletion(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);
    STFUNC(WaitTxCompletionState);

private:
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage);
    void ReplyOkAndDie();

private:
    const TTopicAltererSettings Settings;

    NDescriber::TTopicInfo TopicInfo;

    ui64 SchemeShardTabletId = 0;
    ui64 TxId = 0;
    size_t WaitTxCompletionRetries = 0;
};

} // namespace NKikimr::NPQ::NSchema
