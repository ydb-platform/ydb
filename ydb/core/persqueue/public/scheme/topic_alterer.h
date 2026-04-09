#pragma once

#include "scheme_int.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/scheme/scheme.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NPQ::NScheme {

class TTopicAlterer : public TBaseActor<TTopicAlterer>
                    , public TConstantLogPrefix {
public:
    TTopicAlterer(NKikimrServices::EServiceKikimr service, TTopicAltererSettings&& settings);
    ~TTopicAlterer() = default;

    void Bootstrap();
    void PassAway() override;

    TString BuildLogPrefix() const override;

private:
    void DoDescribe();
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr& ev);
    STFUNC(DescribeState);

private:
    void DoAlter();
    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev);
    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr& ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    STFUNC(AlterState);

private:
    TString GetWorkingDir() const;

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage);
    void SendToTablet(ui64 tabletId, IEventBase *ev);

private:
    const TTopicAltererSettings Settings;

    NDescriber::TTopicInfo TopicInfo;
    ui64 Cookie = 0;
};

} // namespace NKikimr::NPQ::NScheme
