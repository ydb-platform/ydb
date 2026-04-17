#pragma once

#include "schema_int.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NPQ::NSchema {

class TAlterTopicOperationActor: public TBaseActor<TAlterTopicOperationActor>
                               , public TPipeCacheClient
                               , public TConstantLogPrefix {
public:
    TAlterTopicOperationActor(NKikimrServices::EServiceKikimr service, TTopicAltererSettings&& settings);
    ~TAlterTopicOperationActor() = default;

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
    void Handle(TEvSchemaOperationResponse::TPtr& ev);
    STFUNC(AlterState);

private:
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage);
    void ReplyOkAndDie();

private:
    const TTopicAltererSettings Settings;

    NDescriber::TTopicInfo TopicInfo;
};

} // namespace NKikimr::NPQ::NSchema
