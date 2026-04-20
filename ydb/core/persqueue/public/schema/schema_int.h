#pragma once

#include "common.h"

#include <ydb/core/protos/tx_proxy.pb.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NPQ::NSchema {

class ITopicAltererStrategy {
public:
    virtual ~ITopicAltererStrategy() = default;

    virtual bool IsCdcStreamCompatible() const = 0;
    virtual NACLib::EAccessRights GetRequiredPermission() const = 0;

    virtual const TString& GetTopicName() const = 0;

    virtual TResult BuildTransaction(const NDescriber::TTopicInfo& topicInfo, NKikimrTxUserProxy::TTransaction*) = 0;

    virtual IEventBase* CreateSuccessResponse() = 0;
    virtual IEventBase* CreateErrorResponse(Ydb::StatusIds::StatusCode status, TString&& errorMessage) = 0;
};

class IAlterTopicStrategy: public ITopicAltererStrategy {
public:
    bool IsCdcStreamCompatible() const override;
    NACLib::EAccessRights GetRequiredPermission() const override;

    TResult BuildTransaction(const NDescriber::TTopicInfo& topicInfo, NKikimrTxUserProxy::TTransaction* transaction) override;
    virtual TResult ApplyChanges(
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& sourceConfig,
        bool isCdcStream
    ) = 0;

    IEventBase* CreateSuccessResponse() override;
    IEventBase* CreateErrorResponse(Ydb::StatusIds::StatusCode status, TString&& errorMessage) override;

private:
    NKikimrSchemeOp::TModifyScheme ModifyScheme;
};

class IDropTopicStrategy: public ITopicAltererStrategy {
public:
    bool IsCdcStreamCompatible() const override;
    NACLib::EAccessRights GetRequiredPermission() const override;

    TResult BuildTransaction(
        const NDescriber::TTopicInfo& topicInfo, 
        NKikimrTxUserProxy::TTransaction* transaction
    ) override;

    IEventBase* CreateSuccessResponse() override;
    IEventBase* CreateErrorResponse(Ydb::StatusIds::StatusCode status, TString&& errorMessage) override;
};

struct TTopicAltererSettings {
    TActorId ParentId;
    TString Database;
    TString PeerName;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    std::unique_ptr<ITopicAltererStrategy> Strategy;
    bool IfExists = false;
    ui64 Cookie = 0;
};

IActor* CreateTopicAlterer(NKikimrServices::EServiceKikimr service, TTopicAltererSettings&& settings);
        
} // namespace NKikimr::NPQ::NSchema
