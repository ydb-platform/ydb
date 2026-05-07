#pragma once

#include "common.h"

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ::NSchema {

class ICreateTopicStrategy {
public:
    virtual ~ICreateTopicStrategy() = default;

    virtual const TString& GetTopicName() const = 0;

    virtual TResult ApplyChanges(
        const TString& localCluster,
        const TString& database,
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig
    ) = 0;
};

struct TCreateTopicOperationSettings {
    TString Database;
    TString PeerName;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    bool IfNotExists = true;
    bool PrepareOnly = false;
    std::unique_ptr<ICreateTopicStrategy> Strategy;
    ui64 Cookie = 0;
};


IActor* CreateCreateTopicOperationActor(TActorId parentId, TCreateTopicOperationSettings&& settings);

} // namespace NKikimr::NPQ::NSchema
