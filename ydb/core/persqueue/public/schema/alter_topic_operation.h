#pragma once

#include "common.h"

#include <ydb/library/actors/core/actorsystem_fwd.h>

namespace NKikimr::NPQ::NSchema {

class IAlterTopicStrategy {
public:
    virtual ~IAlterTopicStrategy() = default;

    virtual const TString& GetTopicName() const = 0;

    virtual TResult ApplyChanges(
        NKikimrSchemeOp::TModifyScheme& modifyScheme,
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& sourceConfig,
        bool isCdcStream
    ) = 0;
};

struct TAlterTopicOperationSettings {
    TString Database;
    TString PeerName;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    std::unique_ptr<IAlterTopicStrategy> Strategy;
    bool IfExists = false;
    ui64 Cookie = 0;
};


IActor* CreateAlterTopicOperationActor(TActorId parentId, TAlterTopicOperationSettings&& settings);

} // namespace NKikimr::NPQ::NSchema
