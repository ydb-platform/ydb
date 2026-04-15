#pragma once

#include "common.h"

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NPQ::NSchema {

class ITopicAltererStrategy {
public:
    virtual ~ITopicAltererStrategy() = default;

    virtual const TString& GetTopicName() const = 0;
    virtual TResult ApplyChanges(
        NKikimrSchemeOp::TPersQueueGroupDescription& targetConfig,
        const NKikimrSchemeOp::TPersQueueGroupDescription& sourceConfig,
        const bool isCdcStream
    ) = 0;
};

struct TTopicAltererSettings {
    TActorId ParentId;
    TString Database;
    TString PeerName;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    std::unique_ptr<ITopicAltererStrategy> Strategy;
    bool IfExists = false;
    ui64 Cookie = 0;
    bool IsCdcStreamCompatible = true;
};

IActor* CreateTopicAlterer(NKikimrServices::EServiceKikimr service, TTopicAltererSettings&& settings);
        
} // namespace NKikimr::NPQ::NSchema
