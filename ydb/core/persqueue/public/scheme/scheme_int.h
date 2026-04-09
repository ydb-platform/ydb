#pragma once

#include "common.h"

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>

namespace NKikimr::NPQ::NScheme {

class TTopicAltererStrategy {
public:
    virtual ~TTopicAltererStrategy() = default;

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
    std::unique_ptr<TTopicAltererStrategy> Strategy;
    ui64 Cookie = 0;
    bool IsCdcStreamCompatible = true;
};

IActor* CreateTopicAlterer(NKikimrServices::EServiceKikimr service, TTopicAltererSettings&& settings);
        
} // namespace NKikimr::NPQ::NScheme
