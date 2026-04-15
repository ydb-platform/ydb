#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <ydb/core/persqueue/events/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

namespace NACLib {
class TUserToken;
}
    
namespace NKikimr::NPQ::NSchema {

enum EEv : ui32 {
    EvReadResponse = InternalEventSpaceBegin(NPQ::NEvents::EServices::SCHEMA),
    EvAlterTopicResponse,
    EvEnd
};

struct TAlterTopicResponse {
    Ydb::StatusIds::StatusCode Status;
    TString ErrorMessage;
    NKikimrSchemeOp::TModifyScheme ModifyScheme;
};

struct TEvAlterTopicResponse : public NActors::TEventLocal<TEvAlterTopicResponse, EEv::EvAlterTopicResponse>
                             , public TAlterTopicResponse {
    TEvAlterTopicResponse(
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        TString&& errorMessage = {},
        NKikimrSchemeOp::TModifyScheme&& modifyScheme = {}
    )
        : TAlterTopicResponse(status, std::move(errorMessage), std::move(modifyScheme))
    {
    }
};


struct TAlterTopicSettings {
    TString Database;
    TString PeerName;
    Ydb::Topic::AlterTopicRequest Request;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    bool IfExists = false;
    ui64 Cookie = 0;
};

NActors::IActor* CreateAlterTopicActor(const NActors::TActorId& parentId, TAlterTopicSettings&& settings);
NActors::IActor* CreateAlterTopicActor(NThreading::TPromise<TAlterTopicResponse>&& promise, TAlterTopicSettings&& settings);


struct TCreateTopicSettings {
    Ydb::Topic::CreateTopicRequest Request;
};

NActors::IActor* CreateCreateTopicActor(const NActors::TActorId& parentId, TCreateTopicSettings&& settings);


struct TDropTopicSettings {
    Ydb::Topic::DropTopicRequest Request;
};

NActors::IActor* CreateDropTopicActor(const NActors::TActorId& parentId, TDropTopicSettings&& settings);

} // namespace NKikimr::NPQ::NSchema
