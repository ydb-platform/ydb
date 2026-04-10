#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

namespace NACLib {
class TUserToken;
}
    
namespace NKikimr::NPQ::NScheme {

enum EEv : ui32 {
    EvReadResponse = InternalEventSpaceBegin(NPQ::NEvents::EServices::SCHEME),
    EvAlterTopicResponse,
    EvErrorResponse,
    EvEnd
};

struct TEvAlterTopicResponse : public NActors::TEventLocal<TEvAlterTopicResponse, EEv::EvAlterTopicResponse> {
};
    
struct TEvErrorResponse : public NActors::TEventLocal<TEvErrorResponse, EEv::EvErrorResponse> {
    TEvErrorResponse(Ydb::StatusIds::StatusCode status, TString&& errorMessage)
        : Status(status)
        , ErrorMessage(std::move(errorMessage))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    TString ErrorMessage;
};


struct TAlterTopicSettings {
    TString Database;
    TString PeerName;
    Ydb::Topic::AlterTopicRequest Request;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    ui64 Cookie;
};

NActors::IActor* CreateAlterTopicActor(const NActors::TActorId& parentId, TAlterTopicSettings&& settings);


struct TCreateTopicSettings {
    Ydb::Topic::CreateTopicRequest Request;
};

NActors::IActor* CreateCreateTopicActor(const NActors::TActorId& parentId, TCreateTopicSettings&& settings);


struct TDropTopicSettings {
    Ydb::Topic::DropTopicRequest Request;
};

NActors::IActor* CreateDropTopicActor(const NActors::TActorId& parentId, TDropTopicSettings&& settings);

} // namespace NKikimr::NPQ::NScheme
