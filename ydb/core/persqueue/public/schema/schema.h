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
    EvSchemaOperationResponse,
    EvAlterTopicResponse,
    EvCreateTopicResponse,
    EvDropTopicResponse,
    EvEnd
};

struct TEvSchemaOperationResponse: public NActors::TEventLocal<TEvSchemaOperationResponse, EEv::EvSchemaOperationResponse> {
    TEvSchemaOperationResponse(
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        TString&& errorMessage = {}
    )
        : Status(status)
        , ErrorMessage(std::move(errorMessage))
    {
    }

    Ydb::StatusIds::StatusCode Status;
    TString ErrorMessage;
};

//
// Alter Topic
//
struct TAlterTopicResponse {
    Ydb::StatusIds::StatusCode Status;
    TString ErrorMessage;
    NKikimrSchemeOp::TModifyScheme ModifyScheme;
};

struct TEvAlterTopicResponse: public NActors::TEventLocal<TEvAlterTopicResponse, EEv::EvAlterTopicResponse>
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
    bool PrepareOnly = false;
    ui64 Cookie = 0;
};

NActors::IActor* CreateAlterTopicActor(const NActors::TActorId& parentId, TAlterTopicSettings&& settings);
NActors::IActor* CreateAlterTopicActor(NThreading::TPromise<TAlterTopicResponse>&& promise, TAlterTopicSettings&& settings);

//
// Add Consumer
//
struct TAddConsumerSettings {
    TString Database;
    TString PeerName;
    TString Path;
    Ydb::Topic::Consumer Consumer;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    ui64 Cookie = 0;
};

NActors::IActor* CreateAddConsumerActor(const NActors::TActorId& parentId, TAddConsumerSettings&& settings);

//
// Remove Consumer
//
struct TRemoveConsumerSettings {
    TString Database;
    TString PeerName;
    TString Path;
    TString ConsumerName;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    ui64 Cookie = 0;
};

NActors::IActor* CreateRemoveConsumerActor(const NActors::TActorId& parentId, TRemoveConsumerSettings&& settings);

//
// Create Topic
//
struct TCreateTopicResponse {
    Ydb::StatusIds::StatusCode Status;
    TString ErrorMessage;
    NKikimrSchemeOp::TModifyScheme ModifyScheme;
};

struct TEvCreateTopicResponse: public NActors::TEventLocal<TEvCreateTopicResponse, EEv::EvCreateTopicResponse>
                             , public TCreateTopicResponse {
    TEvCreateTopicResponse(
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        TString&& errorMessage = {},
        NKikimrSchemeOp::TModifyScheme&& modifyScheme = {}
    )
        : TCreateTopicResponse(status, std::move(errorMessage), std::move(modifyScheme))
    {
    }
};

struct TCreateTopicSettings {
    TString Database;
    TString PeerName;
    Ydb::Topic::CreateTopicRequest Request;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    bool IfNotExists = false;
    bool PrepareOnly = false;
    ui64 Cookie = 0;
};

NActors::IActor* CreateCreateTopicActor(const NActors::TActorId& parentId, TCreateTopicSettings&& settings);
NActors::IActor* CreateCreateTopicActor(NThreading::TPromise<TCreateTopicResponse>&& promise, TCreateTopicSettings&& settings);

//
// Drop Topic
//
struct TDropTopicResponse {
    Ydb::StatusIds::StatusCode Status;
    TString ErrorMessage;
    NKikimrSchemeOp::TModifyScheme ModifyScheme;
};

struct TEvDropTopicResponse : public NActors::TEventLocal<TEvDropTopicResponse, EEv::EvDropTopicResponse>
                             , public TDropTopicResponse {
    TEvDropTopicResponse(
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        TString&& errorMessage = {},
        NKikimrSchemeOp::TModifyScheme&& modifyScheme = {}
    )
        : TDropTopicResponse(status, std::move(errorMessage), std::move(modifyScheme))
    {
    }
};

struct TDropTopicSettings {
    TString Database;
    TString PeerName;
    TString Path;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    bool IfExists = false;
    ui64 Cookie = 0;
};

NActors::IActor* CreateDropTopicActor(const NActors::TActorId& parentId, TDropTopicSettings&& settings);

} // namespace NKikimr::NPQ::NSchema
