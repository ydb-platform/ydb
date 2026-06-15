#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <library/cpp/threading/future/core/future.h>

namespace NACLib {

class TUserToken;

} // namespace NACLib

namespace NKikimr::NPQ::NSchema {

enum EEv : ui32 {
    EvReadResponse = InternalEventSpaceBegin(NPQ::NEvents::EServices::SCHEMA),
    EvSchemaOperationResponse,
    EvSchemaResponse,
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

struct TSchemaResponse {
    TString Path;
    Ydb::StatusIds::StatusCode Status;
    TString ErrorMessage;
    NKikimrSchemeOp::TModifyScheme ModifyScheme;
};

struct TEvSchemaResponse: public NActors::TEventLocal<TEvSchemaResponse, EEv::EvSchemaResponse>
                        , public TSchemaResponse {
    TEvSchemaResponse(
        const TString& path,
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        TString&& errorMessage = {},
        NKikimrSchemeOp::TModifyScheme&& modifyScheme = {}
    )
        : TSchemaResponse(path, status, std::move(errorMessage), std::move(modifyScheme))
    {
    }
};

//
// Alter Topic
//
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
NActors::IActor* CreateAlterTopicActor(NThreading::TPromise<TSchemaResponse>&& promise, TAlterTopicSettings&& settings);

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
struct TCreateTopicSettings {
    TString Database;
    TString PeerName;
    Ydb::Topic::CreateTopicRequest Request;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    bool IfNotExists = true;
    bool PrepareOnly = false;
    ui64 Cookie = 0;
};

NActors::IActor* CreateCreateTopicActor(const NActors::TActorId& parentId, TCreateTopicSettings&& settings);
NActors::IActor* CreateCreateTopicActor(NThreading::TPromise<TSchemaResponse>&& promise, TCreateTopicSettings&& settings);

//
// Drop Topic
//
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
