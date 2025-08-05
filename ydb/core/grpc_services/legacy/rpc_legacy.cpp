#include "service_legacy.h"

#include <ydb/core/client/server/msgbus_server.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/public/lib/base/msgbus_status.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr::NGRpcService {

namespace NLegacyGrpcService {

namespace NPrivate {

// In real situations we don't return all these codes from grpc layer
ui32 ToMsgBusStatus(Ydb::StatusIds::StatusCode status) {
    switch (status) {
        case Ydb::StatusIds::STATUS_CODE_UNSPECIFIED:
            return NMsgBusProxy::MSTATUS_UNKNOWN;
        case Ydb::StatusIds::SUCCESS:
            return NMsgBusProxy::MSTATUS_OK;
        case Ydb::StatusIds::INTERNAL_ERROR:
            return NMsgBusProxy::MSTATUS_INTERNALERROR;
        case Ydb::StatusIds::ABORTED:
        case Ydb::StatusIds::CANCELLED:
            return NMsgBusProxy::MSTATUS_ABORTED;
        case Ydb::StatusIds::UNAVAILABLE:
        case Ydb::StatusIds::OVERLOADED:
        case Ydb::StatusIds::SESSION_BUSY:
            return NMsgBusProxy::MSTATUS_NOTREADY;
        case Ydb::StatusIds::TIMEOUT:
            return NMsgBusProxy::MSTATUS_TIMEOUT;
        case Ydb::StatusIds::PRECONDITION_FAILED:
            return NMsgBusProxy::MSTATUS_REJECTED;
        // case Ydb::StatusIds::SCHEME_ERROR:
        // case Ydb::StatusIds::GENERIC_ERROR:
        // case Ydb::StatusIds::BAD_SESSION:
        // case Ydb::StatusIds::ALREADY_EXISTS:
        // case Ydb::StatusIds::NOT_FOUND:
        // case Ydb::StatusIds::SESSION_EXPIRED:
        // case Ydb::StatusIds::BAD_REQUEST:
        // case Ydb::StatusIds::UNAUTHORIZED:
        // case Ydb::StatusIds::UNDETERMINED:
        // case Ydb::StatusIds::UNSUPPORTED:
        // case Ydb::StatusIds::EXTERNAL_ERROR:
        default:
            return NMsgBusProxy::MSTATUS_ERROR;
    }
}

}

static bool CheckMsgBusProxy(const NActors::TActorId& msgBusProxy, IRequestNoOpCtx& p) {
    if (!msgBusProxy) {
        NKikimrClient::TResponse resp;
        resp.SetStatus(NMsgBusProxy::MSTATUS_ERROR);
        resp.SetErrorReason("no MessageBus proxy");
        p.Reply(&resp);
        return false;
    }
    return true;
}

static void DoSchemeOperation(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem, std::unique_ptr<IRequestNoOpCtx> p) {
    if (CheckMsgBusProxy(msgBusProxy, *p)) {
        NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_FLAT_TX_REQUEST);
        actorSystem->Send(msgBusProxy, new NMsgBusProxy::TEvBusProxy::TEvFlatTxRequest(ctx));
    }
}

TCreateActorCallback DoSchemeOperation(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem) {
    return [msgBusProxy, actorSystem](std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&) {
        DoSchemeOperation(msgBusProxy, actorSystem, std::move(p));
    };
}

void DoSchemeOperationStatus(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_FLAT_TX_STATUS_REQUEST);
    f.RegisterActor(CreateMessageBusSchemeOperationStatus(ctx));
}

static void DoSchemeDescribe(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem, std::unique_ptr<IRequestNoOpCtx> p) {
    if (CheckMsgBusProxy(msgBusProxy, *p)) {
        NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_FLAT_DESCRIBE_REQUEST);
        actorSystem->Send(msgBusProxy, new NMsgBusProxy::TEvBusProxy::TEvFlatDescribeRequest(ctx));
    }
}

TCreateActorCallback DoSchemeDescribe(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem) {
    return [msgBusProxy, actorSystem](std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&) {
        DoSchemeDescribe(msgBusProxy, actorSystem, std::move(p));
    };
}

void DoChooseProxy(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_CHOOSE_PROXY);
    f.RegisterActor(CreateMessageBusChooseProxy(ctx));
}

static void DoPersQueueRequest(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem, std::unique_ptr<IRequestNoOpCtx> p) {
    if (CheckMsgBusProxy(msgBusProxy, *p)) {
        NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_PERSQUEUE);
        actorSystem->Send(msgBusProxy, new NMsgBusProxy::TEvBusProxy::TEvPersQueue(ctx));
    }
}

TCreateActorCallback DoPersQueueRequest(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem) {
    return [msgBusProxy, actorSystem](std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&) {
        DoPersQueueRequest(msgBusProxy, actorSystem, std::move(p));
    };
}

static void DoSchemeInitRoot(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem, std::unique_ptr<IRequestNoOpCtx> p) {
    if (CheckMsgBusProxy(msgBusProxy, *p)) {
        NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_SCHEME_INITROOT);
        actorSystem->Send(msgBusProxy, new NMsgBusProxy::TEvBusProxy::TEvInitRoot(ctx));
    }
}

TCreateActorCallback DoSchemeInitRoot(const NActors::TActorId& msgBusProxy, NActors::TActorSystem* actorSystem) {
    return [msgBusProxy, actorSystem](std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider&) {
        DoSchemeInitRoot(msgBusProxy, actorSystem, std::move(p));
    };
}

void DoResolveNode(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_RESOLVE_NODE);
    f.RegisterActor(CreateMessageBusResolveNode(ctx));
}

void DoFillNode(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_FILL_NODE);
    f.RegisterActor(CreateMessageBusFillNode(ctx));
}

void DoDrainNode(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_DRAIN_NODE);
    f.RegisterActor(CreateMessageBusDrainNode(ctx));
}

void DoBlobStorageConfig(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_BLOB_STORAGE_CONFIG_REQUEST);
    f.RegisterActor(CreateMessageBusBlobStorageConfig(ctx));
}

void DoHiveCreateTablet(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_HIVE_CREATE_TABLET);
    f.RegisterActor(CreateMessageBusHiveCreateTablet(ctx));
}

void DoTestShardControl(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_TEST_SHARD_CONTROL);
    f.RegisterActor(CreateMessageBusTestShardControl(ctx));
}

void DoConsoleRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    NKikimr::NMsgBusProxy::TBusMessageContext ctx(std::move(p), NMsgBusProxy::MTYPE_CLIENT_CONSOLE_REQUEST);
    f.RegisterActor(CreateMessageBusConsoleRequest(ctx));
}

} // namespace NLegacyGrpcService

} // namespace NKikimr::NGRpcService
