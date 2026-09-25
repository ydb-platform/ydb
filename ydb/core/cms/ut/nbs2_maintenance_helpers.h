#pragma once

#include <ydb/core/cms/cms_impl.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller_events_private.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NKikimr::NCmsTest::NNbs2Test {

namespace NDbsc = NYdb::NBS::NBlockStore::NStorage::NDbsController;
using TRequest = NDbsc::TEvDbsControllerPrivate::TEvNodeMaintenancePermissionRequest;
using TResponse = NDbsc::TEvDbsControllerPrivate::TEvNodeMaintenancePermissionResponse;
using TResponseRecord = NDbsc::NProto::TNodeMaintenancePermissionResponse;
using TResult = NCms::TCms::TEvPrivate::TEvNbs2MaintenanceResult;
using TStatus = NKikimrCms::TStatus;

struct TAttempt {
    ui64 Id;
    TActorId Checker;
    TActorId Pipe;
    TInstant Deadline;
};

enum class EConnectMode {
    Accept,
    Reject,
    Stall,
};

struct TControllerState {
    EConnectMode ConnectMode = EConnectMode::Accept;
    TVector<TActorId> Clients;
    THashMap<TActorId, TActorId> Servers;
    THashSet<TActorId> DisconnectedServers;
    TVector<TRequest::TPtr> Requests;
};

// Mock only tablet discovery and DBSC itself. Both ends of tablet pipe are real.
class TFakeDbsController : public TActor<TFakeDbsController> {
public:
    explicit TFakeDbsController(TControllerState& state)
        : TActor(&TThis::StateWork)
        , State(state)
        , Acceptor(NTabletPipe::CreateConnectAcceptor(MakeDbsControllerID()))
    {
    }

private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletResolver::TEvForward, Handle);
            IgnoreFunc(TEvTabletResolver::TEvTabletProblem);
            hFunc(TEvTabletPipe::TEvConnect, Handle);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            hFunc(TEvTabletPipe::TEvServerDestroyed, Handle);
            hFunc(TRequest, Handle);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    void Handle(TEvTabletResolver::TEvForward::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TabletID, MakeDbsControllerID());
        Send(ev->Sender, new TEvTabletResolver::TEvForwardResult(
            MakeDbsControllerID(), SelfId(), SelfId(), 0), 0, ev->Cookie);
    }

    void Handle(TEvTabletPipe::TEvConnect::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetTabletId(), MakeDbsControllerID());
        const auto client = ActorIdFromProto(ev->Get()->Record.GetClientId());
        State.Clients.push_back(client);
        switch (State.ConnectMode) {
        case EConnectMode::Accept:
            State.Servers[client] = Acceptor->Accept(ev, SelfId(), SelfId());
            break;
        case EConnectMode::Reject:
            Acceptor->Reject(ev, SelfId(), NKikimrProto::ERROR);
            break;
        case EConnectMode::Stall:
            // Leave the real pipe client waiting for the handshake reply.
            break;
        }
    }

    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev) {
        State.DisconnectedServers.insert(ev->Get()->ServerId);
    }

    void Handle(TEvTabletPipe::TEvServerDestroyed::TPtr& ev) {
        Acceptor->Erase(ev);
    }

    void Handle(TRequest::TPtr& ev) {
        State.Requests.emplace_back(ev.Release());
    }

    void PassAway() override {
        Acceptor->Detach(SelfId());
        TActor::PassAway();
    }

private:
    TControllerState& State;
    THolder<NTabletPipe::IConnectAcceptor> Acceptor;
};

} // namespace NKikimr::NCmsTest::NNbs2Test
