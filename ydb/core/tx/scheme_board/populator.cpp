#include "events.h"
#include "helpers.h"
#include "monitorable_actor.h"
#include "populator.h"

#include <contrib/libs/protobuf/src/google/protobuf/util/json_util.h>

#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/yverify_stream.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/log.h>

#include <util/digest/city.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NSchemeBoard {

#define SBP_LOG_T(stream) SB_LOG_T(SCHEME_BOARD_POPULATOR, "" << SelfId() << " " << stream)
#define SBP_LOG_D(stream) SB_LOG_D(SCHEME_BOARD_POPULATOR, "" << SelfId() << " " << stream)
#define SBP_LOG_N(stream) SB_LOG_N(SCHEME_BOARD_POPULATOR, "" << SelfId() << " " << stream)
#define SBP_LOG_E(stream) SB_LOG_E(SCHEME_BOARD_POPULATOR, "" << SelfId() << " " << stream)
#define SBP_LOG_CRIT(stream) SB_LOG_CRIT(SCHEME_BOARD_POPULATOR, "" << SelfId() << " " << stream)

namespace {

    using TDelayedUpdates = TVector<THolder<IEventHandle>>;

    void ReplayUpdates(TDelayedUpdates& updates) {
        for (auto& update : updates) {
            TlsActivationContext->Send(update.Release());
        }

        TDelayedUpdates().swap(updates);
    }

} // anonymous

class TReplicaPopulator: public TMonitorableActor<TReplicaPopulator> {
    void ProcessSync(TSchemeBoardEvents::TEvDescribeResult* msg = nullptr, const TPathId& pathId = TPathId()) {
        if (msg == nullptr) {
            BatchSize = 0;
            Send(Parent, new TSchemeBoardEvents::TEvRequestDescribe(pathId, Replica));
            return;
        }

        if (msg->Commit) {
            auto commit = MakeHolder<TSchemeBoardEvents::TEvCommitRequest>(Owner, Generation);
            Send(Replica, std::move(commit), IEventHandle::FlagTrackDelivery);
            return;
        }

        auto update = msg->HasDescription()
            ? MakeHolder<TSchemeBoardEvents::TEvUpdateBuilder>(Owner, Generation, msg->Description.Record)
            : MakeHolder<TSchemeBoardEvents::TEvUpdateBuilder>(Owner, Generation);

        if (msg->HasDeletedLocalPathIds()) {
            auto& deletedLocalPathIds = *update->Record.MutableDeletedLocalPathIds();

            deletedLocalPathIds.SetBegin(msg->DeletedPathBegin);
            deletedLocalPathIds.SetEnd(msg->DeletedPathEnd);

            CurPathId = TPathId(Owner, msg->DeletedPathEnd);
        }

        if (msg->HasDescription()) {
            auto& record = msg->Description.Record;

            if (!record.HasStatus()) {
                SBP_LOG_E("Ignore description without status");
            } else if (record.GetStatus() != NKikimrScheme::StatusSuccess) {
                SBP_LOG_E("Ignore description"
                    << ": status# " << record.GetStatus()
                    << ", msg# " << record.ShortDebugString());
            } else {
                CurPathId = GetPathId(record);
                update->SetDescribeSchemeResult(std::move(msg->Description));
            }
        }

        if (msg->HasMigratedPath()) {
            SBP_LOG_D("Ignore description of migrated path"
                << ": owner# " << Owner
                << ", localPathId# " << msg->MigratedPathId);
            // this path should be described by another owner (tenant schemeshard)
            auto& migratedLocalPathIds = *update->Record.MutableMigratedLocalPathIds();
            migratedLocalPathIds.SetBegin(msg->MigratedPathId);
            migratedLocalPathIds.SetEnd(msg->MigratedPathId);

            CurPathId = TPathId(Owner, msg->MigratedPathId);
        }

        if (++BatchSize < BatchSizeLimit) {
            CurPathId = CurPathId.NextId();
            Send(Parent, new TSchemeBoardEvents::TEvRequestDescribe(CurPathId, Replica));
        } else {
            update->Record.SetNeedAck(true);
            BatchSize = 0;
        }

        Send(Replica, std::move(update), IEventHandle::FlagTrackDelivery);
    }

    void ResumeSync(const TPathId& fromPathId) {
        ProcessSync(nullptr, fromPathId);
    }

    void EnqueueUpdate(TSchemeBoardEvents::TEvUpdate::TPtr& ev, bool canSend = false) {
        const TPathId pathId = ev->Get()->GetPathId();
        const auto& record = (static_cast<TSchemeBoardEvents::TEvUpdateBuilder*>(ev->Get()))->Record;
        const ui64 version = record.GetIsDeletion() ? Max<ui64>() : GetPathVersion(record.GetDescribeSchemeResult());

        if (canSend && UpdatesInFlight.size() < BatchSizeLimit) {
            bool needSend = true;

            auto it = UpdatesInFlight.find(pathId);
            if (it != UpdatesInFlight.end() && !it->second.empty() && it->second.rbegin()->first >= version) {
                needSend = false;
            }

            it = UpdatesRequested.find(pathId);
            if (it != UpdatesRequested.end()) {
                UpdatesInFlight[pathId].insert(it->second.begin(), it->second.end());
                UpdatesRequested.erase(it);
            }

            UpdatesInFlight[pathId].emplace(version, ev->Cookie);
            if (needSend) {
                Send(Replica, ev->Release().Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
            }
        } else {
            auto it = UpdatesRequested.find(pathId);
            if (it != UpdatesRequested.end()) {
                Updates[pathId].insert(it->second.begin(), it->second.end());
                UpdatesRequested.erase(it);
            }

            Updates[pathId].emplace(version, ev->Cookie);
        }
    }

    void DequeueUpdate(TSchemeBoardEvents::TEvUpdateAck::TPtr& ev) {
        const TPathId pathId = ev->Get()->GetPathId();
        const ui64 version = ev->Get()->Record.GetVersion();

        auto it = UpdatesInFlight.find(pathId);
        if (it != UpdatesInFlight.end()) {
            auto& versions = it->second;
            TSet<ui64> txIds;

            for (auto x = versions.begin(), end = versions.upper_bound(std::make_pair(version, Max<ui64>())); x != end;) {
                txIds.insert(x->second);
                versions.erase(x++);
            }

            if (versions.empty()) {
                UpdatesInFlight.erase(it);
            }

            for (ui64 txId : txIds) {
                Send(Parent, new TSchemeBoardEvents::TEvUpdateAck(Owner, Generation, pathId, version), 0, txId);
            }
        }

        if (UpdatesInFlight.size() < BatchSizeLimit) {
            RequestUpdate();
        }
    }

    bool RequestUpdate() {
        if (UpdatesRequested.size() >= BatchSizeLimit) {
            return false;
        }

        auto it = Updates.begin();
        if (it == Updates.end()) {
            return false;
        }

        UpdatesRequested[it->first].insert(it->second.begin(), it->second.end());
        Send(Parent, new TSchemeBoardEvents::TEvRequestUpdate(it->first));
        Updates.erase(it);

        return true;
    }

    void RequestUpdates() {
        auto move = [](auto& src, auto& dst) {
            dst.insert(src.begin(), src.end());
            src.clear();
        };

        move(UpdatesRequested, Updates);
        move(UpdatesInFlight, Updates);
        while (RequestUpdate());
    }

    template <typename TEvent, typename T>
    bool Check(TEvent& ev, T this_, T that, const TString& what) {
        if (this_ != that) {
            SBP_LOG_E("Suspicious " << TypeName<TEvent>()
                << ": sender# " << ev->Sender
                << ", " << what << "# " << this_
                << ", other " << what << "# " << that);
            return false;
        }

        return true;
    }

    template <typename TEvent>
    bool CheckOwner(TEvent& ev) {
        return Check(ev, Owner, ev->Get()->Record.GetOwner(), "owner");
    }

    template <typename TEvent>
    bool CheckGeneration(TEvent& ev) {
        return Check(ev, Generation, ev->Get()->Record.GetGeneration(), "generation");
    }

    void Handle(TSchemeBoardEvents::TEvHandshakeResponse::TPtr& ev) {
        SBP_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        if (!CheckOwner(ev)) {
            return;
        }

        const ui64 generation = ev->Get()->Record.GetGeneration();
        if (generation > Generation) {
            SBP_LOG_CRIT("Keep calm");
            Become(&TThis::StateCalm);
        } else {
            SBP_LOG_N("Successful handshake"
                << ": replica# " << ev->Sender);

            if (generation < Generation) {
                SBP_LOG_N("Start full sync"
                    << ": replica# " << ev->Sender);
                ProcessSync();
            } else {
                SBP_LOG_N("Resume sync"
                    << ": replica# " << ev->Sender
                    << ", fromPathId# " << LastAckedPathId.NextId());
                ResumeSync(LastAckedPathId.NextId());
            }

            RequestUpdates();
            Become(&TThis::StateWork);
        }
    }

    void Handle(TSchemeBoardEvents::TEvDescribeResult::TPtr& ev) {
        SBP_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        ProcessSync(ev->Get());
    }

    void Handle(TSchemeBoardEvents::TEvUpdate::TPtr& ev) {
        SBP_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie);

        EnqueueUpdate(ev, true);
    }

    void Handle(TSchemeBoardEvents::TEvUpdateAck::TPtr& ev) {
        SBP_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie);

        if (!CheckOwner(ev) || !CheckGeneration(ev)) {
            return;
        }

        if (!ev->Cookie && ev->Get()->GetPathId() == CurPathId) {
            LastAckedPathId = CurPathId;

            CurPathId = CurPathId.NextId();
            Send(Parent, new TSchemeBoardEvents::TEvRequestDescribe(CurPathId, Replica));
        }

        DequeueUpdate(ev);
    }

    void Handle(TSchemeBoardEvents::TEvCommitResponse::TPtr& ev) {
        SBP_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        if (!CheckOwner(ev) || !CheckGeneration(ev)) {
            return;
        }

        LastAckedPathId = CurPathId.PrevId();
    }

    void Handle(TSchemeBoardMonEvents::TEvInfoRequest::TPtr& ev) {
        const auto limit = ev->Get()->Record.GetLimitRepeatedFields();

        auto response = MakeHolder<TSchemeBoardMonEvents::TEvInfoResponse>(SelfId(), ActorActivityType());
        auto& record = *response->Record.MutableReplicaPopulatorResponse();

        ActorIdToProto(Parent, record.MutableParent());
        ActorIdToProto(Replica, record.MutableReplica());

        record.SetOwner(Owner);
        record.SetGeneration(Generation);
        record.SetBatchSize(BatchSize);
        record.SetBatchSizeLimit(BatchSizeLimit);

        record.MutableCurPathId()->SetOwnerId(CurPathId.OwnerId);
        record.MutableCurPathId()->SetLocalPathId(CurPathId.LocalPathId);

        record.MutableLastAckedPathId()->SetOwnerId(LastAckedPathId.OwnerId);
        record.MutableLastAckedPathId()->SetLocalPathId(LastAckedPathId.LocalPathId);

        auto fillUpdates = [limit, &response = response->Record](const auto& from, auto& to) {
            ui32 count = 0;
            for (const auto& [pathId, versions] : from) {
                auto& update = *to.Add();

                update.MutablePathId()->SetOwnerId(pathId.OwnerId);
                update.MutablePathId()->SetLocalPathId(pathId.LocalPathId);

                NKikimrSchemeBoardMon::TReplicaPopulatorResponse::TUpdateInfo::TVersionInfo* info = nullptr;
                TMaybe<ui64> prevVersion;

                for (const auto& [version, txId] : versions) {
                    if (!prevVersion || *prevVersion != version) {
                        info = update.AddVersions();
                    }

                    Y_VERIFY(info);
                    info->SetVersion(version);
                    info->AddTxIds(txId);
                    prevVersion = version;
                }

                if (++count >= limit) {
                    response.SetTruncated(true);
                    break;
                }
            }
        };

        fillUpdates(Updates, *record.MutableUpdates());
        fillUpdates(UpdatesRequested, *record.MutableUpdatesRequested());
        fillUpdates(UpdatesInFlight, *record.MutableUpdatesInFlight());

        Send(ev->Sender, std::move(response), 0, ev->Cookie);
    }

    void SomeSleep() {
        Become(&TThis::StateSleep, TDuration::MilliSeconds(50), new TEvents::TEvWakeup());
    }

    void PassAway() override {
        if (Replica.NodeId() != SelfId().NodeId()) {
            Send(MakeInterconnectProxyId(Replica.NodeId()), new TEvents::TEvUnsubscribe());
        }

        TMonitorableActor::PassAway();
    }

    NJson::TJsonMap MonAttributes() const override {
        return {
            {"Parent", PrintActorIdAttr(NKikimrServices::TActivity::SCHEME_BOARD_POPULATOR_ACTOR, Parent)},
            {"Replica", PrintActorIdAttr(NKikimrServices::TActivity::SCHEME_BOARD_REPLICA_ACTOR, Replica)},
            {"Owner", Owner},
            {"Generation", Generation},
        };
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_REPLICA_POPULATOR_ACTOR;
    }

    explicit TReplicaPopulator(
            const TActorId& parent,
            const TActorId& replica,
            const ui64 owner,
            const ui64 generation)
        : Parent(parent)
        , Replica(replica)
        , Owner(owner)
        , Generation(generation)
        , BatchSize(0)
    {
    }

    void Bootstrap() {
        TMonitorableActor::Bootstrap();

        auto handshake = MakeHolder<TSchemeBoardEvents::TEvHandshakeRequest>(Owner, Generation);
        Send(Replica, std::move(handshake), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession);
        Become(&TThis::StateHandshake);
    }

    STATEFN(StateHandshake) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardEvents::TEvHandshakeResponse, Handle);
            hFunc(TSchemeBoardEvents::TEvUpdate, EnqueueUpdate);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);

            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, SomeSleep);

            cFunc(TEvents::TEvUndelivered::EventType, SomeSleep);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardEvents::TEvDescribeResult, Handle);
            hFunc(TSchemeBoardEvents::TEvUpdate, Handle);
            hFunc(TSchemeBoardEvents::TEvUpdateAck, Handle);
            hFunc(TSchemeBoardEvents::TEvCommitResponse, Handle);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);

            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, SomeSleep);

            cFunc(TEvents::TEvUndelivered::EventType, SomeSleep);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateSleep) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardEvents::TEvUpdate, EnqueueUpdate);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);

            cFunc(TEvents::TEvWakeup::EventType, Bootstrap);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateCalm) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);

            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    const TActorId Parent;
    const TActorId Replica;
    const ui64 Owner;
    const ui64 Generation;

    // TPathId: [version; txId]
    THashMap<TPathId, TSet<std::pair<ui64, ui64>>> Updates;
    THashMap<TPathId, TSet<std::pair<ui64, ui64>>> UpdatesRequested;
    THashMap<TPathId, TSet<std::pair<ui64, ui64>>> UpdatesInFlight;

    // used during sync
    TPathId CurPathId;
    TPathId LastAckedPathId;
    ui32 BatchSize;
    static constexpr ui32 BatchSizeLimit = 100;

}; // TReplicaPopulator

class TPopulator: public TMonitorableActor<TPopulator> {
    TConstArrayRef<TActorId> SelectReplicas(TPathId pathId, TStringBuf path) {
        SelectionReplicaCache.clear();

        const ui64 pathHash = CityHash64(path);
        const ui64 idHash = pathId.Hash();

        TStateStorageInfo::TSelection selection;

        GroupInfo->SelectReplicas(pathHash, &selection);
        SelectionReplicaCache.insert(SelectionReplicaCache.end(), selection.begin(), selection.end());

        GroupInfo->SelectReplicas(idHash, &selection);
        for (const TActorId& replica : selection) {
            if (Find(SelectionReplicaCache, replica) == SelectionReplicaCache.end()) {
                SelectionReplicaCache.emplace_back(replica);
            }
        }

        if (SelectionReplicaCache) {
            return TConstArrayRef<TActorId>(&SelectionReplicaCache.front(), SelectionReplicaCache.size());
        } else {
            return TConstArrayRef<TActorId>();
        }
    }

    void Update(const TPathId pathId, const bool isDeletion, const ui64 cookie) {
        auto it = Descriptions.find(pathId);
        Y_VERIFY(it != Descriptions.end());

        const auto& record = it->second.Record;

        TConstArrayRef<TActorId> replicas = SelectReplicas(pathId, record.GetPath());
        for (const auto& replica : replicas) {
            const TActorId* replicaPopulator = ReplicaToReplicaPopulator.FindPtr(replica);
            Y_VERIFY(replicaPopulator != nullptr);

            auto update = MakeHolder<TSchemeBoardEvents::TEvUpdateBuilder>(Owner, Generation, record, isDeletion);
            if (!isDeletion) {
                update->SetDescribeSchemeResult(it->second);
            }
            update->Record.SetNeedAck(true);

            Send(*replicaPopulator, std::move(update), 0, cookie);
        }
    }

    void Handle(TSchemeBoardEvents::TEvRequestDescribe::TPtr& ev) {
        SBP_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        const TActorId replicaPopulator = ev->Sender;
        const TActorId replica = ev->Get()->Replica;

        if (ReplicaToReplicaPopulator[replica] != replicaPopulator) {
            SBP_LOG_CRIT("Inconsistent replica populator"
                << ": replica# " << replica
                << ", replicaPopulator# " << replicaPopulator);
            return;
        }

        if (Descriptions.empty()) {
            Send(replicaPopulator, new TSchemeBoardEvents::TEvDescribeResult(true));
            return;
        }

        TPathId startPathId = ev->Get()->PathId;
        auto it = Descriptions.end();

        if (ev->Get()->PathId) {
            startPathId = ev->Get()->PathId;
            it = Descriptions.lower_bound(startPathId);
        } else {
            it = Descriptions.begin();
            startPathId = it->first;
        }

        while (it != Descriptions.end()) { // skip irrelevant to the replica
            if (it->second.Record.GetStatus() == NKikimrScheme::StatusPathDoesNotExist) {
                // KIKIMR-13173
                // it is assumed that not deleted pathes present in Descriptions
                // but it might be, since we have the difference at path description and init population
                // and it is in that path description consider path deteleted even when it only planned to delete (for BSV and PQ)
                // mean while init population consider path deteleted consider path deteleted only when it is dpropped
                // globally path description should do the same thing, we are correcting it
                ++it;
                continue;
            }

            TPathId pathId(it->second.Record.GetPathOwnerId(), it->second.Record.GetPathId());
            TConstArrayRef<TActorId> replicas = SelectReplicas(pathId, it->second.Record.GetPath());
            if (Find(replicas, replica) != replicas.end()) {
                break;
            }
            ++it;
        }

        if (it == Descriptions.end()) {
            if (startPathId >= MaxPathId) {
                Send(replicaPopulator, new TSchemeBoardEvents::TEvDescribeResult(true));
                return;
            }

            if (startPathId.OwnerId == Owner) {
                Send(replicaPopulator, new TSchemeBoardEvents::TEvDescribeResult(startPathId.LocalPathId, MaxPathId.LocalPathId));
            } else {
                Send(replicaPopulator, new TSchemeBoardEvents::TEvDescribeResult(1, MaxPathId.LocalPathId));
            }
            return;
        }

        const auto& description = it->second;
        const auto& record = description.Record;
        auto pathId = TPathId(it->second.Record.GetPathOwnerId(), it->second.Record.GetPathId());

        if (pathId.OwnerId != Owner) {
            // this is an alien migrated migrated path from another owner, push it as a dot
            Send(replicaPopulator, new TSchemeBoardEvents::TEvDescribeResult(0, 0, description));
            return;
        }

        TLocalPathId deletedBegin = startPathId.LocalPathId;
        TLocalPathId deletedEnd = record.GetPathId() - 1;

        if (startPathId.OwnerId != Owner) {
            deletedBegin = 1;
        }

        if (deletedEnd <= deletedBegin) {
            // if pushing migrated pathes hasn'n finished jet, we do not set up deteled ranges between them
            deletedBegin = 0;
            deletedEnd = 0;
        }

        if (record.GetStatus() == NKikimrScheme::EStatus::StatusRedirectDomain) {
            // this path has been migrated to another owner
            Send(replicaPopulator, new TSchemeBoardEvents::TEvDescribeResult(deletedBegin, deletedEnd, it->first.LocalPathId));
            return;
        }

        Send(replicaPopulator, new TSchemeBoardEvents::TEvDescribeResult(deletedBegin, deletedEnd, description));
    }

    void Handle(TSchemeBoardEvents::TEvRequestUpdate::TPtr& ev) {
        SBP_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        const TPathId pathId = ev->Get()->PathId;
        THolder<TSchemeBoardEvents::TEvUpdateBuilder> update;

        auto it = Descriptions.find(pathId);
        if (it == Descriptions.end()) {
            update = MakeHolder<TSchemeBoardEvents::TEvUpdateBuilder>(Owner, Generation, pathId);
        } else {
            update = MakeHolder<TSchemeBoardEvents::TEvUpdateBuilder>(Owner, Generation, it->second.Record);
            update->SetDescribeSchemeResult(it->second);
        }

        update->Record.SetNeedAck(true);
        Send(ev->Sender, std::move(update));
    }

    void DelayUpdate(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        SBP_LOG_D("DelayUpdate " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie);

        DelayedUpdates.emplace_back(ev.Release());
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr& ev) {
        SBP_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie);

        auto* msg = static_cast<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder*>(ev->Get());
        auto& record = msg->Record;

        if (!record.HasStatus()) {
            SBP_LOG_E("Description without status");
            return;
        }

        const TPathId pathId = GetPathId(record);
        const bool isDeletion = record.GetStatus() == NKikimrScheme::StatusPathDoesNotExist;
        const ui64 version = isDeletion ? Max<ui64>() : GetPathVersion(record);

        SBP_LOG_N("Update description"
            << ": owner# " << Owner
            << ", pathId# " << pathId
            << ", cookie# " << ev->Cookie
            << ", is deletion# " << (isDeletion ? "true" : "false"));

        if (isDeletion) {
            if (!Descriptions.contains(pathId)) {
                SBP_LOG_N("Immediate ack for deleted path"
                    << ": sender# " << ev->Sender
                    << ", cookie# " << ev->Cookie
                    << ", pathId# " << pathId);

                auto ack = MakeHolder<TSchemeBoardEvents::TEvUpdateAck>(Owner, Generation, pathId, Max<ui64>());
                Send(ev->Sender, std::move(ack), 0, ev->Cookie);
                return;
            }
        } else {
            Descriptions[pathId] = TTwoPartDescription(std::move(msg->PreSerializedData), std::move(record));
            MaxPathId = Max(MaxPathId, pathId.NextId());
        }

        auto it = UpdateAcks.find(ev->Cookie);
        if (it == UpdateAcks.end()) {
            it = UpdateAcks.emplace(ev->Cookie, TUpdateAckInfo{ev->Sender, {}}).first;
        }

        it->second.AckTo = ev->Sender;
        it->second.PathAcks.emplace(std::make_pair(pathId, version), 0);

        Update(pathId, isDeletion, ev->Cookie);

        if (isDeletion) {
            Descriptions.erase(pathId);
        }
    }

    void Handle(TSchemeBoardEvents::TEvUpdateAck::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        SBP_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie);

        auto it = UpdateAcks.find(ev->Cookie);
        if (it == UpdateAcks.end()) {
            SBP_LOG_D("Ack for unknown update (already acked?)"
                << ": sender# " << ev->Sender
                << ", cookie# " << ev->Cookie);
            return;
        }

        const TPathId pathId = ev->Get()->GetPathId();
        const ui64 version = record.GetVersion();

        auto pathIt = it->second.PathAcks.lower_bound({pathId, 0});
        while (pathIt != it->second.PathAcks.end()
               && pathIt->first.first == pathId
               && pathIt->first.second <= version) {
            if (++pathIt->second > (GroupInfo->NToSelect / 2)) {
                SBP_LOG_N("Ack update"
                    << ": ack to# " << it->second.AckTo
                    << ", cookie# " << ev->Cookie
                    << ", pathId# " << pathId
                    << ", version# " << pathIt->first.second);

                auto ack = MakeHolder<TSchemeBoardEvents::TEvUpdateAck>(Owner, Generation, pathId, pathIt->first.second);
                Send(it->second.AckTo, std::move(ack), 0, ev->Cookie);

                auto eraseIt = pathIt;
                ++pathIt;
                it->second.PathAcks.erase(eraseIt);

                if (it->second.PathAcks.empty()) {
                    UpdateAcks.erase(it);
                    break;
                }
            } else {
                ++pathIt;
            }
        }
    }

    void Handle(TEvStateStorage::TEvListSchemeBoardResult::TPtr& ev) {
        SBP_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        const auto& info = ev->Get()->Info;

        if (!info) {
            SBP_LOG_E("Publish on unconfigured SchemeBoard"
                << ": StateStorage group# " << StateStorageGroup);
            Become(&TThis::StateCalm);
            return;
        }

        GroupInfo = info;
        for (auto& replica : info->SelectAllReplicas()) {
            IActor* replicaPopulator = new TReplicaPopulator(SelfId(), replica, Owner, Generation);
            ReplicaToReplicaPopulator.emplace(replica, Register(replicaPopulator, TMailboxType::ReadAsFilled));
        }

        Become(&TThis::StateWork);
        ReplayUpdates(DelayedUpdates);
    }

    void Handle(TSchemeBoardMonEvents::TEvInfoRequest::TPtr& ev) {
        const auto limit = ev->Get()->Record.GetLimitRepeatedFields();

        auto response = MakeHolder<TSchemeBoardMonEvents::TEvInfoResponse>(SelfId(), ActorActivityType());
        auto& record = *response->Record.MutablePopulatorResponse();

        record.SetOwner(Owner);
        record.SetGeneration(Generation);
        record.SetDescriptionsCount(Descriptions.size());

        record.MutableMaxPathId()->SetOwnerId(MaxPathId.OwnerId);
        record.MutableMaxPathId()->SetLocalPathId(MaxPathId.LocalPathId);

        record.SetDelayedUpdatesCount(DelayedUpdates.size());

        for (const auto& [_, replicaPopulator] : ReplicaToReplicaPopulator) {
            ActorIdToProto(replicaPopulator, record.MutableReplicaPopulators()->Add());
        }

        for (const auto& [cookie, info] : UpdateAcks) {
            auto& updateAck = *record.AddUpdateAcks();

            updateAck.SetCookie(cookie);
            ActorIdToProto(info.AckTo, updateAck.MutableAckTo());

            for (const auto& [pathIdVersion, acksCount] : info.PathAcks) {
                auto& pathAck = *updateAck.AddPathAcks();

                pathAck.MutablePathId()->SetOwnerId(pathIdVersion.first.OwnerId);
                pathAck.MutablePathId()->SetLocalPathId(pathIdVersion.first.LocalPathId);

                pathAck.SetVersion(pathIdVersion.second);
                pathAck.SetAcksCount(acksCount);
            }

            if (record.UpdateAcksSize() >= limit) {
                response->SetTruncated();
                break;
            }
        }

        Send(ev->Sender, std::move(response), 0, ev->Cookie);
    }

    void Handle(TSchemeBoardMonEvents::TEvDescribeRequest::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TTwoPartDescription* desc = nullptr;
        if (record.HasPathId()) {
            desc = Descriptions.FindPtr(TPathId(record.GetPathId().GetOwnerId(), record.GetPathId().GetLocalPathId()));
        }

        TString json;
        if (desc) {
            NKikimrScheme::TEvDescribeSchemeResult fullProto;
            Y_PROTOBUF_SUPPRESS_NODISCARD fullProto.ParseFromString(desc->PreSerialized);
            fullProto.MergeFrom(desc->Record);

            using namespace google::protobuf::util;

            JsonPrintOptions opts;
            opts.preserve_proto_field_names = true;
            MessageToJsonString(fullProto, &json, opts);
        } else {
            json = "{}";
        }

        Send(ev->Sender, new TSchemeBoardMonEvents::TEvDescribeResponse(json), 0, ev->Cookie);
    }

    void HandleUndelivered() {
        SBP_LOG_E("Publish on unavailable SchemeBoard"
            << ": StateStorage group# " << StateStorageGroup);
        Become(&TThis::StateCalm);
    }

    void PassAway() override {
        for (const auto& x : ReplicaToReplicaPopulator) {
            Send(x.second, new TEvents::TEvPoisonPill());
        }

        TMonitorableActor::PassAway();
    }

    NJson::TJsonMap MonAttributes() const override {
        return {
            {"Owner", Owner},
            {"Generation", Generation},
        };
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_POPULATOR_ACTOR;
    }

    explicit TPopulator(
            const ui64 owner,
            const ui64 generation,
            const ui32 ssId,
            TMap<TPathId, TTwoPartDescription> descriptions,
            const ui64 maxPathId)
        : Owner(owner)
        , Generation(generation)
        , StateStorageGroup(ssId)
        , Descriptions(std::move(descriptions))
        , MaxPathId(TPathId(owner, maxPathId))
    {
    }

    void Bootstrap() {
        TMonitorableActor::Bootstrap();

        const TActorId proxy = MakeStateStorageProxyID(StateStorageGroup);
        Send(proxy, new TEvStateStorage::TEvListSchemeBoard(), IEventHandle::FlagTrackDelivery);
        Become(&TThis::StateResolve);
    }

    STATEFN(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvListSchemeBoardResult, Handle);

            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, DelayUpdate);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);
            hFunc(TSchemeBoardMonEvents::TEvDescribeRequest, Handle);

            cFunc(TEvents::TEvUndelivered::EventType, HandleUndelivered);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardEvents::TEvRequestDescribe, Handle);
            hFunc(TSchemeBoardEvents::TEvRequestUpdate, Handle);

            hFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            hFunc(TSchemeBoardEvents::TEvUpdateAck, Handle);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);
            hFunc(TSchemeBoardMonEvents::TEvDescribeRequest, Handle);

            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateCalm) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);
            hFunc(TSchemeBoardMonEvents::TEvDescribeRequest, Handle);

            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    const ui64 Owner;
    const ui64 Generation;
    const ui64 StateStorageGroup;

    TMap<TPathId, TTwoPartDescription> Descriptions;
    TPathId MaxPathId;

    TDelayedUpdates DelayedUpdates;

    TIntrusiveConstPtr<TStateStorageInfo> GroupInfo;
    THashMap<TActorId, TActorId> ReplicaToReplicaPopulator;

    TVector<TActorId> SelectionReplicaCache;

    struct TUpdateAckInfo {
        TActorId AckTo;
        TMap<std::pair<TPathId, ui64>, ui32> PathAcks;
    };

    THashMap<ui64, TUpdateAckInfo> UpdateAcks; // ui64 is a cookie

}; // TPopulator

} // NSchemeBoard

IActor* CreateSchemeBoardPopulator(
    const ui64 owner,
    const ui64 generation,
    const ui32 ssId,
    TMap<TPathId, NSchemeBoard::TTwoPartDescription> descriptions,
    const ui64 maxPathId
) {
    return new NSchemeBoard::TPopulator(owner, generation, ssId, std::move(descriptions), maxPathId);
}

} // NKikimr
