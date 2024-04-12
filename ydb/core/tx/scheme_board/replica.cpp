#include "double_indexed.h"
#include "events.h"
#include "events_internal.h"
#include "events_schemeshard.h"
#include "helpers.h"
#include "monitorable_actor.h"
#include "opaque_path_description.h"
#include "replica.h"

#include <ydb/core/scheme/scheme_pathid.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/util/memory_track.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/variant.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NSchemeBoard {

#define SBR_LOG_T(stream) SB_LOG_T(SCHEME_BOARD_REPLICA, "" << SelfId() << " " << stream)
#define SBR_LOG_D(stream) SB_LOG_D(SCHEME_BOARD_REPLICA, "" << SelfId() << " " << stream)
#define SBR_LOG_I(stream) SB_LOG_I(SCHEME_BOARD_REPLICA, "" << SelfId() << " " << stream)
#define SBR_LOG_N(stream) SB_LOG_N(SCHEME_BOARD_REPLICA, "" << SelfId() << " " << stream)
#define SBR_LOG_E(stream) SB_LOG_E(SCHEME_BOARD_REPLICA, "" << SelfId() << " " << stream)

class TReplica: public TMonitorableActor<TReplica> {
    using TDescribeSchemeResult = NKikimrScheme::TEvDescribeSchemeResult;
    using TCapabilities = NKikimrSchemeBoard::TEvSubscribe::TCapabilities;

    struct TEvPrivate {
        enum EEv {
            EvSendStrongNotifications = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),

            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvSendStrongNotifications: public TEventLocal<TEvSendStrongNotifications, EvSendStrongNotifications> {
            static constexpr ui32 BatchSize = 1000;
            const ui64 Owner;

            explicit TEvSendStrongNotifications(ui64 owner)
                : Owner(owner)
            {
            }

            TString ToString() const override {
                return TStringBuilder() << ToStringHeader() << " {"
                    << " Owner: " << Owner
                << " }";
            }
        };
    };

public:
    enum ESubscriptionType {
        SUBSCRIPTION_UNSPECIFIED, // for filtration
        SUBSCRIPTION_BY_PATH,
        SUBSCRIPTION_BY_PATH_ID,
    };

private:
    class TSubscriberInfo {
    public:
        explicit TSubscriberInfo(ESubscriptionType type, ui64 domainOwnerId, const TCapabilities& capabilities)
            : Type(type)
            , DomainOwnerId(domainOwnerId)
            , Capabilities(capabilities)
            , WaitForAck(false)
            , LastVersionSent(0)
            , NotifiedStrongly(true)
            , SyncRequestCookie(0)
            , SyncResponseCookie(0)
        {
        }

        ESubscriptionType GetType() const {
            return Type;
        }

        ui64 GetDomainOwnerId() const {
            return DomainOwnerId;
        }

        const TCapabilities& GetCapabilities() const {
            return Capabilities;
        }

        bool EnqueueVersion(ui64 version, bool strong) {
            if (!Capabilities.GetAckNotifications()) {
                NotifiedStrongly = strong;
                return true;
            }

            if (WaitForAck) {
                return false;
            }

            WaitForAck = true;
            LastVersionSent = version;
            NotifiedStrongly = strong;
            return true;
        }

        bool EnqueueVersion(const NInternalEvents::TEvNotifyBuilder* notify) {
            const auto& record = notify->Record;
            return EnqueueVersion(record.GetVersion(), record.GetStrong());
        }

        bool AckVersion(ui64 version) {
            if (LastVersionSent > version) {
                return false;
            }

            WaitForAck = false;
            return true;
        }

        bool IsWaitForAck() const {
            return WaitForAck;
        }

        void NeedStrongNotification() {
            NotifiedStrongly = false;
        }

        bool IsNotifiedStrongly() const {
            return NotifiedStrongly;
        }

        bool EnqueueSyncRequest(ui64 cookie) {
            if (cookie <= SyncRequestCookie) {
                return false;
            }

            SyncRequestCookie = cookie;
            return true;
        }

        TMaybe<ui64> ProcessSyncRequest() {
            if (SyncRequestCookie == SyncResponseCookie) {
                return Nothing();
            }

            SyncResponseCookie = SyncRequestCookie;
            return SyncResponseCookie;
        }

    private:
        const ESubscriptionType Type;
        const ui64 DomainOwnerId;
        const TCapabilities Capabilities;

        bool WaitForAck;
        ui64 LastVersionSent;
        bool NotifiedStrongly;

        ui64 SyncRequestCookie;
        ui64 SyncResponseCookie;
    };

public:
    class TDescription {
        static constexpr char MemoryLabelDescribeResult[] = "SchemeBoard/Replica/DescribeSchemeResult";

        void Notify() {
            if (!Subscribers) {
                return;
            }

            auto notify = BuildNotify();
            TVector<const TActorId*> subscribers(Reserve(Subscribers.size()));

            for (auto& [subscriber, info] : Subscribers) {
                if (!info.EnqueueVersion(notify.Get())) {
                    continue;
                }

                subscribers.push_back(&subscriber);
            }

            MultiSend(subscribers, Owner->SelfId(), std::move(notify));
        }

        void TrackMemory() const {
            NActors::NMemory::TLabel<MemoryLabelDescribeResult>::Add(
                PathDescription.DescribeSchemeResultSerialized.size()
            );
        }

        void UntrackMemory() const {
            NActors::NMemory::TLabel<MemoryLabelDescribeResult>::Sub(
                PathDescription.DescribeSchemeResultSerialized.size()
            );
        }

        void Move(TDescription&& other) {
            UntrackMemory();
            other.UntrackMemory();

            Owner = other.Owner;
            Path = std::move(other.Path);
            PathId = std::move(other.PathId);
            PathDescription = std::move(other.PathDescription);
            ExplicitlyDeleted = other.ExplicitlyDeleted;
            Subscribers = std::move(other.Subscribers);

            TrackNotify = other.TrackNotify;

            TrackMemory();
            other.TrackMemory();
        }

        auto SelfId() const {
            return Owner->SelfId();
        }

    public:
        explicit TDescription(TReplica* owner, const TString& path)
            : Owner(owner)
            , Path(path)
        {
            TrackMemory();
        }

        explicit TDescription(TReplica* owner, const TPathId& pathId)
            : Owner(owner)
            , PathId(pathId)
        {
            TrackMemory();
        }

        explicit TDescription(TReplica* owner, const TString& path, const TPathId& pathId)
            : Owner(owner)
            , Path(path)
            , PathId(pathId)
        {
            TrackMemory();
        }

        explicit TDescription(
                TReplica* owner,
                const TString& path,
                const TPathId& pathId,
                TOpaquePathDescription&& pathDescription)
            : Owner(owner)
            , Path(path)
            , PathId(pathId)
            , PathDescription(std::move(pathDescription))
        {
            TrackMemory();
        }

        TDescription(TDescription&& other) {
            TrackMemory();
            Move(std::move(other));
        }

        TDescription& operator=(TDescription&& other) {
            Move(std::move(other));
            return *this;
        }

        TDescription(const TDescription& other) = delete;
        TDescription& operator=(const TDescription& other) = delete;

        ~TDescription() {
            UntrackMemory();
        }

        bool operator<(const TDescription& other) const {
            return GetVersion() < other.GetVersion();
        }

        bool operator>(const TDescription& other) const {
            return other < *this;
        }

        TDescription& Merge(TDescription&& other) noexcept {
            Y_ABORT_UNLESS(Owner == other.Owner);

            if (!Path) {
                Path = other.Path;
            }

            Y_VERIFY_S(!other.Path || Path == other.Path, "Descriptions"
                << ": self# " << ToString()
                << ", other# " << other.ToString());

            if (!PathId) {
                PathId = other.PathId;
            }

            Y_VERIFY_S(!other.PathId || PathId == other.PathId, "Descriptions"
                << ": self# " << ToString()
                << ", other# " << other.ToString());

            SBR_LOG_T("Merge descriptions"
                << ": self# " << ToLogString()
                << ", other# " << other.ToLogString());

            UntrackMemory();
            other.UntrackMemory();
            TrackNotify = false;
            other.TrackNotify = false;

            if (*this > other) {
                // this desc is newer then the other
                std::swap(other.PathDescription, PathDescription);
                other.ExplicitlyDeleted = ExplicitlyDeleted;
                other.Notify();
                std::swap(PathDescription, other.PathDescription);
            } else if (*this < other) {
                // this desc is older then the other
                PathDescription = std::move(other.PathDescription);
                ExplicitlyDeleted = other.ExplicitlyDeleted;
                Notify();
            }

            TrackNotify = true;
            other.TrackNotify = true;
            TrackMemory();
            other.TrackMemory();

            Subscribers.insert(other.Subscribers.begin(), other.Subscribers.end());

            return *this;
        }

        TString GetDescribeSchemeResultSerialized() const {
            return PathDescription.DescribeSchemeResultSerialized;
        }

        TString ToString() const {
            return TStringBuilder() << "{"
                << " Path# " << Path
                << " PathId# " << PathId
                << " PathDescription# " << PathDescription.ToString()
                << " ExplicitlyDeleted# " << (ExplicitlyDeleted ? "true" : "false")
            << " }";
        }

        TString ToLogString() const {
            return TStringBuilder() << "{"
                << " Path# " << Path
                << " PathId# " << PathId
                << " Version# " << GetVersion()
                << " ExplicitlyDeleted# " << (ExplicitlyDeleted ? "true" : "false")
            << " }";
        }

        const TString& GetPath() const {
            return Path;
        }

        const TPathId& GetPathId() const {
            return PathId;
        }

        bool IsExplicitlyDeleted() const {
            return ExplicitlyDeleted;
        }

        ui64 GetVersion() const {
            if (ExplicitlyDeleted) {
                return Max<ui64>();
            }
            return PathDescription.PathVersion;
        }

        TDomainId GetDomainId() const {
            if (IsEmpty()) {
                return TDomainId();
            }
            return PathDescription.SubdomainPathId;
        }

        TSet<ui64> GetAbandonedSchemeShardIds() const {
            if (IsEmpty()) {
                return TSet<ui64>();
            }
            return PathDescription.PathAbandonedTenantsSchemeShards;
        }

        bool IsEmpty() const {
            return PathDescription.IsEmpty();
        }

        void Clear() {
            ExplicitlyDeleted = true;
            UntrackMemory();
            {
                TOpaquePathDescription empty;
                std::swap(PathDescription, empty);
            }
            TrackMemory();
            Notify();
        }

        THolder<NInternalEvents::TEvNotifyBuilder> BuildNotify(bool forceStrong = false) const {
            THolder<NInternalEvents::TEvNotifyBuilder> notify;

            const bool isDeletion = IsEmpty();

            if (!PathId) {
                Y_ABORT_UNLESS(isDeletion);
                notify = MakeHolder<NInternalEvents::TEvNotifyBuilder>(Path, isDeletion);
            } else if (!Path) {
                Y_ABORT_UNLESS(isDeletion);
                notify = MakeHolder<NInternalEvents::TEvNotifyBuilder>(PathId, isDeletion);
            } else {
                notify = MakeHolder<NInternalEvents::TEvNotifyBuilder>(Path, PathId, isDeletion);
            }

            if (!isDeletion) {
                notify->SetPathDescription(PathDescription);
                if (TrackNotify) {
                    TrackMemory();
                }
            }

            notify->Record.SetVersion(GetVersion());

            if (!IsEmpty() || IsExplicitlyDeleted() || forceStrong) {
                notify->Record.SetStrong(true);
            }

            return notify;
        }

        void Subscribe(const TActorId& subscriber, const TString&, ui64 domainOwnerId, const TCapabilities& capabilities) {
            Subscribers.emplace(subscriber, TSubscriberInfo(SUBSCRIPTION_BY_PATH, domainOwnerId, capabilities));
        }

        void Subscribe(const TActorId& subscriber, const TPathId&, ui64 domainOwnerId, const TCapabilities& capabilities) {
            Subscribers.emplace(subscriber, TSubscriberInfo(SUBSCRIPTION_BY_PATH_ID, domainOwnerId, capabilities));
        }

        void Unsubscribe(const TActorId& subscriber) {
            Subscribers.erase(subscriber);
        }

        TSubscriberInfo& GetSubscriberInfo(const TActorId& subscriber) {
            auto it = Subscribers.find(subscriber);
            Y_ABORT_UNLESS(it != Subscribers.end());
            return it->second;
        }

        THashMap<TActorId, TSubscriberInfo> GetSubscribers(const ESubscriptionType type = SUBSCRIPTION_UNSPECIFIED) const {
            THashMap<TActorId, TSubscriberInfo> result;

            for (const auto& [subscriber, info] : Subscribers) {
                if (type == SUBSCRIPTION_UNSPECIFIED || type == info.GetType()) {
                    result.emplace(subscriber, info);
                }
            }

            return result;
        }

    private:
        // used to notifications
        TReplica* Owner;

        // data
        TString Path;
        TPathId PathId;
        TOpaquePathDescription PathDescription;

        bool ExplicitlyDeleted = false;

        // subscribers
        THashMap<TActorId, TSubscriberInfo> Subscribers;

        // memory tracking
        bool TrackNotify = true;

    }; // TDescription

    struct TMerger {
        TDescription& operator()(TDescription& dst, TDescription&& src) {
            return dst.Merge(std::move(src));
        }
    };

private:
    struct TPopulatorInfo {
        ui64 Generation = 0;
        ui64 PendingGeneration = 0;
        bool IsCommited = false;
        TActorId PopulatorActor;
    };

    bool IsPopulatorCommited(ui64 ownerId) const {
        auto it = Populators.find(ownerId);
        if (it != Populators.end() && it->second.IsCommited) {
            return true;
        }

        return false;
    }

    // register empty entry by path OR pathId
    template <typename TPath>
    TDescription& UpsertDescription(const TPath& path) {
        SBR_LOG_I("Upsert description"
            << ": path# " << path);

        return Descriptions.Upsert(path, TDescription(this, path));
    }

    // register empty entry by path AND pathId both
    TDescription& UpsertDescription(const TString& path, const TPathId& pathId) {
        SBR_LOG_I("Upsert description"
            << ": path# " << path
            << ", pathId# " << pathId);

        return Descriptions.Upsert(path, pathId, TDescription(this, path, pathId));
    }

    // upsert description only by pathId
    TDescription& UpsertDescriptionByPathId(const TString& path, const TPathId& pathId, TOpaquePathDescription&& pathDescription) {
        SBR_LOG_I("Upsert description"
            << ": pathId# " << pathId
            << ", pathDescription# " << pathDescription.ToString()
        );

        return Descriptions.Upsert(pathId, TDescription(this, path, pathId, std::move(pathDescription)));
    }

    // upsert description by path AND pathId both
    TDescription& UpsertDescription(const TString& path, const TPathId& pathId, TOpaquePathDescription&& pathDescription) {
        SBR_LOG_I("Upsert description"
            << ": path# " << path
            << ", pathId# " << pathId
            << ", pathDescription# " << pathDescription.ToString()
        );

        return Descriptions.Upsert(path, pathId, TDescription(this, path, pathId, std::move(pathDescription)));
    }

    void SoftDeleteDescription(const TPathId& pathId, bool createIfNotExists = false) {
        TDescription* desc = Descriptions.FindPtr(pathId);

        if (!desc) {
            if (createIfNotExists) {
                desc = &UpsertDescription(pathId);
                desc->Clear(); // mark as deleted
            }

            return;
        }

        if (desc->IsEmpty()) {
            return;
        }

        auto path = desc->GetPath();

        SBR_LOG_I("Delete description"
            << ": path# " << path
            << ", pathId# " << pathId);

        if (TDescription* descByPath = Descriptions.FindPtr(path)) {
            if (descByPath != desc && !descByPath->IsEmpty()) {
                if (descByPath->GetPathId().OwnerId != pathId.OwnerId) {
                    auto curPathId = descByPath->GetPathId();
                    auto curDomainId = descByPath->GetDomainId();
                    auto domainId = desc->GetDomainId();

                    if (curDomainId == pathId) { // Deletion from GSS
                        SBR_LOG_N("Delete description by GSS"
                            << ": path# " << path
                            << ", pathId# " << pathId
                            << ", domainId# " << domainId
                            << ", curPathId# " << curPathId
                            << ", curDomainId# " << curDomainId);

                        Descriptions.DeleteIndex(path);
                        UpsertDescription(path, pathId);
                        RelinkSubscribers(descByPath, path);

                        descByPath->Clear();
                    }
                }
            }
        }

        desc->Clear();
    }

    void RelinkSubscribers(TDescription* fromDesc, const TString& path) {
        for (const auto& [subscriber, info] : fromDesc->GetSubscribers(SUBSCRIPTION_BY_PATH)) {
            fromDesc->Unsubscribe(subscriber);
            Subscribers.erase(subscriber);
            SubscribeBy(subscriber, path, info.GetDomainOwnerId(), info.GetCapabilities(), false);
        }
    }

    void SoftDeleteDescriptions(const TPathId& begin, const TPathId& end) {
        const auto& pathIdIndex = Descriptions.GetSecondaryIndex();

        auto it = pathIdIndex.lower_bound(begin);
        if (it == pathIdIndex.end()) {
            return;
        }

        const auto endIt = pathIdIndex.upper_bound(end);
        while (it != endIt) {
            SoftDeleteDescription(it->first);
            ++it;
        }
    }

    // call it _after_ Subscribe() & _before_ Unsubscribe()
    bool IsSingleSubscriberOnNode(const TActorId& subscriber) const {
        const ui32 nodeId = subscriber.NodeId();
        auto it = Subscribers.lower_bound(TActorId(nodeId, 0, 0, 0));
        Y_ABORT_UNLESS(it != Subscribers.end());

        return ++it == Subscribers.end() || it->first.NodeId() != nodeId;
    }

    template <typename TPath>
    void Subscribe(const TActorId& subscriber, const TPath& path, ui64 domainOwnerId, const TCapabilities& capabilities) {
        TDescription* desc = Descriptions.FindPtr(path);
        Y_ABORT_UNLESS(desc);

        SBR_LOG_I("Subscribe"
            << ": subscriber# " << subscriber
            << ", path# " << path
            << ", domainOwnerId# " << domainOwnerId
            << ", capabilities# " << capabilities.ShortDebugString());

        desc->Subscribe(subscriber, path, domainOwnerId, capabilities);

        auto it = Subscribers.find(subscriber);
        Y_DEBUG_ABORT_UNLESS(it == Subscribers.end() || std::holds_alternative<TPath>(it->second) && std::get<TPath>(it->second) == path);
        Subscribers.emplace(subscriber, path);
    }

    template <typename TPath>
    void Unsubscribe(const TActorId& subscriber, const TPath& path) {
        TDescription* desc = Descriptions.FindPtr(path);
        Y_ABORT_UNLESS(desc);

        SBR_LOG_I("Unsubscribe"
            << ": subscriber# " << subscriber
            << ", path# " << path);

        desc->Unsubscribe(subscriber);
        Subscribers.erase(subscriber);
    }

    template <typename TPath>
    void SubscribeBy(const TActorId& subscriber, const TPath& path, ui64 domainOwnerId, const TCapabilities& capabilities,
            bool needNotify = true) {
        TDescription* desc = Descriptions.FindPtr(path);
        if (!desc) {
            desc = &UpsertDescription(path);
        }

        Subscribe(subscriber, path, domainOwnerId, capabilities);

        if (!needNotify) {
            return;
        }

        ui32 flags = 0;
        if (IsSingleSubscriberOnNode(subscriber)) {
            flags = IEventHandle::FlagSubscribeOnSession;
        }

        auto notify = desc->BuildNotify(IsPopulatorCommited(domainOwnerId));

        if (!notify->Record.GetStrong()) {
            auto& info = desc->GetSubscriberInfo(subscriber);
            info.NeedStrongNotification();

            WaitStrongNotifications[domainOwnerId].insert(subscriber);
        }

        Send(subscriber, std::move(notify), flags);
    }

    template <typename TPath>
    void UnsubscribeBy(const TActorId& subscriber, const TPath& path) {
        if (!Descriptions.FindPtr(path) || !Subscribers.contains(subscriber)) {
            return;
        }

        if (IsSingleSubscriberOnNode(subscriber)) {
            Send(MakeInterconnectProxyId(subscriber.NodeId()), new TEvents::TEvUnsubscribe());
        }

        Unsubscribe(subscriber, path);
    }

    template <typename TPath>
    ui64 GetVersion(const TPath& path) const {
        const TDescription* desc = Descriptions.FindPtr(path);
        return desc ? desc->GetVersion() : 0;
    }

    void AckUpdate(NInternalEvents::TEvUpdate::TPtr& ev) {
        const auto& record = ev->Get()->GetRecord();

        const ui64 owner = record.GetOwner();
        const ui64 generation = record.GetGeneration();

        Y_ABORT_UNLESS(Populators.contains(owner));
        Y_ABORT_UNLESS(Populators.at(owner).PendingGeneration == generation);

        if (!record.GetNeedAck()) {
            return;
        }

        TPathId ackPathId;

        if (record.HasDeletedLocalPathIds()) {
            ackPathId = TPathId(owner, record.GetDeletedLocalPathIds().GetEnd());
        }

        if (record.HasLocalPathId()) {
            ackPathId = ev->Get()->GetPathId();
        }

        if (record.HasMigratedLocalPathIds()) {
            ackPathId = TPathId(owner, record.GetMigratedLocalPathIds().GetEnd());
        }

        const ui64 version = GetVersion(ackPathId);
        Send(ev->Sender, new NSchemeshardEvents::TEvUpdateAck(owner, generation, ackPathId, version), 0, ev->Cookie);
    }

    void Handle(NInternalEvents::TEvHandshakeRequest::TPtr& ev) {
        SBR_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        const auto& record = ev->Get()->Record;
        const ui64 owner = record.GetOwner();
        const ui64 generation = record.GetGeneration();

        TPopulatorInfo& info = Populators[owner];
        if (generation < info.PendingGeneration) {
            SBR_LOG_E("Reject handshake from stale populator"
                << ": sender# " << ev->Sender
                << ", owner# " << owner
                << ", generation# " << generation
                << ", pending generation# " << info.PendingGeneration);
            return;
        }

        SBR_LOG_N("Successful handshake"
            << ": owner# " << owner
            << ", generation# " << generation);

        info.PendingGeneration = generation;
        info.PopulatorActor = ev->Sender;

        Send(ev->Sender, new NInternalEvents::TEvHandshakeResponse(owner, info.Generation), 0, ev->Cookie);
    }

    void Handle(NInternalEvents::TEvUpdate::TPtr& ev) {
        SBR_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie
            << ", event size# " << ev->Get()->GetCachedByteSize()
        );

        const TString& path = ev->Get()->GetPath();
        const TPathId& pathId = ev->Get()->GetPathId();

       {
            auto& record = *ev->Get()->MutableRecord();
            const ui64 owner = record.GetOwner();
            const ui64 generation = record.GetGeneration();

            const auto populatorIt = Populators.find(owner);
            if (populatorIt == Populators.end()) {
                SBR_LOG_E("Reject update from unknown populator"
                    << ": sender# " << ev->Sender
                    << ", owner# " << owner
                    << ", generation# " << generation);
                return;
            }
            if (generation != populatorIt->second.PendingGeneration) {
                SBR_LOG_E("Reject update from stale populator"
                    << ": sender# " << ev->Sender
                    << ", owner# " << owner
                    << ", generation# " << generation
                    << ", pending generation# " << populatorIt->second.PendingGeneration);
                return;
            }

            if (record.HasDeletedLocalPathIds()) {
                const TPathId begin(owner, record.GetDeletedLocalPathIds().GetBegin());
                const TPathId end(owner, record.GetDeletedLocalPathIds().GetEnd());
                SoftDeleteDescriptions(begin, end);
            }

            if (!record.HasLocalPathId()) {
                return AckUpdate(ev);
            }

            SBR_LOG_N("Update description"
                << ": path# " << path
                << ", pathId# " << pathId
                << ", deletion# " << (record.GetIsDeletion() ? "true" : "false"));

            if (record.GetIsDeletion()) {
                SoftDeleteDescription(pathId, true);
                return AckUpdate(ev);
            }
       }

        if (TDescription* desc = Descriptions.FindPtr(pathId)) {
            if (desc->IsExplicitlyDeleted()) {
                SBR_LOG_N("Path was explicitly deleted, ignoring"
                    << ": path# " << path
                    << ", pathId# " << pathId);

                return AckUpdate(ev);
            }
        }

        // TEvUpdate is partially consumed here, with DescribeSchemeResult blob being moved out.
        // AckUpdate(ev) calls below are ok, AckUpdate() doesn't use DescribeSchemeResult.
        TOpaquePathDescription pathDescription = ev->Get()->ExtractPathDescription();

        TDescription* desc = Descriptions.FindPtr(path);
        if (!desc) {
            UpsertDescription(path, pathId, std::move(pathDescription));
            return AckUpdate(ev);
        }

        if (!desc->GetPathId()) {
            UpsertDescription(path, pathId, std::move(pathDescription));
            return AckUpdate(ev);
        }

        auto curPathId = desc->GetPathId();

        if (curPathId.OwnerId == pathId.OwnerId || desc->IsEmpty()) {
            if (curPathId > pathId) {
                return AckUpdate(ev);
            }

            if (curPathId < pathId) {
                SoftDeleteDescription(desc->GetPathId());
                Descriptions.DeleteIndex(path);
                RelinkSubscribers(desc, path);
            }

            UpsertDescription(path, pathId, std::move(pathDescription));
            return AckUpdate(ev);
        }

        Y_VERIFY_S(!desc->IsEmpty(), "Description is not filled"
            << ": desc# " << desc->ToLogString());

        auto curDomainId = desc->GetDomainId();
        const auto& domainId = pathDescription.SubdomainPathId;

        auto log = [&](const TString& message) {
            SBR_LOG_N("" << message
                << ": path# " << path
                << ", pathId# " << pathId
                << ", domainId# " << domainId
                << ", curPathId# " << curPathId
                << ", curDomainId# " << curDomainId);
        };

        if (curPathId == domainId) { // Update from TSS, GSS->TSS
            // it is only because we need to manage undo of upgrade subdomain, finally remove it
            const auto& abandonedSchemeShards = desc->GetAbandonedSchemeShardIds();
            if (abandonedSchemeShards.contains(pathId.OwnerId)) { // TSS is ignored, present GSS reverted it
                log("Replace GSS by TSS description is rejected, GSS implicitly knows that TSS has been reverted"
                    ", but still inject description only by pathId for safe");
                UpsertDescriptionByPathId(path, pathId, std::move(pathDescription));
                return AckUpdate(ev);
            }

            log("Replace GSS by TSS description");
            // unlink GSS desc by path
            Descriptions.DeleteIndex(path);
            RelinkSubscribers(desc, path);
            UpsertDescription(path, pathId, std::move(pathDescription));
            return AckUpdate(ev);
        }

        if (curDomainId == pathId) { // Update from GSS, TSS->GSS
            // it is only because we need to manage undo of upgrade subdomain, finally remove it
            const auto& abandonedSchemeShards = pathDescription.PathAbandonedTenantsSchemeShards;
            if (abandonedSchemeShards.contains(curPathId.OwnerId)) { // GSS reverts TSS
                log("Replace TSS by GSS description, TSS was implicitly reverted by GSS");
                // unlink TSS desc by path
                Descriptions.DeleteIndex(path);
                RelinkSubscribers(desc, path);
                UpsertDescription(path, pathId, std::move(pathDescription));
                return AckUpdate(ev);
            }

            log("Inject description only by pathId, it is update from GSS");
            UpsertDescriptionByPathId(path, pathId, std::move(pathDescription));
            return AckUpdate(ev);
        }

        if (curDomainId == domainId) {
            if (curPathId > pathId) {
                log("Totally ignore description, path with obsolete pathId");
                return AckUpdate(ev);
            }

            if (curPathId < pathId) {
                log("Update description by newest path form tenant schemeshard");
                SoftDeleteDescription(desc->GetPathId());
                Descriptions.DeleteIndex(path);
                RelinkSubscribers(desc, path);
            }

            UpsertDescription(path, pathId, std::move(pathDescription));
            return AckUpdate(ev);
        } else if (curDomainId < domainId) {
            log("Update description by newest path with newer domainId");
            Descriptions.DeleteIndex(path);
            RelinkSubscribers(desc, path);
            UpsertDescription(path, pathId, std::move(pathDescription));
            return AckUpdate(ev);
        } else {
            log("Totally ignore description, path with obsolete domainId");
            return AckUpdate(ev);
        }

        Y_FAIL_S("Can't insert old description, no relation between obj"
            << ": path# " << path
            << ", pathId# " << pathId
            << ", domainId# " << domainId
            << ", curPathId# " << curPathId
            << ", curDomainId# " << curDomainId);
    }

    void Handle(NInternalEvents::TEvCommitRequest::TPtr& ev) {
        SBR_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        const auto& record = ev->Get()->Record;
        const ui64 owner = record.GetOwner();
        const ui64 generation = record.GetGeneration();

        auto it = Populators.find(owner);
        if (it == Populators.end()) {
            SBR_LOG_E("Reject commit from unknown populator"
                << ": sender# " << ev->Sender
                << ", owner# " << owner
                << ", generation# " << generation);
            return;
        }

        TPopulatorInfo& info = it->second;
        if (generation != info.PendingGeneration) {
            SBR_LOG_E("Reject commit from stale populator"
                << ": sender# " << ev->Sender
                << ", owner# " << owner
                << ", generation# " << generation
                << ", pending generation# " << info.PendingGeneration);
            return;
        }

        SBR_LOG_N("Commit generation"
            << ": owner# " << owner
            << ", generation# " << generation);

        info.Generation = info.PendingGeneration;
        info.IsCommited = true;
        Send(ev->Sender, new NInternalEvents::TEvCommitResponse(owner, info.Generation), 0, ev->Cookie);

        if (WaitStrongNotifications.contains(owner)) {
            Send(SelfId(), new TEvPrivate::TEvSendStrongNotifications(owner));
        }
    }

    void Handle(TEvPrivate::TEvSendStrongNotifications::TPtr& ev) {
        SBR_LOG_D("Handle " << ev->Get()->ToString());

        const auto owner = ev->Get()->Owner;
        if (!IsPopulatorCommited(owner)) {
            SBR_LOG_N("Populator is not commited"
                << ": owner# " << owner);
            return;
        }

        auto itSubscribers = WaitStrongNotifications.find(owner);
        if (itSubscribers == WaitStrongNotifications.end()) {
            SBR_LOG_E("Invalid owner"
                << ": owner# " << owner);
            return;
        }

        auto& subscribers = itSubscribers->second;
        auto it = subscribers.begin();
        ui32 count = 0;

        const auto limit = ev->Get()->BatchSize;
        while (count++ < limit && it != subscribers.end()) {
            const TActorId subscriber = *it;
            it = subscribers.erase(it);

            auto jt = Subscribers.find(subscriber);
            if (jt == Subscribers.end()) {
                continue;
            }

            TDescription* desc = nullptr;

            if (const TString* path = std::get_if<TString>(&jt->second)) {
                desc = Descriptions.FindPtr(*path);
            } else if (const TPathId* pathId = std::get_if<TPathId>(&jt->second)) {
                desc = Descriptions.FindPtr(*pathId);
            }

            Y_ABORT_UNLESS(desc);
            auto& info = desc->GetSubscriberInfo(subscriber);

            Y_ABORT_UNLESS(info.GetDomainOwnerId() == owner);
            if (info.IsNotifiedStrongly() || info.IsWaitForAck()) {
                continue;
            }

            auto notify = desc->BuildNotify(true);
            info.EnqueueVersion(notify.Get());
            Send(subscriber, std::move(notify));
        }

        if (subscribers) {
            Send(SelfId(), new TEvPrivate::TEvSendStrongNotifications(owner));
        } else {
            WaitStrongNotifications.erase(itSubscribers);
        }
    }

    void Handle(NInternalEvents::TEvSubscribe::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui64 domainOwnerId = record.GetDomainOwnerId();
        const auto& capabilities = record.GetCapabilities();

        SBR_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        if (record.HasPath()) {
            SubscribeBy(ev->Sender, record.GetPath(), domainOwnerId, capabilities);
        } else {
            Y_ABORT_UNLESS(record.HasPathOwnerId() && record.HasLocalPathId());
            SubscribeBy(ev->Sender, TPathId(record.GetPathOwnerId(), record.GetLocalPathId()), domainOwnerId, capabilities);
        }
    }

    void Handle(NInternalEvents::TEvUnsubscribe::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        SBR_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        if (record.HasPath()) {
            UnsubscribeBy(ev->Sender, record.GetPath());
        } else {
            Y_ABORT_UNLESS(record.HasPathOwnerId() && record.HasLocalPathId());
            UnsubscribeBy(ev->Sender, TPathId(record.GetPathOwnerId(), record.GetLocalPathId()));
        }
    }

    void Handle(NInternalEvents::TEvNotifyAck::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        SBR_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender);

        auto it = Subscribers.find(ev->Sender);
        if (it == Subscribers.end()) {
            return;
        }

        TDescription* desc = nullptr;

        if (const TString* path = std::get_if<TString>(&it->second)) {
            desc = Descriptions.FindPtr(*path);
        } else if (const TPathId* pathId = std::get_if<TPathId>(&it->second)) {
            desc = Descriptions.FindPtr(*pathId);
        }

        Y_ABORT_UNLESS(desc);
        auto& info = desc->GetSubscriberInfo(ev->Sender);

        const ui64 version = record.GetVersion();
        if (!info.AckVersion(version)) {
            return;
        }

        if (version < desc->GetVersion()) {
            auto notify = desc->BuildNotify(IsPopulatorCommited(info.GetDomainOwnerId()));
            info.EnqueueVersion(notify.Get());
            Send(ev->Sender, std::move(notify));
        }

        if (auto cookie = info.ProcessSyncRequest()) {
            Send(ev->Sender, new NInternalEvents::TEvSyncVersionResponse(desc->GetVersion()), 0, *cookie);
        }
    }

    void Handle(NInternalEvents::TEvSyncVersionRequest::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        SBR_LOG_D("Handle " << ev->Get()->ToString()
            << ": sender# " << ev->Sender
            << ", cookie# " << ev->Cookie);

        auto it = Subscribers.find(ev->Sender);
        if (it == Subscribers.end()) {
            // for backward compatibility
            ui64 version = 0;

            if (record.HasPath()) {
                version = GetVersion(record.GetPath());
            } else {
                version = GetVersion(TPathId(record.GetPathOwnerId(), record.GetLocalPathId()));
            }

            Send(ev->Sender, new NInternalEvents::TEvSyncVersionResponse(version), 0, ev->Cookie);
            return;
        }

        TDescription* desc = nullptr;

        if (const TString* path = std::get_if<TString>(&it->second)) {
            desc = Descriptions.FindPtr(*path);
        } else if (const TPathId* pathId = std::get_if<TPathId>(&it->second)) {
            desc = Descriptions.FindPtr(*pathId);
        }

        Y_ABORT_UNLESS(desc);
        auto& info = desc->GetSubscriberInfo(ev->Sender);

        if (!info.EnqueueSyncRequest(ev->Cookie) || info.IsWaitForAck()) {
            return;
        }

        auto cookie = info.ProcessSyncRequest();
        Y_ABORT_UNLESS(cookie && *cookie == ev->Cookie);

        Send(ev->Sender, new NInternalEvents::TEvSyncVersionResponse(desc->GetVersion()), 0, *cookie);
    }

    void Handle(TSchemeBoardMonEvents::TEvInfoRequest::TPtr& ev) {
        const auto limit = ev->Get()->Record.GetLimitRepeatedFields();

        auto response = MakeHolder<TSchemeBoardMonEvents::TEvInfoResponse>(SelfId(), ActorActivityType());
        auto& record = *response->Record.MutableReplicaResponse();

        for (const auto& [owner, populator] : Populators) {
            auto& info = *record.AddPopulators();

            info.SetOwner(owner);
            info.SetGeneration(populator.Generation);
            info.SetPendingGeneration(populator.PendingGeneration);
            ActorIdToProto(populator.PopulatorActor, info.MutableActorId());

            if (record.PopulatorsSize() >= limit) {
                response->SetTruncated();
                break;
            }
        }

        record.MutableDescriptions()->SetTotalCount(Descriptions.Size());
        record.MutableDescriptions()->SetByPathCount(Descriptions.GetPrimaryIndex().size());
        record.MutableDescriptions()->SetByPathIdCount(Descriptions.GetSecondaryIndex().size());

        for (const auto& [subscriber, id] : Subscribers) {
            auto& info = *record.AddSubscribers();

            ActorIdToProto(subscriber, info.MutableActorId());
            if (const TString* path = std::get_if<TString>(&id)) {
                info.SetPath(*path);
            } else if (const TPathId* pathId = std::get_if<TPathId>(&id)) {
                info.MutablePathId()->SetOwnerId(pathId->OwnerId);
                info.MutablePathId()->SetLocalPathId(pathId->LocalPathId);
            }

            if (record.SubscribersSize() >= limit) {
                response->SetTruncated();
                break;
            }
        }

        Send(ev->Sender, std::move(response), 0, ev->Cookie);
    }

    void Handle(TSchemeBoardMonEvents::TEvDescribeRequest::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TDescription* desc = nullptr;
        if (record.HasPath()) {
            desc = Descriptions.FindPtr(record.GetPath());
        } else if (record.HasPathId()) {
            desc = Descriptions.FindPtr(TPathId(record.GetPathId().GetOwnerId(), record.GetPathId().GetLocalPathId()));
        }

        TString json;
        if (desc) {
            json = JsonFromDescribeSchemeResult(desc->GetDescribeSchemeResultSerialized());
        } else {
            json = "{}";
        }

        Send(ev->Sender, new TSchemeBoardMonEvents::TEvDescribeResponse(json), 0, ev->Cookie);
    }

    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        const ui32 nodeId = ev->Get()->NodeId;

        SBR_LOG_D("Handle " << ev->Get()->ToString()
            << ": nodeId# " << nodeId);

        auto it = Subscribers.lower_bound(TActorId(nodeId, 0, 0, 0));
        while (it != Subscribers.end() && it->first.NodeId() == nodeId) {
            const TActorId subscriber = it->first;
            const auto id = it->second;
            ++it;

            if (const TString* path = std::get_if<TString>(&id)) {
                Unsubscribe(subscriber, *path);
            } else if (const TPathId* pathId = std::get_if<TPathId>(&id)) {
                Unsubscribe(subscriber, *pathId);
            }
        }

        Send(MakeInterconnectProxyId(nodeId), new TEvents::TEvUnsubscribe());
    }

    void PassAway() override {
        for (const auto& [_, info] : Populators) {
            if (const auto& actorId = info.PopulatorActor) {
                Send(actorId, new TEvStateStorage::TEvReplicaShutdown());
            }
        }

        for (const auto& [actorId, _] : Subscribers) {
            Send(actorId, new TEvStateStorage::TEvReplicaShutdown());
        }

        TMonitorableActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SCHEME_BOARD_REPLICA_ACTOR;
    }

    void Bootstrap() {
        TMonitorableActor::Bootstrap();
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternalEvents::TEvHandshakeRequest, Handle);
            hFunc(NInternalEvents::TEvUpdate, Handle);
            hFunc(NInternalEvents::TEvCommitRequest, Handle);
            hFunc(TEvPrivate::TEvSendStrongNotifications, Handle);
            hFunc(NInternalEvents::TEvSubscribe, Handle);
            hFunc(NInternalEvents::TEvUnsubscribe, Handle);
            hFunc(NInternalEvents::TEvNotifyAck, Handle);
            hFunc(NInternalEvents::TEvSyncVersionRequest, Handle);

            hFunc(TSchemeBoardMonEvents::TEvInfoRequest, Handle);
            hFunc(TSchemeBoardMonEvents::TEvDescribeRequest, Handle);

            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }

private:
    THashMap<ui64, TPopulatorInfo> Populators;
    TDoubleIndexedMap<TString, TPathId, TDescription, TMerger, THashMap, TMap> Descriptions;
    TMap<TActorId, std::variant<TString, TPathId>, TActorId::TOrderedCmp> Subscribers;
    THashMap<ui64, TSet<TActorId>> WaitStrongNotifications;

}; // TReplica

} // NSchemeBoard

IActor* CreateSchemeBoardReplica(const TIntrusivePtr<TStateStorageInfo>&, ui32) {
    return new NSchemeBoard::TReplica();
}

} // NKikimr
