#include "events.h"
#include "helpers.h"
#include "load_test.h"
#include "populator.h"
#include "subscriber.h"

#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/ptr.h>
#include <util/generic/ylimits.h>
#include <util/random/random.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NSchemeBoard {

namespace {

    bool IsDir(const NKikimrScheme::TEvDescribeSchemeResult& record) {
        const auto& self = record.GetPathDescription().GetSelf();
        return self.GetParentPathId() == NSchemeShard::RootPathId;
    }

    bool IsDir(const TTwoPartDescription& desc) {
        return IsDir(desc.Record);
    }

} // anonymous

class TLoadProducer: public TActorBootstrapped<TLoadProducer> {
    using TDescription = NKikimrScheme::TEvDescribeSchemeResult;
    using TDescriptions = TMap<TPathId, TTwoPartDescription>;
    using TDescribeSchemeResult = NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder;

    enum EWakeupTag {
        TAG_MODIFY,
        TAG_CHANGE,
    };

    static TDescriptions GenerateDescriptions(ui64 owner, const TTestConfig& config, ui64& nextPathId) {
        const ui64 version = TlsActivationContext->Now().GetValue();

        TDescriptions descriptions;

        for (ui64 dir = 1; dir <= config.Dirs; ++dir) {
            const TString dirName = TStringBuilder() << "dir-" << dir;
            const TString dirPath = TStringBuilder() << "/Root" << "/" << dirName;

            TTwoPartDescription dirDescTwoPart;
            TDescription& dirDesc = dirDescTwoPart.Record;

            dirDesc.SetStatus(NKikimrScheme::StatusSuccess);
            dirDesc.SetPathOwnerId(owner);
            dirDesc.SetPathId(nextPathId++);
            dirDesc.SetPath(dirPath);

            auto& dirSelf = *dirDesc.MutablePathDescription()->MutableSelf();
            dirSelf.SetName(dirName);
            dirSelf.SetPathId(dirDesc.GetPathId());
            dirSelf.SetSchemeshardId(dirDesc.GetPathOwnerId());
            dirSelf.SetPathType(NKikimrSchemeOp::EPathTypeDir);
            dirSelf.SetCreateFinished(true);
            dirSelf.SetCreateTxId(1);
            dirSelf.SetCreateStep(1);
            dirSelf.SetParentPathId(NSchemeShard::RootPathId);
            dirSelf.SetPathState(NKikimrSchemeOp::EPathStateNoChanges);
            dirSelf.SetPathVersion(version);

            auto& dirChildren = *dirDesc.MutablePathDescription()->MutableChildren();
            dirChildren.Reserve(config.ObjectsPerDir);

            for (ui64 obj = 1; obj <= config.ObjectsPerDir; ++obj) {
                const TString objName = TStringBuilder() << "obj-" << obj;
                const TString objPath = TStringBuilder() << dirPath << "/" << objName;

                TTwoPartDescription objDescTwoPart;
                TDescription& objDesc = objDescTwoPart.Record;

                objDesc.SetStatus(NKikimrScheme::StatusSuccess);
                objDesc.SetPathOwnerId(owner);
                objDesc.SetPathId(nextPathId++);
                objDesc.SetPath(objPath);

                auto& objSelf = *objDesc.MutablePathDescription()->MutableSelf();
                objSelf.SetName(objName);
                objSelf.SetPathType(NKikimrSchemeOp::EPathTypeDir);
                dirChildren.Add()->CopyFrom(objSelf);

                objSelf.SetPathId(objDesc.GetPathId());
                objSelf.SetSchemeshardId(objDesc.GetPathOwnerId());
                objSelf.SetCreateFinished(true);
                objSelf.SetCreateTxId(1);
                objSelf.SetCreateStep(1);
                objSelf.SetParentPathId(dirDesc.GetPathId());
                objSelf.SetPathState(NKikimrSchemeOp::EPathStateNoChanges);
                objSelf.SetPathVersion(version);

                descriptions[TPathId(owner, objDesc.GetPathId())] = std::move(objDescTwoPart);
            }

            descriptions[TPathId(owner, dirDesc.GetPathId())] = std::move(dirDescTwoPart);
        }

        return descriptions;
    }

    TPathId RandomPathId(bool allowDir = true) const {
        while (true) {
            const TPathId pathId(Owner, RandomNumber(NextPathId));

            if (!Descriptions.contains(pathId)) {
                continue;
            }

            if (!allowDir && IsDir(Descriptions.at(pathId))) {
                continue;
            }

            return pathId;
        }

        Y_ABORT("Unreachable");
    }

    void Modify(TPathId pathId) {
        Y_ABORT_UNLESS(Descriptions.contains(pathId));

        TDescription& description = Descriptions.at(pathId).Record;
        auto& self = *description.MutablePathDescription()->MutableSelf();
        self.SetPathVersion(TlsActivationContext->Now().GetValue());

        auto describeSchemeResult = MakeHolder<TDescribeSchemeResult>();
        describeSchemeResult->Record.CopyFrom(description);
        Send(Populator, std::move(describeSchemeResult));

        ++*ModifiedPaths;
    }

    void Delete(TPathId pathId) {
        Y_ABORT_UNLESS(Descriptions.contains(pathId));

        TDescription& description = Descriptions.at(pathId).Record;
        Y_ABORT_UNLESS(!IsDir(description));

        description.SetStatus(NKikimrScheme::StatusPathDoesNotExist);

        auto describeSchemeResult = MakeHolder<TDescribeSchemeResult>();
        describeSchemeResult->Record.CopyFrom(description);
        Send(Populator, std::move(describeSchemeResult));

        Modify(TPathId(description.GetPathDescription().GetSelf().GetSchemeshardId(),
                       description.GetPathDescription().GetSelf().GetParentPathId()));
        Descriptions.erase(pathId);

        ++*DeletedPaths;
    }

    void Modify() {
        Modify(RandomPathId());

        const TDuration delta = TDuration::MicroSeconds(1000000 / Config.InFlightModifications);
        Schedule(delta, new TEvents::TEvWakeup(TAG_MODIFY));
    }

    void Change() {
        const bool create = RandomNumber((ui8)2);

        if (create) {
            //Create(++NextPathId);
        } else {
            Delete(RandomPathId(false));
        }

        const TDuration delta = TDuration::MicroSeconds(1000000 / Config.InFlightChanges);
        Schedule(delta, new TEvents::TEvWakeup(TAG_CHANGE));
    }

    void Boot() {
        const TActorId proxy = MakeStateStorageProxyID();
        Send(proxy, new TEvStateStorage::TEvListSchemeBoard(false), IEventHandle::FlagTrackDelivery);

        Become(&TThis::StateBoot);
    }

    void Populate() {
        Descriptions = GenerateDescriptions(Owner, Config, NextPathId);
        Populator = Register(CreateSchemeBoardPopulator(
            Owner, Max<ui64>(), std::vector<std::pair<TPathId, TTwoPartDescription>>(Descriptions.begin(), Descriptions.end()), NextPathId
        ));

        TPathId pathId(Owner, NextPathId - 1);
        Y_ABORT_UNLESS(Descriptions.contains(pathId));
        const TString& topPath = Descriptions.at(pathId).Record.GetPath();

        // subscriber will help us to know when sync is completed
        Subscriber = Register(CreateSchemeBoardSubscriber(
            SelfId(), topPath,
            ESchemeBoardSubscriberDeletionPolicy::Majority
        ));

        *TotalPaths = NextPathId - 1 - NSchemeShard::RootPathId;

        Become(&TThis::StatePopulate);
    }

    void Test() {
        Modify();
        //Change();

        Become(&TThis::StateTest);
    }

    void Handle(TEvStateStorage::TEvListSchemeBoardResult::TPtr& ev) {
        if (!ev->Get()->Info) {
            return Boot();
        }

        Populate();
    }

    void Handle(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev) {
        const auto* msg = ev->Get();

        Y_ABORT_UNLESS(ev->Sender == Subscriber);
        Y_ABORT_UNLESS(msg->PathId.LocalPathId == NextPathId - 1);

        Send(Subscriber, new TEvents::TEvPoisonPill());
        Subscriber = TActorId();

        const TInstant ts = TInstant::FromValue(NSchemeBoard::GetPathVersion(msg->DescribeSchemeResult));
        *SyncDuration = (TlsActivationContext->Now() - ts).MilliSeconds();

        Test();
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        switch (ev->Get()->Tag) {
        case TAG_MODIFY:
            Modify();
            break;

        case TAG_CHANGE:
            Change();
            break;

        default:
            Y_DEBUG_ABORT("Unknown wakeup tag");
            break;
        }
    }

    void PassAway() override {
        Send(Populator, new TEvents::TEvPoisonPill());
        if (Subscriber) {
            Send(Subscriber, new TEvents::TEvPoisonPill());
        }

        TActor::PassAway();
    }

public:
    explicit TLoadProducer(ui64 owner, const TTestConfig& config)
        : Owner(owner)
        , Config(config)
        , NextPathId(NSchemeShard::RootPathId + 1)
    {
        SyncDuration = Config.Counters->GetCounter("Producer/SyncDuration", false);
        TotalPaths = Config.Counters->GetCounter("Producer/TotalPaths", false);
        ModifiedPaths = Config.Counters->GetCounter("Producer/ModifiedPaths", false);
        CreatedPaths = Config.Counters->GetCounter("Producer/CreatedPaths", false);
        DeletedPaths = Config.Counters->GetCounter("Producer/DeletedPaths", false);
    }

    void Bootstrap() {
        Boot();
    }

    STATEFN(StateBoot) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvListSchemeBoardResult, Handle);
            cFunc(TEvents::TEvUndelivered::EventType, Boot);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StatePopulate) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardEvents::TEvNotifyUpdate, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

    STATEFN(StateTest) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvWakeup, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    const ui64 Owner;
    const TTestConfig Config;
    ::NMonitoring::TDynamicCounters::TCounterPtr SyncDuration;
    ::NMonitoring::TDynamicCounters::TCounterPtr TotalPaths;
    ::NMonitoring::TDynamicCounters::TCounterPtr ModifiedPaths;
    ::NMonitoring::TDynamicCounters::TCounterPtr CreatedPaths;
    ::NMonitoring::TDynamicCounters::TCounterPtr DeletedPaths;

    TDescriptions Descriptions;
    ui64 NextPathId;
    TActorId Populator;
    TActorId Subscriber;

}; // TLoadProducer

class TLoadConsumer: public TActorBootstrapped<TLoadConsumer> {
    void Subscribe(const TPathId& pathId) {
        for (ui32 i = 0; i < Config.SubscriberMulti; ++i) {
            const TActorId subscriber = Register(CreateSchemeBoardSubscriber(
                SelfId(), pathId,
                ESchemeBoardSubscriberDeletionPolicy::Majority
            ));

            Subscribers.push_back(subscriber);
            ++*SubscribersCount;
        }
    }

    void BatchSubscribe(ui64 nextPathId, ui32 batchSizeLimit = 100) {
        ui32 batchSize = 0;
        while (nextPathId <= MaxPathId && batchSize++ < batchSizeLimit) {
            Subscribe(TPathId(Owner, nextPathId++));
        }

        if (nextPathId > MaxPathId) {
            return;
        }

        Schedule(TDuration::MilliSeconds(20), new TEvents::TEvWakeup(nextPathId));
    }

    void Handle(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev) {
        const auto* msg = ev->Get();

        const TInstant ts = TInstant::FromValue(NSchemeBoard::GetPathVersion(msg->DescribeSchemeResult));
        if (IsDir(msg->DescribeSchemeResult)) {
            LatencyDir->Collect((TlsActivationContext->Now() - ts).MilliSeconds());
        } else {
            Latency->Collect((TlsActivationContext->Now() - ts).MilliSeconds());
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        BatchSubscribe(ev->Get()->Tag);
    }

    void PassAway() override {
        for (const auto& subscriber : Subscribers) {
            Send(subscriber, new TEvents::TEvPoisonPill());
        }

        TActor::PassAway();
    }

public:
    explicit TLoadConsumer(ui64 owner, const TTestConfig& config)
        : Owner(owner)
        , Config(config)
    {
        MaxPathId = NSchemeShard::RootPathId
                    + Config.Dirs
                    + Config.Dirs * Config.ObjectsPerDir;

        Latency = Config.Counters->GetHistogram("Consumer/Latency", NMonitoring::ExponentialHistogram(15, 2, 1));
        LatencyDir = Config.Counters->GetHistogram("Consumer/LatencyDir", NMonitoring::ExponentialHistogram(10, 4, 1));
        SubscribersCount = Config.Counters->GetCounter("Consumer/Subscribers", false);
    }

    void Bootstrap() {
        BatchSubscribe(NSchemeShard::RootPathId + 1);
        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSchemeBoardEvents::TEvNotifyUpdate, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }

private:
    const ui64 Owner;
    const TTestConfig Config;
    NMonitoring::THistogramPtr Latency;
    NMonitoring::THistogramPtr LatencyDir;
    ::NMonitoring::TDynamicCounters::TCounterPtr SubscribersCount;

    ui64 MaxPathId;
    TVector<TActorId> Subscribers;

}; // TLoadConsumer

} // NSchemeBoard

IActor* CreateSchemeBoardLoadProducer(ui64 owner, const NSchemeBoard::TTestConfig& config) {
    return new NSchemeBoard::TLoadProducer(owner, config);
}

IActor* CreateSchemeBoardLoadConsumer(ui64 owner, const NSchemeBoard::TTestConfig& config) {
    return new NSchemeBoard::TLoadConsumer(owner, config);
}

} // NKikimr
