#include "counters.h"
#include "net_classifier.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/net_classifier_updater.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/protos/netclassifier.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/hash.h>
#include <util/stream/file.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>

#include <vector>

namespace NKikimr::NNetClassifier {

using namespace NAddressClassifier;
using namespace NConsole;
using namespace NCounters;

inline auto& Ctx() {
    return TActivationContext::AsActorContext();
}

class TNetClassifier: public TActorBootstrapped<TNetClassifier> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::NET_CLASSIFIER_ACTOR;
    }

    void Bootstrap() {
        Become(&TThis::WaitingForSubscribers);
    }

private:
    static TMaybe<TInstant> GetNetDataFileModTimestamp(const TString& filePath) {
        try {
            TFileStat stat(filePath);
            if (stat.IsFile() && !stat.IsNull()) {
                return TInstant::Seconds(stat.MTime);
            } else {
                ythrow yexception() << "file does not exist: " << filePath;
            }
        } catch (const yexception& ex) {
            LOG_ERROR_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "failed to get NetData file stats: " << ex.what());
        }

        return Nothing();
    }

    const auto& Cfg() const {
        return AppData(Ctx())->NetClassifierConfig;
    }

    void AddSubscriber(const TActorId subscriberId) {
        Subscribers.insert(subscriberId);

        *Counters->SubscribersCount = Subscribers.size();
    }

    STATEFN(WaitingForSubscribers) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvNetClassifier::TEvSubscribe, HandleWhileWaiting);
        }
    }

    void SubscribeForConfigUpdates() {
        const auto kind = static_cast<ui32>(NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem);
        Send(MakeConfigsDispatcherID(Ctx().SelfID.NodeId()), new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(kind));
    }

    void HandleWhileWaiting(TEvNetClassifier::TEvSubscribe::TPtr& ev) {
        Become(&TThis::Initing);

        InitCounters();

        AddSubscriber(ev->Sender);

        const auto& netDataFilePath = Cfg().GetNetDataFilePath();
        if (netDataFilePath) {
            MaybeNetDataFilePath = netDataFilePath;

            MaybeNetDataFileModTs = GetNetDataFileModTimestamp(*MaybeNetDataFilePath);
        }

        SubscribeForConfigUpdates();
    }

    STATEFN(Initing) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvNetClassifier::TEvSubscribe, HandleWhileIniting);
            hFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleWhileIniting);
            hFunc(TEvConfigsDispatcher::TEvGetConfigResponse, HandleWhileIniting);
            hFunc(TEvents::TEvWakeup, HandleWhileIniting);
            hFunc(TEvNetClassifier::TEvUpdateTimedCounters, UpdateTimedCounters);
        default:
            EnqueueEvent(ev);
            break;
        }
    }

    void EnqueueEvent(TAutoPtr<IEventHandle> &ev) {
        EventsQueue.push_back(ev);
    }

    void ProcessEnqueuedEvents() {
        while (!EventsQueue.empty()) {
            TAutoPtr<IEventHandle> &ev = EventsQueue.front();
            TlsActivationContext->Send(ev.Release());
            EventsQueue.pop_front();
        }
    }

    void HandleWhileIniting(TEvNetClassifier::TEvSubscribe::TPtr& ev) {
        AddSubscriber(ev->Sender);

        // all subsribers get classifier update during a transition to a Working state
    }

    void RequestDistributableConfig() {
        const auto kind = static_cast<ui32>(NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem);
        Send(MakeConfigsDispatcherID(Ctx().SelfID.NodeId()), new TEvConfigsDispatcher::TEvGetConfigRequest(kind));
    }

    void HandleWhileIniting(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_INFO_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "subscribed to ConfigsDispatcher for NetData updates");

        // Subscription is set but the console tablet may still be unavailable hence it's a good idea to schedule a timeout event
        Schedule(TDuration::Seconds(Cfg().GetCmsConfigTimeoutSeconds()), new TEvents::TEvWakeup);

        RequestDistributableConfig();

        // This leads to Case 1 or Case 2
    }

    // Case 1: GetConfig request timed out
    void HandleWhileIniting(TEvents::TEvWakeup::TPtr&) {
        LOG_WARN_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "TIMEOUT: failed to get distributable config, fall back to file: " << MaybeNetDataFilePath);
        InitFromFile();
    }

    template<typename TConfPtr>
    bool ShouldUseDistributableConfigOnInit(const TConfPtr& ptr) const {
        if (!ptr) {
            return false;
        }

        if (!ptr->HasNetClassifierDistributableConfig()) {
            return false;
        }

        const auto& config = ptr->GetNetClassifierDistributableConfig();
        if (config.GetLastUpdateDatetimeUTC() && config.GetLastUpdateTimestamp()) {
            if (MaybeNetDataFileModTs) {
                return TInstant::MicroSeconds(config.GetLastUpdateTimestamp()) > *MaybeNetDataFileModTs;
            }

            return true;
        }

        return false;
    }

    // Case 2: GetConfig request succeeded
    void HandleWhileIniting(TEvConfigsDispatcher::TEvGetConfigResponse::TPtr& ev) {
        if (ShouldUseDistributableConfigOnInit(ev->Get()->Config)) {
            LOG_INFO_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "will initialize from distributable config");

            if (UpdateFromDistributableConfig(ev->Get()->Config->GetNetClassifierDistributableConfig())) {
                ReportGoodConfig();
                UpdateNetDataSource(ENetDataSourceType::DistributableConfig);

                LOG_INFO_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "successfully initialized from distributable config");

                CompleteInitialization();
                return;
            } else {
                LOG_WARN_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "failed to initialize from distributed config");
            }
        }

        ReportBadConfig();
        LOG_WARN_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "distributable config is empty, broken or outdated, will use file: " << MaybeNetDataFilePath);
        InitFromFile();
    }

    void InitFromFile() {
        // NetData file may be outdated hence warning log level is used
        LOG_WARN_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "will try to initialize from file: " << MaybeNetDataFilePath);
        LabeledAddressClassifier = TryReadNetDataFromFile();
        if (LabeledAddressClassifier) {
            NetDataUpdateTimestamp = MaybeNetDataFileModTs;
            UpdateNetDataSource(ENetDataSourceType::File);

            LOG_WARN_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "successfully initialized from file: " << MaybeNetDataFilePath);
        } else {
            LOG_WARN_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "failed to initialize from file: " << MaybeNetDataFilePath);
        }

        CompleteInitialization();
    }

    bool CheckDistributableConfig(const NKikimrNetClassifier::TNetClassifierDistributableConfig& config) const {
        return config.GetPackedNetData() && config.GetLastUpdateDatetimeUTC() && config.GetLastUpdateTimestamp();
    }

    bool UpdateFromDistributableConfig(const NKikimrNetClassifier::TNetClassifierDistributableConfig& config) {
        if (CheckDistributableConfig(config)) {
            LOG_INFO_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "got new config with datetime: " << config.GetLastUpdateDatetimeUTC());

            auto labeledAddressClassifier = BuildNetClassifierFromPackedNetData(config.GetPackedNetData());
            if (labeledAddressClassifier) {
                LabeledAddressClassifier = std::move(labeledAddressClassifier);
                NetDataUpdateTimestamp = TInstant::MicroSeconds(config.GetLastUpdateTimestamp());

                return true;
            } else {
                LOG_ERROR_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "failed to parse NetData from distributable configuration");
            }
        } else {
            LOG_ERROR_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "got bad distributable configuration");
        }

        return false;
    }

    void CompleteInitialization() {
        Become(&TThis::Working);

        NActors::TMon* mon = AppData(Ctx())->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage * page = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(page, "netclassifier", "NetClassifier", false, Ctx().ExecutorThread.ActorSystem, Ctx().SelfID);
        }

        ProcessEnqueuedEvents();

        BroadcastClassifierUpdate();
    }

    STATEFN(Working) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvConsole::TEvConfigNotificationRequest, HandleWhileWorking);
            hFunc(TEvNetClassifier::TEvSubscribe, HandleWhileWorking);
            hFunc(NMon::TEvHttpInfo, HandleHttpRequest);
            hFunc(TEvNetClassifier::TEvUpdateTimedCounters, UpdateTimedCounters);
        }
    }

    static TString FormatTs(const TMaybe<TInstant>& time) {
        if (time) {
            return time->ToRfc822String();
        }
        return "NULL";
    }

    void HandleHttpRequest(NMon::TEvHttpInfo::TPtr& ev)
    {
        TStringBuilder message;
        message << "NetClassifier info: \n";
        message << "\tUnique subscribers: " << Subscribers.size() << "\n";
        message << "\tClassifier updates sent: " << ClassifierUpdatesSent << "\n";
        message << "\tLast classifier update broadcast ts: " << FormatTs(LastClassifierUpdateBroadcastTs) << "\n";
        message << "\tNetData file path: " << MaybeNetDataFilePath << "\n";
        message << "\tNetData file ts: " << FormatTs(MaybeNetDataFileModTs) << "\n";
        message << "\tCurrent NetData ts: " << FormatTs(NetDataUpdateTimestamp) << "\n";
        message << "\tGood config notifications: " << GoodConfigNotifications << "\n";
        message << "\tBroken or empty config notifications: " << BrokenOrEmptyConfigNotifications << "\n";
        message << "\tCurrent NetData source: " << ToString(CurrentNetDataSource) << "\n";
        message << "\tClassifier: " << (LabeledAddressClassifier ? "OK" : "NULL");

        Send(ev->Sender, new NMon::TEvHttpInfoRes(TString(NMonitoring::HTTPOKTEXT) + TString(message), 0,
                                                  NMon::IEvHttpInfoRes::EContentType::Custom));
    }

    void HandleWhileWorking(TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        const auto& rec = ev->Get()->Record;

        if (UpdateFromDistributableConfig(rec.GetConfig().GetNetClassifierDistributableConfig())) {
            ReportGoodConfig();
            UpdateNetDataSource(ENetDataSourceType::DistributableConfig);

            BroadcastClassifierUpdate();
        } else {
            ReportBadConfig();
        }

        // report success
        auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);
        Send(ev->Sender, resp.Release(), 0, ev->Cookie);
    }

    void HandleWhileWorking(TEvNetClassifier::TEvSubscribe::TPtr& ev) {
        AddSubscriber(ev->Sender);

        // respond instantly with the current classifier
        SendClassifierUpdate(ev->Sender);
    }

    void SendClassifierUpdate(const TActorId recipient) {
        auto ev = MakeHolder<TEvNetClassifier::TEvClassifierUpdate>();
        ev->Classifier = LabeledAddressClassifier;
        ev->NetDataUpdateTimestamp = NetDataUpdateTimestamp;

        Send(recipient, ev.Release());

        ++ClassifierUpdatesSent;
    }

    void BroadcastClassifierUpdate() {
        for (const auto& subscriberId : Subscribers) {
            SendClassifierUpdate(subscriberId);
        }

        LastClassifierUpdateBroadcastTs = Ctx().Now();
    }

    TLabeledAddressClassifier::TConstPtr BuildNetClassifierFromPackedNetData(const TString& packedNetData) const {
        const TString serializedNetData = NNetClassifierUpdater::UnpackNetData(packedNetData);
        if (!serializedNetData) {
            LOG_ERROR_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "empty packed networks");
            return nullptr;
        }

        NKikimrNetClassifier::TNetData netData;
        if (!netData.ParseFromString(serializedNetData)) {
            LOG_ERROR_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "can't deserialize networks data protobuf");
            return nullptr;
        }

        return BuildNetClassifierFromNetData(netData);
    }

    TLabeledAddressClassifier::TConstPtr BuildNetClassifierFromNetData(const NKikimrNetClassifier::TNetData& netData) const {
        auto labeledAddressClassifier = BuildLabeledAddressClassifierFromNetData(netData);
        if (!labeledAddressClassifier) {
            LOG_ERROR_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "invalid NetData format");
        }

        return labeledAddressClassifier;
    }

    TLabeledAddressClassifier::TConstPtr TryReadNetDataFromFile() const {
        if (MaybeNetDataFilePath) {
            try {
                return ReadNetDataFromFile(*MaybeNetDataFilePath);
            } catch (const yexception& ex) {
                LOG_ERROR_S(Ctx(), NKikimrServices::NET_CLASSIFIER, "failed to read NetData from file: " << ex.what());
            }
        }

        return nullptr;
    }

    TLabeledAddressClassifier::TConstPtr ReadNetDataFromFile(const TString& path) const {
        NKikimrNetClassifier::TNetData netData;

        // Parse TSV-format
        TFileInput knownNetsFile(path);
        TString line;
        while (knownNetsFile.ReadLine(line)) {
            TStringBuf mask, label;
            if (!TStringBuf(line).TryRSplit('\t', mask, label)) {
                ythrow yexception() << "NetData file violates tsv format";
            }

            auto& subnet = *netData.AddSubnets();
            subnet.SetMask(TString(mask));
            subnet.SetLabel(TString(label));
        }

        return BuildNetClassifierFromNetData(netData);
    }

    // Monitoring features

    void ReportGoodConfig() {
        ++GoodConfigNotifications;
        Counters->GoodConfigNotificationsCount->Inc();
    }

    void ReportBadConfig() {
        ++BrokenOrEmptyConfigNotifications;
        Counters->BrokenConfigNotificationsCount->Inc();
    }

    void UpdateNetDataSource(const ENetDataSourceType newSource) {
        CurrentNetDataSource = newSource;
        *Counters->NetDataSourceType = CurrentNetDataSource;
    }

    void UpdateTimedCounters(TEvNetClassifier::TEvUpdateTimedCounters::TPtr&) {
        if (NetDataUpdateTimestamp) {
            (*Counters->NetDataUpdateLagSeconds) = Ctx().Now().Seconds() - NetDataUpdateTimestamp->Seconds();
        }

        Schedule(TDuration::Seconds(Cfg().GetTimedCountersUpdateIntervalSeconds()), new TEvNetClassifier::TEvUpdateTimedCounters);
    }

    void InitCounters() {
        Counters = MakeIntrusive<TNetClassifierCounters>(GetServiceCounters(AppData(Ctx())->Counters, "netclassifier")->GetSubgroup("subsystem", "distributor"));

        Send(Ctx().SelfID, new TEvNetClassifier::TEvUpdateTimedCounters);
    }

private:
    TMaybe<TString> MaybeNetDataFilePath;
    TMaybe<TInstant> MaybeNetDataFileModTs;
    TDeque<TAutoPtr<IEventHandle>> EventsQueue;

    TLabeledAddressClassifier::TConstPtr LabeledAddressClassifier;
    TMaybe<TInstant> NetDataUpdateTimestamp;
    THashSet<TActorId> Subscribers;

    ui64 ClassifierUpdatesSent = 0;
    ui64 GoodConfigNotifications = 0;
    ui64 BrokenOrEmptyConfigNotifications = 0;
    TMaybe<TInstant> LastClassifierUpdateBroadcastTs;
    ENetDataSourceType CurrentNetDataSource = ENetDataSourceType::None;

    TNetClassifierCounters::TPtr Counters;
};

NActors::IActor* CreateNetClassifier() {
    return new TNetClassifier();
}

} // namespace NKikimr::NNetClassifier
