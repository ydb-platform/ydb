#include "net_classifier_updater.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/netclassifier.pb.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/json/json_reader.h>

#include <util/stream/zlib.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_NOTICE
#error log macro definition clash
#endif

#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS, stream)
#define BLOG_NOTICE(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS, stream)
#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS, stream)


namespace NKikimr::NNetClassifierUpdater {

using namespace NConsole;

// helps to find a particular config item. DO NOT CHANGE this constant
static const TString COOKIE = "NetClassifierPackedNetDataFromExternalSource";

// prevents duplicates in config list. DO NOT CHANGE this constant
static constexpr ui32 CONFIG_ORDER = 10;

static TString PackNetData(const TString& netData, size_t level = 6) {
    TString result;
    result.reserve(std::size(netData));
    TStringOutput stream(result);
    TZLibCompress(&stream, ZLib::GZip, level).Write(netData);
    return result;
}

/*
    ***NetClassifier Updater algorithm***

    The main idea is to keep only one special config item to periodically modify it.
    All discovery nodes are subscribed to config dispatcher to get updates.

    Initing stage (requires WakeUp event to start):
        1) Look for the config item via cookie
            if has distributable config item:
                GOTO Working stage
            else:
                Add distributable empty config item with cookie and order
                GOTO Working stage if added successfully
    Working stage (also requires WakeUp event to start):
        1) Get Networks Data from an external http(s) resource
        2) Find current config id and generation through GetConfigItem
        3) Send ModifyEvent using step 2 information.
        4) Schedule WakeUp event on success

    In case of any error algorithm falls back to Initing stage and schedules WakeUp.
*/

class NetClassifierUpdater : public TActorBootstrapped<NetClassifierUpdater> {
private:
    using TBase = TActorBootstrapped<NetClassifierUpdater>;

public:
    NetClassifierUpdater(TActorId localConsole)
        : LocalConsole(localConsole)
        , HttpSensors(std::make_shared<NMonitoring::TMetricRegistry>())
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::NET_CLASSIFIER_UPDATER;
    }

    void Bootstrap() {
        Become(&TThis::Initing);
        Send(SelfId(), new TEvents::TEvWakeup);
    }

private:
    const auto& UpdaterConfig() const {
        return AppData()->NetClassifierConfig.GetUpdaterConfig();
    }

    void HandleWhileIniting(TEvents::TEvWakeup::TPtr&) {
        // start the config check
        RequestCurrentConfigViaCookie();
    }

    void RequestCurrentConfigViaCookie() {
        BLOG_D("NetClassifierUpdater requested distributable config item via cookie");

        auto event = MakeHolder<TEvConsole::TEvGetConfigItemsRequest>();

        event->Record.AddItemKinds(static_cast<ui32>(NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem));
        event->Record.MutableCookieFilter()->AddCookies(COOKIE);

        Send(LocalConsole, event.Release());
    }

    void InitDefaultConfiguration() {
        LOG_INFO_S(*TlsActivationContext, NKikimrServices::CMS_CONFIGS,
                        "NetClassifierUpdate is adding distributable config item with cookie");

        auto event = MakeHolder<TEvConsole::TEvConfigureRequest>();

        auto& configItem = *event->Record.AddActions()->MutableAddConfigItem()->MutableConfigItem();
        configItem.MutableConfig()->MutableNetClassifierDistributableConfig(); // just initialize the field

        configItem.SetKind(static_cast<ui32>(NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem));

        configItem.SetOrder(CONFIG_ORDER); // prevents config item duplicates
        configItem.SetCookie(COOKIE);

        Send(LocalConsole, event.Release());
    }

    void HandleWhileIniting(TEvConsole::TEvConfigureResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
            BLOG_D("NetClassifierUpdater created a new distributable config item");
            CompleteInitialization();
        } else {
            BLOG_ERROR("NetClassifierUpdater failed to add config item: " << record.ShortDebugString());
            InitializeAgain();
        }
    }

    void HandleWhileIniting(TEvConsole::TEvGetConfigItemsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
            if (record.ConfigItemsSize() == 0) {
                // cookied config item is missing, add it
                InitDefaultConfiguration();
            } else {
                Y_ABORT_UNLESS(record.ConfigItemsSize() == 1); // only one config item should have the cookie

                BLOG_D("NetClassifierUpdater found the distributable config via cookie");

                CompleteInitialization();
            }
        } else {
            BLOG_ERROR("NetClassifierUpdater failed get current distributable config version: " << record.ShortDebugString());
            InitializeAgain();
        }
    }

    STATEFN(Initing) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvConsole::TEvConfigureResponse, HandleWhileIniting);
            hFunc(TEvConsole::TEvGetConfigItemsResponse, HandleWhileIniting);
            hFunc(TEvents::TEvWakeup, HandleWhileIniting);
            hFunc(TEvents::TEvPoisonPill, HandlePoison);
        }
    }

    void CompleteInitialization() {
        BLOG_D("NetClassifierUpdater has been initialized");

        Become(&TThis::Working);
        Send(SelfId(), new TEvents::TEvWakeup);
    }

    STATEFN(Working) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleWhileWorking);
            hFunc(TEvConsole::TEvConfigureResponse, HandleWhileWorking);
            hFunc(TEvConsole::TEvGetConfigItemsResponse, HandleWhileWorking);
            hFunc(TEvents::TEvWakeup, HandleWhileWorking);
            hFunc(TEvents::TEvPoisonPill, HandlePoison);
        }
    }

    auto FormNetDataFromJson(TStringBuf jsonData) const {
        NKikimrNetClassifier::TNetData netData;
        THashSet<TString> tagsToFilter(UpdaterConfig().GetNetBoxTags().begin(), UpdaterConfig().GetNetBoxTags().end());
        NJson::TJsonValue value;
        bool res = NJson::ReadJsonTree(jsonData, &value);
        if (!res)
            return netData;
        if (!value["results"].IsArray())
            return netData;
        for (auto& v : value["results"].GetArray()) {
            if (!v["prefix"].IsString())
                return NKikimrNetClassifier::TNetData{};
            TString mask = v["prefix"].GetString();

            TString label;
            auto customFields = v.GetValueByPath("custom_fields");
            if (customFields) {
                if (!customFields->IsMap() || !(*customFields)["owner"].IsString()) {
                    return NKikimrNetClassifier::TNetData{};
                }
                auto owner = (*customFields)["owner"].GetString();
                if (tagsToFilter.empty() || tagsToFilter.contains(owner)) {
                    label = owner;
                }
            } else {
                if (!v["tags"].IsArray() || v["tags"].GetArray().size() == 0)
                    return NKikimrNetClassifier::TNetData{};
                const auto& tags = v["tags"].GetArray();
                for (auto& tag : tags) {
                    if (!tag.IsString())
                        return NKikimrNetClassifier::TNetData{};
                    if (tagsToFilter.contains(tag.GetString())) {
                        label = tag.GetString();
                        break;
                    }
                }
                if (tagsToFilter.empty()) {
                    label = tags.front().GetString();
                }
            }
            if (!label) {
                continue;
            }
            auto& subnet = *netData.AddSubnets();
            subnet.SetMask(mask);
            subnet.SetLabel(label);
        }
        return netData;
    }

    auto FormNetData(TStringBuf tsvData) const {
        NKikimrNetClassifier::TNetData netData;

        while (auto record = tsvData.NextTok('\n')) {
            auto& subnet = *netData.AddSubnets();
            subnet.SetMask(TString(record.NextTok('\t')));
            subnet.SetLabel(TString(record.NextTok('\t')));
        }

        return netData;
    }

    void ScheduleNextUpdate() {
        const TDuration interval = TDuration::Seconds(UpdaterConfig().GetNetDataUpdateIntervalSeconds());
        Schedule(interval, new TEvents::TEvWakeup);
    }

    void InitializeAgain() {
        PackedNetData = {};
        LastUpdateTimestamp = {};
        LastUpdateDatetimeUTC = {};

        Become(&TThis::Initing);

        const TDuration interval = TDuration::Seconds(UpdaterConfig().GetRetryIntervalSeconds());
        Schedule(interval, new TEvents::TEvWakeup);
    }

    void HandleWhileWorking(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& ev) {
        if (ev->Get()->Error.empty()) {
            if (ev->Get()->Response->Status == "200") {
                const auto netData = UpdaterConfig().GetFormat() == NKikimrNetClassifier::TNetClassifierUpdaterConfig::TSV
                                                    ? FormNetData(ev->Get()->Response->Body)
                                                    : FormNetDataFromJson(ev->Get()->Response->Body);
                if (netData.SubnetsSize() != 0) {
                    PackedNetData = PackAcquiredSubnets(netData);

                    LastUpdateTimestamp = TActivationContext::Now();
                    LastUpdateDatetimeUTC = LastUpdateTimestamp.ToRfc822String(); // for viewer

                    // To modify the config it's essential to find the current id and generation
                    RequestCurrentConfigViaCookie();
                    return;
                } else {
                    BLOG_ERROR("NetClassifierUpdater failed to get subnets: got empty subnets list");
                }
            } else {
                BLOG_ERROR("NetClassifierUpdater failed to get subnets: http_status=" <<ev->Get()->Response->Status);
            }
        } else {
            BLOG_ERROR("NetClassifierUpdater failed to get subnets: " << ev->Get()->Error);
        }
        InitializeAgain();
    }

    void HandleWhileWorking(TEvConsole::TEvConfigureResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
            // hurray! the update is finished
            ScheduleNextUpdate();
        } else {
            BLOG_ERROR("NetClassifierUpdater failed to update distributable config: " << record.ShortDebugString());
            InitializeAgain();
        }
    }

    void HandleWhileWorking(TEvConsole::TEvGetConfigItemsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetStatus().GetCode() == Ydb::StatusIds::SUCCESS) {
            Y_ABORT_UNLESS(record.ConfigItemsSize() == 1); // only one config item should have the cookie

            auto event = MakeHolder<TEvConsole::TEvConfigureRequest>();

            auto& configItem = *event->Record.AddActions()->MutableModifyConfigItem()->MutableConfigItem();

            // copy id, generation and cookie
            configItem.CopyFrom(ev->Get()->Record.GetConfigItems(0));

            auto& distributableConfig = *configItem.MutableConfig()->MutableNetClassifierDistributableConfig();
            distributableConfig.SetPackedNetData(PackedNetData);
            distributableConfig.SetLastUpdateDatetimeUTC(LastUpdateDatetimeUTC);
            distributableConfig.SetLastUpdateTimestamp(LastUpdateTimestamp.MicroSeconds());

            Send(LocalConsole, event.Release());
        } else {
            BLOG_ERROR("NetClassifierUpdater failed to get current distributable config version: " << record.ShortDebugString());
            InitializeAgain();
        }
    }

    TString PackAcquiredSubnets(const NKikimrNetClassifier::TNetData& netData) const {
        TString serializedProto;
        Y_PROTOBUF_SUPPRESS_NODISCARD netData.SerializeToString(&serializedProto);

        return PackNetData(serializedProto);
    }

    void HandleWhileWorking(TEvents::TEvWakeup::TPtr&) {
        // it's high time to update the networks data!
        if (!HttpProxyId) {
            HttpProxyId = Register(NHttp::CreateHttpProxy(HttpSensors));
        }

        NHttp::THttpOutgoingRequestPtr httpRequest =
            NHttp::THttpOutgoingRequest::CreateRequestGet(UpdaterConfig().GetNetDataSourceUrl());

        Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr&) {
        PassAway();
    }

private:
    TActorId LocalConsole;
    TActorId HttpProxyId;
    std::shared_ptr<NMonitoring::TMetricRegistry> HttpSensors;

    TString PackedNetData;
    TString LastUpdateDatetimeUTC;
    TInstant LastUpdateTimestamp;
};

IActor* MakeNetClassifierUpdaterActor(TActorId localConsole) {
    return new NetClassifierUpdater(localConsole);
}

TString UnpackNetData(const TString& packedNetData) {
    TStringInput stream(packedNetData);
    try {
        return TZLibDecompress(&stream, ZLib::GZip).ReadAll();
    } catch (const yexception& e) {
        return {};
    }
}

} // namespace NKikimr::NNetClassifierUpdater
