#include "config_helpers.h"
#include "configs_dispatcher.h"
#include "console_configs_subscriber.h"
#include "console.h"
#include "http.h"
#include "util.h"

#include <ydb/core/cms/console/util/config_index.h>
#include <ydb/library/yaml_config/util.h>
#include <ydb/library/yaml_config/yaml_config.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/config/init/mock.h>
#include <ydb/core/base/counters.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <ydb/core/config/init/init.h>

#include <util/generic/bitmap.h>
#include <util/generic/ptr.h>
#include <util/string/join.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR || defined BLOG_TRACE
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)
#define BLOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)
#define BLOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)
#define BLOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)
#define BLOG_TRACE(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::CONFIGS_DISPATCHER, stream)

namespace NKikimr::NConsole {

using namespace NConfig;

const THashSet<ui32> DYNAMIC_KINDS({
    (ui32)NKikimrConsole::TConfigItem::ActorSystemConfigItem,
    (ui32)NKikimrConsole::TConfigItem::BootstrapConfigItem,
    (ui32)NKikimrConsole::TConfigItem::CmsConfigItem,
    (ui32)NKikimrConsole::TConfigItem::CompactionConfigItem,
    (ui32)NKikimrConsole::TConfigItem::ConfigsDispatcherConfigItem,
    (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem,
    (ui32)NKikimrConsole::TConfigItem::HiveConfigItem,
    (ui32)NKikimrConsole::TConfigItem::ImmediateControlsConfigItem,
    (ui32)NKikimrConsole::TConfigItem::LogConfigItem,
    (ui32)NKikimrConsole::TConfigItem::MonitoringConfigItem,
    (ui32)NKikimrConsole::TConfigItem::NameserviceConfigItem,
    (ui32)NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem,
    (ui32)NKikimrConsole::TConfigItem::NodeBrokerConfigItem,
    (ui32)NKikimrConsole::TConfigItem::QueryServiceConfigItem,
    (ui32)NKikimrConsole::TConfigItem::SchemeShardConfigItem,
    (ui32)NKikimrConsole::TConfigItem::SharedCacheConfigItem,
    (ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem,
    (ui32)NKikimrConsole::TConfigItem::TableServiceConfigItem,
    (ui32)NKikimrConsole::TConfigItem::TenantPoolConfigItem,
    (ui32)NKikimrConsole::TConfigItem::TenantSlotBrokerConfigItem,
    (ui32)NKikimrConsole::TConfigItem::AllowEditYamlInUiItem,
    (ui32)NKikimrConsole::TConfigItem::BackgroundCleaningConfigItem,
    (ui32)NKikimrConsole::TConfigItem::TracingConfigItem,
    (ui32)NKikimrConsole::TConfigItem::BlobStorageConfigItem,
    (ui32)NKikimrConsole::TConfigItem::MetadataCacheConfigItem,
    (ui32)NKikimrConsole::TConfigItem::MemoryControllerConfigItem,
});

const THashSet<ui32> NON_YAML_KINDS({
    (ui32)NKikimrConsole::TConfigItem::NameserviceConfigItem,
    (ui32)NKikimrConsole::TConfigItem::NetClassifierDistributableConfigItem,
});

class TConfigsDispatcher : public TActorBootstrapped<TConfigsDispatcher> {
private:
    using TBase = TActorBootstrapped<TConfigsDispatcher>;

    struct TConfig {
        NKikimrConfig::TConfigVersion Version;
        NKikimrConfig::TAppConfig Config;
    };

    struct TYamlVersion {
        ui64 Version;
        TMap<ui64, ui64> VolatileVersions;
    };

    /**
     * Structure to describe configs subscription shared by multiple
     * dispatcher subscribers.
     */
    struct TSubscription : public TThrRefBase {
        using TPtr = TIntrusivePtr<TSubscription>;

        TDynBitMap Kinds;
        THashMap<TActorId, ui64> Subscribers;

        // Set to true for all yaml kinds.
        // Some 'legacy' kinds, which is usually managed by some automation e.g. NetClassifierDistributableConfigItem
        // Left this field false and consume old console configs
        bool Yaml = false;

        // Set to true if there were no config update notifications
        // Last config which was delivered to all subscribers.
        TConfig CurrentConfig;

        // If any yaml config delivered to all subscribers and acknowleged by them
        // This field is set to version from this yaml config
        std::optional<TYamlVersion> YamlVersion;

        // Config update which is currently delivered to subscribers.
        THolder<TEvConsole::TEvConfigNotificationRequest> UpdateInProcess = nullptr;
        NKikimrConfig::TConfigVersion UpdateInProcessConfigVersion;
        ui64 UpdateInProcessCookie;
        std::optional<TYamlVersion> UpdateInProcessYamlVersion;

        // Subscribers who didn't respond yet to the latest config update.
        THashSet<TActorId> SubscribersToUpdate;

    };

    /**
     * Structure to describe subscribers. Subscriber can have multiple
     * subscriptions but we allow only single subscription per external
     * client. The only client who can have multiple subscriptions is
     * dispatcher itself.
     */
    struct TSubscriber : public TThrRefBase {
        using TPtr = TIntrusivePtr<TSubscriber>;

        TActorId Subscriber;
        THashSet<TSubscription::TPtr> Subscriptions;
        NKikimrConfig::TConfigVersion CurrentConfigVersion;
    };

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::CONFIGS_DISPATCHER_ACTOR;
    }

    TConfigsDispatcher(const TConfigsDispatcherInitInfo& initInfo);

    void Bootstrap();

    void EnqueueEvent(TAutoPtr<IEventHandle> &ev);
    void ProcessEnqueuedEvents();

    void SendUpdateToSubscriber(TSubscription::TPtr subscription, TActorId subscriber);

    TSubscription::TPtr FindSubscription(const TActorId &subscriber);
    TSubscription::TPtr FindSubscription(const TDynBitMap &kinds);

    TSubscriber::TPtr FindSubscriber(TActorId aid);

    void UpdateYamlVersion(const TSubscription::TPtr &kinds) const;

    struct TCheckKindsResult {
        bool HasYamlKinds = false;
        bool HasNonYamlKinds = false;
    };

    TCheckKindsResult CheckKinds(const TVector<ui32>& kinds, const char* errorContext) const;

    NKikimrConfig::TAppConfig ParseYamlProtoConfig();

    TDynBitMap FilterKinds(const TDynBitMap& in);

    void UpdateCandidateStartupConfig(TEvConsole::TEvConfigSubscriptionNotification::TPtr &ev);

    void Handle(NMon::TEvHttpInfo::TPtr &ev);
    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev);
    void Handle(TEvConsole::TEvConfigSubscriptionNotification::TPtr &ev);
    void Handle(TEvConsole::TEvConfigSubscriptionError::TPtr &ev);
    void Handle(TEvConsole::TEvConfigNotificationResponse::TPtr &ev);
    void Handle(TEvConfigsDispatcher::TEvGetConfigRequest::TPtr &ev);
    void Handle(TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest::TPtr &ev);
    void Handle(TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest::TPtr &ev);
    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev);
    void Handle(TEvConsole::TEvGetNodeLabelsRequest::TPtr &ev);

    void ReplyMonJson(TActorId mailbox);

    STATEFN(StateInit)
    {
        TRACE_EVENT(NKikimrServices::CONFIGS_DISPATCHER);
        switch (ev->GetTypeRewrite()) {
            // Monitoring page
            hFuncTraced(NMon::TEvHttpInfo, Handle);
            hFuncTraced(TEvInterconnect::TEvNodesInfo, Handle);
            // Updates from console
            hFuncTraced(TEvConsole::TEvConfigSubscriptionNotification, Handle);
            hFuncTraced(TEvConsole::TEvConfigSubscriptionError, Handle);
            // Events from clients
            hFuncTraced(TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest, Handle);
            hFuncTraced(TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest, Handle);
            // Resolve
            hFunc(TEvConsole::TEvGetNodeLabelsRequest, Handle);
        default:
            EnqueueEvent(ev);
            break;
        }
    }

    STATEFN(StateWork)
    {
        TRACE_EVENT(NKikimrServices::CONFIGS_DISPATCHER);
        switch (ev->GetTypeRewrite()) {
            // Monitoring page
            hFuncTraced(NMon::TEvHttpInfo, Handle);
            hFuncTraced(TEvInterconnect::TEvNodesInfo, Handle);
            // Updates from console
            hFuncTraced(TEvConsole::TEvConfigSubscriptionNotification, Handle);
            hFuncTraced(TEvConsole::TEvConfigSubscriptionError, Handle);
            // Events from clients
            hFuncTraced(TEvConfigsDispatcher::TEvGetConfigRequest, Handle);
            hFuncTraced(TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest, Handle);
            hFuncTraced(TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest, Handle);
            hFuncTraced(TEvConsole::TEvConfigNotificationResponse, Handle);
            IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            // Resolve
            hFunc(TEvConsole::TEvGetNodeLabelsRequest, Handle);

            // Ignore these console requests until we get rid of persistent subscriptions-related code
            IgnoreFunc(TEvConsole::TEvAddConfigSubscriptionResponse);
            IgnoreFunc(TEvConsole::TEvGetNodeConfigResponse);
            // Pretend we got this
            hFuncTraced(TEvConsole::TEvConfigNotificationRequest, Handle);
        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }


private:
    const TMap<TString, TString> Labels;
    const std::variant<std::monostate, TDenyList, TAllowList> ItemsServeRules;
    const NKikimrConfig::TAppConfig BaseConfig;
    NKikimrConfig::TAppConfig CurrentConfig;
    NKikimrConfig::TAppConfig CandidateStartupConfig;
    bool StartupConfigProcessError = false;
    bool StartupConfigProcessDiff = false;
    TString StartupConfigInfo;
    ::NMonitoring::TDynamicCounters::TCounterPtr StartupConfigChanged;
    const std::optional<TDebugInfo> DebugInfo;
    std::shared_ptr<NConfig::TRecordedInitialConfiguratorDeps> RecordedInitialConfiguratorDeps;
    std::vector<TString> Args;
    ui64 NextRequestCookie;
    TVector<TActorId> HttpRequests;
    TActorId CommonSubscriptionClient;
    TDeque<TAutoPtr<IEventHandle>> EventsQueue;

    THashMap<TActorId, TSubscription::TPtr> SubscriptionsBySubscriber;
    THashMap<TDynBitMap, TSubscription::TPtr> SubscriptionsByKinds;
    THashMap<TActorId, TSubscriber::TPtr> Subscribers;

    TString YamlConfig;
    TMap<ui64, TString> VolatileYamlConfigs;
    TMap<ui64, size_t> VolatileYamlConfigHashes;
    TString ResolvedYamlConfig;
    TString ResolvedJsonConfig;
    NKikimrConfig::TAppConfig YamlProtoConfig;
    bool YamlConfigEnabled = false;

};

TConfigsDispatcher::TConfigsDispatcher(const TConfigsDispatcherInitInfo& initInfo)
        : Labels(initInfo.Labels)
        , ItemsServeRules(initInfo.ItemsServeRules)
        , BaseConfig(initInfo.InitialConfig)
        , CurrentConfig(initInfo.InitialConfig)
        , CandidateStartupConfig(initInfo.InitialConfig)
        , DebugInfo(initInfo.DebugInfo)
        , RecordedInitialConfiguratorDeps(std::move(initInfo.RecordedInitialConfiguratorDeps))
        , Args(initInfo.Args)
        , NextRequestCookie(Now().GetValue())
{}

void TConfigsDispatcher::Bootstrap()
{
    BLOG_D("TConfigsDispatcher Bootstrap");

    NActors::TMon *mon = AppData()->Mon;
    if (mon) {
        NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
        mon->RegisterActorPage(actorsMonPage, "configs_dispatcher", "Configs Dispatcher", false, TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
    }
    TIntrusivePtr<NMonitoring::TDynamicCounters> rootCounters = AppData()->Counters;
    TIntrusivePtr<NMonitoring::TDynamicCounters> authCounters = GetServiceCounters(rootCounters, "config");
    NMonitoring::TDynamicCounterPtr counters = authCounters->GetSubgroup("subsystem", "ConfigsDispatcher");
    StartupConfigChanged = counters->GetCounter("StartupConfigChanged", true);

    auto commonClient = CreateConfigsSubscriber(
        SelfId(),
        TVector<ui32>(DYNAMIC_KINDS.begin(), DYNAMIC_KINDS.end()),
        CurrentConfig,
        0,
        true,
        1);
    CommonSubscriptionClient = RegisterWithSameMailbox(commonClient);

    Become(&TThis::StateInit);
}

void TConfigsDispatcher::EnqueueEvent(TAutoPtr<IEventHandle> &ev)
{
    BLOG_D("Enqueue event type: " << ev->GetTypeRewrite());
    EventsQueue.push_back(ev);
}

void TConfigsDispatcher::ProcessEnqueuedEvents()
{
    while (!EventsQueue.empty()) {
        TAutoPtr<IEventHandle> &ev = EventsQueue.front();
        BLOG_D("Dequeue event type: " << ev->GetTypeRewrite());
        TlsActivationContext->Send(ev.Release());
        EventsQueue.pop_front();
    }
}

void TConfigsDispatcher::SendUpdateToSubscriber(TSubscription::TPtr subscription, TActorId subscriber)
{
    Y_ABORT_UNLESS(subscription->UpdateInProcess);

    subscription->SubscribersToUpdate.insert(subscriber);

    auto notification = MakeHolder<TEvConsole::TEvConfigNotificationRequest>();
    notification->Record.CopyFrom(subscription->UpdateInProcess->Record);

    BLOG_TRACE("Send TEvConsole::TEvConfigNotificationRequest to " << subscriber
                << ": " << notification->Record.ShortDebugString());

    Send(subscriber, notification.Release(), 0, subscription->UpdateInProcessCookie);
}

TConfigsDispatcher::TSubscription::TPtr TConfigsDispatcher::FindSubscription(const TDynBitMap &kinds)
{
    if (auto it = SubscriptionsByKinds.find(kinds); it != SubscriptionsByKinds.end())
        return it->second;

    return nullptr;
}

TConfigsDispatcher::TSubscription::TPtr TConfigsDispatcher::FindSubscription(const TActorId &id)
{
    if (auto it = SubscriptionsBySubscriber.find(id); it != SubscriptionsBySubscriber.end())
        return it->second;

    return nullptr;
}

TConfigsDispatcher::TSubscriber::TPtr TConfigsDispatcher::FindSubscriber(TActorId aid)
{
    if (auto it = Subscribers.find(aid); it != Subscribers.end())
        return it->second;

    return nullptr;
}

NKikimrConfig::TAppConfig TConfigsDispatcher::ParseYamlProtoConfig()
{
    NKikimrConfig::TAppConfig newYamlProtoConfig = {};

    try {
        NYamlConfig::ResolveAndParseYamlConfig(
            YamlConfig,
            VolatileYamlConfigs,
            Labels,
            newYamlProtoConfig,
            &ResolvedYamlConfig,
            &ResolvedJsonConfig);
    } catch (const yexception& ex) {
        BLOG_ERROR("Got invalid config from console error# " << ex.what());
    }

    return newYamlProtoConfig;
}

void TConfigsDispatcher::Handle(NMon::TEvHttpInfo::TPtr &ev)
{
    if (auto it = ev->Get()->Request.GetHeaders().FindHeader("Content-Type"); it && it->Value() == "application/json") {
        ReplyMonJson(ev->Sender);
        return;
    }

    if (HttpRequests.empty())
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes);

    HttpRequests.push_back(ev->Sender);
}

void TConfigsDispatcher::ReplyMonJson(TActorId mailbox) {
    TStringStream str;
    str << NMonitoring::HTTPOKJSON;

    NJson::TJsonValue response;
    response.SetType(NJson::EJsonValueType::JSON_MAP);

    auto& labels = response["labels"];
    labels.SetType(NJson::EJsonValueType::JSON_ARRAY);
    for (auto &[key, value] : Labels) {
        NJson::TJsonValue label;
        label.SetType(NJson::EJsonValueType::JSON_MAP);
        label.InsertValue("name", key);
        label.InsertValue("value", value);
        labels.AppendValue(std::move(label));
    }

    response.InsertValue("yaml_config", YamlConfig);
    response.InsertValue("resolved_json_config", NJson::ReadJsonFastTree(ResolvedJsonConfig, true));
    response.InsertValue("current_json_config", NJson::ReadJsonFastTree(NProtobufJson::Proto2Json(CurrentConfig, NYamlConfig::GetProto2JsonConfig()), true));

    if (DebugInfo) {
        // TODO: write custom json serializer for security fields
        // for now json info not documented and used only for some very specifigc
        // debug purproses, so we can disable it for now without any risks
        // and postpone implementation
        //
        // response.InsertValue("initial_json_config", NJson::ReadJsonFastTree(NProtobufJson::Proto2Json(DebugInfo->StaticConfig, NYamlConfig::GetProto2JsonConfig()), true));
        response.InsertValue("initial_cms_json_config", NJson::ReadJsonFastTree(NProtobufJson::Proto2Json(DebugInfo->OldDynConfig, NYamlConfig::GetProto2JsonConfig()), true));
        response.InsertValue("initial_cms_yaml_json_config", NJson::ReadJsonFastTree(NProtobufJson::Proto2Json(DebugInfo->NewDynConfig, NYamlConfig::GetProto2JsonConfig()), true));
    }

    NJson::WriteJson(&str, &response, {});

    Send(mailbox, new NMon::TEvHttpInfoRes(str.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
}

void TConfigsDispatcher::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev)
{
    const auto &rec = ev->Get()->Record;
    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);

    BLOG_TRACE("Send TEvConfigNotificationResponse: " << resp->Record.ShortDebugString());

    Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}


TDynBitMap TConfigsDispatcher::FilterKinds(const TDynBitMap& in) {
    TDynBitMap out;

    if (const auto* denyList = std::get_if<TDenyList>(&ItemsServeRules)) {
        Y_FOR_EACH_BIT(kind, in) {
            if (!denyList->Items.contains(kind)) {
                out.Set(kind);
            }
        }
    } else if (const auto* allowList = std::get_if<TAllowList>(&ItemsServeRules)) {
        Y_FOR_EACH_BIT(kind, in) {
            if (allowList->Items.contains(kind)) {
                out.Set(kind);
            }
        }
    } else {
        out = in;
    }

    return out;
}

void TConfigsDispatcher::Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev)
{
    Y_UNUSED(ev);
    TStringStream str;
    str << NMonitoring::HTTPOKHTML;
    HTML(str) {
        HEAD() {
            str << "<link rel='stylesheet' href='../cms/ext/bootstrap.min.css'>" << Endl
                << "<script language='javascript' type='text/javascript' src='../cms/ext/jquery.min.js'></script>" << Endl
                << "<script language='javascript' type='text/javascript' src='../cms/ext/bootstrap.bundle.min.js'></script>" << Endl
                << "<script language='javascript' type='text/javascript'>" << Endl
                << "var nodeNames = [";

            for (auto &node: ev->Get()->Nodes) {
                str << "{'nodeName':'" << node.Host << "'}, ";
            }

            str << "];" << Endl
                << "</script>" << Endl
                << "<script src='../cms/ext/fuse.min.js'></script>" << Endl
                << "<script src='../cms/common.js'></script>" << Endl
                << "<script src='../cms/ext/fuzzycomplete.min.js'></script>" << Endl
                << "<link rel='stylesheet' href='../cms/ext/fuzzycomplete.min.css'>" << Endl
                << "<link rel='stylesheet' href='../cms/cms.css'>" << Endl
                << "<script data-main='../cms/configs_dispatcher_main' src='../cms/ext/require.min.js'></script>" << Endl;

        }
        NHttp::OutputStyles(str);

        DIV() {
            OL_CLASS("breadcrumb") {
                LI_CLASS("breadcrumb-item") {
                    str << "<a href='..' id='host-ref'>YDB Developer UI</a>" << Endl;
                }
                LI_CLASS("breadcrumb-item") {
                    str << "<a href='.'>Actors</a>" << Endl;
                }
                LI_CLASS("breadcrumb-item active") {
                    str << "Configs Dispatcher" << Endl;
                }
            }
        }

        DIV_CLASS("container") {
            DIV_CLASS("navbar navbar-expand-lg navbar-light bg-light") {
                DIV_CLASS("navbar-collapse") {
                    UL_CLASS("navbar-nav mr-auto") {
                        LI_CLASS("nav-item") {
                            str << "<a class='nav-link' href=\"../cms?#page=yaml-config\">Console</a>" << Endl;
                        }
                    }
                    FORM_CLASS("form-inline my-2 my-lg-0") {
                        str << "<input type='text' id='nodePicker' class='form-control mr-sm-2' name='nodes' placeholder='Nodes...'>" << Endl;
                        str << "<a type='button' class='btn btn-primary my-2 my-sm-0' id='nodesGo'>Go</a>" << Endl;
                    }
                }
            }
            DIV_CLASS("tab-left") {
                COLLAPSED_REF_CONTENT("node-labels", "Node labels") {
                    PRE() {
                        for (auto &[key, value] : Labels) {
                            str << key << " = " << value << Endl;
                        }
                    }
                }
                str << "<br />" << Endl;
                COLLAPSED_REF_CONTENT("effective-config", "Effective config") {
                    str << "<div class=\"alert alert-primary tab-left\" role=\"alert\">" << Endl;
                    str << "It is assumed that all config items marked dynamic are consumed by corresponding components at runtime." << Endl;
                    str << "<br />" << Endl;
                    str << "If any component is unable to update some config at runtime check \"Effective startup config\" below to see actual config for this component." << Endl;
                    str << "<br />" << Endl;
                    str << "Coloring: \"<font color=\"red\">config not set</font>\","
                        << " \"<font color=\"green\">config set in dynamic config</font>\", \"<font color=\"#007bff\">config set in static config</font>\"" << Endl;
                    str << "</div>" << Endl;
                    NHttp::OutputRichConfigHTML(str, BaseConfig, YamlProtoConfig, CurrentConfig, DYNAMIC_KINDS, NON_YAML_KINDS, YamlConfigEnabled);
                }
                str << "<br />" << Endl;
                COLLAPSED_REF_CONTENT("effective-startup-config", "Effective startup config") {
                    str << "<div class=\"alert alert-primary tab-left\" role=\"alert\">" << Endl;
                    str << "Some of these configs may be overwritten by dynamic ones." << Endl;
                    str << "</div>" << Endl;
                    NHttp::OutputConfigHTML(str, BaseConfig);
                }
                str << "<br />" << Endl;
                COLLAPSED_REF_CONTENT("effective-dynamic-config", "Effective dynamic config") {
                    str << "<div class=\"alert alert-primary tab-left\" role=\"alert\">" << Endl;
                    str << "Some subscribers may get static configs if they exist even if dynamic one of corresponding kind is not present." << Endl;
                    str << "</div>" << Endl;
                    NKikimrConfig::TAppConfig trunc;
                    if (YamlConfigEnabled) {
                        ReplaceConfigItems(YamlProtoConfig, trunc, FilterKinds(KindsToBitMap(DYNAMIC_KINDS)), BaseConfig);
                        ReplaceConfigItems(CurrentConfig, trunc, FilterKinds(KindsToBitMap(NON_YAML_KINDS)), trunc, false);
                    } else {
                        ReplaceConfigItems(CurrentConfig, trunc, FilterKinds(KindsToBitMap(DYNAMIC_KINDS)), BaseConfig);
                    }
                    NHttp::OutputConfigHTML(str, trunc);
                }
                str << "<br />" << Endl;
                COLLAPSED_REF_CONTENT("debug-info", "Debug info") {
                    DIV_CLASS("tab-left") {
                        COLLAPSED_REF_CONTENT("args", "Startup process args") {
                            PRE() {
                                for (auto& arg : Args) {
                                    str << "\"" << arg << "\" ";
                                }
                            }
                        }
                        str << "<br />" << Endl;
                        COLLAPSED_REF_CONTENT("candidate-startup-config", "Candidate startup config") {
                            str << "<div class=\"alert alert-primary tab-left\" role=\"alert\">" << Endl;
                            if (StartupConfigProcessError) {
                                str << "<b>Error: </b>" << Endl;
                                PRE() {
                                    str << StartupConfigInfo;
                                }
                            } else if (StartupConfigProcessDiff) {
                                str << "<b>Configs are different: </b>" << Endl;
                                PRE() {
                                    str << StartupConfigInfo;
                                }
                            } else {
                                str << "<b>Configs are same.</b>" << Endl;
                            }
                            str << "</div>" << Endl;
                            NHttp::OutputConfigHTML(str, CandidateStartupConfig);
                        }
                        str << "<br />" << Endl;
                        COLLAPSED_REF_CONTENT("effective-config-debug-info", "Effective config debug info") {
                            NHttp::OutputConfigDebugInfoHTML(
                                str,
                                BaseConfig,
                                YamlProtoConfig,
                                CurrentConfig,
                                {DebugInfo ? DebugInfo->InitInfo : THashMap<ui32, TConfigItemInfo>{}},
                                DYNAMIC_KINDS,
                                NON_YAML_KINDS,
                                YamlConfigEnabled);
                        }
                        str << "<br />" << Endl;
                        COLLAPSED_REF_CONTENT("state", "State") {
                            PRE() {
                                str << "SelfId: " << SelfId() << Endl;
                                auto s = CurrentStateFunc();
                                str << "State: " << ( s == &TThis::StateWork      ? "StateWork"
                                                    : s == &TThis::StateInit      ? "StateInit"
                                                                                  : "Unknown" ) << Endl;
                                str << "YamlConfigEnabled: " << YamlConfigEnabled << Endl;
                                str << "Subscriptions: " << Endl;
                                for (auto &[kinds, subscription] : SubscriptionsByKinds) {
                                    str << "- Kinds: " << KindsToString(kinds) << Endl
                                        << "  Subscription: " << Endl
                                        << "    Yaml: " << subscription->Yaml << Endl
                                        << "    Subscribers: " << Endl;
                                    for (auto &[id, updates] : subscription->Subscribers) {
                                        str << "    - Actor: " << id << Endl;
                                        str << "      UpdatesSent: " << updates << Endl;
                                    }
                                    if (subscription->YamlVersion) {
                                        str << "    YamlVersion: " << subscription->YamlVersion->Version << ".[";
                                        bool first = true;
                                        for (auto &[id, hash] : subscription->YamlVersion->VolatileVersions) {
                                            str << (first ? "" : ",") << id << "." << hash;
                                            first = false;
                                        }
                                        str << "]" << Endl;
                                    } else {
                                        str << "    CurrentConfigId: " << subscription->CurrentConfig.Version.ShortDebugString() << Endl;
                                    }
                                    str << "    CurrentConfig: " << subscription->CurrentConfig.Config.ShortDebugString() << Endl;
                                    if (subscription->UpdateInProcess) {
                                        str << "    UpdateInProcess: " << subscription->UpdateInProcess->Record.ShortDebugString() << Endl
                                            << "    SubscribersToUpdate:";
                                        for (auto &id : subscription->SubscribersToUpdate) {
                                            str << " " << id;
                                        }
                                        str << Endl;
                                        str << "    UpdateInProcessConfigVersion: " << subscription->UpdateInProcessConfigVersion.ShortDebugString() << Endl
                                            << "    UpdateInProcessCookie: " << subscription->UpdateInProcessCookie << Endl;
                                        if (subscription->UpdateInProcessYamlVersion) {
                                            str << "    UpdateInProcessYamlVersion: " << subscription->UpdateInProcessYamlVersion->Version << Endl;
                                        }
                                    }
                                }
                                str << "Subscribers:" << Endl;
                                for (auto &[subscriber, _] : SubscriptionsBySubscriber) {
                                    str << "- " << subscriber << Endl;
                                }
                            }
                        }
                        str << "<br />" << Endl;
                        COLLAPSED_REF_CONTENT("yaml-config", "YAML config") {
                            DIV() {
                                TAG(TH5) {
                                    str << "Persistent Config" << Endl;
                                }
                                TAG_CLASS_STYLE(TDiv, "configs-dispatcher", "padding: 0 12px;") {
                                    TAG_ATTRS(TDiv, {{"class", "yaml-sticky-btn-wrap fold-yaml-config yaml-btn-3"}, {"id", "fold-yaml-config"}, {"title", "fold"}}) {
                                        DIV_CLASS("yaml-sticky-btn") { }
                                    }
                                    TAG_ATTRS(TDiv, {{"class", "yaml-sticky-btn-wrap unfold-yaml-config yaml-btn-2"}, {"id", "unfold-yaml-config"}, {"title", "unfold"}}) {
                                        DIV_CLASS("yaml-sticky-btn") { }
                                    }
                                    TAG_ATTRS(TDiv, {{"class", "yaml-sticky-btn-wrap copy-yaml-config yaml-btn-1"}, {"id", "copy-yaml-config"}, {"title", "copy"}}) {
                                        DIV_CLASS("yaml-sticky-btn") { }
                                    }
                                    TAG_ATTRS(TDiv, {{"id", "yaml-config-item"}, {"name", "yaml-config-itemm"}}) {
                                        str << YamlConfig;
                                    }
                                }
                                str << "<hr/>" << Endl;
                                for (auto &[id, config] : VolatileYamlConfigs) {
                                    DIV() {
                                        TAG(TH5) {
                                            str << "Volatile Config Id: " << id << Endl;
                                        }
                                        TAG_CLASS_STYLE(TDiv, "configs-dispatcher", "padding: 0 12px;") {
                                            TAG_ATTRS(TDiv, {{"class", "yaml-sticky-btn-wrap fold-yaml-config yaml-btn-3"}, {"title", "fold"}}) {
                                                DIV_CLASS("yaml-sticky-btn") { }
                                            }
                                            TAG_ATTRS(TDiv, {{"class", "yaml-sticky-btn-wrap unfold-yaml-config yaml-btn-2"}, {"title", "unfold"}}) {
                                                DIV_CLASS("yaml-sticky-btn") { }
                                            }
                                            TAG_ATTRS(TDiv, {{"class", "yaml-sticky-btn-wrap copy-yaml-config yaml-btn-1"}, {"title", "copy"}}) {
                                                DIV_CLASS("yaml-sticky-btn") { }
                                            }
                                            DIV_CLASS("yaml-config-item") {
                                                str << config;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        str << "<br />" << Endl;
                        COLLAPSED_REF_CONTENT("resolved-yaml-config", "Resolved YAML config") {
                            TAG_CLASS_STYLE(TDiv, "configs-dispatcher", "padding: 0 12px;") {
                                TAG_ATTRS(TDiv, {{"class", "yaml-sticky-btn-wrap fold-yaml-config yaml-btn-3"}, {"id", "fold-resolved-yaml-config"}, {"title", "fold"}}) {
                                    DIV_CLASS("yaml-sticky-btn") { }
                                }
                                TAG_ATTRS(TDiv, {{"class", "yaml-sticky-btn-wrap unfold-yaml-config yaml-btn-2"}, {"id", "unfold-resolved-yaml-config"}, {"title", "unfold"}}) {
                                    DIV_CLASS("yaml-sticky-btn") { }
                                }
                                TAG_ATTRS(TDiv, {{"class", "yaml-sticky-btn-wrap copy-yaml-config yaml-btn-1"}, {"id", "copy-resolved-yaml-config"}, {"title", "copy"}}) {
                                    DIV_CLASS("yaml-sticky-btn") { }
                                }
                                TAG_ATTRS(TDiv, {{"id", "resolved-yaml-config-item"}, {"name", "resolved-yaml-config-itemm"}}) {
                                    str << ResolvedYamlConfig;
                                }
                            }
                        }
                        str << "<br />" << Endl;
                        COLLAPSED_REF_CONTENT("resolved-json-config", "Resolved JSON config") {
                            PRE() {
                                str << ResolvedJsonConfig << Endl;
                            }
                        }
                        str << "<br />" << Endl;
                        COLLAPSED_REF_CONTENT("yaml-proto-config", "YAML proto config") {
                            NHttp::OutputConfigHTML(str, YamlProtoConfig);
                        }
                        str << "<br />" << Endl;
                        COLLAPSED_REF_CONTENT("current-config", "Current config") {
                            NHttp::OutputConfigHTML(str, CurrentConfig);
                        }
                        str << "<br />" << Endl;
                        COLLAPSED_REF_CONTENT("initial-config", "Initial config") {
                            NHttp::OutputConfigHTML(str, BaseConfig);
                        }
                        if  (DebugInfo) {
                            str << "<br />" << Endl;
                            COLLAPSED_REF_CONTENT("initial-cms-config", "Initial CMS config") {
                                NHttp::OutputConfigHTML(str, DebugInfo->OldDynConfig);
                            }
                            str << "<br />" << Endl;
                            COLLAPSED_REF_CONTENT("initial-cms-yaml-config", "Initial CMS YAML config") {
                                NHttp::OutputConfigHTML(str, DebugInfo->NewDynConfig);
                            }
                        }
                    }
                }
            }
        }
    }

    for (auto &actor : HttpRequests) {
        Send(actor, new NMon::TEvHttpInfoRes(str.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
    }

    HttpRequests.clear();
}

class TConfigurationResult
    : public IConfigurationResult
{
public:
    // TODO make ref
    const NKikimrConfig::TAppConfig& GetConfig() const {
        return Config;
    }

    bool HasYamlConfig() const {
        return !YamlConfig.empty();
    }

    const TString& GetYamlConfig() const {
        return YamlConfig;
    }

    TMap<ui64, TString> GetVolatileYamlConfigs() const {
        return VolatileYamlConfigs;
    }

    NKikimrConfig::TAppConfig Config;
    TString YamlConfig;
    TMap<ui64, TString> VolatileYamlConfigs;
};

void TConfigsDispatcher::UpdateCandidateStartupConfig(TEvConsole::TEvConfigSubscriptionNotification::TPtr &ev)
try {
    if (!RecordedInitialConfiguratorDeps) {
        CandidateStartupConfig = {};
        StartupConfigProcessError = true;
        StartupConfigProcessDiff = false;
        StartupConfigInfo = "Startup params not recorded. Corresponding functionality won't work.";
        *StartupConfigChanged = 0;
        return;
    }

    auto &rec = ev->Get()->Record;

    auto dcClient = std::make_unique<TDynConfigClientMock>();
    auto configs = std::make_shared<TConfigurationResult>();
    dcClient->SavedResult = configs;
    configs->Config = rec.GetRawConsoleConfig();
    configs->YamlConfig = rec.GetYamlConfig();
    // TODO volatile
    RecordedInitialConfiguratorDeps->DynConfigClient = std::move(dcClient);
    auto deps = RecordedInitialConfiguratorDeps->GetDeps();
    NConfig::TInitialConfigurator initCfg(deps);

    std::vector<const char*> argv;

    for (const auto& arg : Args) {
        argv.push_back(arg.data());
    }

    NLastGetopt::TOpts opts;
    initCfg.RegisterCliOptions(opts);
    deps.ProtoConfigFileProvider.RegisterCliOptions(opts);

    NLastGetopt::TOptsParseResultException parseResult(&opts, argv.size(), argv.data());

    initCfg.ValidateOptions(opts, parseResult);
    initCfg.Parse(parseResult.GetFreeArgs());

    NKikimrConfig::TAppConfig appConfig;
    ui32 nodeId;
    TKikimrScopeId scopeId;
    TString tenantName;
    TBasicKikimrServicesMask servicesMask;
    TString clusterName;
    NConfig::TConfigsDispatcherInitInfo configsDispatcherInitInfo;

    initCfg.Apply(
        appConfig,
        nodeId,
        scopeId,
        tenantName,
        servicesMask,
        clusterName,
        configsDispatcherInitInfo);

    CandidateStartupConfig = appConfig;
    StartupConfigProcessError = false;
    StartupConfigProcessDiff = false;
    StartupConfigInfo.clear();
    google::protobuf::util::MessageDifferencer md;
    auto fieldComparator = google::protobuf::util::DefaultFieldComparator();
    md.set_field_comparator(&fieldComparator);
    md.ReportDifferencesToString(&StartupConfigInfo);
    StartupConfigProcessDiff = !md.Compare(BaseConfig, CandidateStartupConfig);
    *StartupConfigChanged = StartupConfigProcessDiff ? 1 : 0;
}
catch (...) {
    CandidateStartupConfig = {};
    StartupConfigProcessError = true;
    StartupConfigProcessDiff = false;
    StartupConfigInfo = "Got exception while processing candidate config.";
    *StartupConfigChanged = 1;
}

void TConfigsDispatcher::Handle(TEvConsole::TEvConfigSubscriptionNotification::TPtr &ev)
{
    auto &rec = ev->Get()->Record;

    UpdateCandidateStartupConfig(ev);

    CurrentConfig = rec.GetConfig();

    const auto& newYamlConfig = rec.GetYamlConfig();

    bool isYamlChanged = newYamlConfig != YamlConfig;

    if (rec.VolatileConfigsSize() != VolatileYamlConfigs.size()) {
        isYamlChanged = true;
    }

    for (auto &volatileConfig : rec.GetVolatileConfigs()) {
        if (auto it = VolatileYamlConfigHashes.find(volatileConfig.GetId());
            it == VolatileYamlConfigHashes.end() || it->second != THash<TString>()(volatileConfig.GetConfig())) {
            isYamlChanged = true;
        }
    }

    if (isYamlChanged) {
        YamlConfig = newYamlConfig;
        VolatileYamlConfigs.clear();
        VolatileYamlConfigHashes.clear();
        for (auto &volatileConfig : rec.GetVolatileConfigs()) {
            VolatileYamlConfigs[volatileConfig.GetId()] = volatileConfig.GetConfig();
            VolatileYamlConfigHashes[volatileConfig.GetId()] = THash<TString>()(volatileConfig.GetConfig());
        }
    }

    NKikimrConfig::TAppConfig newYamlProtoConfig = {};

    bool yamlConfigTurnedOff = false;

    if (!YamlConfig.empty() && isYamlChanged) {
        newYamlProtoConfig = ParseYamlProtoConfig();
        bool wasYamlConfigEnabled = YamlConfigEnabled;
        YamlConfigEnabled = newYamlProtoConfig.HasYamlConfigEnabled() && newYamlProtoConfig.GetYamlConfigEnabled();
        yamlConfigTurnedOff = wasYamlConfigEnabled && !YamlConfigEnabled;
    } else if (YamlConfig.empty()) {
        bool wasYamlConfigEnabled = YamlConfigEnabled;
        YamlConfigEnabled = false;
        yamlConfigTurnedOff = wasYamlConfigEnabled && !YamlConfigEnabled;
    } else {
        newYamlProtoConfig = YamlProtoConfig;
    }

    AppData()->YamlConfigEnabled = YamlConfigEnabled;

    std::swap(YamlProtoConfig, newYamlProtoConfig);

    THashSet<ui32> affectedKinds;
    for (const auto& kind : ev->Get()->Record.GetAffectedKinds()) {
        affectedKinds.insert(kind);
    }

    for (auto &[kinds, subscription] : SubscriptionsByKinds) {
        if (subscription->UpdateInProcess) {
            subscription->UpdateInProcess = nullptr;
            subscription->SubscribersToUpdate.clear();
        }

        NKikimrConfig::TAppConfig trunc;

        bool hasAffectedKinds = false;

        if (subscription->Yaml && YamlConfigEnabled) {
            ReplaceConfigItems(YamlProtoConfig, trunc, FilterKinds(subscription->Kinds), BaseConfig);
        } else {
            Y_FOR_EACH_BIT(kind, kinds) {
                if (affectedKinds.contains(kind)) {
                    hasAffectedKinds = true;
                }
            }

            // we try resend all configs if yaml config was turned off
            if (!hasAffectedKinds && !yamlConfigTurnedOff && CurrentStateFunc() != &TThis::StateInit) {
                continue;
            }

            ReplaceConfigItems(ev->Get()->Record.GetConfig(), trunc, FilterKinds(kinds), BaseConfig);
        }

        if (hasAffectedKinds || !CompareConfigs(subscription->CurrentConfig.Config, trunc) || CurrentStateFunc() == &TThis::StateInit) {
            subscription->UpdateInProcess = MakeHolder<TEvConsole::TEvConfigNotificationRequest>();
            subscription->UpdateInProcess->Record.MutableConfig()->CopyFrom(trunc);
            subscription->UpdateInProcess->Record.SetLocal(true);
            Y_FOR_EACH_BIT(kind, kinds) {
                subscription->UpdateInProcess->Record.AddItemKinds(kind);
            }
            subscription->UpdateInProcessCookie = ++NextRequestCookie;
            subscription->UpdateInProcessConfigVersion = FilterVersion(ev->Get()->Record.GetConfig().GetVersion(), kinds);

            if (YamlConfigEnabled) {
                UpdateYamlVersion(subscription);
            }

            for (auto &[subscriber, updates] : subscription->Subscribers) {
                auto k = kinds;
                BLOG_TRACE("Sending for kinds: " << KindsToString(k));
                SendUpdateToSubscriber(subscription, subscriber);
                ++updates;
            }
        } else if (YamlConfigEnabled && subscription->Yaml) {
            UpdateYamlVersion(subscription);
        } else if (!YamlConfigEnabled) {
            subscription->YamlVersion = std::nullopt;
        }
    }

    if (CurrentStateFunc() == &TThis::StateInit) {
        Become(&TThis::StateWork);
        ProcessEnqueuedEvents();
    }
}

void TConfigsDispatcher::UpdateYamlVersion(const TSubscription::TPtr &subscription) const
{
    TYamlVersion yamlVersion;
    yamlVersion.Version = NYamlConfig::GetVersion(YamlConfig);
    for (auto &[id, hash] : VolatileYamlConfigHashes) {
        yamlVersion.VolatileVersions[id] = hash;
    }
    subscription->UpdateInProcessYamlVersion = yamlVersion;
}

void TConfigsDispatcher::Handle(TEvConsole::TEvConfigSubscriptionError::TPtr &ev)
{
    // The only reason we can get this response is ambiguous domain
    // So it is okay to fail here
    Y_ABORT("Can't start Configs Dispatcher: %s",
            ev->Get()->Record.GetReason().c_str());
}

void TConfigsDispatcher::Handle(TEvConfigsDispatcher::TEvGetConfigRequest::TPtr &ev)
{
    auto resp = MakeHolder<TEvConfigsDispatcher::TEvGetConfigResponse>();

    auto [yamlKinds, _] = CheckKinds(
        ev->Get()->ConfigItemKinds,
        "TEvGetConfigRequest handler");

    auto trunc = std::make_shared<NKikimrConfig::TAppConfig>();
    auto kinds = KindsToBitMap(ev->Get()->ConfigItemKinds);
    if (YamlConfigEnabled && yamlKinds) {
        ReplaceConfigItems(YamlProtoConfig, *trunc, FilterKinds(kinds), BaseConfig);
    } else {
        ReplaceConfigItems(CurrentConfig, *trunc, FilterKinds(kinds), BaseConfig);
    }
    resp->Config = trunc;

    BLOG_TRACE("Send TEvConfigsDispatcher::TEvGetConfigResponse"
        " to " << ev->Sender << ": " << resp->Config->ShortDebugString());

    Send(ev->Sender, std::move(resp), 0, ev->Cookie);
}

TConfigsDispatcher::TCheckKindsResult TConfigsDispatcher::CheckKinds(const TVector<ui32>& kinds, const char* errorContext) const {
    bool yamlKinds = false;
    bool nonYamlKinds = false;
    for (auto kind : kinds) {
        if (!DYNAMIC_KINDS.contains(kind)) {
            TStringStream sstr;
            sstr << static_cast<NKikimrConsole::TConfigItem::EKind>(kind);
            Y_ABORT("unexpected kind in %s: %s", errorContext, sstr.Str().data());
        }

        if (NON_YAML_KINDS.contains(kind)) {
            nonYamlKinds = true;
        } else {
            yamlKinds = true;
        }
    }

    if (yamlKinds && nonYamlKinds) {
        Y_ABORT("both yaml and non yaml kinds in %s", errorContext);
    }

    return {yamlKinds, nonYamlKinds};
}

void TConfigsDispatcher::Handle(TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest::TPtr &ev)
{
    auto [yamlKinds, nonYamlKinds] = CheckKinds(
        ev->Get()->ConfigItemKinds,
        "SetConfigSubscriptionRequest handler");
    Y_UNUSED(nonYamlKinds);
    auto kinds = KindsToBitMap(ev->Get()->ConfigItemKinds);
    auto subscriberActor = ev->Get()->Subscriber ? ev->Get()->Subscriber : ev->Sender;

    auto subscription = FindSubscription(kinds);
    if (!subscription) {
        subscription = new TSubscription;
        subscription->Kinds = kinds;
        subscription->Yaml = yamlKinds;

        SubscriptionsByKinds.emplace(kinds, subscription);
    }
    auto [subscriberIt, _] = subscription->Subscribers.emplace(subscriberActor, 0);
    SubscriptionsBySubscriber.emplace(subscriberActor, subscription);

    auto subscriber = FindSubscriber(subscriberActor);
    if (!subscriber) {
        subscriber = new TSubscriber;
        subscriber->Subscriber = subscriberActor;
        Subscribers.emplace(subscriberActor, subscriber);
    }
    subscriber->Subscriptions.insert(subscription);

    // We don't care about versions and kinds here
    Send(ev->Sender, new TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);

    if (CurrentStateFunc() != &TThis::StateInit) {
        // first time we send even empty config
        if (!subscription->UpdateInProcess) {
            subscription->UpdateInProcess = MakeHolder<TEvConsole::TEvConfigNotificationRequest>();
            NKikimrConfig::TAppConfig trunc;
            if (YamlConfigEnabled) {
                ReplaceConfigItems(YamlProtoConfig, trunc, FilterKinds(kinds), BaseConfig);
            } else {
                ReplaceConfigItems(CurrentConfig, trunc, FilterKinds(kinds), BaseConfig);
            }
            subscription->UpdateInProcess->Record.MutableConfig()->CopyFrom(trunc);
            Y_FOR_EACH_BIT(kind, kinds) {
                subscription->UpdateInProcess->Record.AddItemKinds(kind);
            }
            subscription->UpdateInProcessCookie = ++NextRequestCookie;
            subscription->UpdateInProcessConfigVersion = FilterVersion(CurrentConfig.GetVersion(), kinds);
        }
        BLOG_TRACE("Sending for kinds: " << KindsToString(kinds));
        SendUpdateToSubscriber(subscription, subscriber->Subscriber);
        ++(subscriberIt->second);
    }
}

void TConfigsDispatcher::Handle(TEvConfigsDispatcher::TEvRemoveConfigSubscriptionRequest::TPtr &ev)
{
    auto subscriberActor = ev->Get()->Subscriber ? ev->Get()->Subscriber : ev->Sender;
    auto subscriber = FindSubscriber(subscriberActor);
    if (!subscriber) {
        return;
    }

    for (auto &subscription : subscriber->Subscriptions) {
        subscription->SubscribersToUpdate.erase(subscriberActor);
        if (subscription->SubscribersToUpdate.empty()) {
            if (subscription->UpdateInProcess) {
                subscription->CurrentConfig.Version = subscription->UpdateInProcessConfigVersion;
                subscription->CurrentConfig.Config = subscription->UpdateInProcess->Record.GetConfig();
            }
            subscription->YamlVersion = subscription->UpdateInProcessYamlVersion;
            subscription->UpdateInProcessYamlVersion = std::nullopt;
            subscription->UpdateInProcess = nullptr;
        }

        subscription->Subscribers.erase(subscriberActor);
        if (subscription->Subscribers.empty()) {
            SubscriptionsByKinds.erase(subscription->Kinds);
        }
    }

    Subscribers.erase(subscriberActor);
    SubscriptionsBySubscriber.erase(subscriberActor);

    Send(ev->Sender, new TEvConfigsDispatcher::TEvRemoveConfigSubscriptionResponse);
}


void TConfigsDispatcher::Handle(TEvConsole::TEvConfigNotificationResponse::TPtr &ev)
{
    auto rec = ev->Get()->Record;
    auto subscription = FindSubscription(ev->Sender);

    // Probably subscription was cleared up due to tenant's change.
    if (!subscription) {
        BLOG_ERROR("Got notification response for unknown subscription " << ev->Sender);
        return;
    }

    if (!subscription->UpdateInProcess) {
        BLOG_D("Notification was ignored for subscription " << ev->Sender);
        return;
    }

    if (ev->Cookie != subscription->UpdateInProcessCookie) {
        BLOG_ERROR("Notification cookie mismatch for subscription " << ev->Sender << " " << ev->Cookie << " != " << subscription->UpdateInProcessCookie);
        // TODO fix clients
        return;
    }

    if (!subscription->SubscribersToUpdate.contains(ev->Sender)) {
        BLOG_ERROR("Notification from unexpected subscriber for subscription " << ev->Sender);
        return;
    }

    Subscribers.at(ev->Sender)->CurrentConfigVersion = subscription->UpdateInProcessConfigVersion;

    // If all subscribers responded then send response to CMS.
    subscription->SubscribersToUpdate.erase(ev->Sender);

    if (subscription->SubscribersToUpdate.empty()) {
        subscription->CurrentConfig.Config = subscription->UpdateInProcess->Record.GetConfig();
        subscription->CurrentConfig.Version = subscription->UpdateInProcessConfigVersion;
        subscription->YamlVersion = subscription->UpdateInProcessYamlVersion;
        subscription->UpdateInProcessYamlVersion = std::nullopt;
        subscription->UpdateInProcess = nullptr;
    }
}


void TConfigsDispatcher::Handle(TEvConsole::TEvGetNodeLabelsRequest::TPtr &ev) {
    auto Response = MakeHolder<TEvConsole::TEvGetNodeLabelsResponse>();

    for (const auto& [label, value] : Labels) {
        auto *labelSer = Response->Record.MutableResponse()->add_labels();
        labelSer->set_label(label);
        labelSer->set_value(value);
    }

    Send(ev->Sender, Response.Release());
}

IActor *CreateConfigsDispatcher(const TConfigsDispatcherInitInfo& initInfo) {
    return new TConfigsDispatcher(initInfo);
}

} // namespace NKikimr::NConsole
