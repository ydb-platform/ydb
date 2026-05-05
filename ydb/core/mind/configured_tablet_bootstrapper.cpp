#include "configured_tablet_bootstrapper.h"

#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/control/lib/dynamic_control_board_impl.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

// for 'create' funcs
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/backup/controller/tablet.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/replication/controller/controller.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/sequenceshard/sequenceshard.h>
#include <ydb/core/keyvalue/keyvalue.h>
#include <ydb/core/cms/cms.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/mind/node_broker.h>
#include <ydb/core/mind/tenant_slot_broker.h>
#include <ydb/core/kesus/tablet/tablet.h>
#include <ydb/core/sys_view/processor/processor.h>
#include <ydb/core/test_tablet/test_tablet.h>
#include <ydb/core/tablet/simple_tablet.h>
#include <ydb/core/blob_depot/blob_depot.h>
#include <ydb/core/statistics/aggregator/aggregator.h>
#include <ydb/core/tablet_flat/flat_executor_recovery.h>

#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <google/protobuf/util/message_differencer.h>

namespace NKikimr {

class TConfiguredTabletBootstrapper : public TActorBootstrapped<TConfiguredTabletBootstrapper> {

    static constexpr ui32 BootstrapperControlsWakeupIntervalSeconds = 5;

    struct TTabletState {
        std::optional<TActorId> BootstrapperInstance;
        NKikimrConfig::TBootstrap::TTablet Config;
        TControlWrapper DisableBootstrapper;
        bool HasDcbControl = false;
    };

    THashMap<ui64, TTabletState> TabletStates;
    TControlWrapper DisableLocalBootstrappers;
    bool WasLocalBootstrappersDisabled = false;
    NKikimrConfig::TBootstrap LastBootstrapConfig;

    void RegisterTabletBootstrapperControl(ui64 tabletId) {
        auto &state = TabletStates[tabletId];
        if (!state.HasDcbControl) {
            TControlWrapper control(0);
            TString controlName = "DisableBootstrapper_" + ToString(tabletId);
            auto* appData = AppData();
            if (appData && appData->Dcb) {
                appData->Dcb->RegisterSharedControl(control, controlName);
                state.DisableBootstrapper = control;
                state.HasDcbControl = true;
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                    "Registered DCB control '" << controlName << "' for tablet " << tabletId);
            }
        }
    }

    bool IsTabletBootstrapperDisabled(ui64 tabletId) const {
        auto it = TabletStates.find(tabletId);
        if (it != TabletStates.end() && it->second.HasDcbControl) {
            return it->second.DisableBootstrapper != 0;
        }
        return false;
    }

    bool ShouldRunOnThisNode(const TTabletState& state) const {
        if (DisableLocalBootstrappers) {
            return false;
        }

        if (state.HasDcbControl && state.DisableBootstrapper != 0) {
            return false;
        }

        const ui32 selfNode = SelfId().NodeId();

        return Find(state.Config.GetNode(), selfNode) != state.Config.GetNode().end();
    }

    void ReconcileTablet(ui64 tabletId, TTabletState& state) {
        bool shouldRun = ShouldRunOnThisNode(state);
        bool isRunning = state.BootstrapperInstance.has_value();

        if (shouldRun && !isRunning) {
            StartBootstrapper(tabletId, state);
        } else if (!shouldRun && isRunning) {
            StopBootstrapper(tabletId, state);
        }
    }

    void ProcessTabletsFromConfig(const NKikimrConfig::TBootstrap &bootstrapConfig) {
        NKikimrConfig::TBootstrap newLast;
        newLast.CopyFrom(bootstrapConfig);
        newLast.ClearTablet();

        // check if bootstrappers are disabled via ICB
        if (DisableLocalBootstrappers && !WasLocalBootstrappersDisabled) {
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER, "Local bootstrappers are disabled via ICB on node " << SelfId().NodeId());
            WasLocalBootstrappersDisabled = true;
        } else if (!DisableLocalBootstrappers && WasLocalBootstrappersDisabled) {
            LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER, "Local bootstrappers are re-enabled via ICB on node " << SelfId().NodeId());
            WasLocalBootstrappersDisabled = false;
        }

        THashSet<ui64> currentTabletIds;
        for (const auto &tablet : bootstrapConfig.GetTablet()) {
            auto normalizedTablet = tablet;
            RestoreTabletInfoFromStartupConfig(normalizedTablet);

            if (!HasCompleteTabletChannels(normalizedTablet)) {
                if (normalizedTablet.HasInfo() && normalizedTablet.GetInfo().HasTabletID()) {
                    const ui64 tabletId = normalizedTablet.GetInfo().GetTabletID();
                    if (const auto it = TabletStates.find(tabletId); it != TabletStates.end()) {
                        currentTabletIds.insert(tabletId);
                        newLast.AddTablet()->CopyFrom(it->second.Config);
                    }
                    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                        "Skipping invalid tablet config update with incomplete channels for tablet "
                        << tabletId << " on node " << SelfId().NodeId());
                } else {
                    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                        "Skipping invalid tablet config update without tablet id on node " << SelfId().NodeId());
                }
                continue;
            }

            newLast.AddTablet()->CopyFrom(normalizedTablet);

            const ui64 tabletId = normalizedTablet.GetInfo().GetTabletID();
            currentTabletIds.insert(tabletId);
            RegisterTabletBootstrapperControl(tabletId);
            UpdateTablet(normalizedTablet);
        }

        // remove tablets that are no longer in config
        for (auto it = TabletStates.begin(); it != TabletStates.end(); ) {
            if (!currentTabletIds.contains(it->first)) {
                StopBootstrapper(it->first, it->second);
                TabletStates.erase(it++);
            } else {
                ++it;
            }
        }

        LastBootstrapConfig.Swap(&newLast);
    }

    void StopBootstrapper(ui64 tabletId, TTabletState& state) {
        if (!state.BootstrapperInstance) {
            return;
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "Stopping tablet " << tabletId << " bootstrapper on node " << SelfId().NodeId());

        Send(*state.BootstrapperInstance, new TEvents::TEvPoisonPill());
        TActivationContext::ActorSystem()->RegisterLocalService(MakeBootstrapperID(tabletId, SelfId().NodeId()), TActorId());
        state.BootstrapperInstance.reset();
    }

    void Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr &ev) {
        const auto &record = ev->Get()->Record;

        if (record.GetConfig().HasBootstrapConfig()) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "Received bootstrap config update on node " << SelfId().NodeId());
            const auto &bootstrapConfig = record.GetConfig().GetBootstrapConfig();

            ProcessTabletsFromConfig(bootstrapConfig);
        }

        Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
    }

    void UpdateTablet(const NKikimrConfig::TBootstrap::TTablet &tabletConfig) {
        ui64 tabletId = tabletConfig.GetInfo().GetTabletID();
        auto &state = TabletStates[tabletId];

        google::protobuf::util::MessageDifferencer md;
        bool configChanged = !md.Compare(state.Config, tabletConfig);

        if (configChanged) {
            bool wasRunning = state.BootstrapperInstance.has_value();

            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "Tablet " << tabletId << " config changed on node " << SelfId().NodeId());

            state.Config.CopyFrom(tabletConfig);

            if (wasRunning) {
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                    "Tablet " << tabletId << " config changed, stopping running bootstrapper due to config change on node " << SelfId().NodeId());
                StopBootstrapper(tabletId, state);
            }
        }

        ReconcileTablet(tabletId, state);
    }

    void RestoreTabletInfoFromStartupConfig(NKikimrConfig::TBootstrap::TTablet& tabletConfig) const {
        if (!tabletConfig.HasInfo() || !tabletConfig.GetInfo().HasTabletID()) {
            return;
        }

        auto* info = tabletConfig.MutableInfo();

        for (const auto& startupTablet : InitialBootstrapConfig.GetTablet()) {
            if (startupTablet.HasInfo() && startupTablet.GetInfo().HasTabletID() && startupTablet.GetInfo().GetTabletID() == info->GetTabletID()) {
                info->MutableChannels()->CopyFrom(startupTablet.GetInfo().GetChannels());
                return;
            }
        }
    }

    static bool HasCompleteTabletChannels(const NKikimrConfig::TBootstrap::TTablet& tabletConfig) {
        if (!tabletConfig.HasInfo() || !tabletConfig.GetInfo().HasTabletID()) {
            return false;
        }

        const auto& info = tabletConfig.GetInfo();
        if (!info.ChannelsSize()) {
            return false;
        }

        for (const auto& channelInfo : info.GetChannels()) {
            const bool hasChannelType = channelInfo.HasChannelType();
            const bool hasErasureNameField = channelInfo.HasChannelErasureName();
            const bool hasValidErasureName = hasErasureNameField && !channelInfo.GetChannelErasureName().empty();
            if (hasChannelType && hasErasureNameField) {
                return false;
            }
            if (!hasChannelType && !hasValidErasureName) {
                return false;
            }
            if (!channelInfo.HistorySize()) {
                return false;
            }
        }

        return true;
    }

    void StartBootstrapper(ui64 tabletId, TTabletState &state) {
        Y_ABORT_UNLESS(!state.BootstrapperInstance);
        const auto* appData = AppData();
        const ui32 selfNode = SelfId().NodeId();
        const auto& config = state.Config;

        TIntrusivePtr<TTabletStorageInfo> storageInfo = TabletStorageInfoFromProto(config.GetInfo());
        if (config.HasBootType()) {
            storageInfo->BootType = BootTypeFromProto(config.GetBootType());
        }

        // extract from kikimr_services_initializer
        const TTabletTypes::EType tabletType = BootstrapperTypeToTabletType(config.GetType());

        if (storageInfo->TabletType == TTabletTypes::TypeInvalid)
            storageInfo->TabletType = tabletType;

        TIntrusivePtr<TTabletSetupInfo> tabletSetupInfo = MakeTabletSetupInfo(tabletType, storageInfo->BootType,
            appData->UserPoolId, appData->SystemPoolId);

        TIntrusivePtr<TBootstrapperInfo> bi = new TBootstrapperInfo(tabletSetupInfo.Get());
        for (ui32 node : config.GetNode()) {
            bi->Nodes.emplace_back(node);
        }
        if (config.HasWatchThreshold())
            bi->WatchThreshold = TDuration::MilliSeconds(config.GetWatchThreshold());
        if (config.HasStartFollowers())
            bi->StartFollowers = config.GetStartFollowers();

        bool standby = config.GetStandBy();
        state.BootstrapperInstance = Register(CreateBootstrapper(storageInfo.Get(), bi.Get(), standby), TMailboxType::HTSwap, appData->SystemPoolId);

        TActivationContext::ActorSystem()->RegisterLocalService(MakeBootstrapperID(tabletId, selfNode), *state.BootstrapperInstance);

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
            "Started tablet " << tabletId << " bootstrapper on node " << selfNode);
    }

    TString MakeBootstrapperNodeUrl(ui32 nodeId) const {
        if (nodeId == SelfId().NodeId()) {
            return "/actors/bootstrapper";
        }
        return "/node/" + ToString(nodeId) + "/actors/bootstrapper";
    }

    TString NormalizeRedirectPath(TStringBuf requestedPath) const {
        const TString fallback = MakeBootstrapperNodeUrl(SelfId().NodeId());

        if (requestedPath == "/actors/bootstrapper") {
            return TString(requestedPath);
        }

        static constexpr TStringBuf prefix = "/node/";
        static constexpr TStringBuf suffix = "/actors/bootstrapper";

        if (!requestedPath.StartsWith(prefix) || !requestedPath.EndsWith(suffix)) {
            return fallback;
        }

        const size_t nodePartBegin = prefix.size();
        const size_t nodePartSize = requestedPath.size() - prefix.size() - suffix.size();
        if (nodePartSize == 0) {
            return fallback;
        }

        TStringBuf nodePart(requestedPath.data() + nodePartBegin, nodePartSize);
        for (char ch : nodePart) {
            if (ch < '0' || ch > '9') {
                return fallback;
            }
        }

        return TString(requestedPath);
    }

    void RenderRedirectPage(TStringStream& str, TStringBuf redirectPath) const {
        const TString target = NormalizeRedirectPath(redirectPath);

        HTML(str) {
            HEAD() {
                str << "<meta http-equiv='refresh' content='0;url=" << target << "'>";
            }
            BODY() {
                str << "<p>Redirecting...</p>";
                str << "<p>If you are not redirected automatically, <a href='" << target << "'>click here</a>.</p>";
            }
        }
    }

    void SetLocalBootstrappersDisabled(TAppData* appData, bool disable) {
        if (appData && appData->Icb) {
            TControlBoard::SetValue(disable, appData->Icb->BootstrapperControls.DisableLocalBootstrappers);
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "Local bootstrappers " << (disable ? "disabled" : "enabled")
                << " via HTTP on node " << SelfId().NodeId());
        }
    }

    void SetTabletBootstrapperDisabled(TAppData* appData, ui64 tabletId, bool disable) {
        if (appData && appData->Dcb) {
            TString controlName = "DisableBootstrapper_" + ToString(tabletId);
            TAtomic prevValue;
            appData->Dcb->SetValue(controlName, static_cast<TAtomic>(disable), prevValue);

            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "Tablet " << tabletId << " bootstrapper "
                << (disable ? "disabled" : "enabled")
                << " via HTTP on node " << SelfId().NodeId());
        }

        auto it = TabletStates.find(tabletId);
        if (it != TabletStates.end()) {
            ReconcileTablet(tabletId, it->second);
        }
    }

    bool TryHandleHttpPost(NMon::TEvHttpInfo::TPtr &ev, TStringStream& str) {
        if (ev->Get()->Request.GetMethod() != HTTP_METHOD_POST) {
            return false;
        }

        auto* appData = AppData();
        const auto& postParams = ev->Get()->Request.GetPostParams();
        const TStringBuf redirectPath = ev->Get()->Request.GetPath();

        if (!appData || !postParams.Has("action")) {
            return false;
        }

        const TString action = postParams.Get("action");

        if (action == "disable" || action == "enable") {
            SetLocalBootstrappersDisabled(appData, action == "disable");
            ProcessTabletsFromConfig(LastBootstrapConfig);
            RenderRedirectPage(str, redirectPath);
            return true;
        }

        if (action == "disable_tablet" || action == "enable_tablet") {
            if (!postParams.Has("tablet_id")) {
                RenderRedirectPage(str, redirectPath);
                return true;
            }

            ui64 tabletId;
            if (!TryFromString(postParams.Get("tablet_id"), tabletId)) {
                LOG_WARN_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                    "Invalid tablet ID " << postParams.Get("tablet_id") << " in HTTP request");
                RenderRedirectPage(str, redirectPath);
                return true;
            }

            SetTabletBootstrapperDisabled(appData, tabletId, action == "disable_tablet");
            RenderRedirectPage(str, redirectPath);
            return true;
        }

        return false;
    }

    void RenderInfoBanner(TStringStream& str) const {
        auto& __stream = str;

        DIV_CLASS("alert alert-info") {
            str << "<strong>Note:</strong> This control affects only THIS NODE. "
                << "Other nodes in the cluster are not affected. "
                << "To disable bootstrappers on other nodes, use their respective HTTP endpoints or ICB controls.";
        }
    }

    void RenderStatusPanel(TStringStream& str, bool isDisabled) const {
        auto& __stream = str;

        DIV_CLASS("panel panel-default") {
            DIV_CLASS("panel-heading") {
                str << "Status";
            }
            DIV_CLASS("panel-body") {
                str << "Node ID: " << SelfId().NodeId() << "<br/>";
                str << "Local bootstrappers: "
                    << (isDisabled
                        ? "<span style='color:red'>DISABLED</span>"
                        : "<span style='color:green'>ENABLED</span>")
                    << "<br/>";
                str << "Active bootstrappers on this node: " << CountTabletsOnThisNode() << "<br/>";
                str << "Total tablets in config: " << LastBootstrapConfig.TabletSize() << "<br/>";
            }
        }
    }

    void RenderControlPanel(TStringStream& str, bool isDisabled) const {
        auto& __stream = str;

        DIV_CLASS("panel panel-default") {
            DIV_CLASS("panel-heading") {
                str << "Control";
            }
            DIV_CLASS("panel-body") {
                str << "<form method='POST'>";
                if (isDisabled) {
                    str << "<button type='submit' name='action' value='enable' class='btn btn-success'>Enable Local Bootstrappers</button>";
                } else {
                    str << "<button type='submit' name='action' value='disable' class='btn btn-danger'>Disable Local Bootstrappers</button>";
                }
                str << "</form>";
                str << "<br/><small>Note: You can also control via ICB at <a href='/actors/icb'>/actors/icb</a> "
                    << "using parameter <code>BootstrapperControls.DisableLocalBootstrappers</code></small>";
            }
        }
    }

    void RenderTabletNodes(TStringStream& str, const NKikimrConfig::TBootstrap::TTablet& tablet) const {
        static constexpr size_t maxVisibleNodes = 10;

        const size_t nodeCount = static_cast<size_t>(tablet.NodeSize());
        const size_t visibleCount = Min(nodeCount, maxVisibleNodes);

        for (size_t i = 0; i < visibleCount; ++i) {
            if (i > 0) {
                str << ", ";
            }

            const ui32 nodeId = tablet.GetNode(i);
            str << "<a href='" << MakeBootstrapperNodeUrl(nodeId) << "'>" << nodeId << "</a>";
        }

        if (nodeCount > visibleCount) {
            str << " ... (" << nodeCount << " total)";
        }
    }

    void RenderTabletAction(TStringStream& str, ui64 tabletId, bool isDisabled, bool isTabletDisabled, bool hasBootstrapper) const {
        if (isDisabled) {
            str << "<small>All disabled</small>";
        } else if (isTabletDisabled) {
            str << "<form method='POST' style='display:inline;'>";
            str << "<button type='submit' name='action' value='enable_tablet' class='btn btn-xs btn-success'>Enable</button>";
            str << "<input type='hidden' name='tablet_id' value='" << tabletId << "'/>";
            str << "</form>";
        } else if (hasBootstrapper) {
            str << "<form method='POST' style='display:inline;'>";
            str << "<button type='submit' name='action' value='disable_tablet' class='btn btn-xs btn-danger'>Disable</button>";
            str << "<input type='hidden' name='tablet_id' value='" << tabletId << "'/>";
            str << "</form>";
        } else {
            str << "-";
        }
    }

    void RenderTabletRow(TStringStream& str, const NKikimrConfig::TBootstrap::TTablet& tablet, bool isDisabled) const {
        const ui64 tabletId = tablet.GetInfo().GetTabletID();
        auto it = TabletStates.find(tabletId);
        const bool hasBootstrapper = it != TabletStates.end() && it->second.BootstrapperInstance.has_value();
        const bool isTabletDisabled = IsTabletBootstrapperDisabled(tabletId);
        auto& __stream = str;

        TABLER() {
            TABLED() { str << tabletId; }
            TABLED() { str << NKikimrConfig::TBootstrap::ETabletType_Name(tablet.GetType()); }
            TABLED() {
                if (isDisabled) {
                    str << "<span style='color:orange'>All Disabled</span>";
                } else if (isTabletDisabled) {
                    str << "<span style='color:red'>Disabled</span>";
                } else if (hasBootstrapper) {
                    str << "<span style='color:green'>Running</span>";
                } else {
                    str << "<span style='color:gray'>Not on this node</span>";
                }
            }
            TABLED() {
                RenderTabletNodes(str, tablet);
            }
            TABLED() {
                RenderTabletAction(str, tabletId, isDisabled, isTabletDisabled, hasBootstrapper);
            }
        }
    }

    void RenderTabletsPanel(TStringStream& str, bool isDisabled) const {
        auto& __stream = str;

        DIV_CLASS("panel panel-default") {
            DIV_CLASS("panel-heading") {
                str << "Tablets";
            }
            DIV_CLASS("panel-body") {
                TABLE_SORTABLE_CLASS("table table-striped") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() { str << "Tablet ID"; }
                            TABLEH() { str << "Type"; }
                            TABLEH() { str << "Status"; }
                            TABLEH() { str << "Nodes"; }
                            TABLEH() { str << "Action"; }
                        }
                    }
                    TABLEBODY() {
                        for (const auto& tablet : LastBootstrapConfig.GetTablet()) {
                            RenderTabletRow(str, tablet, isDisabled);
                        }
                    }
                }
            }
        }
    }

    void RenderHttpPage(TStringStream& str) const {
        const bool isDisabled = DisableLocalBootstrappers;

        HTML(str) {
            TAG(TH2) {
                str << "Tablet Bootstrapper Control (Node " << SelfId().NodeId() << ")";
            }

            RenderInfoBanner(str);
            RenderStatusPanel(str, isDisabled);
            RenderControlPanel(str, isDisabled);
            RenderTabletsPanel(str, isDisabled);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CONFIGURED_BOOTSTRAPPER;
    }

    NKikimrConfig::TBootstrap InitialBootstrapConfig;

    TConfiguredTabletBootstrapper(const NKikimrConfig::TBootstrap &bootstrapConfig)
        : InitialBootstrapConfig(bootstrapConfig)
    {
    }

    void Bootstrap() {
        auto *appData = AppData();

        if (appData && appData->Icb) {
            TControlBoard::RegisterSharedControl(DisableLocalBootstrappers, appData->Icb->BootstrapperControls.DisableLocalBootstrappers);
        }

        if (appData && appData->Mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = appData->Mon->RegisterIndexPage("actors", "Actors");
            appData->Mon->RegisterActorPage(actorsMonPage, "bootstrapper", "Tablet Bootstrapper", false,
                TActivationContext::ActorSystem(), SelfId());
        }

        ProcessTabletsFromConfig(InitialBootstrapConfig);

        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
            new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(NKikimrConsole::TConfigItem::BootstrapConfigItem));

        Schedule(TDuration::Seconds(BootstrapperControlsWakeupIntervalSeconds), new TEvents::TEvWakeup());
        Become(&TThis::StateWork);
    }

    void HandleWakeup() {
        bool currentlyDisabled = DisableLocalBootstrappers;

        if (currentlyDisabled != WasLocalBootstrappersDisabled) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BOOTSTRAPPER,
                "ICB control DisableLocalBootstrappers changed to " << currentlyDisabled << " on node " << SelfId().NodeId());
            ProcessTabletsFromConfig(LastBootstrapConfig);
        } else {
            for (auto& [tabletId, state] : TabletStates) {
                ReconcileTablet(tabletId, state);
            }
        }
        Schedule(TDuration::Seconds(BootstrapperControlsWakeupIntervalSeconds), new TEvents::TEvWakeup());
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev) {
        TStringStream str;

        if (TryHandleHttpPost(ev, str)) {
            Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
            return;
        }

        RenderHttpPage(str);
        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    size_t CountTabletsOnThisNode() const {
        size_t count = 0;
        for (const auto& [tabletId, state] : TabletStates) {
            if (state.BootstrapperInstance) {
                ++count;
            }
        }
        return count;
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        }
    }
};


TTabletTypes::EType BootstrapperTypeToTabletType(ui32 type) {
    switch (type) {
    case NKikimrConfig::TBootstrap::TX_DUMMY:
        return TTabletTypes::Dummy;
    case NKikimrConfig::TBootstrap::HIVE:
    case NKikimrConfig::TBootstrap::FLAT_HIVE:
        return TTabletTypes::Hive;
    case NKikimrConfig::TBootstrap::TX_COORDINATOR:
    case NKikimrConfig::TBootstrap::FLAT_TX_COORDINATOR:
        return TTabletTypes::Coordinator;
    case NKikimrConfig::TBootstrap::TX_MEDIATOR:
        return TTabletTypes::Mediator;
    case NKikimrConfig::TBootstrap::BS_DOMAINCONTROLLER:
    case NKikimrConfig::TBootstrap::FLAT_BS_CONTROLLER:
        return TTabletTypes::BSController;
    case NKikimrConfig::TBootstrap::DATASHARD:
    case NKikimrConfig::TBootstrap::FAKE_DATASHARD:
        return TTabletTypes::DataShard;
    case NKikimrConfig::TBootstrap::SCHEMESHARD:
    case NKikimrConfig::TBootstrap::FLAT_SCHEMESHARD:
        return TTabletTypes::SchemeShard;
    case NKikimrConfig::TBootstrap::KEYVALUEFLAT:
        return TTabletTypes::KeyValue;
    case NKikimrConfig::TBootstrap::TX_PROXY:
    case NKikimrConfig::TBootstrap::FLAT_TX_PROXY:
    case NKikimrConfig::TBootstrap::TX_ALLOCATOR:
        return TTabletTypes::TxAllocator;
    case NKikimrConfig::TBootstrap::CMS:
        return TTabletTypes::Cms;
    case NKikimrConfig::TBootstrap::NODE_BROKER:
        return TTabletTypes::NodeBroker;
    case NKikimrConfig::TBootstrap::TENANT_SLOT_BROKER:
        return TTabletTypes::TenantSlotBroker;
    case NKikimrConfig::TBootstrap::CONSOLE:
        return TTabletTypes::Console;
    default:
        Y_ABORT("unknown tablet type");
    }
    return TTabletTypes::TypeInvalid;
}

TIntrusivePtr<TTabletSetupInfo> MakeTabletSetupInfo(
        TTabletTypes::EType tabletType,
        ETabletBootType bootType,
        ui32 poolId, ui32 tabletPoolId)
{
    TTabletSetupInfo::TTabletCreationFunc createFunc;

    switch (tabletType) {
    case TTabletTypes::BSController:
        createFunc = &CreateFlatBsController;
        break;
    case TTabletTypes::Hive:
        createFunc = &CreateDefaultHive;
        break;
    case TTabletTypes::Coordinator:
        createFunc = &CreateFlatTxCoordinator;
        break;
    case TTabletTypes::Mediator:
        createFunc = &CreateTxMediator;
        break;
    case TTabletTypes::TxAllocator:
        createFunc = &CreateTxAllocator;
        break;
    case TTabletTypes::DataShard:
        createFunc = &CreateDataShard;
        break;
    case TTabletTypes::SchemeShard:
        createFunc = &CreateFlatTxSchemeShard;
        break;
    case TTabletTypes::KeyValue:
        createFunc = &CreateKeyValueFlat;
        break;
    case TTabletTypes::Cms:
        createFunc = &NCms::CreateCms;
        break;
    case TTabletTypes::NodeBroker:
        createFunc = &NNodeBroker::CreateNodeBroker;
        break;
    case TTabletTypes::TenantSlotBroker:
        createFunc = &NTenantSlotBroker::CreateTenantSlotBroker;
        break;
    case TTabletTypes::Console:
        createFunc = &NConsole::CreateConsole;
        break;
    case TTabletTypes::Kesus:
        createFunc = &NKesus::CreateKesusTablet;
        break;
    case TTabletTypes::SysViewProcessor:
        createFunc = &NSysView::CreateSysViewProcessor;
        break;
    case TTabletTypes::TestShard:
        createFunc = &NTestShard::CreateTestShard;
        break;
    case TTabletTypes::SequenceShard:
        createFunc = &NSequenceShard::CreateSequenceShard;
        break;
    case TTabletTypes::ReplicationController:
        createFunc = &NReplication::CreateController;
        break;
    case TTabletTypes::BlobDepot:
        createFunc = &NBlobDepot::CreateBlobDepot;
        break;
    case TTabletTypes::StatisticsAggregator:
        createFunc = &NStat::CreateStatisticsAggregator;
        break;
    case TTabletTypes::BackupController:
        createFunc = &NBackup::CreateBackupController;
        break;
    case TTabletTypes::Dummy:
        createFunc = &CreateSimpleTablet;
        break;
    default:
        return nullptr;
    }

    if (bootType == ETabletBootType::Recovery) {
        createFunc = &NTabletFlatExecutor::NRecovery::CreateRecoveryShard;
    }

    return new TTabletSetupInfo(createFunc, TMailboxType::ReadAsFilled, poolId, TMailboxType::ReadAsFilled, tabletPoolId);
}

IActor* CreateConfiguredTabletBootstrapper(const NKikimrConfig::TBootstrap &bootstrapConfig) {
    return new TConfiguredTabletBootstrapper(bootstrapConfig);
}

}
