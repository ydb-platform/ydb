#include "labels_maintainer.h"
#include "tenant_pool.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {

using namespace NConsole;

class TLabelsMaintainer : public TActorBootstrapped<TLabelsMaintainer> {
    using TActorBase = TActorBootstrapped<TLabelsMaintainer>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::LABELS_MAINTAINER_ACTOR;
    }

    TLabelsMaintainer(const NKikimrConfig::TMonitoringConfig &config)
        : InitializedLocalOptions(false)
        , CurrentHostLabel("")
    {
        ParseConfig(config);
    }

    STFUNC(StateWork) {
        TRACE_EVENT(NKikimrServices::LABELS_MAINTAINER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
            HFunc(TEvTenantPool::TEvTenantPoolStatus, Handle);
            IgnoreFunc(TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);

        default:
            Y_ABORT("unexpected event type: %" PRIx32 " event: %s",
                   ev->GetTypeRewrite(), ev->ToString().data());
            break;
        }
    }

    void Bootstrap(const TActorContext &ctx)
    {
        LOG_DEBUG(ctx, NKikimrServices::LABELS_MAINTAINER, "Bootstrap");

        Send(MakeTenantPoolRootID(), new TEvents::TEvSubscribe);
        SubscribeForConfig(ctx);
        ReportHistoryCounter(ctx);

        Become(&TThis::StateWork);
    }

private:
    void SubscribeForConfig(const TActorContext &ctx)
    {
        auto kind = (ui32)NKikimrConsole::TConfigItem::MonitoringConfigItem;
        ctx.Send(MakeConfigsDispatcherID(ctx.SelfID.NodeId()),
                 new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(kind));
    }

    /**
     * Add or update tenant labels for service counters.
     * List of affected services is obtained via GetTenantSensorServices.
     * If there is a single tenant assigned then tenant label holds its
     * name. If multiple tenants are attached then tenant label is 'multiple'.
     * If no tenant is attached then tenant label becomes 'none'.
     * If dynamic tenant is attached then there is an attached slot name to
     * be used for slot label. Otherwise slot type is used as slot label
     *('static' or 'dynamic').
     *
     * If ForceDatabaseLabels is set to 'false' then labels for unassigned
     * nodes and nodes assigned to domain are omitted.
     */
    void UpdateDatabaseLabels(const TActorContext &ctx)
    {
        auto res = RecomputeLabels();
        if (res.first) {
            RemoveLabels(ctx);
            ResetDerivCounters(ctx);
            AddLabels(ctx);
        } else if (res.second) {
            ResetDerivCounters(ctx);
        }
    }

    std::pair<bool, bool> RecomputeLabels()
    {
        THashSet<TString> tenants;
        THashMap<TString, TString> attrs;
        TString database;
        TString host = "";
        TString slot;

        for (auto &slotStatus : CurrentStatus.GetSlots()) {
            if (!slotStatus.GetAssignedTenant())
                continue;
            slot = slotStatus.GetLabel();
            for (auto &attr : slotStatus.GetTenantAttributes())
                attrs[attr.GetKey()] = attr.GetValue();
            tenants.insert(slotStatus.GetAssignedTenant());
        }

        if (tenants.empty()) {
            host = ForceDatabaseLabels ? "unassigned": "";
            database = ForceDatabaseLabels ? NoneDatabasetLabelValue : "";
        } else if (tenants.size() == 1) {
            database = *tenants.begin();
            TString domain = TString(ExtractDomain(database));
            if (IsEqualPaths(database, domain)) {
                if (ForceDatabaseLabels)
                    database = CanonizePath(database);
                else {
                    database = "";
                }
            }
            if (slot.StartsWith("slot-")) {
                // dynamic slot
                host = slot;
                if (!DataCenter.empty())
                    host = to_lower(DataCenter) + "-" + slot;
            } else {
                // static slot
                if (!HostLabelOverride.empty())
                    host = HostLabelOverride;
            }
        } else {
            host = "multiple";
            database = MultipleDatabaseLabelValue;
            attrs.clear();
        }

        std::pair<bool, bool> res;
        res.first = (CurrentDatabaseLabel != database
                     || CurrentHostLabel != host
                     || CurrentAttributes != attrs);
        res.second = CurrentTenants != tenants;

        CurrentDatabaseLabel = database;
        CurrentHostLabel = host;
        CurrentAttributes = attrs;
        CurrentTenants = tenants;

        return res;
    }

    void ResetDerivCounters(const TActorContext &ctx)
    {
        auto root = AppData(ctx)->Counters;
        for (auto &service : DatabaseSensorServices) {
            LOG_DEBUG_S(ctx, NKikimrServices::LABELS_MAINTAINER,
                        "Reset counters for " << service.data());

            auto serviceGroup = GetServiceCounters(root, service);
            serviceGroup->ResetCounters(true);
        }
        for (auto &service : DatabaseAttributeSensorServices) {
            LOG_DEBUG_S(ctx, NKikimrServices::LABELS_MAINTAINER,
                        "Reset counters for " << service.data());

            auto serviceGroup = GetServiceCounters(root, service);
            serviceGroup->ResetCounters(true);
        }
    }

    void RemoveLabels(const TActorContext &ctx)
    {
        RemoveDatabaseLabels(ctx);
        RemoveAttributeLabels(ctx);
    }

    void RemoveDatabaseLabels(const TActorContext &ctx)
    {
        auto root = AppData(ctx)->Counters;
        for (auto &service : DatabaseSensorServices) {
            LOG_DEBUG_S(ctx, NKikimrServices::LABELS_MAINTAINER,
                        "Removing database labels from " << service << " counters");

            ReplaceSubgroup(root, service);
        }
    }

    void ReportHistoryCounter(const TActorContext &ctx)
    {
        if (!ProcessLocation.empty()) {
            auto root = AppData(ctx)->Counters;
            auto historyCounter = GetServiceCounters(root, "utils")->GetSubgroup("location", ProcessLocation)->GetCounter("history", false);
            *historyCounter = 1;
        }
    }

    void RemoveAttributeLabels(const TActorContext &ctx)
    {
        auto root = AppData(ctx)->Counters;
        for (auto &service : DatabaseAttributeSensorServices) {
            LOG_DEBUG_S(ctx, NKikimrServices::LABELS_MAINTAINER,
                        "Removing database attribute labels from " << service << " counters");

            ReplaceSubgroup(root, service);
        }
    }

    void AddLabels(const TActorContext &ctx)
    {
        AddDatabaseLabels(ctx);
        AddAttributeLabels(ctx);
    }

    void AddDatabaseLabels(const TActorContext &ctx)
    {
        if (!DatabaseLabelsEnabled)
            return;

        if (!CurrentDatabaseLabel)
            return;

        auto root = AppData(ctx)->Counters;
        TSmallVec<std::pair<TString, TString>> labels;
        if (GroupAllMetrics) {
            labels.push_back({DATABASE_LABEL, ""});
        } else {
            labels.push_back({DATABASE_LABEL, CurrentDatabaseLabel});
        }

        labels.push_back({SLOT_LABEL, "static"});
        if (!CurrentHostLabel.empty()) {
            labels.push_back({"host", CurrentHostLabel});
        }

        AddLabelsToServices(ctx, labels, DatabaseSensorServices);
    }

    void AddLabelsToServices(const TActorContext& ctx, const TSmallVec<std::pair<TString, TString>> &labels, const THashSet<TString> &services) {
        if (!labels.empty()) {
            auto root = AppData(ctx)->Counters;
            for(auto &service: services) {
                LOG_DEBUG_S(ctx, NKikimrServices::LABELS_MAINTAINER,
                            "Add labels to service " << service << " counters"
                            << " labels=" << PrintLabels(labels));
                const auto &[svc, subSvc] = ExtractSubServiceName(service);
                auto oldGroup = root->GetSubgroup("counters", svc);
                if (!subSvc.empty())
                    oldGroup = oldGroup->GetSubgroup("subsystem", subSvc);
                TIntrusivePtr<::NMonitoring::TDynamicCounters> serviceGroup = new ::NMonitoring::TDynamicCounters;
                TIntrusivePtr<::NMonitoring::TDynamicCounters> curGroup = serviceGroup;

                const auto* actualLabels = &labels;

                TSmallVec<std::pair<TString, TString>> ydbLabels;
                if (DatabaseAttributeSensorServices.contains(service)) {
                    // explicitly remove "slot" label for external services ("ydb")
                    ydbLabels = labels;
                    if (auto it = std::find_if(ydbLabels.begin(), ydbLabels.end(), [](auto& el){ return el.first == SLOT_LABEL; });
                        it != ydbLabels.end())
                    {
                        ydbLabels.erase(it);
                        actualLabels = &ydbLabels;
                    }
                }

                for (size_t i = 0; i < actualLabels->size() - 1; ++i) {
                    curGroup = curGroup->GetSubgroup((*actualLabels)[i].first, (*actualLabels)[i].second);
                }
                curGroup->RegisterSubgroup(actualLabels->back().first, actualLabels->back().second, oldGroup);

                auto rt = GetServiceCountersRoot(root, service);
                rt->ReplaceSubgroup(subSvc.empty() ? "counters" : "subsystem", subSvc.empty() ? svc : subSvc, serviceGroup);
            }
        }
    }

    void AddAttributeLabels(const TActorContext &ctx)
    {
        if (!DatabaseAttributeLabelsEnabled)
            return;

        TSmallVec<std::pair<TString, TString>> labels;
        for (auto &attr : GetDatabaseAttributeLabels())
            if (CurrentAttributes.contains(attr))
                labels.push_back(*CurrentAttributes.find(attr));

        AddLabelsToServices(ctx, labels, DatabaseAttributeSensorServices);
    }

    void ApplyConfig(const NKikimrConfig::TMonitoringConfig &config,
                     const TActorContext &ctx)
    {
        RemoveLabels(ctx);
        ResetDerivCounters(ctx);
        ParseConfig(config);
        // Reset one more time because services set might change.
        ResetDerivCounters(ctx);
        RecomputeLabels();
        AddLabels(ctx);
    }

    void ParseConfig(const NKikimrConfig::TMonitoringConfig &config)
    {
        ForceDatabaseLabels = config.GetForceDatabaseLabels();

        auto &dbLabels = config.GetDatabaseLabels();
        DatabaseLabelsEnabled = dbLabels.GetEnabled();
        GroupAllMetrics = dbLabels.GetGroupAllMetrics();
        DatabaseSensorServices.clear();
        for (auto &service : dbLabels.GetServices())
            DatabaseSensorServices.insert(service);
        if (DatabaseSensorServices.empty())
            DatabaseSensorServices = GetDatabaseSensorServices();

        NoneDatabasetLabelValue = dbLabels.GetNoneDatabasetLabelValue();
        MultipleDatabaseLabelValue = dbLabels.GetMultipleDatabaseLabelValue();

        auto &attrLabels = config.GetDatabaseAttributeLabels();
        DatabaseAttributeLabelsEnabled = attrLabels.GetEnabled();
        DatabaseAttributeSensorServices.clear();
        for (auto &group : attrLabels.GetAttributeGroups())
            for (auto &service : group.GetServices())
                DatabaseAttributeSensorServices.insert(service);
        if (DatabaseAttributeSensorServices.empty())
            DatabaseAttributeSensorServices = GetDatabaseAttributeSensorServices();

        if (!InitializedLocalOptions) {
            InitializedLocalOptions = true;
            DataCenter = config.GetDataCenter();
            HostLabelOverride = config.GetHostLabelOverride();
            ProcessLocation = config.GetProcessLocation();
        }
    }

    TString PrintLabels(const TSmallVec<std::pair<TString, TString>> &labels) const
    {
        TStringStream ss;
        for (auto &pr : labels)
            ss << "(" << pr.first << ":" << pr.second << ")";
        return ss.Str();
    }

    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                const TActorContext &ctx)
    {
        auto &rec = ev->Get()->Record;

        LOG_INFO_S(ctx, NKikimrServices::LABELS_MAINTAINER,
                   "Got new config: " << rec.GetConfig().ShortDebugString());

        ApplyConfig(rec.GetConfig().GetMonitoringConfig(), ctx);

        auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);

        LOG_TRACE_S(ctx, NKikimrServices::LABELS_MAINTAINER,
                    "Send TEvConfigNotificationResponse: " << resp->Record.ShortDebugString());

        ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
    }

    void Handle(TEvTenantPool::TEvTenantPoolStatus::TPtr &ev,
                const TActorContext &ctx)
    {
        CurrentStatus.CopyFrom(ev->Get()->Record);

        LOG_INFO_S(ctx, NKikimrServices::LABELS_MAINTAINER,
                    "Got new pool status: " << CurrentStatus.ShortDebugString());

        UpdateDatabaseLabels(ctx);
    }

private:
    bool DatabaseLabelsEnabled;
    bool GroupAllMetrics;
    bool DatabaseAttributeLabelsEnabled;
    bool ForceDatabaseLabels;
    bool InitializedLocalOptions;

    THashSet<TString> DatabaseSensorServices;
    THashSet<TString> DatabaseAttributeSensorServices;

    TString NoneDatabasetLabelValue;
    TString MultipleDatabaseLabelValue;
    TString CurrentDatabaseLabel;

    TString CurrentHostLabel;
    TString HostLabelOverride;
    TString DataCenter;
    TString ProcessLocation;

    THashSet<TString> CurrentTenants;
    THashMap<TString, TString> CurrentAttributes;
    NKikimrTenantPool::TTenantPoolStatus CurrentStatus;
};

IActor* CreateLabelsMaintainer(const NKikimrConfig::TMonitoringConfig &config)
{
    return new TLabelsMaintainer(config);
}

} // namespace NKikimr
