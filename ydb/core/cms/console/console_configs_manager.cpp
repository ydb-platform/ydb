#include "console_configs_manager.h"

#include "configs_dispatcher.h"
#include "console_audit.h"
#include "console_configs_provider.h"
#include "console_impl.h"
#include "http.h"

#include <ydb/core/cms/console/validators/registry.h>
#include <ydb/core/base/feature_flags.h>

#include <ydb/library/yql/public/issue/protos/issue_severity.pb.h>

#include <util/generic/bitmap.h>
#include <util/random/random.h>
#include <util/string/split.h>

namespace NKikimr::NConsole {

bool TConfigsManager::CheckRights(const TString &userToken) {
    if (AppData()->AdministrationAllowedSIDs.empty()) {
        return true;
    }

    NACLib::TUserToken token(userToken);

    for (auto &sid : AppData()->AdministrationAllowedSIDs) {
        if (token.IsExist(sid)) {
            return true;
        }
    }

    return false;
}

void TConfigsManager::ClearState()
{
    ConfigIndex.Clear();
}

void TConfigsManager::SetConfig(const NKikimrConsole::TConfigsConfig &config)
{
    Config.Parse(config);
    Send(ConfigsProvider, new TConfigsProvider::TEvPrivate::TEvSetConfig(Config));
}

bool TConfigsManager::CheckConfig(const NKikimrConsole::TConfigsConfig &config,
                                  Ydb::StatusIds::StatusCode &code,
                                  TString &error)
{
    if (!TConfigsConfig::Check(config, error)) {
        code = Ydb::StatusIds::BAD_REQUEST;
        return false;
    }

    TConfigsConfig newConfig;
    newConfig.Parse(config);
    for (auto &pr : ConfigIndex.GetConfigItems()) {
        if (!IsConfigItemScopeAllowed(pr.second, newConfig)) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = "config conflicts with usage scope of " + pr.second->ToString();
            return false;
        }
    }

    return true;
}

void TConfigsManager::Bootstrap(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "TConfigsManager::Bootstrap");
    Become(&TThis::StateWork);

    ClusterName = AppData(ctx)->ClusterName;

    TxProcessor = Self.GetTxProcessor()->GetSubProcessor("configs",
                                                         ctx,
                                                         false,
                                                         NKikimrServices::CMS_CONFIGS);
    ConfigsProvider = ctx.Register(new TConfigsProvider(ctx.SelfID));

    ui32 item = (ui32)NKikimrConsole::TConfigItem::AllowEditYamlInUiItem;
    ctx.Send(MakeConfigsDispatcherID(SelfId().NodeId()),
             new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(item));
}

void TConfigsManager::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                                            const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    YamlReadOnly = !rec.GetConfig().GetAllowEditYamlInUi();

    auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);
    ctx.Send(ev->Sender, resp.Release(), 0, ev->Cookie);
}

void TConfigsManager::Detach()
{
    Send(ConfigsProvider, new TEvents::TEvPoisonPill);
    PassAway();
}

void TConfigsManager::ApplyPendingConfigModifications(const TActorContext &ctx,
                                                      TAutoPtr<IEventHandle> ev)
{
    LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "Applying pending config modifications");

    for (auto &pr : PendingConfigModifications.RemovedItems)
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "Remove " << ConfigIndex.GetItem(pr.first)->ToString());
    for (auto &pr : PendingConfigModifications.ModifiedItems)
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "Remove modified " << pr.second->ToString());
    for (auto &pr : PendingConfigModifications.ModifiedItems)
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "Add modified " << pr.second->ToString());
    for (auto item : PendingConfigModifications.AddedItems)
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "Add new " << item->ToString());

    PendingConfigModifications.ApplyTo(ConfigIndex);

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS, "Send configs update to configs provider.");
    auto req = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateConfigs>(PendingConfigModifications, ev);
    ctx.Send(ConfigsProvider, req.Release());

    PendingConfigModifications.Clear();
}

void TConfigsManager::ApplyPendingSubscriptionModifications(const TActorContext &ctx,
                                                            TAutoPtr<IEventHandle> ev)
{
    LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "Applying pending subscription midifications");

    for (auto &id : PendingSubscriptionModifications.RemovedSubscriptions) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Remove subscription " << SubscriptionIndex.GetSubscription(id)->ToString());
        SubscriptionIndex.RemoveSubscription(id);
    }
    for (auto &subscription : PendingSubscriptionModifications.AddedSubscriptions) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Add subscription " << subscription->ToString());
        SubscriptionIndex.AddSubscription(subscription);
    }
    for (auto &pr : PendingSubscriptionModifications.ModifiedLastProvided) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Modify last provided config for subscription id=" << pr.first
                    << " lastprovidedconfig=" << pr.second.ToString());
        SubscriptionIndex.GetSubscription(pr.first)->LastProvidedConfig = pr.second;
    }
    for (auto &pr : PendingSubscriptionModifications.ModifiedCookies) {
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Modify cookie for subscription id=" << pr.first
                    << " cookie=" << pr.second);
        SubscriptionIndex.GetSubscription(pr.first)->Cookie = pr.second;
    }

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS, "Send subscriptions update to configs provider.");
    auto req = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateSubscriptions>(PendingSubscriptionModifications, ev);
    ctx.Send(ConfigsProvider, req.Release());

    PendingSubscriptionModifications.Clear();
}

bool TConfigsManager::MakeNewSubscriptionChecks(TSubscription::TPtr subscription,
                                                Ydb::StatusIds::StatusCode &code,
                                                TString &error)
{
    if (subscription->Id) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "subscription id shouldn't be defined";
        return false;
    }

    if (!subscription->Subscriber.TabletId && !subscription->Subscriber.ServiceId) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "no subscriber specified";
        return false;
    }

    if (subscription->Subscriber.ServiceId && !subscription->Subscriber.ServiceId.IsService()) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "wrong service id";
        return false;
    }

    if (subscription->ItemKinds.empty()) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "no item kinds specified";
        return false;
    }

    return true;
}

bool TConfigsManager::IsConfigItemScopeAllowed(TConfigItem::TPtr item)
{
    return IsConfigItemScopeAllowed(item, Config);
}

bool TConfigsManager::IsConfigItemScopeAllowed(TConfigItem::TPtr item,
                                               const TConfigsConfig &config)
{
    if (item->UsageScope.NodeIds.size())
        return config.AllowedNodeIdScopeKinds.contains(item->Kind);
    if (item->UsageScope.Hosts.size()) {
        for (auto &host : item->UsageScope.Hosts)
            if (host.find(' ') != TString::npos)
                return false;
        return config.AllowedHostScopeKinds.contains(item->Kind);
    }
    return !config.DisallowedDomainScopeKinds.contains(item->Kind);
}

bool TConfigsManager::IsSupportedMergeStrategy(ui32 value) const
{
    return NKikimrConsole::TConfigItem::EMergeStrategy_IsValid(value);
}

void TConfigsManager::DumpStateHTML(IOutputStream &os) const
{
    HTML(os) {
        PRE() {
            os << "Used config:" << Endl
               << Self.GetConfig().GetConfigsConfig().DebugString() << Endl;
        }
        COLLAPSED_REF_CONTENT("configs-items-div", "Config items by kind") {
            DIV_CLASS("tab-left") {
                TOrderedConfigItemsMap itemsByKind;
                TSet<ui32> kinds;
                for (auto &pr : ConfigIndex.GetConfigItems()) {
                    itemsByKind[pr.second->Kind].insert(pr.second);
                    kinds.insert(pr.second->Kind);
                }
                for (auto kind : kinds) {
                    auto &items = itemsByKind.at(kind);
                    TString id = TStringBuilder() << "configs-items-" << kind;
                    COLLAPSED_REF_CONTENT(id, TConfigItem::KindName(kind) + "s") {
                        DIV_CLASS("tab-left") {
                            for (auto item : items) {
                                PRE() {
                                    os << "#" << item->Id << Endl
                                       << "Scope: " << item->UsageScope.ToString() << Endl
                                       << "Merge: " << item->MergeStrategyName() << Endl
                                       << "Cookie: " << item->Cookie << Endl
                                       << "Config: " << Endl
                                       << item->Config.DebugString();
                                }
                            }
                        }
                    }
                    os << "<br/>" << Endl;
                }
            }
        }
        os << "<br/>" << Endl;
        COLLAPSED_REF_CONTENT("subscriptions-div", "Subscriptions") {
            DIV_CLASS("tab-left") {
                THashMap<TDynBitMap, TSubscriptionSet> subscriptionsByKind;
                for (auto &pr : SubscriptionIndex.GetSubscriptions()) {
                    TDynBitMap kinds;
                    for (auto &kind : pr.second->ItemKinds)
                        kinds.Set(kind);
                    subscriptionsByKind[kinds].insert(pr.second);
                }

                for (auto &pr : subscriptionsByKind) {
                    TString kinds;
                    bool first = true;
                    Y_FOR_EACH_BIT(kind, pr.first) {
                        kinds += (first ? TString() : TString(", ")) + TConfigItem::KindName(kind);
                        first = false;
                    }
                    TString id = TStringBuilder() << "subscriptions-" << (void*)&pr.second;
                    COLLAPSED_REF_CONTENT(id, kinds) {
                        DIV_CLASS("tab-left") {
                            for (auto subscription : pr.second) {
                                PRE() {
                                    os << "#" << subscription->Id << Endl
                                       << "Subscriber: ";
                                    if (subscription->Subscriber.TabletId)
                                        os << "tablet " << subscription->Subscriber.TabletId << Endl;
                                    else
                                        os << "service " << subscription->Subscriber.ServiceId << Endl;
                                    os << "Config node ID: " << subscription->NodeId << Endl
                                       << "Config host: " << subscription->Host << Endl
                                       << "Config tenant: " << subscription->Tenant << Endl
                                       << "Config node type: " << subscription->NodeType << Endl
                                       << "LastProvidedConfig: " << subscription->LastProvidedConfig.ToString() << Endl
                                       << "CurrentConfigId: " << subscription->CurrentConfigId.ToString() << Endl
                                       << "Worker: " << subscription->Worker << Endl
                                       << "Cookie: " << subscription->Cookie << Endl
                                       << "Current config:" << Endl
                                       << subscription->CurrentConfig.DebugString();
                                }
                            }
                        }
                    }
                    os << "<br/>" << Endl;
                }
            }
        }
        os << "<br/>" << Endl;
        os << "<a href='../actors/console_configs_provider'>Configs Provider</a>" << Endl;
    }
}

void TConfigsManager::DbApplyPendingConfigModifications(TTransactionContext &txc,
                                                        const TActorContext &ctx) const
{
    for (auto item : PendingConfigModifications.AddedItems)
        DbUpdateItem(item, txc, ctx);
    for (auto &pr : PendingConfigModifications.ModifiedItems)
        DbUpdateItem(pr.second, txc, ctx);
    for (auto &[id, _] : PendingConfigModifications.RemovedItems)
        DbRemoveItem(id, txc, ctx);
}

void TConfigsManager::DbApplyPendingSubscriptionModifications(TTransactionContext &txc,
                                                              const TActorContext &ctx) const
{
    for (auto &id : PendingSubscriptionModifications.RemovedSubscriptions)
        DbRemoveSubscription(id, txc, ctx);
    for (auto &subscription : PendingSubscriptionModifications.AddedSubscriptions)
        DbUpdateSubscription(subscription, txc, ctx);
    for (auto &pr : PendingSubscriptionModifications.ModifiedLastProvided)
        DbUpdateSubscriptionLastProvidedConfig(pr.first, pr.second, txc, ctx);
}

bool TConfigsManager::DbLoadState(TTransactionContext &txc,
                                  const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::CMS_CONFIGS, "Loading configs state");

    NIceDb::TNiceDb db(txc.DB);
    auto nextConfigItemIdRow = db.Table<Schema::Config>().Key(TConsole::ConfigKeyNextConfigItemId).Select<Schema::Config::Value>();
    auto nextSubscriptionIdRow = db.Table<Schema::Config>().Key(TConsole::ConfigKeyNextSubscriptionId).Select<Schema::Config::Value>();
    auto nextLogItemIdRow = db.Table<Schema::Config>().Key(TConsole::ConfigKeyNextLogItemId).Select<Schema::Config::Value>();
    auto minLogItemIdRow = db.Table<Schema::Config>().Key(TConsole::ConfigKeyMinLogItemId).Select<Schema::Config::Value>();
    auto configItemRowset = db.Table<Schema::ConfigItems>().Range().Select<Schema::ConfigItems::TColumns>();
    auto subscriptionRowset = db.Table<Schema::ConfigSubscriptions>().Range().Select<Schema::ConfigSubscriptions::TColumns>();
    auto validatorsRowset = db.Table<Schema::DisabledValidators>().Range().Select<Schema::DisabledValidators::TColumns>();
    auto yamlConfigRowset = db.Table<Schema::YamlConfig>().Reverse().Select<Schema::YamlConfig::TColumns>();

    if (!configItemRowset.IsReady()
        || !nextConfigItemIdRow.IsReady()
        || !nextSubscriptionIdRow.IsReady()
        || !subscriptionRowset.IsReady()
        || !validatorsRowset.IsReady()
        || !yamlConfigRowset.IsReady())
        return false;

    if (nextConfigItemIdRow.IsValid()) {
        TString value = nextConfigItemIdRow.GetValue<Schema::Config::Value>();
        NextConfigItemId = FromString<ui64>(value);
    } else {
        NextConfigItemId = 1;
    }

    if (nextSubscriptionIdRow.IsValid()) {
        TString value = nextSubscriptionIdRow.GetValue<Schema::Config::Value>();
        NextSubscriptionId = FromString<ui64>(value);
    } else {
        NextSubscriptionId = 1;
    }

    if (nextLogItemIdRow.IsValid()) {
        TString value = nextLogItemIdRow.GetValue<Schema::Config::Value>();
        Logger.SetNextLogItemId(FromString<ui64>(value));
    }

    if (minLogItemIdRow.IsValid()) {
        TString value = minLogItemIdRow.GetValue<Schema::Config::Value>();
        Logger.SetMinLogItemId(FromString<ui64>(value));
    }

    if (!yamlConfigRowset.EndOfSet()) {
        YamlVersion = yamlConfigRowset.template GetValue<Schema::YamlConfig::Version>();
        YamlConfig = yamlConfigRowset.template GetValue<Schema::YamlConfig::Config>();
        // ignore this as deprecated
        // now used only for disabling new config layout for older console
        YamlDropped = false;
    }

    while (!configItemRowset.EndOfSet()) {
        ui64 id = configItemRowset.GetValue<Schema::ConfigItems::Id>();
        ui64 generation = configItemRowset.GetValue<Schema::ConfigItems::Generation>();
        ui32 kind = configItemRowset.GetValue<Schema::ConfigItems::Kind>();
        TVector<ui32> nodes = configItemRowset.GetValue<Schema::ConfigItems::NodeIds>();
        TString hostsVal = configItemRowset.GetValue<Schema::ConfigItems::Hosts>();
        TVector<TString> hosts = StringSplitter(hostsVal).Split(' ').SkipEmpty().ToList<TString>();
        TString tenant = configItemRowset.GetValue<Schema::ConfigItems::Tenant>();
        TString nodeType = configItemRowset.GetValue<Schema::ConfigItems::NodeType>();
        ui32 order = configItemRowset.GetValue<Schema::ConfigItems::Order>();
        ui32 merge = configItemRowset.GetValue<Schema::ConfigItems::Merge>();
        TString config = configItemRowset.GetValue<Schema::ConfigItems::Config>();
        TString cookie = configItemRowset.GetValue<Schema::ConfigItems::Cookie>();

        TConfigItem::TPtr item = new TConfigItem;
        item->Id = id;
        item->Generation = generation;
        item->Kind = kind;
        for (auto id : nodes)
            item->UsageScope.NodeIds.insert(id);
        for (auto &host : hosts)
            item->UsageScope.Hosts.insert(host);
        item->UsageScope.Tenant = tenant;
        item->UsageScope.NodeType = nodeType;
        item->UsageScope.Order = order;
        item->MergeStrategy = merge;
        Y_PROTOBUF_SUPPRESS_NODISCARD item->Config.ParseFromArray(config.data(), config.size());
        item->Cookie = cookie;
        ConfigIndex.AddItem(item);

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS, "Loaded " << item->ToString());

        if (!configItemRowset.Next())
            return false;
    }

    while (!subscriptionRowset.EndOfSet()) {
        ui64 id = subscriptionRowset.GetValue<Schema::ConfigSubscriptions::Id>();
        ui64 tabletId = subscriptionRowset.GetValue<Schema::ConfigSubscriptions::TabletId>();
        TActorId serviceId = subscriptionRowset.GetValue<Schema::ConfigSubscriptions::ServiceId>();
        ui32 nodeId = subscriptionRowset.GetValue<Schema::ConfigSubscriptions::NodeId>();
        TString host = subscriptionRowset.GetValue<Schema::ConfigSubscriptions::Host>();
        TString tenant = subscriptionRowset.GetValue<Schema::ConfigSubscriptions::Tenant>();
        TString nodeType = subscriptionRowset.GetValue<Schema::ConfigSubscriptions::NodeType>();
        TVector<ui32> kinds = subscriptionRowset.GetValue<Schema::ConfigSubscriptions::ItemKinds>();
        TVector<std::pair<ui64, ui64>> configId = subscriptionRowset.GetValue<Schema::ConfigSubscriptions::LastProvidedConfig>();

        TSubscription::TPtr subscription = new TSubscription;
        subscription->Id = id;
        subscription->Subscriber.TabletId = tabletId;
        subscription->Subscriber.ServiceId = serviceId;
        subscription->NodeId = nodeId;
        subscription->Host = host;
        subscription->Tenant = tenant;
        subscription->NodeType = nodeType;
        subscription->ItemKinds.insert(kinds.begin(), kinds.end());
        subscription->LastProvidedConfig.ItemIds = std::move(configId);
        subscription->Cookie = RandomNumber<ui64>();

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Loaded subscription: " << subscription->ToString());

        SubscriptionIndex.AddSubscription(subscription);

        if (!subscriptionRowset.Next())
            return false;
    }

    auto registry = TValidatorsRegistry::Instance();
    registry->EnableValidators();
    DisabledValidators.clear();

    while (!validatorsRowset.EndOfSet()) {
        TString name = validatorsRowset.GetValue<Schema::DisabledValidators::Name>();

        DisabledValidators.insert(name);
        registry->DisableValidator(name);

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                    "Disable validator " << name);

        if (!validatorsRowset.Next())
            return false;
    }

    return true;
}

void TConfigsManager::DbRemoveItem(ui64 id,
                                   TTransactionContext &txc,
                                   const TActorContext &ctx) const
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS, "Database: removing config item #" << id);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::ConfigItems>().Key(id).Delete();
}

void TConfigsManager::DbRemoveSubscription(ui64 id,
                                           TTransactionContext &txc,
                                           const TActorContext &ctx) const
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS, "Database: removing subscription id=" << id);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::ConfigSubscriptions>().Key(id).Delete();
}

void TConfigsManager::DbUpdateItem(TConfigItem::TPtr item,
                                   TTransactionContext &txc,
                                   const TActorContext &ctx) const
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS, "Database: "
                << (ConfigIndex.GetItem(item->Id) ? "updating " : "adding ") << item->ToString());

    TString config;
    Y_PROTOBUF_SUPPRESS_NODISCARD item->Config.SerializeToString(&config);
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::ConfigItems>().Key(item->Id)
        .Update(NIceDb::TUpdate<Schema::ConfigItems::Generation>(item->Generation))
        .Update(NIceDb::TUpdate<Schema::ConfigItems::Kind>(item->Kind))
        .Update(NIceDb::TUpdate<Schema::ConfigItems::NodeIds>({item->UsageScope.NodeIds.begin(), item->UsageScope.NodeIds.end()}))
        .Update(NIceDb::TUpdate<Schema::ConfigItems::Hosts>(JoinSeq(" ", item->UsageScope.Hosts)))
        .Update(NIceDb::TUpdate<Schema::ConfigItems::Tenant>(item->UsageScope.Tenant))
        .Update(NIceDb::TUpdate<Schema::ConfigItems::NodeType>(item->UsageScope.NodeType))
        .Update(NIceDb::TUpdate<Schema::ConfigItems::Order>(item->UsageScope.Order))
        .Update(NIceDb::TUpdate<Schema::ConfigItems::Merge>(item->MergeStrategy))
        .Update(NIceDb::TUpdate<Schema::ConfigItems::Config>(config))
        .Update(NIceDb::TUpdate<Schema::ConfigItems::Cookie>(item->Cookie));
}

void TConfigsManager::DbUpdateNextConfigItemId(TTransactionContext &txc,
                                               const TActorContext &ctx) const
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Database: update NextConfigItemId: " << NextConfigItemId);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Config>().Key(TConsole::ConfigKeyNextConfigItemId)
        .Update(NIceDb::TUpdate<Schema::Config::Value>(ToString(NextConfigItemId)));
}

void TConfigsManager::DbUpdateNextSubscriptionId(TTransactionContext &txc,
                                                 const TActorContext &ctx) const
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Database: update NextSubscriptionId: " << NextSubscriptionId);

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::Config>().Key(TConsole::ConfigKeyNextSubscriptionId)
        .Update(NIceDb::TUpdate<Schema::Config::Value>(ToString(NextSubscriptionId)));
}

void TConfigsManager::DbUpdateSubscription(TSubscription::TPtr subscription,
                                           TTransactionContext &txc,
                                           const TActorContext &ctx) const
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Database: update subscription:" << subscription->ToString());

    TVector<ui32> kinds(subscription->ItemKinds.begin(), subscription->ItemKinds.end());
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::ConfigSubscriptions>().Key(subscription->Id)
        .Update(NIceDb::TUpdate<Schema::ConfigSubscriptions::TabletId>(subscription->Subscriber.TabletId))
        .Update(NIceDb::TUpdate<Schema::ConfigSubscriptions::ServiceId>(subscription->Subscriber.ServiceId))
        .Update(NIceDb::TUpdate<Schema::ConfigSubscriptions::NodeId>(subscription->NodeId))
        .Update(NIceDb::TUpdate<Schema::ConfigSubscriptions::Host>(subscription->Host))
        .Update(NIceDb::TUpdate<Schema::ConfigSubscriptions::Tenant>(subscription->Tenant))
        .Update(NIceDb::TUpdate<Schema::ConfigSubscriptions::NodeType>(subscription->NodeType))
        .Update(NIceDb::TUpdate<Schema::ConfigSubscriptions::ItemKinds>(kinds))
        .Update(NIceDb::TUpdate<Schema::ConfigSubscriptions::LastProvidedConfig>(subscription->LastProvidedConfig.ItemIds));
}

void TConfigsManager::DbUpdateSubscriptionLastProvidedConfig(ui64 id,
                                                             const TConfigId &configId,
                                                             TTransactionContext &txc,
                                                             const TActorContext &ctx) const
{
    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS, "Database: "
                << "update last provided config for subscription"
                << " id=" << id
                << " lastprovidedconfig=" << configId.ToString());

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::ConfigSubscriptions>().Key(id)
        .Update(NIceDb::TUpdate<Schema::ConfigSubscriptions::LastProvidedConfig>(configId.ItemIds));
}

void TConfigsManager::Handle(TEvConsole::TEvGetLogTailRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxGetLogTail(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvAddConfigSubscriptionRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxAddConfigSubscription(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvConfigNotificationResponse::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxUpdateLastProvidedConfig(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvConfigureRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxConfigure(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvListConfigValidatorsRequest::TPtr &ev, const TActorContext &ctx)
{
    auto response = MakeHolder<TEvConsole::TEvListConfigValidatorsResponse>();
    response->Record.MutableStatus()->SetCode(Ydb::StatusIds::SUCCESS);

    auto registry = TValidatorsRegistry::Instance();
    for (auto &pr : registry->GetValidators()) {
        auto &entry = *response->Record.AddValidators();
        entry.SetName(pr.first);
        entry.SetDescription(pr.second->GetDescription());
        for (auto kind : pr.second->GetCheckedConfigItemKinds())
            entry.AddCheckedItemKinds(kind);
        entry.SetEnabled(pr.second->IsEnabled());
    }

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Send TEvListConfigValidatorsResponse: " << response->Record.ShortDebugString());

    ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
}

void TConfigsManager::Handle(TEvConsole::TEvRemoveConfigSubscriptionRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxRemoveConfigSubscription(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvRemoveConfigSubscriptionsRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxRemoveConfigSubscriptions(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvReplaceConfigSubscriptionsRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxReplaceConfigSubscriptions(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvToggleConfigValidatorRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxToggleConfigValidator(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxReplaceYamlConfig(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvSetYamlConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxSetYamlConfig(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvDropConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxDropYamlConfig(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvIsYamlReadOnlyRequest::TPtr &ev, const TActorContext &ctx)
{
    auto response = MakeHolder<TEvConsole::TEvIsYamlReadOnlyResponse>();
    response->Record.SetReadOnly(YamlReadOnly);
    ctx.Send(ev->Sender, response.Release());
}

void TConfigsManager::Handle(TEvConsole::TEvGetAllConfigsRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxGetYamlConfig(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvGetNodeLabelsRequest::TPtr &ev, const TActorContext &ctx)
{
    if (!AppData()->FeatureFlags.GetEnableGetNodeLabels()) {
        auto response = MakeHolder<TEvConsole::TEvDisabled>();
        ctx.Send(ev->Sender, response.Release());
    } else {
        ctx.Send(ev->Forward(MakeConfigsDispatcherID(ev->Get()->Record.GetRequest().node_id())));
    }
}

void TConfigsManager::Handle(TEvConsole::TEvGetAllMetadataRequest::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxGetYamlMetadata(ev), ctx);
}

void TConfigsManager::Handle(TEvConsole::TEvResolveConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record.GetRequest();
    try {
        auto config = rec.config();
        auto tree = NFyaml::TDocument::Parse(config);

        for (auto &volatileConfig : rec.volatile_configs()) {
            auto str = volatileConfig.config();
            auto d = NFyaml::TDocument::Parse(str);
            if (d.Root().Type() != NFyaml::ENodeType::Sequence) {
                auto node = d.Root().Map().at("selector_config");
                NYamlConfig::AppendVolatileConfigs(tree, node);
            } else {
                NYamlConfig::AppendVolatileConfigs(tree, d);
            }
        }

        TSet<NYamlConfig::TNamedLabel> namedLabels;
        for (auto &label : rec.labels()) {
            namedLabels.insert(NYamlConfig::TNamedLabel{label.label(), label.value()});
        }

        auto resolved = NYamlConfig::Resolve(tree, namedLabels);

        auto response = MakeHolder<TEvConsole::TEvResolveConfigResponse>();

        TStringStream resolvedStr;
        resolvedStr << resolved.second;

        response->Record.MutableResponse()->set_config(resolvedStr.Str());

        ctx.Send(ev->Sender, response.Release());
    } catch (const yexception& ex) {
        auto response = MakeHolder<TEvConsole::TEvGenericError>();
        response->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
        auto *issue = response->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(ex.what());
        ctx.Send(ev->Sender, response.Release());
    }
}

void TConfigsManager::Handle(TEvConsole::TEvResolveAllConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record.GetRequest();
    try {
        auto config = rec.config();
        auto tree = NFyaml::TDocument::Parse(config);

        for (auto &volatileConfig : rec.volatile_configs()) {
            auto str = volatileConfig.config();
            auto d = NFyaml::TDocument::Parse(str);
            if (d.Root().Type() != NFyaml::ENodeType::Sequence) {
                auto node = d.Root().Map().at("selector_config");
                NYamlConfig::AppendVolatileConfigs(tree, node);
            } else {
                NYamlConfig::AppendVolatileConfigs(tree, d);
            }
        }

        auto resolved = NYamlConfig::ResolveAll(tree);

        auto Response = MakeHolder<TEvConsole::TEvResolveAllConfigResponse>();

        auto convert = [] (const NYamlConfig::TLabel::EType& label) -> Ydb::DynamicConfig::YamlLabelExt::LabelType {
            switch(label) {
            case NYamlConfig::TLabel::EType::Negative:
                return Ydb::DynamicConfig::YamlLabelExt::NOT_SET;
            case NYamlConfig::TLabel::EType::Common:
                return Ydb::DynamicConfig::YamlLabelExt::COMMON;
            case NYamlConfig::TLabel::EType::Empty:
                return Ydb::DynamicConfig::YamlLabelExt::EMPTY;
            default:
                Y_ABORT("unexpected enum value");
            }
        };

        if (!rec.verbose_response()) {
            TStringStream resolvedStr;

            bool first = true;

            for (auto &[labelSets, config] : resolved.Configs) {
                if (!first) {
                    resolvedStr << "\n";
                }
                resolvedStr << "---\n";
                resolvedStr << "# applicable to: \n";
                for (auto &labelSet : labelSets) {
                    resolvedStr << "# [";
                    for (size_t i = 0; i < labelSet.size(); ++i) {
                        auto &label = labelSet[i];
                        resolvedStr << resolved.Labels[i] << ":" << (
                            label.Type == NYamlConfig::TLabel::EType::Common   ? label.Value.Quote() :
                            label.Type == NYamlConfig::TLabel::EType::Negative ? " - "               :
                                                                                 " _ "               )
                                << ", ";
                    }
                    resolvedStr << "] \n";
                }
                resolvedStr << config.second << Endl;
                first = false;
            }

            Response->Record.MutableResponse()->set_config(resolvedStr.Str());
        } else {
            for (auto &[labelSets, config] : resolved.Configs) {
                auto *serConfig = Response->Record.MutableResponse()->add_configs();
                for (auto &labelSet : labelSets) {
                    auto *serLabelSet = serConfig->add_label_sets();
                    for (size_t i = 0; i < labelSet.size(); ++i) {
                        auto &label = labelSet[i];
                        auto &name = resolved.Labels[i];
                        auto* serLabel = serLabelSet->add_labels();
                        serLabel->set_label(name);
                        serLabel->set_type(convert(label.Type));
                        serLabel->set_value(label.Value);
                    }
                }
                TStringStream configStr;
                configStr << config.second;
                serConfig->set_config(configStr.Str());
            }
        }

        ctx.Send(ev->Sender, Response.Release());
    } catch (const yexception& ex) {
        auto response = MakeHolder<TEvConsole::TEvGenericError>();
        response->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
        auto *issue = response->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(ex.what());
        ctx.Send(ev->Sender, response.Release());
    }
}

void TConfigsManager::Handle(TEvConsole::TEvAddVolatileConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record.GetRequest();

    try {
        auto response = MakeHolder<TEvConsole::TEvAddVolatileConfigResponse>();
        auto cfg = rec.config();
        auto metadata = NYamlConfig::GetVolatileMetadata(cfg);

        if (!metadata.Id || !metadata.Cluster || !metadata.Version) {
            ythrow yexception() << "Invalid metadata";
        }

        cfg = NYamlConfig::ReplaceMetadata(cfg, metadata);

        auto clusterName = metadata.Cluster.value();
        auto id = metadata.Id.value();
        auto version = metadata.Version.value();

        auto doc = NFyaml::TDocument::Parse(cfg);
        NYamlConfig::ValidateVolatileConfig(doc);
        auto node = doc.Root().Map().at("selector_config");

        if (VolatileYamlConfigs.empty() || VolatileYamlConfigs.rbegin()->first + 1 == id) {
            auto config = YamlConfig;
            auto tree = NFyaml::TDocument::Parse(config);

            for (auto &[_, config] : VolatileYamlConfigs) {
                auto doc = NFyaml::TDocument::Parse(config);
                auto node = doc.Root().Map().at("selector_config");
                NYamlConfig::AppendVolatileConfigs(tree, node);
            }

            NYamlConfig::AppendVolatileConfigs(tree, node);

            auto resolved = NYamlConfig::ResolveAll(tree);

            for (auto &[_, config] : resolved.Configs) {
                auto cfg = NYamlConfig::YamlToProto(config.second, true);
            }

            if (ClusterName != clusterName) {
                ythrow yexception() << "ClusterName mismatch";
            }

            if (YamlVersion != version) {
                ythrow yexception() << "Version mismatch";
            }

            VolatileYamlConfigs.try_emplace(id, cfg);

            auto resp = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig>(
                YamlConfig,
                VolatileYamlConfigs);
            ctx.Send(ConfigsProvider, resp.Release());
        } else if (auto it = VolatileYamlConfigs.find(id); it == VolatileYamlConfigs.end() || it->second != cfg) {
            ythrow yexception() << "Config already exists";
        }

        ctx.Send(ev->Sender, response.Release());
    } catch (const yexception& ex) {
        auto response = MakeHolder<TEvConsole::TEvGenericError>();
        response->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
        auto *issue = response->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(ex.what());
        ctx.Send(ev->Sender, response.Release());
    }
}

void TConfigsManager::Handle(TEvConsole::TEvRemoveVolatileConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record.GetRequest();

    try {
        if (!rec.force()) {
            if (ClusterName != rec.identity().cluster()) {
                ythrow yexception() << "ClusterName mismatch";
            }

            if (YamlVersion != rec.identity().version()) {
                ythrow yexception() << "Version mismatch";
            }
        }

        int toRemove = 0;

        for (auto &id : rec.ids().ids()) {
            toRemove += (VolatileYamlConfigs.find(id) != VolatileYamlConfigs.end()) ? 1 : 0;
        }

        if (rec.all()) {
            VolatileYamlConfigs.clear();
        } else if (rec.ids().ids_size() == toRemove) {
            for (auto &id : rec.ids().ids()) {
                VolatileYamlConfigs.erase(id);
            }
        } else {
            ythrow yexception() << "Incorrect id('s)";
        }

        auto resp = MakeHolder<TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig>(
            YamlConfig,
            VolatileYamlConfigs);
        ctx.Send(ConfigsProvider, resp.Release());

        auto response = MakeHolder<TEvConsole::TEvRemoveVolatileConfigResponse>();
        ctx.Send(ev->Sender, response.Release());
    } catch (const yexception& ex) {
        auto response = MakeHolder<TEvConsole::TEvGenericError>();
        response->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
        auto *issue = response->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(ex.what());
        ctx.Send(ev->Sender, response.Release());
    }
}

void TConfigsManager::Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev, const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxCleanupSubscriptions(ev), ctx);
}

void TConfigsManager::Handle(TEvPrivate::TEvStateLoaded::TPtr &/*ev*/, const TActorContext &ctx)
{
    ctx.Send(ConfigsProvider, new TConfigsProvider::TEvPrivate::TEvSetConfigs(ConfigIndex.GetConfigItems()));
    ctx.Send(ConfigsProvider, new TConfigsProvider::TEvPrivate::TEvSetSubscriptions(SubscriptionIndex.GetSubscriptions()));
    ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
    if (!YamlConfig.empty()) {
        ctx.Send(ConfigsProvider, new TConfigsProvider::TEvPrivate::TEvUpdateYamlConfig(YamlConfig, VolatileYamlConfigs));
    }
    ScheduleLogCleanup(ctx);
}

void TConfigsManager::Handle(TEvPrivate::TEvCleanupSubscriptions::TPtr &/*ev*/, const TActorContext &ctx)
{
    ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
}

void TConfigsManager::ForwardToConfigsProvider(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx)
{
    ctx.Forward(ev, ConfigsProvider);
}

void TConfigsManager::ScheduleSubscriptionsCleanup(const TActorContext &ctx)
{
    auto *event = new TConfigsManager::TEvPrivate::TEvCleanupSubscriptions;
    SubscriptionsCleanupTimerCookieHolder.Reset(ISchedulerCookie::Make2Way());
    CreateLongTimer(ctx, TDuration::Minutes(5),
                    new IEventHandle(SelfId(), SelfId(), event),
                    AppData(ctx)->SystemPoolId,
                    SubscriptionsCleanupTimerCookieHolder.Get());
}

void TConfigsManager::CleanupLog(const TActorContext &ctx)
{
    TxProcessor->ProcessTx(CreateTxLogCleanup(), ctx);
}

void TConfigsManager::ScheduleLogCleanup(const TActorContext &ctx)
{
    LogCleanupTimerCookieHolder.Reset(ISchedulerCookie::Make2Way());
    CreateLongTimer(ctx, TDuration::Minutes(15),
                    new IEventHandle(SelfId(), SelfId(), new TEvPrivate::TEvCleanupLog),
                    AppData(ctx)->SystemPoolId,
                    LogCleanupTimerCookieHolder.Get());
}

void TConfigsManager::HandleUnauthorized(TEvConsole::TEvReplaceYamlConfigRequest::TPtr &ev, const TActorContext &) {
    AuditLogReplaceConfigTransaction(
        /* peer = */ ev->Get()->Record.GetPeerName(),
        /* userSID = */ ev->Get()->Record.GetUserToken(),
        /* oldConfig = */ YamlConfig,
        /* newConfig = */ ev->Get()->Record.GetRequest().config(),
        /* reason = */ "Unauthorized.",
        /* success = */ false);
}

void TConfigsManager::HandleUnauthorized(TEvConsole::TEvSetYamlConfigRequest::TPtr &ev, const TActorContext &) {
    AuditLogReplaceConfigTransaction(
        /* peer = */ ev->Get()->Record.GetPeerName(),
        /* userSID = */ ev->Get()->Record.GetUserToken(),
        /* oldConfig = */ YamlConfig,
        /* newConfig = */ ev->Get()->Record.GetRequest().config(),
        /* reason = */ "Unauthorized.",
        /* success = */ false);
}

} // namespace NKikimr::NConsole
