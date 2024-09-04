#include "test_env.h"
#include "helpers.h"

#include <ydb/core/blockstore/core/blockstore.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/metering/metering.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard_private.h>
#include <ydb/core/tx/sequenceproxy/sequenceproxy.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/filestore/core/filestore.h>

#include <library/cpp/testing/unittest/registar.h>


bool NSchemeShardUT_Private::TTestEnv::ENABLE_SCHEMESHARD_LOG = true;
static const bool ENABLE_DATASHARD_LOG = false;
static const bool ENABLE_COORDINATOR_MEDIATOR_LOG = false;
static const bool ENABLE_SCHEMEBOARD_LOG = false;
static const bool ENABLE_COLUMNSHARD_LOG = false;
static const bool ENABLE_EXPORT_LOG = false;
static const bool ENABLE_TOPIC_LOG = false;

using namespace NKikimr;
using namespace NSchemeShard;

// BlockStoreVolume mock for testing schemeshard
class TFakeBlockStoreVolume : public TActor<TFakeBlockStoreVolume>, public NTabletFlatExecutor::TTabletExecutedFlat {
public:
    TFakeBlockStoreVolume(const TActorId& tablet, TTabletStorageInfo* info)
        : TActor(&TThis::StateInit)
          , TTabletExecutedFlat(info, tablet,  new NMiniKQL::TMiniKQLFactory)
    {}

    void DefaultSignalTabletActive(const TActorContext&) override {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext& ctx) override {
        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
    }

    void OnDetach(const TActorContext& ctx) override {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override {
        Y_UNUSED(ev);
        Die(ctx);
    }

    STFUNC(StateInit) {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlockStore::TEvUpdateVolumeConfig, Handle);
        default:
            HandleDefaultEvents(ev, SelfId());
        }
    }

private:
    void Handle(TEvBlockStore::TEvUpdateVolumeConfig::TPtr& ev, const TActorContext& ctx) {
        const auto& request = ev->Get()->Record;
        TAutoPtr<TEvBlockStore::TEvUpdateVolumeConfigResponse> response =
            new TEvBlockStore::TEvUpdateVolumeConfigResponse();
        response->Record.SetTxId(request.GetTxId());
        response->Record.SetOrigin(TabletID());
        response->Record.SetStatus(NKikimrBlockStore::OK);
        ctx.Send(ev->Sender, response.Release());
    }
};

class TFakeFileStore : public TActor<TFakeFileStore>, public NTabletFlatExecutor::TTabletExecutedFlat {
public:
    TFakeFileStore(const TActorId& tablet, TTabletStorageInfo* info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet,  new NMiniKQL::TMiniKQLFactory)
    {}

    void DefaultSignalTabletActive(const TActorContext&) override {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext& ctx) override {
        Become(&TThis::StateWork);
        SignalTabletActive(ctx);
    }

    void OnDetach(const TActorContext& ctx) override {
        Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override {
        Y_UNUSED(ev);
        Die(ctx);
    }

    STFUNC(StateInit) {
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvFileStore::TEvUpdateConfig, Handle);
        default:
            HandleDefaultEvents(ev, SelfId());
        }
    }

private:
    void Handle(TEvFileStore::TEvUpdateConfig::TPtr& ev, const TActorContext& ctx) {
        const auto& request = ev->Get()->Record;
        TAutoPtr<TEvFileStore::TEvUpdateConfigResponse> response =
            new TEvFileStore::TEvUpdateConfigResponse();
        response->Record.SetTxId(request.GetTxId());
        response->Record.SetOrigin(TabletID());
        response->Record.SetStatus(NKikimrFileStore::OK);
        ctx.Send(ev->Sender, response.Release());
    }
};

class TFakeConfigDispatcher : public TActor<TFakeConfigDispatcher> {
public:
    TFakeConfigDispatcher()
        : TActor<TFakeConfigDispatcher>(&TFakeConfigDispatcher::StateWork)
    {
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest, Handle);
        }
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        ctx.Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationRequest(), 0, ev->Cookie);
    }
};

// Automatically resend notification requests to Schemeshard if it gets restarted
class TTxNotificationSubscriber : public TActor<TTxNotificationSubscriber> {
public:
    explicit TTxNotificationSubscriber(ui64 schemeshardTabletId)
        : TActor<TTxNotificationSubscriber>(&TTxNotificationSubscriber::StateWork)
        , SchemeshardTabletId(schemeshardTabletId)
    {}

private:
    void StateWork(TAutoPtr<NActors::IEventHandle> &ev) {
        switch (ev->GetTypeRewrite()) {
        HFunc(TEvTabletPipe::TEvClientConnected, Handle);
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        HFunc(TEvSchemeShard::TEvNotifyTxCompletion, Handle);
        HFunc(TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        };
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        if (msg->Status == NKikimrProto::OK)
            return;

        DoHandleDisconnect(ev->Get()->ClientId, ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        DoHandleDisconnect(ev->Get()->ClientId, ctx);
    }

    void DoHandleDisconnect(TActorId pipeClient, const TActorContext &ctx) {
        const auto found = PipeToTx.find(pipeClient);
        if (found != PipeToTx.end()) {
            const auto pipeActor = found->first;
            const auto txId = found->second;

            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "tests -- TTxNotificationSubscriber for txId " << txId << ": disconnected from schemeshard, resend EvNotifyTxCompletion");

            // Remove entry from the tx-pipe mapping. Pipe actor has already died.
            PipeToTx.erase(pipeActor);
            TxToPipe.erase(txId);

            // Recreate pipe to schemeshard and resend notification request for txId
            SendToSchemeshard(txId, ctx);
        }
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletion::TPtr &ev, const TActorContext &ctx) {
        ui64 txId = ev->Get()->Record.GetTxId();

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "tests -- TTxNotificationSubscriber for txId " << txId << ": send EvNotifyTxCompletion");

        // Add txId, add waiter, recreate pipe and send notification request

        SchemeTxWaiters[txId].insert(ev->Sender);

        if (TxToPipe.contains(txId)) {
            DropTxPipe(txId, ctx);
        }

        SendToSchemeshard(txId, ctx);
    }

    void Handle(TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr &ev, const TActorContext &ctx) {
        ui64 txId = ev->Get()->Record.GetTxId();

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "tests -- TTxNotificationSubscriber for txId " << txId << ": got EvNotifyTxCompletionResult");

        if (!SchemeTxWaiters.contains(txId))
            return;

        // Notify all waiters, forget txId, drop pipe

        for (TActorId waiter : SchemeTxWaiters[txId]) {
            LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                        "tests -- TTxNotificationSubscriber for txId " << txId <<": satisfy waiter " << waiter);
            ctx.Send(waiter, new TEvSchemeShard::TEvNotifyTxCompletionResult(txId));
        }
        SchemeTxWaiters.erase(txId);

        DropTxPipe(txId, ctx);
    }

    void DropTxPipe(ui64 txId, const TActorContext &ctx) {
        // Remove entry from the tx-pipe mapping
        auto pipeActor = TxToPipe.at(txId);
        PipeToTx.erase(pipeActor);
        TxToPipe.erase(txId);

        // Destroy pipe actor
        NTabletPipe::CloseClient(ctx, pipeActor);
    }

    void SendToSchemeshard(ui64 txId, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "tests -- TTxNotificationSubscriber, SendToSchemeshard, txId " << txId);

        // NOTE: the only reason why we should send every EvNotifyTxCompletion to schemeshard
        // with a separate pipe is to avoid out-of-order reception of 2 events on the schemeshard side:
        // 1. ModifyScheme -- that starts operation/Tx
        // 2. NotifyTxCompletion -- that subscribes to that operation completion
        //
        // Helper methods (Async...()) our tests use, send every new ModifyScheme through TabletResolver
        // (with ForwardToTablet()) and if TestWaitNotification() does not use TabletResolver
        // (in some way or another) to send every new NotifyTxCompletion, then NotifyTxCompletion could be quicker
        // and on the schemeshard side NotifyTxCompletion could be processed before ModifyScheme registers as an operation.
        // Which will allow test to proceed before operation actually will have complete.
        // Been there, caught that in flaky tests.
        //
        // Event sending paths for ModifyScheme and NotifyTxCompletion should provide sequential delivery.
        // Pipe creation also uses TabletResolver. Hence: new pipe for every NotifyTxCompletion.

        auto pipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, SchemeshardTabletId, GetPipeConfigWithRetries()));
        NTabletPipe::SendData(ctx, pipe, new TEvSchemeShard::TEvNotifyTxCompletion(txId));

        // Add entry to the tx-pipe mapping
        TxToPipe[txId] = pipe;
        PipeToTx[pipe] = txId;
    }

    void Handle(TEvents::TEvPoisonPill::TPtr, const TActorContext &ctx) {
        // Destroy pipe actors
        for (auto& [pipeActor, _] : PipeToTx) {
            NTabletPipe::CloseClient(ctx, pipeActor);
        }
        // Cleanup tx-pipe mapping
        PipeToTx.clear();
        TxToPipe.clear();

        Die(ctx);
    }

private:
    ui64 SchemeshardTabletId;
    // txId to waiters map
    THashMap<ui64, THashSet<TActorId>> SchemeTxWaiters;
    // txId to/from pipe-to-schemeshard mapping
    THashMap<ui64, TActorId> TxToPipe;
    THashMap<TActorId, ui64> PipeToTx;
};


// Automatically resend notification requests to Schemeshard if it gets restarted
class TFakeMetering : public TActor<TFakeMetering> {
    TVector<TString> Jsons;

public:
    explicit TFakeMetering()
        : TActor<TFakeMetering>(&TFakeMetering::StateWork)
    {}

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            HFunc(NMetering::TEvMetering::TEvWriteMeteringJson, HandleWriteMeteringJson);
        default:
            HandleUnexpectedEvent(ev);
            break;
        }
    }

    void HandlePoisonPill(const TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void HandleWriteMeteringJson(
        const NMetering::TEvMetering::TEvWriteMeteringJson::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ctx);

        LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "tests -- TFakeMetering got TEvMetering::TEvWriteMeteringJson");

        const auto* msg = ev->Get();

        Jsons.push_back(msg->MeteringJson);
    }

    void HandleUnexpectedEvent(STFUNC_SIG)
    {
        ALOG_DEBUG(NKikimrServices::FLAT_TX_SCHEMESHARD,
                    "TFakeMetering:"
                        << " unhandled event type: " << ev->GetTypeRewrite()
                        << " event: " << ev->ToString());
    }

private:
};


// Automatically resend transaction requests to Schemeshard if it gets restarted
class TTxReliablePropose : public TActor<TTxReliablePropose> {
public:
    explicit TTxReliablePropose(ui64 schemeshardTabletId)
        : TActor<TTxReliablePropose>(&TTxReliablePropose::StateWork)
          , SchemeshardTabletId(schemeshardTabletId)
    {}

private:
    using TPreSerializedMessage = std::pair<ui32, TIntrusivePtr<TEventSerializedData>>; // ui32 it's a type

private:
    void StateWork(TAutoPtr<NActors::IEventHandle> &ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

            HFunc(TEvSchemeShard::TEvModifySchemeTransaction, Handle);
            HFunc(TEvSchemeShard::TEvModifySchemeTransactionResult, Handle);

            HFunc(TEvSchemeShard::TEvCancelTx, Handle);
            HFunc(TEvSchemeShard::TEvCancelTxResult, Handle);

            HFunc(TEvExport::TEvCancelExportRequest, Handle);
            HFunc(TEvExport::TEvCancelExportResponse, Handle);

            HFunc(TEvExport::TEvForgetExportRequest, Handle);
            HFunc(TEvExport::TEvForgetExportResponse, Handle);

            HFunc(TEvImport::TEvCancelImportRequest, Handle);
            HFunc(TEvImport::TEvCancelImportResponse, Handle);
        };
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        if (msg->Status == NKikimrProto::OK)
            return;

        DoHandleDisconnect(ev->Get()->ClientId, ctx);
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        DoHandleDisconnect(ev->Get()->ClientId, ctx);
    }

    void DoHandleDisconnect(TActorId pipeClient, const TActorContext &ctx) {
        if (pipeClient == SchemeShardPipe) {
            SchemeShardPipe = TActorId();
            // Resend all
            for (const auto& w : SchemeTxWaiters) {
                SendToSchemeshard(w.first, ctx);
            }
        }
    }

    template<class TEventPtr>
    void HandleRequest(TEventPtr &ev, const TActorContext &ctx) {
        ui64 txId = ev->Get()->Record.GetTxId();
        if (SchemeTxWaiters.contains(txId))
            return;

        // Save TxId, forward to schemeshard
        SchemeTxWaiters[txId] = ev->Sender;
        OnlineRequests[txId] = GetSerializedMessage(ev->ReleaseBase());
        SendToSchemeshard(txId, ctx);
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransaction::TPtr &ev, const TActorContext &ctx) {
        HandleRequest(ev, ctx);
    }

    void Handle(TEvSchemeShard::TEvCancelTx::TPtr &ev, const TActorContext &ctx) {
        HandleRequest(ev, ctx);
    }

    void Handle(TEvExport::TEvCancelExportRequest::TPtr &ev, const TActorContext &ctx) {
        HandleRequest(ev, ctx);
    }

    void Handle(TEvExport::TEvForgetExportRequest::TPtr &ev, const TActorContext &ctx) {
        HandleRequest(ev, ctx);
    }

    void Handle(TEvImport::TEvCancelImportRequest::TPtr &ev, const TActorContext &ctx) {
        HandleRequest(ev, ctx);
    }

    template<class TEventPtr>
    void HandleResponse(TEventPtr &ev, const TActorContext &ctx) {
        ui64 txId = ev->Get()->Record.GetTxId();
        if (!SchemeTxWaiters.contains(txId))
            return;

        ctx.Send(SchemeTxWaiters[txId], ev->ReleaseBase().Release());

        SchemeTxWaiters.erase(txId);
        OnlineRequests.erase(txId);
    }

    void Handle(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr &ev, const TActorContext &ctx) {
        HandleResponse(ev, ctx);
    }

    void Handle(TEvSchemeShard::TEvCancelTxResult::TPtr &ev, const TActorContext &ctx) {
        HandleResponse(ev, ctx);
    }

    void Handle(TEvExport::TEvCancelExportResponse::TPtr &ev, const TActorContext &ctx) {
        HandleResponse(ev, ctx);
    }

    void Handle(TEvExport::TEvForgetExportResponse::TPtr &ev, const TActorContext &ctx) {
        HandleResponse(ev, ctx);
    }

    void Handle(TEvImport::TEvCancelImportResponse::TPtr &ev, const TActorContext &ctx) {
        HandleResponse(ev, ctx);
    }

    void SendToSchemeshard(ui64 txId, const TActorContext &ctx) {
        if (!SchemeShardPipe) {
            SchemeShardPipe = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, SchemeshardTabletId, GetPipeConfigWithRetries()));
        }

        TPreSerializedMessage& preSerializedMessages = OnlineRequests[txId];
        NTabletPipe::SendData(ctx, SchemeShardPipe, preSerializedMessages.first, preSerializedMessages.second, 0);
    }

    void Handle(TEvents::TEvPoisonPill::TPtr, const TActorContext &ctx) {
        if (SchemeShardPipe) {
            NTabletPipe::CloseClient(ctx, SchemeShardPipe);
        }
        Die(ctx);
    }

    TPreSerializedMessage GetSerializedMessage(TAutoPtr<IEventBase> message) {
        TAllocChunkSerializer serializer;
        const bool success = message->SerializeToArcadiaStream(&serializer);
        Y_ABORT_UNLESS(success);
        TIntrusivePtr<TEventSerializedData> data = serializer.Release(message->CreateSerializationInfo());
        return TPreSerializedMessage(message->Type(), data);
    }

private:
    ui64 SchemeshardTabletId;
    TActorId SchemeShardPipe;
    THashMap<ui64, TPreSerializedMessage> OnlineRequests;
    THashMap<ui64, TActorId> SchemeTxWaiters;
};


// Globally enable/disable log batching at datashard creation time in test
NSchemeShardUT_Private::TTestWithReboots::TDatashardLogBatchingSwitch::TDatashardLogBatchingSwitch(bool newVal) {
    PrevVal = NKikimr::NDataShard::gAllowLogBatchingDefaultValue;
    NKikimr::NDataShard::gAllowLogBatchingDefaultValue = newVal;
}

NSchemeShardUT_Private::TTestWithReboots::TDatashardLogBatchingSwitch::~TDatashardLogBatchingSwitch() {
    NKikimr::NDataShard::gAllowLogBatchingDefaultValue = PrevVal;
}

NSchemeShardUT_Private::TTestEnv::TTestEnv(TTestActorRuntime& runtime, const TTestEnvOptions& opts, TSchemeShardFactory ssFactory, std::shared_ptr<NKikimr::NDataShard::IExportFactory> dsExportFactory)
    : SchemeShardFactory(ssFactory)
    , HiveState(new TFakeHiveState)
    , CoordinatorState(new TFakeCoordinator::TState)
    , ChannelsCount(opts.NChannels_)
{
    ui64 hive = TTestTxConfig::Hive;
    ui64 schemeRoot = TTestTxConfig::SchemeShard;
    ui64 coordinator = TTestTxConfig::Coordinator;
    ui64 txAllocator = TTestTxConfig::TxAllocator;

    TAppPrepare app(dsExportFactory ? dsExportFactory : static_cast<std::shared_ptr<NKikimr::NDataShard::IExportFactory>>(std::make_shared<TDataShardExportFactory>()));

    app.SetEnableDataColumnForIndexTable(true);
    app.SetEnableSystemViews(opts.EnableSystemViews_);
    app.SetEnablePersistentQueryStats(opts.EnablePersistentQueryStats_);
    app.SetEnablePersistentPartitionStats(opts.EnablePersistentPartitionStats_);
    app.SetAllowUpdateChannelsBindingOfSolomonPartitions(opts.AllowUpdateChannelsBindingOfSolomonPartitions_);
    app.SetEnableNotNullColumns(opts.EnableNotNullColumns_);
    app.SetEnableProtoSourceIdInfo(opts.EnableProtoSourceIdInfo_);
    app.SetEnablePqBilling(opts.EnablePqBilling_);
    app.SetEnableBackgroundCompaction(opts.EnableBackgroundCompaction_);
    app.SetEnableBorrowedSplitCompaction(opts.EnableBorrowedSplitCompaction_);
    app.FeatureFlags.SetEnablePublicApiExternalBlobs(true);
    app.FeatureFlags.SetEnableTableDatetime64(true);
    app.FeatureFlags.SetEnableVectorIndex(true);
    app.SetEnableMoveIndex(opts.EnableMoveIndex_);
    app.SetEnableChangefeedInitialScan(opts.EnableChangefeedInitialScan_);
    app.SetEnableNotNullDataColumns(opts.EnableNotNullDataColumns_);
    app.SetEnableAlterDatabaseCreateHiveFirst(opts.EnableAlterDatabaseCreateHiveFirst_);
    app.SetEnableTopicDiskSubDomainQuota(opts.EnableTopicDiskSubDomainQuota_);
    app.SetEnablePQConfigTransactionsAtSchemeShard(opts.EnablePQConfigTransactionsAtSchemeShard_);
    app.SetEnableTopicSplitMerge(opts.EnableTopicSplitMerge_);
    app.SetEnableChangefeedDynamoDBStreamsFormat(opts.EnableChangefeedDynamoDBStreamsFormat_);
    app.SetEnableChangefeedDebeziumJsonFormat(opts.EnableChangefeedDebeziumJsonFormat_);
    app.SetEnableTablePgTypes(opts.EnableTablePgTypes_);
    app.SetEnableServerlessExclusiveDynamicNodes(opts.EnableServerlessExclusiveDynamicNodes_);
    app.SetEnableAddColumsWithDefaults(opts.EnableAddColumsWithDefaults_);
    app.SetEnableReplaceIfExistsForExternalEntities(opts.EnableReplaceIfExistsForExternalEntities_);
    app.SetEnableChangefeedsOnIndexTables(opts.EnableChangefeedsOnIndexTables_);

    app.ColumnShardConfig.SetDisabledOnSchemeShard(false);

    if (opts.DisableStatsBatching_.value_or(false)) {
        app.SchemeShardConfig.SetStatsMaxBatchSize(0);
        app.SchemeShardConfig.SetStatsBatchTimeoutMs(0);
    }

    // graph settings
    if (opts.GraphBackendType_) {
        app.GraphConfig.SetBackendType(opts.GraphBackendType_.value());
    }
    app.GraphConfig.SetAggregateCheckPeriodSeconds(5); // 5 seconds
    {
        auto& set = *app.GraphConfig.AddAggregationSettings();
        set.SetPeriodToStartSeconds(60); // 1 minute to clear
        set.SetMinimumStepSeconds(10); // 10 seconds
    }
    {
        auto& set = *app.GraphConfig.AddAggregationSettings();
        set.SetPeriodToStartSeconds(40); // 40 seconds
        set.SetSampleSizeSeconds(10); // 10 seconds
        set.SetMinimumStepSeconds(10); // 10 seconds
    }
    {
        auto& set = *app.GraphConfig.AddAggregationSettings();
        set.SetPeriodToStartSeconds(5); // 4 seconds
        set.SetSampleSizeSeconds(5); // 5 seconds
        set.SetMinimumStepSeconds(5); // 5 seconds
    }
    //

    for (const auto& sid : opts.SystemBackupSIDs_) {
        app.AddSystemBackupSID(sid);
    }

    AddDomain(runtime, app, TTestTxConfig::DomainUid, hive, schemeRoot);

    SetupLogging(runtime);
    SetupChannelProfiles(app, ChannelsCount);

    for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
        SetupSchemeCache(runtime, node, app.Domains->GetDomain(TTestTxConfig::DomainUid).Name);
    }

    SetupTabletServices(runtime, &app);
    if (opts.EnablePipeRetries_) {
        EnableSchemeshardPipeRetriesGuard = EnableSchemeshardPipeRetries(runtime);
    }

    if (opts.RunFakeConfigDispatcher_) {
        for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
            runtime.RegisterService(NConsole::MakeConfigsDispatcherID(runtime.GetNodeId(node)),
                runtime.Register(new TFakeConfigDispatcher(), node), node);
        }
    }

    if (opts.InitYdbDriver_) {
        YdbDriver = MakeHolder<NYdb::TDriver>(NYdb::TDriverConfig());
        runtime.GetAppData().YdbDriver = YdbDriver.Get();
    }

    TActorId sender = runtime.AllocateEdgeActor();
    //CreateTestBootstrapper(runtime, CreateTestTabletInfo(MakeBSControllerID(TTestTxConfig::DomainUid), TTabletTypes::BSController), &CreateFlatBsController);
    BootSchemeShard(runtime, schemeRoot);
    BootTxAllocator(runtime, txAllocator);
    BootFakeCoordinator(runtime, coordinator, CoordinatorState);
    BootFakeHive(runtime, hive, HiveState, &GetTabletCreationFunc);

    InitRootStoragePools(runtime, schemeRoot, sender, TTestTxConfig::DomainUid);

    for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
        IActor* txProxy = CreateTxProxy(runtime.GetTxAllocatorTabletIds());
        TActorId txProxyId = runtime.Register(txProxy, node);
        runtime.RegisterService(MakeTxProxyID(), txProxyId, node);
    }

    // Create sequence proxies
    for (size_t i = 0; i < runtime.GetNodeCount(); ++i) {
        IActor* sequenceProxy = NSequenceProxy::CreateSequenceProxy();
        TActorId sequenceProxyId = runtime.Register(sequenceProxy, i);
        runtime.RegisterService(NSequenceProxy::MakeSequenceProxyServiceID(), sequenceProxyId, i);
    }

    //SetupBoxAndStoragePool(runtime, sender, TTestTxConfig::DomainUid);

    TxReliablePropose = runtime.Register(new TTxReliablePropose(schemeRoot));
    CreateFakeMetering(runtime);

    SetSplitMergePartCountLimit(&runtime, -1);
}

NSchemeShardUT_Private::TTestEnv::TTestEnv(TTestActorRuntime &runtime, ui32 nchannels, bool enablePipeRetries,
        NSchemeShardUT_Private::TTestEnv::TSchemeShardFactory ssFactory, bool enableSystemViews)
    : TTestEnv(runtime, TTestEnvOptions()
        .NChannels(nchannels)
        .EnablePipeRetries(enablePipeRetries)
        .EnableSystemViews(enableSystemViews), ssFactory)
{
}

void NSchemeShardUT_Private::TTestEnv::SetupLogging(TTestActorRuntime &runtime) {
    runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::PERSQUEUE_READ_BALANCER, NActors::NLog::PRI_ERROR);
    if (ENABLE_TOPIC_LOG) {
        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PERSQUEUE_READ_BALANCER, NActors::NLog::PRI_DEBUG);
    }

    runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::PIPE_CLIENT, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::PIPE_SERVER, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::TABLET_RESOLVER, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_ERROR);


    runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);
    runtime.SetLogPriority(NKikimrServices::SCHEMESHARD_DESCRIBE, NActors::NLog::PRI_NOTICE);
    runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_NOTICE);
    if (ENABLE_SCHEMESHARD_LOG) {
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::SCHEMESHARD_DESCRIBE, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
    }

    runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_NOTICE);
    runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_NOTICE);
    if (ENABLE_EXPORT_LOG) {
        runtime.SetLogPriority(NKikimrServices::EXPORT, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::DATASHARD_BACKUP, NActors::NLog::PRI_TRACE);
    }

    if (ENABLE_SCHEMEBOARD_LOG) {
        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_TRACE);
    }

    runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_ERROR);
    if (ENABLE_DATASHARD_LOG) {
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
    }

    runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NActors::NLog::PRI_ERROR);
    runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TABLETQUEUE, NActors::NLog::PRI_ERROR);
    if (ENABLE_COORDINATOR_MEDIATOR_LOG) {
        runtime.SetLogPriority(NKikimrServices::TX_COORDINATOR, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TIMECAST, NActors::NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::TX_MEDIATOR_TABLETQUEUE, NActors::NLog::PRI_DEBUG);
    }

    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_NOTICE);
    if (ENABLE_COLUMNSHARD_LOG) {
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
    }
}

void NSchemeShardUT_Private::TTestEnv::AddDomain(TTestActorRuntime &runtime, TAppPrepare &app, ui32 domainUid, ui64 hive, ui64 schemeRoot) {
    app.ClearDomainsAndHive();
    ui32 planResolution = 50;
    auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
                "MyRoot", domainUid, schemeRoot,
                planResolution,
                TVector<ui64>{TDomainsInfo::MakeTxCoordinatorIDFixed(1)},
                TVector<ui64>{},
                TVector<ui64>{TDomainsInfo::MakeTxAllocatorIDFixed(1)},
                DefaultPoolKinds(2));

    TVector<ui64> ids = runtime.GetTxAllocatorTabletIds();
    ids.insert(ids.end(), domain->TxAllocators.begin(), domain->TxAllocators.end());
    runtime.SetTxAllocatorTabletIds(ids);

    app.AddDomain(domain.Release());
    app.AddHive(hive);
}

TFakeHiveState::TPtr NSchemeShardUT_Private::TTestEnv::GetHiveState() const {
    return HiveState;
}

TAutoPtr<ITabletScheduledEventsGuard> NSchemeShardUT_Private::TTestEnv::EnableSchemeshardPipeRetries(TTestActorRuntime &runtime) {
    TActorId sender = runtime.AllocateEdgeActor();
    TVector<ui64> tabletIds;
    // Add schemeshard tabletId to white list
    tabletIds.push_back((ui64)TTestTxConfig::SchemeShard);
    return CreateTabletScheduledEventsGuard(tabletIds, runtime, sender);
}

NActors::TActorId NSchemeShardUT_Private::CreateNotificationSubscriber(NActors::TTestActorRuntime &runtime, ui64 schemeshardId) {
    return runtime.Register(new TTxNotificationSubscriber(schemeshardId));
}

NActors::TActorId NSchemeShardUT_Private::CreateFakeMetering(NActors::TTestActorRuntime &runtime) {
    NActors::TActorId actorId = runtime.Register(new TFakeMetering());
    runtime.RegisterService(NMetering::MakeMeteringServiceID(), actorId);
    return NMetering::MakeMeteringServiceID();
}

void NSchemeShardUT_Private::TestWaitNotification(NActors::TTestActorRuntime &runtime, TSet<ui64> txIds, TActorId subscriberActorId) {

    TActorId sender = runtime.AllocateEdgeActor();

    for (ui64 txId : txIds) {
        Cerr << Endl << "TestWaitNotification wait txId: " << txId << Endl;
        auto ev = new TEvSchemeShard::TEvNotifyTxCompletion(txId);
        runtime.Send(new IEventHandle(subscriberActorId, sender, ev));
    }

    TAutoPtr<IEventHandle> handle;
    while (txIds.size()) {
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
        UNIT_ASSERT(event);
        ui64 eventTxId = event->Record.GetTxId();
        Cerr << Endl << "TestWaitNotification: OK eventTxId " << eventTxId << Endl;
        UNIT_ASSERT(txIds.find(eventTxId) != txIds.end());
        txIds.erase(eventTxId);
    }
}

void NSchemeShardUT_Private::TTestEnv::TestWaitNotification(NActors::TTestActorRuntime &runtime, TSet<ui64> txIds, ui64 schemeshardId) {
    if (!TxNotificationSubscribers.contains(schemeshardId)) {
        TxNotificationSubscribers[schemeshardId] = CreateNotificationSubscriber(runtime, schemeshardId);
    }

    NSchemeShardUT_Private::TestWaitNotification(runtime, txIds, TxNotificationSubscribers.at(schemeshardId));
}

void NSchemeShardUT_Private::TTestEnv::TestWaitNotification(TTestActorRuntime &runtime, int txId, ui64 schemeshardId) {
    TestWaitNotification(runtime, (ui64)txId, schemeshardId);
}

void NSchemeShardUT_Private::TTestEnv::TestWaitNotification(NActors::TTestActorRuntime &runtime, ui64 txId, ui64 schemeshardId) {
    TSet<ui64> ids;
    ids.insert(txId);
    TestWaitNotification(runtime, ids, schemeshardId);
}

void NSchemeShardUT_Private::TTestEnv::TestWaitTabletDeletion(NActors::TTestActorRuntime &runtime, TSet<ui64> tabletIds, ui64 hive) {
    TActorId sender = runtime.AllocateEdgeActor();

    for (ui64 tabletId : tabletIds) {
        Cerr << "wait until " << tabletId << " is deleted" << Endl;
        auto ev = new TEvFakeHive::TEvSubscribeToTabletDeletion(tabletId);
        ForwardToTablet(runtime, hive, sender, ev);
    }

    TAutoPtr<IEventHandle> handle;
    while (tabletIds.size()) {
        auto event = runtime.GrabEdgeEvent<TEvHive::TEvResponseHiveInfo>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.TabletsSize(), 1);

        ui64 tabletId = event->Record.GetTablets(0).GetTabletID();
        Cerr << Endl << "Deleted tabletId " << tabletId << Endl;
        UNIT_ASSERT(tabletIds.find(tabletId) != tabletIds.end());
        tabletIds.erase(tabletId);
    }
}

void NSchemeShardUT_Private::TTestEnv::TestWaitTabletDeletion(NActors::TTestActorRuntime &runtime, ui64 tabletId, ui64 hive) {
    TestWaitTabletDeletion(runtime, TSet<ui64>{tabletId}, hive);
}

void NSchemeShardUT_Private::TTestEnv::TestWaitShardDeletion(NActors::TTestActorRuntime &runtime, ui64 schemeShard, TSet<TShardIdx> shardIds) {
    TActorId sender = runtime.AllocateEdgeActor();

    for (auto shardIdx : shardIds) {
        Cerr << "Waiting until shard idx " << shardIdx << " is deleted" << Endl;
        auto ev = new TEvPrivate::TEvSubscribeToShardDeletion(shardIdx);
        ForwardToTablet(runtime, schemeShard, sender, ev);
    }

    while (!shardIds.empty()) {
        auto ev = runtime.GrabEdgeEvent<TEvPrivate::TEvNotifyShardDeleted>(sender);
        auto shardIdx = ev->Get()->ShardIdx;
        Cerr << "Deleted shard idx " << shardIdx << Endl;
        shardIds.erase(shardIdx);
    }
}

void NSchemeShardUT_Private::TTestEnv::TestWaitShardDeletion(NActors::TTestActorRuntime &runtime, ui64 schemeShard, TSet<ui64> localIds) {
    TSet<TShardIdx> shardIds;
    for (ui64 localId : localIds) {
        shardIds.emplace(schemeShard, localId);
    }
    TestWaitShardDeletion(runtime, schemeShard, std::move(shardIds));
}

void NSchemeShardUT_Private::TTestEnv::TestWaitShardDeletion(NActors::TTestActorRuntime &runtime, TSet<ui64> localIds) {
    TestWaitShardDeletion(runtime, TTestTxConfig::SchemeShard, std::move(localIds));
}

void NSchemeShardUT_Private::TTestEnv::SimulateSleep(NActors::TTestActorRuntime &runtime, TDuration duration) {
    auto sender = runtime.AllocateEdgeActor();
    runtime.Schedule(new IEventHandle(sender, sender, new TEvents::TEvWakeup()), duration);
    runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(sender);
}

std::function<NActors::IActor *(const NActors::TActorId &, NKikimr::TTabletStorageInfo *)> NSchemeShardUT_Private::TTestEnv::GetTabletCreationFunc(ui32 type) {
    switch (type) {
    case TTabletTypes::BlockStoreVolume:
        return [](const TActorId& tablet, TTabletStorageInfo* info) {
            return new TFakeBlockStoreVolume(tablet, info);
        };
    case TTabletTypes::FileStore:
        return [](const TActorId& tablet, TTabletStorageInfo* info) {
            return new TFakeFileStore(tablet, info);
        };
    default:
        return nullptr;
    }
}

void NSchemeShardUT_Private::TTestEnv::TestServerlessComputeResourcesModeInHive(TTestActorRuntime& runtime,
    const TString& path, NKikimrSubDomains::EServerlessComputeResourcesMode serverlessComputeResourcesMode, ui64 hive)
{
    auto record = DescribePath(runtime, path);
    const auto& pathDescr = record.GetPathDescription();
    const TSubDomainKey subdomainKey(pathDescr.GetDomainDescription().GetDomainKey());

    const TActorId sender = runtime.AllocateEdgeActor();
    auto ev = MakeHolder<TEvFakeHive::TEvRequestDomainInfo>(subdomainKey);
    ForwardToTablet(runtime, hive, sender, ev.Release());

    const auto event = runtime.GrabEdgeEvent<TEvFakeHive::TEvRequestDomainInfoReply>(sender);
    UNIT_ASSERT(event);
    UNIT_ASSERT_VALUES_EQUAL(event->Get()->DomainInfo.ServerlessComputeResourcesMode, serverlessComputeResourcesMode);
}

TEvSchemeShard::TEvInitRootShardResult::EStatus NSchemeShardUT_Private::TTestEnv::InitRoot(NActors::TTestActorRuntime &runtime, ui64 schemeRoot, const NActors::TActorId &sender, const TString& domainName, const TDomainsInfo::TDomain::TStoragePoolKinds& StoragePoolTypes, const TString& owner) {
    auto ev = new TEvSchemeShard::TEvInitRootShard(sender, 32, domainName);
    for (const auto& [kind, pool] : StoragePoolTypes) {
        auto* p = ev->Record.AddStoragePools();
        p->SetKind(kind);
        p->SetName(pool.GetName());
    }
    if (owner) {
        ev->Record.SetOwner(owner);
    }

    runtime.SendToPipe(schemeRoot, sender, ev, 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvInitRootShardResult>(handle);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetOrigin(), schemeRoot);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetOrigin(), schemeRoot);

    return (TEvSchemeShard::TEvInitRootShardResult::EStatus)event->Record.GetStatus();
}

void NSchemeShardUT_Private::TTestEnv::InitRootStoragePools(NActors::TTestActorRuntime &runtime, ui64 schemeRoot, const NActors::TActorId &sender, ui64 domainUid) {
    const TDomainsInfo::TDomain& domain = runtime.GetAppData().DomainsInfo->GetDomain(domainUid);

    auto evTx = new TEvSchemeShard::TEvModifySchemeTransaction(1, TTestTxConfig::SchemeShard);
    auto transaction = evTx->Record.AddTransaction();
    transaction->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterSubDomain);
    transaction->SetWorkingDir("/");
    auto op = transaction->MutableSubDomain();
    op->SetName(domain.Name);

    for (const auto& [kind, pool] : domain.StoragePoolTypes) {
        auto* p = op->AddStoragePools();
        p->SetKind(kind);
        p->SetName(pool.GetName());
    }

    runtime.SendToPipe(schemeRoot, sender, evTx, 0, GetPipeConfigWithRetries());

    {
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvModifySchemeTransactionResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetSchemeshardId(), schemeRoot);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetStatus(), NKikimrScheme::EStatus::StatusAccepted);
    }

    auto evSubscribe = new TEvSchemeShard::TEvNotifyTxCompletion(1);
    runtime.SendToPipe(schemeRoot, sender, evSubscribe, 0, GetPipeConfigWithRetries());

    {
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvSchemeShard::TEvNotifyTxCompletionResult>(handle);
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetTxId(), 1);
    }
}

void NSchemeShardUT_Private::TTestEnv::BootSchemeShard(NActors::TTestActorRuntime &runtime, ui64 schemeRoot) {
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(schemeRoot, TTabletTypes::SchemeShard), SchemeShardFactory);
}

void NSchemeShardUT_Private::TTestEnv::BootTxAllocator(NActors::TTestActorRuntime &runtime, ui64 tabletId) {
    CreateTestBootstrapper(runtime, CreateTestTabletInfo(tabletId, TTabletTypes::TxAllocator), &CreateTxAllocator);
}

NSchemeShardUT_Private::TTestWithReboots::TTestWithReboots(bool killOnCommit, NSchemeShardUT_Private::TTestEnv::TSchemeShardFactory ssFactory)
    : EnvOpts(GetDefaultTestEnvOptions())
    , SchemeShardFactory(ssFactory)
    , HiveTabletId(TTestTxConfig::Hive)
    , SchemeShardTabletId(TTestTxConfig::SchemeShard)
    , CoordinatorTabletId(TTestTxConfig::Coordinator)
    , TxAllocatorId(TTestTxConfig::TxAllocator)
    , KillOnCommit(killOnCommit)
{
    TabletIds.push_back(HiveTabletId);
    TabletIds.push_back(CoordinatorTabletId);
    TabletIds.push_back(SchemeShardTabletId);
    TabletIds.push_back(TxAllocatorId);

    ui64 datashard = TTestTxConfig::FakeHiveTablets;
    TabletIds.push_back(datashard+0);
    TabletIds.push_back(datashard+1);
    TabletIds.push_back(datashard+2);
    TabletIds.push_back(datashard+3);
    TabletIds.push_back(datashard+4);
    TabletIds.push_back(datashard+5);
    TabletIds.push_back(datashard+6);
    TabletIds.push_back(datashard+7);
    TabletIds.push_back(datashard+8);
}

void NSchemeShardUT_Private::TTestWithReboots::Run(std::function<void (TTestActorRuntime &, bool &)> testScenario) {
    //NOTE: Run testScenario only with (datashard) log batching disabled.
    // It is safe because log batching could only hide problems by potentially "glueing"
    // sequential transactions together and thus eliminating a chance of something bad happen
    // in between those transactions. So running tests without log batching is more thorough.
    // (Also tests run slightly faster without log batching).
    Run(testScenario, false);
}

void NSchemeShardUT_Private::TTestWithReboots::Run(std::function<void (TTestActorRuntime &, bool &)> testScenario, bool allowLogBatching) {
    TDatashardLogBatchingSwitch logBatchingSwitch(allowLogBatching);

    Cerr << "==== RunWithTabletReboots" << Endl;
    RunWithTabletReboots(testScenario);
    Cerr << "==== RunWithPipeResets" << Endl;
    RunWithPipeResets(testScenario);
    //RunWithDelays(testScenario);
}

struct NSchemeShardUT_Private::TTestWithReboots::TFinalizer {
    NSchemeShardUT_Private::TTestWithReboots& TestWithReboots;

    explicit TFinalizer(NSchemeShardUT_Private::TTestWithReboots& testContext)
        : TestWithReboots(testContext)
    {}

    ~TFinalizer() {
        TestWithReboots.Finalize();
    }
};

void NSchemeShardUT_Private::TTestWithReboots::RunWithTabletReboots(std::function<void (TTestActorRuntime &, bool &)> testScenario) {
    RunTestWithReboots(TabletIds,
                       [&]() {
        return PassUserRequests;
    },
    [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(*this);
        Prepare(dispatchName, setup, activeZone);

        activeZone = true;
        testScenario(*Runtime, activeZone);
    }, Max<ui32>(), Max<ui64>(), 0, 0, KillOnCommit);
}

void NSchemeShardUT_Private::TTestWithReboots::RunWithPipeResets(std::function<void (TTestActorRuntime &, bool &)> testScenario) {
    RunTestWithPipeResets(TabletIds,
                          [&]() {
        return PassUserRequests;
    },
    [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(*this);
        Prepare(dispatchName, setup, activeZone);

        activeZone = true;
        testScenario(*Runtime, activeZone);
    });
}

void NSchemeShardUT_Private::TTestWithReboots::RunWithDelays(std::function<void (TTestActorRuntime &, bool &)> testScenario) {
    RunTestWithDelays(TRunWithDelaysConfig(), TabletIds,
                      [&](const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& activeZone) {
        TFinalizer finalizer(*this);
        Prepare(dispatchName, setup, activeZone);

        activeZone = true;
        testScenario(*Runtime, activeZone);
    });
}

void NSchemeShardUT_Private::TTestWithReboots::RestoreLogging() {
    TestEnv->SetupLogging(*Runtime);
}

NSchemeShardUT_Private::TTestEnv* NSchemeShardUT_Private::TTestWithReboots::CreateTestEnv() {
    return new TTestEnv(*Runtime, GetTestEnvOptions());
}



void NSchemeShardUT_Private::TTestWithReboots::Prepare(const TString &dispatchName, std::function<void (TTestActorRuntime &)> setup, bool &outActiveZone) {
    Cdbg << Endl << "=========== RUN: "<< dispatchName << " ===========" << Endl;

    outActiveZone = false;

    Runtime.Reset(new TTestBasicRuntime);
    setup(*Runtime);

    //TestEnv.Reset(new TTestEnv(*Runtime, 4, false, SchemeShardFactory));

    TestEnv.Reset(CreateTestEnv());

    RestoreLogging();

    TxId = 1000;

    TestMkDir(*Runtime, TxId++, "/MyRoot", "DirA");

    // This allows tablet resolver to detect deleted tablets
    EnableTabletResolverScheduling();

    outActiveZone = true;
}

void NSchemeShardUT_Private::TTestWithReboots::EnableTabletResolverScheduling(ui32 nodeIdx) {
    auto actorId = Runtime->GetLocalServiceId(MakeTabletResolverID(), nodeIdx);
    Y_ABORT_UNLESS(actorId);
    Runtime->EnableScheduleForActor(actorId);
}

void NSchemeShardUT_Private::TTestWithReboots::Finalize() {
    TestEnv.Reset();
    Runtime.Reset();
}

bool NSchemeShardUT_Private::TTestWithReboots::PassUserRequests(TTestActorRuntimeBase &runtime, TAutoPtr<IEventHandle> &event) {
    Y_UNUSED(runtime);
    return event->Type == TEvSchemeShard::EvModifySchemeTransaction ||
           event->Type == TEvSchemeShard::EvDescribeScheme ||
           event->Type == TEvSchemeShard::EvNotifyTxCompletion ||
           event->Type == TEvSchemeShard::EvMeasureSelfResponseTime ||
           event->Type == TEvSchemeShard::EvWakeupToMeasureSelfResponseTime ||
           event->Type == TEvTablet::EvLocalMKQL ||
           event->Type == TEvFakeHive::EvSubscribeToTabletDeletion ||
           event->Type == TEvSchemeShard::EvCancelTx ||
           event->Type == TEvExport::EvCreateExportRequest ||
           event->Type == TEvIndexBuilder::EvCreateRequest ||
           event->Type == TEvIndexBuilder::EvGetRequest ||
           event->Type == TEvIndexBuilder::EvCancelRequest ||
           event->Type == TEvIndexBuilder::EvForgetRequest
        ;
}

NSchemeShardUT_Private::TTestEnvOptions& NSchemeShardUT_Private::TTestWithReboots::GetTestEnvOptions() {
    return EnvOpts;
}

const NSchemeShardUT_Private::TTestEnvOptions& NSchemeShardUT_Private::TTestWithReboots::GetTestEnvOptions() const {
    return EnvOpts;
}

NSchemeShardUT_Private::TTestEnvOptions NSchemeShardUT_Private::TTestWithReboots::GetDefaultTestEnvOptions() {
    return TTestEnvOptions()
            .EnablePipeRetries(false)
            .EnableNotNullColumns(true)
            .EnableProtoSourceIdInfo(true)
            .DisableStatsBatching(true)
            .EnableMoveIndex(true)
            ;
}
