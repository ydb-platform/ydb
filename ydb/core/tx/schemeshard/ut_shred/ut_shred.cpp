#include <ydb/core/blobstorage/base/blobstorage_shred_events.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/storage_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/shred_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

namespace {
    TTestEnv SetupEnv(TTestBasicRuntime& runtime, TVector<TIntrusivePtr<NFake::TProxyDS>>& dsProxies) {
        TTestEnv env(runtime, TTestEnvOptions()
            .NChannels(4)
            .EnablePipeRetries(true)
            .DSProxies(dsProxies));

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(50);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(10);

        return env;
    }

    void FillData(TTestBasicRuntime& runtime, ui64 schemeshardId, ui64& txId, const TVector<ui64>& shardsIds, TVector<TIntrusivePtr<NFake::TProxyDS>>& dsProxies, const TString& valueToDelete) {
        TString value(size_t(100 * 1024), 't');
        ui32 keyToDelete = 42;

        for (ui32 key : xrange(100)) {
            int partitionIdx = shardsIds.size() == 1 || key < 50 ? 0 : 1;
            WriteRow(runtime, schemeshardId, ++txId, "/MyRoot/Database1/Simple", partitionIdx, key, key == keyToDelete ? valueToDelete : value);
        }

        auto tableVersion = TestDescribeResult(DescribePath(runtime, schemeshardId, "/MyRoot/Database1/Simple"), {NLs::PathExist});
        for (const auto& shardsId : shardsIds) {
            const auto result = CompactTable(runtime, shardsId, tableVersion.PathId);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrTxDataShard::TEvCompactTableResult::OK);
        }

        DeleteRow(runtime, schemeshardId, ++txId, "/MyRoot/Database1/Simple", 0, keyToDelete);

        // BlobStorage should contain deleted value yet
        UNIT_ASSERT(BlobStorageContains(dsProxies, valueToDelete));
    }

    void CheckShredStatus(TTestBasicRuntime& runtime, TActorId sender, bool completed) {
        auto request = MakeHolder<TEvSchemeShard::TEvShredInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvShredInfoResponse>(handle);

        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 1, response->Record.GetGeneration());
        if (completed) {
            UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvShredInfoResponse::COMPLETED);
        } else {
            UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvShredInfoResponse::IN_PROGRESS_TENANT);
        }
    }

    void CheckShredStatus(TTestBasicRuntime& runtime, TActorId sender, TVector<TIntrusivePtr<NFake::TProxyDS>>& dsProxies, const TString& valueToDelete, bool completed) {
        CheckShredStatus(runtime, sender, completed);
        if (completed) {
            UNIT_ASSERT(!BlobStorageContains(dsProxies, valueToDelete));
        } else {
            UNIT_ASSERT(BlobStorageContains(dsProxies, valueToDelete));
        }
    }
}

Y_UNIT_TEST_SUITE(TenantShredTest) {
    Y_UNIT_TEST(ShredWithGeneration0IsCompleted) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        ui64 txId = 100;
        ui64 tenantSchemeShard = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true});
        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(0), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvTenantShredResponse>(handle);
        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 0, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL_C(response->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Record.GetStatus()));
    }

    Y_UNIT_TEST(ShredOneTime) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        ui64 txId = 100;
        ui64 tenantSchemeShard = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true});

        auto checkTenantShredResponseGen1 = [](IEventHandle& ev) -> bool {
            if (ev.GetTypeRewrite() != TEvSchemeShard::TEvTenantShredResponse::EventType) {
                return false;
            }
            TEventHandle<TEvSchemeShard::TEvTenantShredResponse>* response = reinterpret_cast<TEventHandle<TEvSchemeShard::TEvTenantShredResponse>*>(&ev);
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetGeneration(), 1, response->Get()->Record.GetGeneration());
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Get()->Record.GetStatus()));
            return true;
        };

        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(1), 0, GetPipeConfigWithRetries());
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(checkTenantShredResponseGen1, 1));
        runtime.DispatchEvents(options);
    }

    Y_UNIT_TEST(SendPreviousGeneration) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        ui64 txId = 100;
        ui64 tenantSchemeShard = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true});

        auto checkTenantShredResponseGen2 = [](IEventHandle& ev) -> bool {
            if (ev.GetTypeRewrite() != TEvSchemeShard::TEvTenantShredResponse::EventType) {
                return false;
            }
            TEventHandle<TEvSchemeShard::TEvTenantShredResponse>* response = reinterpret_cast<TEventHandle<TEvSchemeShard::TEvTenantShredResponse>*>(&ev);
            if (response->Get()->Record.GetGeneration() != 2) {
                return false;
            }
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetGeneration(), 2, response->Get()->Record.GetGeneration());
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Get()->Record.GetStatus()));
            return true;
        };
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(checkTenantShredResponseGen2, 1));
        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(2), 0, GetPipeConfigWithRetries());
        runtime.DispatchEvents(options);
        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(1), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvTenantShredResponse>(handle);
        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 2, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL_C(response->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Record.GetStatus()));
    }

    Y_UNIT_TEST(SendPreviousGenerationUncompleted) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        ui64 txId = 100;
        ui64 tenantSchemeShard = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true});

        // block CollectGarbage requests to suspend Vacuum
        TBlockEvents<TEvBlobStorage::TEvCollectGarbage> collectGarbageReqs(runtime);

        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(2), 0, GetPipeConfigWithRetries());
        runtime.WaitFor("collect garbage", [&collectGarbageReqs]{ return collectGarbageReqs.size() >= 1; });

        // request with previous generation should not be responded
        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(1), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto* response1 = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvTenantShredResponse>(handle, TDuration::Seconds(20));
        UNIT_ASSERT_C(!response1, "Unexpected EvTenantShredResponse");

        // EvTenantShredResponse should be sent after unblocking and finishing Vacuum
        collectGarbageReqs.Stop().Unblock();
        auto checkTenantShredResponseGen2 = [](IEventHandle& ev) -> bool {
            if (ev.GetTypeRewrite() != TEvSchemeShard::TEvTenantShredResponse::EventType) {
                return false;
            }
            TEventHandle<TEvSchemeShard::TEvTenantShredResponse>* response = reinterpret_cast<TEventHandle<TEvSchemeShard::TEvTenantShredResponse>*>(&ev);
            if (response->Get()->Record.GetGeneration() != 2) {
                return false;
            }
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetGeneration(), 2, response->Get()->Record.GetGeneration());
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Get()->Record.GetStatus()));
            return true;
        };
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(checkTenantShredResponseGen2, 1));
        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(2), 0, GetPipeConfigWithRetries());
        runtime.DispatchEvents(options);
    }

    Y_UNIT_TEST(SendPreviousGenerationLastGenerationCompleted) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        ui64 txId = 100;
        ui64 tenantSchemeShard = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true});

        auto checkTenantShredResponseGen1 = [](IEventHandle& ev) -> bool {
            if (ev.GetTypeRewrite() != TEvSchemeShard::TEvTenantShredResponse::EventType) {
                return false;
            }
            TEventHandle<TEvSchemeShard::TEvTenantShredResponse>* response = reinterpret_cast<TEventHandle<TEvSchemeShard::TEvTenantShredResponse>*>(&ev);
            if (response->Get()->Record.GetGeneration() != 1) {
                return false;
            }
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetGeneration(), 1, response->Get()->Record.GetGeneration());
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Get()->Record.GetStatus()));
            return true;
        };
        auto checkTenantShredResponseGen2 = [](IEventHandle& ev) -> bool {
            if (ev.GetTypeRewrite() != TEvSchemeShard::TEvTenantShredResponse::EventType) {
                return false;
            }
            TEventHandle<TEvSchemeShard::TEvTenantShredResponse>* response = reinterpret_cast<TEventHandle<TEvSchemeShard::TEvTenantShredResponse>*>(&ev);
            if (response->Get()->Record.GetGeneration() != 2) {
                return false;
            }
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetGeneration(), 2, response->Get()->Record.GetGeneration());
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Get()->Record.GetStatus()));
            return true;
        };

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(checkTenantShredResponseGen1, 1));
        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(1), 0, GetPipeConfigWithRetries());
        runtime.DispatchEvents(options);
        options.FinalEvents.clear();
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(checkTenantShredResponseGen2, 1));
        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(2), 0, GetPipeConfigWithRetries());
        runtime.DispatchEvents(options);

        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(1), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvTenantShredResponse>(handle);
        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 2, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL_C(response->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Record.GetStatus()));
    }

    Y_UNIT_TEST(HandleRootSchemeShardDisconnect) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        ui64 txId = 100;
        ui64 tenantSchemeShard = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true});

        TActorId tenantActorId;
        auto checkTenantShredResponseGen1 = [&](const TEvSchemeShard::TEvTenantShredResponse::TPtr& ev) -> bool {
            if (ev->Get()->Record.GetGeneration() != 1) {
                return false;
            }
            UNIT_ASSERT_EQUAL_C(ev->Get()->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(ev->Get()->Record.GetStatus()));
            tenantActorId = ev->Sender;
            return true;
        };
        TWaitForFirstEvent<TEvSchemeShard::TEvTenantShredResponse> firstShredResponse(runtime, checkTenantShredResponseGen1);

        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(1), 0, GetPipeConfigWithRetries());
        firstShredResponse.Wait();
        TActorId firstTenantActorId = tenantActorId;
        firstShredResponse.Stop();

        TWaitForFirstEvent<TEvSchemeShard::TEvTenantShredResponse> secondShredResponse(runtime, checkTenantShredResponseGen1);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        secondShredResponse.Wait();
        secondShredResponse.Stop();
        UNIT_ASSERT_VALUES_EQUAL(firstTenantActorId, tenantActorId);
    }

    Y_UNIT_TEST(HandleShardDisconnect) {
        TTestBasicRuntime runtime;
        TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies {
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(0)),
        };
        auto env = SetupEnv(runtime, dsProxies);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        ui64 txId = 100;
        ui64 tenantSchemeShard = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true});

        auto shards = GetTableShards(runtime, tenantSchemeShard, "/MyRoot/Database1/Simple");
        TString value(size_t(100 * 1024), 'd');
        FillData(runtime, tenantSchemeShard, txId, shards, dsProxies, value);

        auto checkTenantShredResponseGen1 = [](IEventHandle& ev) -> bool {
            if (ev.GetTypeRewrite() != TEvSchemeShard::TEvTenantShredResponse::EventType) {
                return false;
            }
            TEventHandle<TEvSchemeShard::TEvTenantShredResponse>* response = reinterpret_cast<TEventHandle<TEvSchemeShard::TEvTenantShredResponse>*>(&ev);
            if (response->Get()->Record.GetGeneration() != 1) {
                return false;
            }
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetGeneration(), 1, response->Get()->Record.GetGeneration());
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Get()->Record.GetStatus()));
            return true;
        };

        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(1), 0, GetPipeConfigWithRetries());
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(checkTenantShredResponseGen1, 1));
        runtime.DispatchEvents(options);

        auto tableDesc = DescribePath(runtime, tenantSchemeShard, "/MyRoot/Database1/Simple", true);
        auto tabletToDisconnect = tableDesc.GetPathDescription().GetTablePartitions(0).GetDatashardId();

        // block CollectGarbage requests to suspend Vacuum
        TBlockEvents<TEvBlobStorage::TEvCollectGarbage> collectGarbageReqs(runtime, [&](const TEvBlobStorage::TEvCollectGarbage::TPtr& ev) {
            return ev->Get()->TabletId == tabletToDisconnect;
        });

        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(2), 0, GetPipeConfigWithRetries());
        runtime.WaitFor("collect garbage", [&collectGarbageReqs]{ return collectGarbageReqs.size() >= 1; });

        bool gen2completed = false;
        auto observer = runtime.AddObserver<TEvSchemeShard::TEvTenantShredResponse>([&](const TEvSchemeShard::TEvTenantShredResponse::TPtr& ev) {
            if (ev->Get()->Record.GetGeneration() == 2 && ev->Get()->Record.GetStatus() == NKikimrScheme::TEvTenantShredResponse::COMPLETED) {
                gen2completed = true;
            }
        });

        collectGarbageReqs.Stop(); // don't unblock, just drop
        RebootTablet(runtime, tabletToDisconnect, sender);
        runtime.WaitFor("gen 2 completed", [&]{ return gen2completed; });
    }

    Y_UNIT_TEST(HandleTenantSchemeShardDisconnect) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        ui64 txId = 100;
        ui64 tenantSchemeShard = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true});

        TBlockEvents<TEvDataShard::TEvVacuum> vacuumReqs(runtime);

        TWaitForFirstEvent<TEvSchemeShard::TEvTenantShredRequest> firstShredRequest(runtime);

        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, new TEvSchemeShard::TEvShredManualStartupRequest(), 0, GetPipeConfigWithRetries());
        firstShredRequest.Wait();

        runtime.WaitFor("vacuum reqs", [&vacuumReqs]{ return vacuumReqs.size() >= 1; });
        vacuumReqs.Stop().clear(); // drop EvVacuum requests, should be retried after reboot 

        TWaitForFirstEvent<TEvSchemeShard::TEvTenantShredRequest> secondShredRequest(runtime);
        RebootTablet(runtime, tenantSchemeShard, sender);

        secondShredRequest.Wait();
    }

    Y_UNIT_TEST(HandleTenantSchemeShardRestart) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        ui64 txId = 100;
        ui64 tenantSchemeShard = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true});

        auto checkTenantShredResponseGen1 = [](IEventHandle& ev) -> bool {
            if (ev.GetTypeRewrite() != TEvSchemeShard::TEvTenantShredResponse::EventType) {
                return false;
            }
            TEventHandle<TEvSchemeShard::TEvTenantShredResponse>* response = reinterpret_cast<TEventHandle<TEvSchemeShard::TEvTenantShredResponse>*>(&ev);
            if (response->Get()->Record.GetGeneration() != 1) {
                return false;
            }
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetGeneration(), 1, response->Get()->Record.GetGeneration());
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Get()->Record.GetStatus()));
            return true;
        };

        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(1), 0, GetPipeConfigWithRetries());
        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(checkTenantShredResponseGen1, 1));
        runtime.DispatchEvents(options);

        auto checkTenantShredResponseGen2 = [](IEventHandle& ev) -> bool {
            if (ev.GetTypeRewrite() != TEvSchemeShard::TEvTenantShredResponse::EventType) {
                return false;
            }
            TEventHandle<TEvSchemeShard::TEvTenantShredResponse>* response = reinterpret_cast<TEventHandle<TEvSchemeShard::TEvTenantShredResponse>*>(&ev);
            if (response->Get()->Record.GetGeneration() != 2) {
                return false;
            }
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetGeneration(), 2, response->Get()->Record.GetGeneration());
            UNIT_ASSERT_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrScheme::TEvTenantShredResponse::COMPLETED, static_cast<ui32>(response->Get()->Record.GetStatus()));
            return true;
        };
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(checkTenantShredResponseGen2, 1));
        RebootTablet(runtime, tenantSchemeShard, sender);
        runtime.SendToPipe(tenantSchemeShard, sender, new TEvSchemeShard::TEvTenantShredRequest(2), 0, GetPipeConfigWithRetries());
        // After reboot shred status COMPLETE, Generation = 1
        // Waiting for events complete shred generation 1 and generation 2
        runtime.DispatchEvents(options);
    }
}

Y_UNIT_TEST_SUITE(TestShred) {
    void RunShred(TTestBasicRuntime& runtime, ui32 expectedGeneration) {
        auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender,
            new TEvSchemeShard::TEvShredManualStartupRequest(), 0, GetPipeConfigWithRetries());

        TDispatchOptions options;
        options.FinalEvents.push_back(
            TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        auto request = MakeHolder<TEvSchemeShard::TEvShredInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvShredInfoResponse>(handle);
        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), expectedGeneration,
            response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(),
            NKikimrScheme::TEvShredInfoResponse::COMPLETED);
    }

    void SimpleShredTest(const TSchemeObject& createSchemeObject, ui64 currentBscGeneration = 0) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(3);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        // Change BSC counter value between shred iterations
        if (currentBscGeneration > 1) {
            auto request = MakeHolder<TEvBlobStorage::TEvControllerShredRequest>(currentBscGeneration);
            runtime.SendToPipe(MakeBSControllerID(), sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1", createSchemeObject);
        CreateTestExtSubdomain(runtime, env, &txId, "Database2", createSchemeObject);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, (currentBscGeneration >  1 ? 4 : 3)));
        runtime.DispatchEvents(options);

        auto request = MakeHolder<TEvSchemeShard::TEvShredInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvShredInfoResponse>(handle);

        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 1, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvShredInfoResponse::COMPLETED);
    }

    Y_UNIT_TEST(SimpleTestForTables) {
        SimpleShredTest({.Table = true, .Topic = false});
    }

    Y_UNIT_TEST(SimpleTestForTopic) {
        SimpleShredTest({.Table = false, .Topic = true});
    }

    Y_UNIT_TEST(SimpleTestForAllSupportedObjects) {
        SimpleShredTest({.Table = true, .Topic = true});
    }

    Y_UNIT_TEST(SchemeShardCounterDoesNotConsistWithBscCounter) {
        SimpleShredTest({.Table = true, .Topic = false}, /*currentBscGeneration*/ 47);
    }

    void ShredRun3Cycles(const TSchemeObject& createSchemeObject) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(3);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1", createSchemeObject);
        CreateTestExtSubdomain(runtime, env, &txId, "Database2", createSchemeObject);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 9));
        runtime.DispatchEvents(options);

        auto request = MakeHolder<TEvSchemeShard::TEvShredInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvShredInfoResponse>(handle);

        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 3, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvShredInfoResponse::COMPLETED);
    }

    Y_UNIT_TEST(Run3CyclesForTables) {
        ShredRun3Cycles({.Table = true, .Topic = false});
    }

    Y_UNIT_TEST(Run3CyclesForTopics) {
        ShredRun3Cycles({.Table = false, .Topic = true});
    }

    Y_UNIT_TEST(Run3CyclesForAllSupportedObjects) {
        ShredRun3Cycles({.Table = true, .Topic = true});
    }

    Y_UNIT_TEST(ShredManualLaunch) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0); // do not schedule
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1");
        CreateTestExtSubdomain(runtime, env, &txId, "Database2");

        {
            auto request = MakeHolder<TEvSchemeShard::TEvShredManualStartupRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
        }

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        auto request = MakeHolder<TEvSchemeShard::TEvShredInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvShredInfoResponse>(handle);

        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 1, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvShredInfoResponse::COMPLETED);
    }

    Y_UNIT_TEST(ShredManualLaunchWithReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            }
        );

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0); // do not schedule
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;
        CreateTestExtSubdomain(runtime, env, &txId, "Database1");
        CreateTestExtSubdomain(runtime, env, &txId, "Database2");

        {
            auto request = MakeHolder<TEvSchemeShard::TEvShredManualStartupRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
        }

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);
        auto infoRequest = MakeHolder<TEvSchemeShard::TEvShredInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, infoRequest.Release(), 0, GetPipeConfigWithRetries());

        TAutoPtr<IEventHandle> handle;
        auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvShredInfoResponse>(handle);
        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 1, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvShredInfoResponse::COMPLETED);

        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        {
            auto request = MakeHolder<TEvSchemeShard::TEvShredManualStartupRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        runtime.DispatchEvents(options);
        infoRequest = MakeHolder<TEvSchemeShard::TEvShredInfoRequest>();
        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, infoRequest.Release(), 0, GetPipeConfigWithRetries());

        response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvShredInfoResponse>(handle);
        UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), 2, response->Record.GetGeneration());
        UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvShredInfoResponse::COMPLETED);
    }

    Y_UNIT_TEST(ManualLaunch3Cycles) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0); // do not schedule
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1");
        CreateTestExtSubdomain(runtime, env, &txId, "Database2");

        auto runShred = [&runtime](ui32 expectedGeneration) {
            auto sender = runtime.AllocateEdgeActor();
            {
                auto request = MakeHolder<TEvSchemeShard::TEvShredManualStartupRequest>();
                runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
            }

            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
            runtime.DispatchEvents(options);

            auto request = MakeHolder<TEvSchemeShard::TEvShredInfoRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvShredInfoResponse>(handle);

            UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), expectedGeneration, response->Record.GetGeneration());
            UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvShredInfoResponse::COMPLETED);
        };

        runShred(1);
        runShred(2);
        runShred(3);
    }

    Y_UNIT_TEST(ManualLaunch3CyclesWithNotConsistentCountersInSchemeShardAndBSC) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0); // do not schedule
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1");
        CreateTestExtSubdomain(runtime, env, &txId, "Database2");

        auto runShred = [&runtime](ui32 expectedGeneration, ui32 requiredCountShredResponses) {
            auto sender = runtime.AllocateEdgeActor();
            {
                auto request = MakeHolder<TEvSchemeShard::TEvShredManualStartupRequest>();
                runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
            }

            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, requiredCountShredResponses));
            runtime.DispatchEvents(options);

            auto request = MakeHolder<TEvSchemeShard::TEvShredInfoRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvShredInfoResponse>(handle);

            UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), expectedGeneration, response->Record.GetGeneration());
            UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvShredInfoResponse::COMPLETED);
        };

        runShred(1, 3);
        // Change BSC counter value between shred iterations
        {
            auto request = MakeHolder<TEvBlobStorage::TEvControllerShredRequest>(50);
            runtime.SendToPipe(MakeBSControllerID(), sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        runShred(2, 4);
        // Change BSC counter value between shred iterations
        {
            auto request = MakeHolder<TEvBlobStorage::TEvControllerShredRequest>(100);
            runtime.SendToPipe(MakeBSControllerID(), sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        runShred(3, 4);
    }

    Y_UNIT_TEST(ManualLaunchWithNotConsistentCountersInSchemeShardAndBSCReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
                return new TFakeBSController(tablet, info);
            });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0); // do not schedule
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        CreateTestExtSubdomain(runtime, env, &txId, "Database1");
        CreateTestExtSubdomain(runtime, env, &txId, "Database2");

        auto runShred = [&runtime](ui32 expectedGeneration, ui32 requiredCountShredResponses) {
            auto sender = runtime.AllocateEdgeActor();
            {
                auto request = MakeHolder<TEvSchemeShard::TEvShredManualStartupRequest>();
                runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());
            }

            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, requiredCountShredResponses));
            runtime.DispatchEvents(options);

            auto request = MakeHolder<TEvSchemeShard::TEvShredInfoRequest>();
            runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, request.Release(), 0, GetPipeConfigWithRetries());

            TAutoPtr<IEventHandle> handle;
            auto response = runtime.GrabEdgeEventRethrow<TEvSchemeShard::TEvShredInfoResponse>(handle);

            UNIT_ASSERT_EQUAL_C(response->Record.GetGeneration(), expectedGeneration, response->Record.GetGeneration());
            UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NKikimrScheme::TEvShredInfoResponse::COMPLETED);
        };

        runShred(1, 3);
        // Change BSC counter value between shred iterations
        {
            auto request = MakeHolder<TEvBlobStorage::TEvControllerShredRequest>(50);
            runtime.SendToPipe(MakeBSControllerID(), sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        runShred(2, 4);
        runShred(3, 3);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        runShred(4, 4);
        // Change BSC counter value between shred iterations
        {
            auto request = MakeHolder<TEvBlobStorage::TEvControllerShredRequest>(100);
            runtime.SendToPipe(MakeBSControllerID(), sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        runShred(5, 4);
    }

    Y_UNIT_TEST(ShredWithCopyTable) {
        TTestBasicRuntime runtime;
        TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies {
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(0)),
        };
        auto env = SetupEnv(runtime, dsProxies);
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        auto schemeshardId = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true, .Topic = false});
        auto shards = GetTableShards(runtime, schemeshardId, "/MyRoot/Database1/Simple");
        TString value(size_t(100 * 1024), 'd');
        FillData(runtime, schemeshardId, txId, shards, dsProxies, value);

        // catch and hold borrow returns
        TBlockEvents<TEvDataShard::TEvReturnBorrowedPart> borrowReturns(runtime);

        TestCopyTable(runtime, schemeshardId, ++txId, "/MyRoot/Database1", "SimpleCopy", "/MyRoot/Database1/Simple");

        runtime.WaitFor("borrow return", [&borrowReturns]{ return borrowReturns.size() >= 1; });

        // data cleanup should not be finished due to holded borrow returns
        CheckShredStatus(runtime, sender, dsProxies, value, false);

        // return borrow
        borrowReturns.Stop().Unblock();

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        // data cleanup should be finished after returned borrows
        CheckShredStatus(runtime, sender, dsProxies, value, true);
    }

    Y_UNIT_TEST(ShredWithSplit) {
        TTestBasicRuntime runtime;
        TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies {
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(0)),
        };
        auto env = SetupEnv(runtime, dsProxies);
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        auto schemeshardId = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = false, .Topic = false});

        TestCreateTable(runtime, schemeshardId, ++txId, "/MyRoot/Database1",
            R"____(
                Name: "Simple"
                Columns { Name: "key1"  Type: "Uint32"}
                Columns { Name: "Value" Type: "Utf8"}
                KeyColumnNames: ["key1"]
            )____");
        env.TestWaitNotification(runtime, txId, schemeshardId);
        auto shards1 = GetTableShards(runtime, schemeshardId, "/MyRoot/Database1/Simple");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 1);

        TString valueToDelete(size_t(100 * 1024), 'd');
        FillData(runtime, schemeshardId, txId, shards1, dsProxies, valueToDelete);

        // block borrow returns to suspend SplitTable
        TBlockEvents<TEvDataShard::TEvReturnBorrowedPart> borrowReturns(runtime);

        // block CollectGarbage requests to suspend Vacuum
        TBlockEvents<TEvBlobStorage::TEvCollectGarbage> collectGarbageReqs(runtime);
        runtime.WaitFor("collect garbage", [&collectGarbageReqs]{ return collectGarbageReqs.size() >= 1; });

        TestSplitTable(runtime, schemeshardId, ++txId, "/MyRoot/Database1/Simple", Sprintf(
            R"(
                SourceTabletId: %lu
                SplitBoundary {
                    KeyPrefix {
                        Tuple { Optional { Uint32: 50 } }
                    }
                }
            )", shards1.at(0)));
        env.TestWaitNotification(runtime, txId, schemeshardId);

        runtime.WaitFor("borrow return", [&borrowReturns]{ return borrowReturns.size() >= 1; });

        // Shred should be in progress because of SplitTable and Vacuum have been suspended
        CheckShredStatus(runtime, sender, dsProxies, valueToDelete, false);

        auto shards2 = GetTableShards(runtime, schemeshardId, "/MyRoot/Database1/Simple");
        UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 2);

        collectGarbageReqs.Stop().Unblock();
        borrowReturns.Stop().Unblock();

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        // now data cleanup should be finished
        CheckShredStatus(runtime, sender, dsProxies, valueToDelete, true);
    }

    Y_UNIT_TEST(ShredWithMerge) {
        TTestBasicRuntime runtime;
        TVector<TIntrusivePtr<NFake::TProxyDS>> dsProxies {
            MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(0)),
        };
        auto env = SetupEnv(runtime, dsProxies);
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;

        auto schemeshardId = CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = false, .Topic = false});

        TestCreateTable(runtime, schemeshardId, ++txId, "/MyRoot/Database1",
            R"____(
                Name: "Simple"
                Columns { Name: "key1"  Type: "Uint32"}
                Columns { Name: "Value" Type: "Utf8"}
                KeyColumnNames: ["key1"]
                SplitBoundary {
                    KeyPrefix {
                        Tuple { Optional { Uint32: 50 } }
                    }
                }
                PartitionConfig {
                    PartitioningPolicy {
                        MinPartitionsCount: 1
                        MaxPartitionsCount: 2
                    }
                }
            )____");
        env.TestWaitNotification(runtime, txId, schemeshardId);
        auto shards1 = GetTableShards(runtime, schemeshardId, "/MyRoot/Database1/Simple");
        UNIT_ASSERT_VALUES_EQUAL(shards1.size(), 2);

        TString valueToDelete(size_t(100 * 1024), 'd');
        FillData(runtime, schemeshardId, txId, shards1, dsProxies, valueToDelete);

        // block borrow returns to suspend SplitTable
        TBlockEvents<TEvDataShard::TEvReturnBorrowedPart> borrowReturns(runtime);

        // block CollectGarbage requests to suspend Vacuum
        TBlockEvents<TEvBlobStorage::TEvCollectGarbage> collectGarbageReqs(runtime);
        runtime.WaitFor("collect garbage", [&collectGarbageReqs]{ return collectGarbageReqs.size() >= 1; });

        TestSplitTable(runtime, schemeshardId, ++txId, "/MyRoot/Database1/Simple", Sprintf(
            R"(
                SourceTabletId: %lu
                SourceTabletId: %lu
            )", shards1.at(0), shards1.at(1)));
        env.TestWaitNotification(runtime, txId, schemeshardId);

        runtime.WaitFor("borrow return", [&borrowReturns]{ return borrowReturns.size() >= 1; });

        // Shred should be in progress because of SplitTable and Vacuum have been suspended
        CheckShredStatus(runtime, sender, dsProxies, valueToDelete, false);

        auto shards2 = GetTableShards(runtime, schemeshardId, "/MyRoot/Database1/Simple");
        UNIT_ASSERT_VALUES_EQUAL(shards2.size(), 1);

        collectGarbageReqs.Stop().Unblock();
        borrowReturns.Stop().Unblock();

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        // now data cleanup should be finished
        CheckShredStatus(runtime, sender, dsProxies, valueToDelete, true);
    }

    Y_UNIT_TEST(ShredWhenDeletingTenant) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0);
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);
        shredConfig.SetInflightLimit(1);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        ui64 txId = 100;
        CreateTestExtSubdomain(runtime, env, &txId, "Database1", {.Table = true});
        CreateTestExtSubdomain(runtime, env, &txId, "Database2", {.Table = true});

        runtime.SendToPipe(TTestTxConfig::SchemeShard, sender, new TEvSchemeShard::TEvShredManualStartupRequest(), 0, GetPipeConfigWithRetries());
        TestForceDropExtSubDomain(runtime, ++txId, "/MyRoot", "Database2");

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvBlobStorage::EvControllerShredResponse, 3));
        runtime.DispatchEvents(options);

        CheckShredStatus(runtime, sender, true);
    }

    Y_UNIT_TEST(CleanupOldGenerationsAfterMultipleCycles) {
        // Verifies that CleanupOldGenerations() keeps at most the last two records
        // in DataErasureGenerations after each new shred cycle.
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0); // do not auto-schedule
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        auto sender = runtime.AllocateEdgeActor();
        // Reboot SchemaShard is required in order to apply config.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);

        ui64 txId = 100;
        // The test logic doesn't depend on the number of tenants; two databases were added just in case, to emulate a real-world scenario.
        ui64 tenantSS = CreateTestExtSubdomain(runtime, env, &txId, "Database1");
        CreateTestExtSubdomain(runtime, env, &txId, "Database2");

        // Query DataErasureGenerations (root SS) via local MiniKQL.
        // SelectRange returns Optional<Struct{List, Truncated}>, so:
        //   GetStruct(0)       — the SetResult("Result", ...) value
        //   .GetOptional()     — unwrap Optional
        //   .GetStruct(0)      — "List" field (alphabetically first in the range result struct)
        //   .ListSize()        — number of rows
        //   .GetList(i)        — row i
        //   .GetStruct(0)      — first (and only) field: Generation
        //   .GetUint64()       — the value
        const TString shredGenerationsQuery = R"(
            (
                (let range '('('Generation (Null) (Void))))
                (let fields '('Generation))
                (return (AsList
                    (SetResult 'Result (SelectRange 'DataErasureGenerations range fields '()))
                ))
            )
        )";

        auto countShredGenerations = [&]() -> ui32 {
            auto result = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, shredGenerationsQuery);
            return result.GetValue().GetStruct(0).GetOptional().GetStruct(0).ListSize();
        };

        // Returns sorted vector of generation values currently stored in DataErasureGenerations.
        auto getShredGenerations = [&]() -> TVector<ui64> {
            auto result = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, shredGenerationsQuery);
            const auto& list = result.GetValue().GetStruct(0).GetOptional().GetStruct(0);
            TVector<ui64> generations;
            generations.reserve(list.ListSize());
            for (size_t i = 0; i < list.ListSize(); ++i) {
                generations.push_back(list.GetList(i).GetStruct(0).GetUint64());
            }
            Sort(generations);
            return generations;
        };

        // Verifies that all generation values in DataErasureGenerations are within
        // {expectedGeneration, expectedGeneration - 1}: at most two records allowed.
        auto checkShredGenerationValues = [&](ui64 expectedGeneration) {
            auto gens = getShredGenerations();
            for (ui64 gen : gens) {
                UNIT_ASSERT_C(
                    gen == 0 || gen == expectedGeneration || (expectedGeneration > 0 && gen == expectedGeneration - 1),
                    "Unexpected generation " << gen << " in DataErasureGenerations"
                    << " after completing cycle " << expectedGeneration
                    << "; allowed values are {" << expectedGeneration
                    << (expectedGeneration > 0 ? TStringBuilder() << ", " << (expectedGeneration - 1) : TStringBuilder())
                    << "}");
            }
        };

        // Query TenantDataErasureGenerations (tenant SS) via local MiniKQL.
        const TString tenantShredGenerationsQuery = R"(
            (
                (let range '('('Generation (Null) (Void))))
                (let fields '('Generation))
                (return (AsList
                    (SetResult 'Result (SelectRange 'TenantDataErasureGenerations range fields '()))
                ))
            )
        )";

        auto countTenantShredGenerations = [&]() -> ui32 {
            auto result = LocalMiniKQL(runtime, tenantSS, tenantShredGenerationsQuery);
            return result.GetValue().GetStruct(0).GetOptional().GetStruct(0).ListSize();
        };

        // Returns sorted vector of generation values currently stored in TenantDataErasureGenerations.
        auto getTenantShredGenerations = [&]() -> TVector<ui64> {
            auto result = LocalMiniKQL(runtime, tenantSS, tenantShredGenerationsQuery);
            const auto& list = result.GetValue().GetStruct(0).GetOptional().GetStruct(0);
            TVector<ui64> generations;
            generations.reserve(list.ListSize());
            for (size_t i = 0; i < list.ListSize(); ++i) {
                generations.push_back(list.GetList(i).GetStruct(0).GetUint64());
            }
            Sort(generations);
            return generations;
        };

        // Verifies that all generation values in TenantDataErasureGenerations are within
        // {expectedGeneration, expectedGeneration - 1}: at most two records allowed.
        auto checkTenantShredGenerationValues = [&](ui64 expectedGeneration) {
            auto gens = getTenantShredGenerations();
            for (ui64 gen : gens) {
                UNIT_ASSERT_C(
                    gen == 0 || gen == expectedGeneration || (expectedGeneration > 0 && gen == expectedGeneration - 1),
                    "Unexpected generation " << gen << " in TenantDataErasureGenerations"
                    << " after completing cycle " << expectedGeneration
                    << "; allowed values are {" << expectedGeneration
                    << (expectedGeneration > 0 ? TStringBuilder() << ", " << (expectedGeneration - 1) : TStringBuilder())
                    << "}");
            }
        };

        // Each RunShred(N) call increments Generation to N and runs CleanupOldGenerationsOnShred
        // which deletes the key for Generation - 2 before writing the new key N.
        // After the BSC confirms completion both tables contain at most two records.
        // No generation older than N - 1 must survive.

        // Cycle 1: init writes gen=0, CleanupOldGenerations deletes nothing, writes gen=1.
        RunShred(runtime, 1);
        {
            ui32 count = countShredGenerations();
            UNIT_ASSERT_C(count <= 2u,
                "After cycle 1: too many rows in DataErasureGenerations: " << count);
            checkShredGenerationValues(1);

            ui32 tenantCount = countTenantShredGenerations();
            UNIT_ASSERT_C(tenantCount <= 1u,
                "After cycle 1: too many rows in TenantDataErasureGenerations: " << tenantCount);
            checkTenantShredGenerationValues(1);
        }

        // Cycle 2: CleanupOldGenerations deletes gen=0, writes gen=2.
        RunShred(runtime, 2);
        {
            ui32 count = countShredGenerations();
            UNIT_ASSERT_C(count <= 2u,
                "After cycle 2: too many rows in DataErasureGenerations: " << count);
            checkShredGenerationValues(2);

            ui32 tenantCount = countTenantShredGenerations();
            UNIT_ASSERT_C(tenantCount <= 2u,
                "After cycle 2: too many rows in TenantDataErasureGenerations: " << tenantCount);
            checkTenantShredGenerationValues(2);
        }

        // Cycle 3: CleanupOldGenerations deletes gen=1, writes gen=3.
        RunShred(runtime, 3);
        {
            ui32 count = countShredGenerations();
            UNIT_ASSERT_C(count <= 2u,
                "After cycle 3: too many rows in DataErasureGenerations: " << count);
            checkShredGenerationValues(3);

            ui32 tenantCount = countTenantShredGenerations();
            UNIT_ASSERT_C(tenantCount <= 2u,
                "After cycle 3: too many rows in TenantDataErasureGenerations: " << tenantCount);
            checkTenantShredGenerationValues(3);
        }

        // Cycle 4: CleanupOldGenerations deletes gen=2, writes gen=4.
        RunShred(runtime, 4);
        {
            ui32 count = countShredGenerations();
            UNIT_ASSERT_C(count <= 2u,
                "After cycle 4: too many rows in DataErasureGenerations: " << count);
            checkShredGenerationValues(4);

            ui32 tenantCount = countTenantShredGenerations();
            UNIT_ASSERT_C(tenantCount <= 2u,
                "After cycle 4: too many rows in TenantDataErasureGenerations: " << tenantCount);
            checkTenantShredGenerationValues(4);
        }
    }

    Y_UNIT_TEST(CleanupOldGenerationsOnRestoreWithStaleRows) {
        // Prepopulates DataErasureGenerations (root SS) with four rows whose generation
        // values span a range (1, 2, 5, 9), and TenantDataErasureGenerations (tenant SS)
        // with four analogous rows (1, 3, 6, 9).
        // After reboot Restore() calls CleanupOldGenerationsOnRestore() on both managers,
        // which must delete every generation that is not the maximum (9 in both cases).
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto info = CreateTestTabletInfo(MakeBSControllerID(), TTabletTypes::BSController);
        CreateTestBootstrapper(runtime, info, [](const TActorId &tablet, TTabletStorageInfo *info) -> IActor* {
            return new TFakeBSController(tablet, info);
        });

        runtime.GetAppData().FeatureFlags.SetEnableDataErasure(true);
        auto& shredConfig = runtime.GetAppData().ShredConfig;
        shredConfig.SetDataErasureIntervalSeconds(0); // do not schedule
        shredConfig.SetBlobStorageControllerRequestIntervalSeconds(1);

        ui64 txId = 100;

        auto tenantSS = CreateTestExtSubdomain(runtime, env, &txId, "Database1");
        auto sender = runtime.AllocateEdgeActor();
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        RebootTablet(runtime, tenantSS, sender);

        // Insert four stale rows directly into root DataErasureGenerations.
        // Status = 1 (COMPLETED), StartTime = 0.
        LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, R"(
            (
                (let status '('Status (Uint32 '1)))
                (let startTime '('StartTime (Timestamp '0)))
                (return (AsList
                    (UpdateRow 'DataErasureGenerations '('('Generation (Uint64 '1))) '(status startTime))
                    (UpdateRow 'DataErasureGenerations '('('Generation (Uint64 '2))) '(status startTime))
                    (UpdateRow 'DataErasureGenerations '('('Generation (Uint64 '5))) '(status startTime))
                    (UpdateRow 'DataErasureGenerations '('('Generation (Uint64 '9))) '(status startTime))
                ))
            )
        )");

        // Insert four stale rows directly into tenant TenantDataErasureGenerations.
        // Status = 1 (COMPLETED). TenantDataErasureGenerations has no StartTime column.
        LocalMiniKQL(runtime, tenantSS, R"(
            (
                (let status '('Status (Uint32 '1)))
                (return (AsList
                    (UpdateRow 'TenantDataErasureGenerations '('('Generation (Uint64 '1))) '(status))
                    (UpdateRow 'TenantDataErasureGenerations '('('Generation (Uint64 '3))) '(status))
                    (UpdateRow 'TenantDataErasureGenerations '('('Generation (Uint64 '6))) '(status))
                    (UpdateRow 'TenantDataErasureGenerations '('('Generation (Uint64 '9))) '(status))
                ))
            )
        )");

        // Reboot both: Restore() -> CleanupOldGenerationsOnRestore() deletes all but max on each.
        RebootTablet(runtime, TTestTxConfig::SchemeShard, sender);
        RebootTablet(runtime, tenantSS, sender);

        // Verify root SS: exactly one row left.
        const TString rootQueryRead = R"(
            (
                (let range '('('Generation (Null) (Void))))
                (let fields '('Generation))
                (return (AsList
                    (SetResult 'Result (SelectRange 'DataErasureGenerations range fields '()))
                ))
            )
        )";
        {
            auto result = LocalMiniKQL(runtime, TTestTxConfig::SchemeShard, rootQueryRead);
            const auto& list = result.GetValue().GetStruct(0).GetOptional().GetStruct(0);
            UNIT_ASSERT_VALUES_EQUAL_C(list.ListSize(), 1u,
                "After reboot: expected exactly 1 row in DataErasureGenerations, got " << list.ListSize());
            // TODO https://github.com/ydb-platform/ydb/issues/44326 it's very suspicious that generation is set to 0 after reboot.    
            // ui64 survivingGen = list.GetList(0).GetStruct(0).GetUint64();
            // UNIT_ASSERT_VALUES_EQUAL_C(survivingGen, 9u,
            //     "After reboot: expected surviving generation to be 9 in DataErasureGenerations, got " << survivingGen);
        }

        // Verify tenant SS: exactly one row left.
        const TString tenantQueryRead = R"(
            (
                (let range '('('Generation (Null) (Void))))
                (let fields '('Generation))
                (return (AsList
                    (SetResult 'Result (SelectRange 'TenantDataErasureGenerations range fields '()))
                ))
            )
        )";
        {
            auto result = LocalMiniKQL(runtime, tenantSS, tenantQueryRead);
            const auto& list = result.GetValue().GetStruct(0).GetOptional().GetStruct(0);
            UNIT_ASSERT_VALUES_EQUAL_C(list.ListSize(), 1u,
                "After reboot: expected exactly 1 row in TenantDataErasureGenerations, got " << list.ListSize());
            // TODO https://github.com/ydb-platform/ydb/issues/44326 it's very suspicious that generation is set to 0 after reboot.    
            // ui64 survivingGen = list.GetList(0).GetStruct(0).GetUint64();
            // UNIT_ASSERT_VALUES_EQUAL_C(survivingGen, 9u,
            //     "After reboot: expected surviving generation to be 9 in TenantDataErasureGenerations, got " << survivingGen);
        }

        RunShred(runtime, 10);
    }
}
