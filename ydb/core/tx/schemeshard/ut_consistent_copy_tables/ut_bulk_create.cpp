#include <ydb/core/base/hive.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/common/network.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

namespace {

const TString TableDescription = R"(
    Name: "Source"
    Columns { Name: "key" Type: "Uint64" }
    Columns { Name: "value" Type: "Utf8" }
    KeyColumnNames: ["key"]
    UniformPartitionsCount: 4
)";

void CheckRequests(const TVector<NKikimrHive::TEvCreateTablet>& requests, bool bulk) {
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), bulk ? 1 : 4);
    if (bulk) {
        UNIT_ASSERT_VALUES_EQUAL(requests.front().GetCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(requests.front().OwnerIdxsSize(), 0);
    } else {
        for (const auto& request : requests) {
            UNIT_ASSERT(!TEvHive::TEvCreateTablet::IsBatch(request));
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TSchemeShardBulkCreate) {
    Y_UNIT_TEST_FLAG(CreateAndBackupCopy, bulk) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableHiveBulkCreate(bulk);
        TVector<NKikimrHive::TEvCreateTablet> requests;
        auto observer = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            if (ev->Get()->Record.GetTabletType() == TTabletTypes::DataShard) {
                requests.push_back(ev->Get()->Record);
            }
        });
        TestCreateTable(runtime, 100, "/MyRoot", TableDescription);
        env.TestWaitNotification(runtime, 100);
        CheckRequests(requests, bulk);
        requests.clear();

        TestConsistentCopyTables(runtime, 101, "/MyRoot", R"(
            CopyTableDescriptions {
                SrcPath: "/MyRoot/Source"
                DstPath: "/MyRoot/Backup"
                IsBackup: true
            }
        )");
        env.TestWaitNotification(runtime, 101);
        CheckRequests(requests, bulk);
        for (const auto& request : requests) {
            UNIT_ASSERT(request.GetIsBackup());
        }
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Backup"), {NLs::PathExist, NLs::IsTable});
    }

    Y_UNIT_TEST_FLAG(SparseRetryOnlyForFailedItems, legacyRetry) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableHiveBulkCreate(true);
        TVector<ui64> failed;
        TVector<ui64> retried;
        ui32 requests = 0;
        ui32 replies = 0;
        auto requestObserver = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (!TEvHive::TEvCreateTablet::IsBatch(record)) {
                return;
            }
            if (++requests == 1) {
                UNIT_ASSERT_VALUES_EQUAL(record.GetCount(), 4);
                const ui64 first = record.GetOwnerIdx();
                // Fake Hive creates just the successful subset of the original request.
                failed = {first + 1, first + 3};
                record.ClearCount();
                record.ClearOwnerIdx();
                record.AddOwnerIdxs(first);
                record.AddOwnerIdxs(first + 2);
            } else {
                UNIT_ASSERT(!record.HasCount());
                UNIT_ASSERT(!record.HasOwnerIdx());
                retried.assign(record.GetOwnerIdxs().begin(), record.GetOwnerIdxs().end());
                if (legacyRetry) {
                    // Old Hive rejects a sparse request: there is no scalar OwnerIdx,
                    // so even the error reply has to be routed by its pipe cookie.
                    auto reply = MakeHolder<TEvHive::TEvCreateTabletReply>();
                    reply->Record.SetOwner(record.GetOwner());
                    reply->Record.SetStatus(NKikimrProto::ERROR);
                    reply->Record.SetErrorReason(NKikimrHive::ERROR_REASON_INVALID_ARGUMENTS);
                    runtime.Send(new IEventHandle(ev->Sender, ev->GetRecipientRewrite(), reply.Release(), 0, ev->Cookie));
                    ev.Reset();
                }
            }
        });
        auto replyObserver = runtime.AddObserver<TEvHive::TEvCreateTabletReply>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (record.GetIsBatch() && ++replies == 1) {
                for (ui64 idx : failed) {
                    auto* result = record.AddResults();
                    result->SetOwnerIdx(idx);
                    result->SetStatus(NKikimrProto::TRYLATER);
                }
            }
        });
        TestCreateTable(runtime, 100, "/MyRoot", TableDescription);
        env.TestWaitNotification(runtime, 100);
        UNIT_ASSERT_VALUES_EQUAL(requests, 2);
        UNIT_ASSERT_VALUES_EQUAL(replies, legacyRetry ? 1 : 2);
        UNIT_ASSERT(retried == failed);
        TestDescribeResult(DescribePath(runtime, "/MyRoot/Source"), {NLs::PathExist, NLs::IsTable});
    }

    Y_UNIT_TEST(RetryBackoffResetsAfterProgress) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableHiveBulkCreate(true);
        TVector<TInstant> requests;
        TVector<TInstant> replies;
        auto requestObserver = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            if (TEvHive::TEvCreateTablet::IsBatch(ev->Get()->Record)) {
                requests.push_back(runtime.GetCurrentTime());
            }
        });
        auto replyObserver = runtime.AddObserver<TEvHive::TEvCreateTabletReply>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (!record.GetIsBatch()) {
                return;
            }
            replies.push_back(runtime.GetCurrentTime());
            // Two attempts without progress, then one successful item, then success.
            if (replies.size() <= 3) {
                const size_t firstRetry = replies.size() == 3 ? 1 : 0;
                for (size_t i = firstRetry; i < record.ResultsSize(); ++i) {
                    auto* result = record.MutableResults(i);
                    result->SetStatus(NKikimrProto::TRYLATER);
                    result->ClearTabletID();
                }
            }
        });
        TestCreateTable(runtime, 100, "/MyRoot", TableDescription);
        env.TestWaitNotification(runtime, 100);
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(replies.size(), 4);
        UNIT_ASSERT(requests[2] - replies[1] >= TDuration::Seconds(2));
        const auto delayAfterProgress = requests[3] - replies[2];
        UNIT_ASSERT(delayAfterProgress >= TDuration::Seconds(1));
        UNIT_ASSERT(delayAfterProgress < TDuration::Seconds(2));
    }

    Y_UNIT_TEST(LostReplyAndSchemeShardReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableHiveBulkCreate(true);
        TBlockEvents<TEvHive::TEvCreateTabletReply> replies(runtime, [](const auto& ev) {
            return ev->Get()->Record.GetIsBatch();
        });
        TestCreateTable(runtime, 100, "/MyRoot", TableDescription);
        runtime.WaitFor("committed batch reply", [&] { return !replies.empty(); });
        const auto committed = replies.front()->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(committed.ResultsSize(), 4);
        replies.Stop().clear();

        NKikimrHive::TEvCreateTabletReply retry;
        auto observer = runtime.AddObserver<TEvHive::TEvCreateTabletReply>([&](auto& ev) {
            if (ev->Get()->Record.GetIsBatch()) {
                retry = ev->Get()->Record;
            }
        });
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        env.TestWaitNotification(runtime, 100);
        UNIT_ASSERT_VALUES_EQUAL(retry.ResultsSize(), 4);
        for (size_t i = 0; i < committed.ResultsSize(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(retry.GetResults(i).GetOwnerIdx(), committed.GetResults(i).GetOwnerIdx());
            UNIT_ASSERT_VALUES_EQUAL(retry.GetResults(i).GetTabletID(), committed.GetResults(i).GetTabletID());
        }
    }

    Y_UNIT_TEST(SchemeShardRebootWhileRealTenantHiveWaitsForIds) {
        TPortManager ports;
        Tests::TServerSettings settings(ports.GetPort());
        settings.SetDomainName("Root").SetUseRealThreads(false).SetDynamicNodeCount(1);
        settings.FeatureFlags.SetEnableHiveBulkCreate(true);
        auto* hiveConfig = settings.AppConfig->MutableHiveConfig();
        hiveConfig->SetMinRequestSequenceSize(1);
        hiveConfig->SetRequestSequenceSize(1);
        hiveConfig->SetMaxRequestSequenceSize(1);
        Tests::TServer::TPtr server = new Tests::TServer(settings);
        auto& runtime = *server->GetRuntime();
        const ui64 rootSchemeShard = Tests::ChangeStateStorage(Tests::SchemeRoot, settings.Domain);
        const auto edge = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(edge);

        const auto rootSubscriber = CreateNotificationSubscriber(runtime, rootSchemeShard);
        TestCreateExtSubDomain(runtime, rootSchemeShard, 100, "/Root", R"(Name: "Tenant")");
        TestWaitNotification(runtime, {100}, rootSubscriber);
        Tests::TTenants tenants(server);
        tenants.Run("/Root/Tenant", 1);
        TestAlterExtSubDomain(runtime, rootSchemeShard, 101, "/Root", R"(
            Name: "Tenant"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            ExternalSchemeShard: true
            ExternalHive: true
            StoragePools { Name: "/Root:test" Kind: "test" }
        )");
        TestWaitNotification(runtime, {101}, rootSubscriber);
        const auto domain = DescribePath(runtime, rootSchemeShard, "/Root/Tenant");
        const auto& params = domain.GetPathDescription().GetDomainDescription().GetProcessingParams();
        const ui64 schemeShard = params.GetSchemeShard();
        const ui64 hive = params.GetHive();
        UNIT_ASSERT(schemeShard && schemeShard != rootSchemeShard);
        UNIT_ASSERT(hive && hive != Tests::ChangeStateStorage(Tests::Hive, settings.Domain));

        auto listShards = [&] {
            auto request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
            request->Record.SetTabletType(TTabletTypes::DataShard);
            runtime.SendToPipe(hive, edge, request.Release(), 0, GetPipeConfigWithRetries());
            const auto info = runtime.GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(edge);
            THashMap<ui64, ui64> result;
            for (const auto& tablet : info->Get()->Record.GetTablets()) {
                if (tablet.GetTabletOwner().GetOwner() == schemeShard) {
                    UNIT_ASSERT(result.emplace(tablet.GetTabletOwner().GetOwnerIdx(), tablet.GetTabletID()).second);
                }
            }
            return result;
        };
        UNIT_ASSERT(listShards().empty());

        TVector<NKikimrHive::TEvCreateTablet> requests;
        TVector<TActorId> senders;
        THashMap<ui64, ui64> acknowledged;
        size_t successfulReplies = 0;
        auto requestObserver = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetOwner() == schemeShard && record.GetTabletType() == TTabletTypes::DataShard) {
                UNIT_ASSERT(TEvHive::TEvCreateTablet::IsBatch(record));
                requests.push_back(record);
                senders.push_back(ev->Sender);
            }
        });
        auto replyObserver = runtime.AddObserver<TEvHive::TEvCreateTabletReply>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (!record.GetIsBatch() || record.GetOwner() != schemeShard) {
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(record.GetOrigin(), hive);
            UNIT_ASSERT_VALUES_EQUAL(record.ResultsSize(), 4);
            ++successfulReplies;
            for (const auto& result : record.GetResults()) {
                // No TRYLATER/client backoff after the SchemeShard reconnects.
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::OK);
                const auto [it, inserted] = acknowledged.emplace(result.GetOwnerIdx(), result.GetTabletID());
                UNIT_ASSERT(inserted || it->second == result.GetTabletID());
            }
        });
        TBlockEvents<TEvHive::TEvResponseTabletIdSequence> refills(runtime, [=](const auto& ev) {
            return ev->Get()->Record.GetOwner().GetOwner() == hive;
        });
        TestCreateTable(runtime, schemeShard, 200, "/Root/Tenant", TableDescription);
        runtime.WaitFor("real tenant Hive waits for IDs", [&] { return !refills.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(successfulReplies, 0);
        UNIT_ASSERT(listShards().empty());

        RebootTablet(runtime, schemeShard, runtime.AllocateEdgeActor());
        runtime.WaitFor("rebooted SchemeShard replayed its batch", [&] { return requests.size() >= 2; });
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);
        UNIT_ASSERT(senders[0] != senders[1]);
        UNIT_ASSERT_VALUES_EQUAL(requests[0].SerializeAsString(), requests[1].SerializeAsString());
        refills.Stop().Unblock();
        const auto subscriber = CreateNotificationSubscriber(runtime, schemeShard);
        TestWaitNotification(runtime, {200}, subscriber);
        UNIT_ASSERT(successfulReplies > 0);
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2); // No timer-based third attempt was needed.
        UNIT_ASSERT_VALUES_EQUAL(acknowledged.size(), 4);
        const auto actual = listShards();
        UNIT_ASSERT(actual == acknowledged);
        THashSet<ui64> tabletIds;
        for (const auto& [idx, tabletId] : actual) {
            UNIT_ASSERT(tabletId);
            UNIT_ASSERT(tabletIds.insert(tabletId).second);
            runtime.SendToPipe(hive, edge, new TEvHive::TEvLookupTablet(schemeShard, idx), 0, GetPipeConfigWithRetries());
            const auto lookup = runtime.GrabEdgeEventRethrow<TEvHive::TEvCreateTabletReply>(edge);
            UNIT_ASSERT_VALUES_EQUAL(lookup->Get()->Record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(lookup->Get()->Record.GetTabletID(), tabletId);
        }
        const auto table = DescribePath(runtime, schemeShard, "/Root/Tenant/Source", true);
        TestDescribeResult(table, {NLs::PathExist, NLs::IsTable});
        THashSet<ui64> partitions;
        for (const auto& partition : table.GetPathDescription().GetTablePartitions()) {
            UNIT_ASSERT(partitions.insert(partition.GetDatashardId()).second);
        }
        UNIT_ASSERT(partitions == tabletIds);
    }

    Y_UNIT_TEST(LegacyHiveReplyFallsBackToSingles) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableHiveBulkCreate(true);
        ui32 batches = 0;
        ui32 singles = 0;
        auto observer = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (record.GetTabletType() != TTabletTypes::DataShard) {
                return;
            }
            if (record.HasCount()) {
                ++batches;
                record.ClearCount(); // emulate an old Hive ignoring the new protobuf field
            } else {
                ++singles;
            }
        });
        TestCreateTable(runtime, 100, "/MyRoot", TableDescription);
        env.TestWaitNotification(runtime, 100);
        UNIT_ASSERT_VALUES_EQUAL(batches, 1);
        UNIT_ASSERT_VALUES_EQUAL(singles, 3);
    }

    Y_UNIT_TEST(IncompleteReplyDoesNotAcknowledgeBatch) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableHiveBulkCreate(true);
        TBlockEvents<TEvHive::TEvCreateTabletReply> replies(runtime, [](const auto& ev) {
            return ev->Get()->Record.GetIsBatch();
        });
        TestCreateTable(runtime, 100, "/MyRoot", TableDescription);
        runtime.WaitFor("batch reply", [&] { return !replies.empty(); });
        replies.Stop();
        const auto& original = replies.front();
        auto incomplete = MakeHolder<TEvHive::TEvCreateTabletReply>();
        incomplete->Record = original->Get()->Record;
        incomplete->Record.MutableResults()->RemoveLast();
        runtime.Send(new IEventHandle(original->GetRecipientRewrite(), original->Sender,
            incomplete.Release(), 0, original->Cookie));
        runtime.SimulateSleep(TDuration::MilliSeconds(100));
        replies.Unblock();
        env.TestWaitNotification(runtime, 100);
    }

    Y_UNIT_TEST(PermanentConflictDoesNotFallBackToUpsertAfterReboot) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        runtime.GetAppData().FeatureFlags.SetEnableHiveBulkCreate(true);
        ui64 failedIdx = 0;
        ui32 failures = 0;
        ui32 unknownSingles = 0;
        auto requestObserver = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetTabletType() == TTabletTypes::DataShard
                && !record.HasTabletID() && !TEvHive::TEvCreateTablet::IsBatch(record)) {
                ++unknownSingles;
            }
        });
        auto replyObserver = runtime.AddObserver<TEvHive::TEvCreateTabletReply>([&](auto& ev) {
            auto& record = ev->Get()->Record;
            if (!record.GetIsBatch()) {
                return;
            }
            if (!failedIdx) {
                failedIdx = record.GetResults(1).GetOwnerIdx();
            }
            for (auto& result : *record.MutableResults()) {
                if (result.GetOwnerIdx() == failedIdx) {
                    result.SetStatus(NKikimrProto::ERROR);
                    result.SetErrorReason(NKikimrHive::ERROR_REASON_CREATE_CONFLICT);
                    ++failures;
                }
            }
        });
        TestCreateTable(runtime, 100, "/MyRoot", TableDescription);
        runtime.WaitFor("permanent create conflict", [&] { return failures == 1; });
        runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(failures, 1);
        RebootTablet(runtime, TTestTxConfig::SchemeShard, runtime.AllocateEdgeActor());
        runtime.WaitFor("conflict is checked again after recovery", [&] { return failures == 2; });
        runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(failures, 2);
        UNIT_ASSERT_VALUES_EQUAL(unknownSingles, 0);
    }
}
