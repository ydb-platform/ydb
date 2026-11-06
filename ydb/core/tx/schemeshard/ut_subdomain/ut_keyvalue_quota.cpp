#include <ydb/core/keyvalue/keyvalue_const.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TKeyValueStorageQuota) {

struct TEnv {
    TTestBasicRuntime Runtime;
    TTestEnv Env{Runtime};
    TActorId Edge = Runtime.AllocateEdgeActor();
    ui64 TxId = 100;
    TVector<ui64> Tablets;

    explicit TEnv(bool separatePools) {
        Runtime.GetAppData().FeatureFlags.SetEnableSeparateDiskSpaceQuotas(separatePools);
        TestCreateSubDomain(Runtime, ++TxId, "/MyRoot", R"(
            Name: "Tenant"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools { Name: "data" Kind: "ssd" }
            DatabaseQuotas {
                data_size_hard_quota: 10
                storage_quotas { unit_kind: "ssd" data_size_hard_quota: 10 }
            }
        )");
        Env.TestWaitNotification(Runtime, TxId);
        TestCreateSolomon(Runtime, ++TxId, "/MyRoot/Tenant", R"(
            Name: "KV"
            PartitionCount: 2
            StorageConfig {
                Channel { PreferredPoolKind: "ssd" }
                Channel { PreferredPoolKind: "ssd" }
                Channel { PreferredPoolKind: "ssd" }
            }
        )");
        Env.TestWaitNotification(Runtime, TxId);
        const auto desc = DescribePath(Runtime, "/MyRoot/Tenant/KV");
        for (const auto& part : desc.GetPathDescription().GetSolomonDescription().GetPartitions()) {
            Tablets.push_back(part.GetTabletId());
        }

        UNIT_ASSERT_VALUES_EQUAL(Tablets.size(), 2);
    }

    void CheckUsage(ui64 bytes, bool exceeded) {
        const auto desc = DescribePath(Runtime, "/MyRoot/Tenant");
        const auto& domain = desc.GetPathDescription().GetDomainDescription();
        UNIT_ASSERT_VALUES_EQUAL_C(domain.GetDiskSpaceUsage().GetKeyValueBytes(), bytes, domain.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(domain.GetDomainState().GetDiskQuotaExceeded(), exceeded);
    }

    template<class TRequest, class TResponse>
    auto Request(ui64 tablet, TRequest* request) {
        ForwardToTablet(Runtime, tablet, Edge, request);
        return Runtime.GrabEdgeEventRethrow<TResponse>(Edge)->Get()->Record;
    }

    NKikimrClient::TResponse LegacyRequest(ui64 tablet, TEvKeyValue::TEvRequest* request) {
        return Request<TEvKeyValue::TEvRequest, TEvKeyValue::TEvResponse>(tablet, request);
    }

    void Refresh(ui64 tablet) {
        ForwardToTablet(Runtime, tablet, Edge, new TEvKeyValue::TEvPeriodicRefresh);
        Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    }
};

Y_UNIT_TEST(EnforceAcrossPartitionsAndReboot) {
    TEnv env(false);
    const ui64 first = env.Tablets[0];
    const ui64 second = env.Tablets[1];
    const auto write = [&](ui64 tablet, bool mixed = false) {
        auto request = new TEvKeyValue::TEvExecuteTransaction;
        if (mixed) {
            request->Record.add_commands()->mutable_delete_range()->mutable_range();
        }

        auto* cmd = request->Record.add_commands()->mutable_write();
        cmd->set_key("k");
        cmd->set_value("123456");
        cmd->set_storage_channel(tablet == first ? NKeyValue::InlineStorageChannelInPublicApi : NKeyValue::MainStorageChannelInPublicApi);
        return env.Request<TEvKeyValue::TEvExecuteTransaction, TEvKeyValue::TEvExecuteTransactionResponse>(tablet, request).status();
    };

    UNIT_ASSERT_VALUES_EQUAL(write(first), NKikimrKeyValue::Statuses::RSTATUS_OK);
    env.Refresh(first);
    env.CheckUsage(6, false);
    UNIT_ASSERT_VALUES_EQUAL(write(second), NKikimrKeyValue::Statuses::RSTATUS_OK);
    env.Refresh(second);
    env.CheckUsage(12, true);
    UNIT_ASSERT_VALUES_EQUAL(write(first, true), NKikimrKeyValue::Statuses::RSTATUS_DISK_QUOTA_EXCEEDED);

    auto blockUpdates = env.Runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>([](auto& event) {
        event.Reset();
    });
    RebootTablet(env.Runtime, first, env.Edge);
    UNIT_ASSERT_VALUES_EQUAL(write(first), NKikimrKeyValue::Statuses::RSTATUS_DISK_QUOTA_EXCEEDED);
    auto* patch = new TEvKeyValue::TEvRequest;
    patch->Record.AddCmdPatch();
    UNIT_ASSERT_VALUES_EQUAL(env.LegacyRequest(first, patch).GetStatus(), NMsgBusProxy::MSTATUS_REJECTED);

    auto* read = new TEvKeyValue::TEvRequest;
    read->Record.AddCmdRead()->SetKey("k");
    UNIT_ASSERT_VALUES_EQUAL(env.LegacyRequest(first, read).GetReadResult(0).GetValue(), "123456");

    blockUpdates.Remove();
    auto* del = new TEvKeyValue::TEvRequest;
    del->Record.AddCmdDeleteRange()->MutableRange();
    UNIT_ASSERT_VALUES_EQUAL(env.LegacyRequest(first, del).GetStatus(), NMsgBusProxy::MSTATUS_OK);
    // The first tablet missed its initial watch notification while rebooting.
    RebootTablet(env.Runtime, first, env.Edge);
    env.Refresh(first);
    env.CheckUsage(6, false);
    UNIT_ASSERT_VALUES_EQUAL(write(second), NKikimrKeyValue::Statuses::RSTATUS_OK);
}

Y_UNIT_TEST(IgnoreOldReportsAndReleasePoolQuotaOnDrop) {
    TEnv env(true);
    const ui64 tablet = env.Tablets[0];
    const auto report = [&](ui32 generation, ui64 round, ui64 bytes) {
        auto* ev = new TEvKeyValue::TEvStorageUsageReport;
        ev->Record.set_tablet_id(tablet);
        ev->Record.set_generation(generation);
        ev->Record.set_round(round);
        (*ev->Record.mutable_bytes_by_channel())[2] = bytes;
        ForwardToTablet(env.Runtime, TTestTxConfig::SchemeShard, env.Edge, ev);
        env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    };

    report(100, 2, 20);
    env.CheckUsage(20, true);
    RebootTablet(env.Runtime, TTestTxConfig::SchemeShard, env.Edge);
    env.CheckUsage(20, true);
    report(100, 2, 20);
    report(100, 1, 4);
    report(99, 3, 1);
    env.CheckUsage(20, true);

    TestDropSolomon(env.Runtime, ++env.TxId, "/MyRoot/Tenant", "KV");
    env.Env.TestWaitNotification(env.Runtime, env.TxId);
    report(101, 1, 30);
    env.CheckUsage(0, false);
}

} // Y_UNIT_TEST_SUITE
