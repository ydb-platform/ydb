#include <ydb/core/keyvalue/keyvalue_const.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <yql/essentials/public/issue/protos/issue_id.pb.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TKeyValueStorageQuota) {

struct TSettings {
    bool SeparatePools = false;
    bool ReportStorageUsage = true;
    bool ExternalSubdomain = false;
};

struct TEnv {
    TTestBasicRuntime Runtime;
    TTestEnv Env{Runtime};
    TActorId Edge = Runtime.AllocateEdgeActor();
    ui64 TxId = 100;
    ui64 SchemeShard = TTestTxConfig::SchemeShard;
    TVector<ui64> Tablets;

    explicit TEnv(TSettings settings = {}) {
        Runtime.GetAppData().FeatureFlags.SetEnableSeparateDiskSpaceQuotas(settings.SeparatePools);
        Runtime.GetAppData().FeatureFlags.SetEnableKeyValueStorageUsageReports(settings.ReportStorageUsage);
        const TString tenant = R"(
            Name: "Tenant"
            PlanResolution: 50
            Coordinators: 1
            Mediators: 1
            TimeCastBucketsPerMediator: 2
            StoragePools { Name: "data" Kind: "ssd" }
            StoragePools { Name: "cold" Kind: "hdd" }
            DatabaseQuotas {
                data_size_hard_quota: 10
                storage_quotas { unit_kind: "ssd" data_size_hard_quota: 10 }
                storage_quotas { unit_kind: "hdd" data_size_hard_quota: 20 }
            }
        )";
        if (settings.ExternalSubdomain) {
            TestCreateExtSubDomain(Runtime, ++TxId, "/MyRoot", R"(Name: "Tenant")");
            Env.TestWaitNotification(Runtime, TxId);
            TestAlterExtSubDomain(Runtime, ++TxId, "/MyRoot", tenant + "ExternalSchemeShard: true");
            Env.TestWaitNotification(Runtime, TxId);
            TestDescribeResult(DescribePath(Runtime, "/MyRoot/Tenant"), {NLs::ExtractTenantSchemeshard(&SchemeShard)});
        } else {
            TestCreateSubDomain(Runtime, ++TxId, "/MyRoot", tenant);
            Env.TestWaitNotification(Runtime, TxId);
        }

        TestCreateSolomon(Runtime, SchemeShard, ++TxId, "/MyRoot/Tenant", R"(
            Name: "KV"
            PartitionCount: 2
            StorageConfig {
                Channel { PreferredPoolKind: "ssd" }
                Channel { PreferredPoolKind: "ssd" }
                Channel { PreferredPoolKind: "ssd" }
                Channel { PreferredPoolKind: "hdd" }
            }
        )");
        Env.TestWaitNotification(Runtime, TxId, SchemeShard);
        const auto desc = DescribePath(Runtime, SchemeShard, "/MyRoot/Tenant/KV");
        for (const auto& part : desc.GetPathDescription().GetSolomonDescription().GetPartitions()) {
            Tablets.push_back(part.GetTabletId());
        }

        UNIT_ASSERT_VALUES_EQUAL(Tablets.size(), 2);
    }

    void CheckUsage(ui64 bytes, bool exceeded) {
        const auto desc = DescribePath(Runtime, SchemeShard, "/MyRoot/Tenant");
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

Y_UNIT_TEST(ToggleStorageUsageReports) {
    TEnv env({.ReportStorageUsage = false});
    const ui64 tablet = env.Tablets.front();
    ui64 reports = 0;
    auto observer = env.Runtime.AddObserver<TEvKeyValue::TEvStorageUsageReport>([&](auto& ev) {
        if (ev->Get()->Record.tablet_id() == tablet) {
            ++reports;
        }
    });

    RebootTablet(env.Runtime, tablet, env.Edge);
    env.Refresh(tablet);
    UNIT_ASSERT_VALUES_EQUAL(reports, 0);

    auto& flags = env.Runtime.GetAppData().FeatureFlags;
    flags.SetEnableKeyValueStorageUsageReports(true);
    env.Refresh(tablet);
    UNIT_ASSERT_GT(reports, 0);

    flags.SetEnableKeyValueStorageUsageReports(false);
    const ui64 beforeRefresh = reports;
    env.Refresh(tablet);
    UNIT_ASSERT_VALUES_EQUAL(reports, beforeRefresh);
}

Y_UNIT_TEST(EnforceAcrossPartitionsAndReboot) {
    TEnv env;
    const ui64 first = env.Tablets[0];
    const ui64 second = env.Tablets[1];
    const auto checkWrite = [&](ui64 tablet, NKikimrKeyValue::Statuses::ReplyStatus expected, bool mixed = false) {
        auto request = new TEvKeyValue::TEvExecuteTransaction;
        if (mixed) {
            request->Record.add_commands()->mutable_delete_range()->mutable_range();
        }

        auto* cmd = request->Record.add_commands()->mutable_write();
        cmd->set_key("k");
        cmd->set_value("123456");
        cmd->set_storage_channel(tablet == first ? NKeyValue::InlineStorageChannelInPublicApi : NKeyValue::MainStorageChannelInPublicApi);
        const auto response = env.Request<TEvKeyValue::TEvExecuteTransaction, TEvKeyValue::TEvExecuteTransactionResponse>(tablet, request);
        UNIT_ASSERT_C(response.status() == expected, response.ShortDebugString());
        if (expected == NKikimrKeyValue::Statuses::RSTATUS_DISK_QUOTA_EXCEEDED) {
            UNIT_ASSERT_VALUES_EQUAL(response.issues_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response.issues(0).issue_code(),
                static_cast<ui32>(NYql::TIssuesIds::KIKIMR_DATABASE_DISK_SPACE_QUOTA_EXCEEDED));
        }
    };

    checkWrite(first, NKikimrKeyValue::Statuses::RSTATUS_OK);
    env.Refresh(first);
    env.CheckUsage(6, false);
    checkWrite(second, NKikimrKeyValue::Statuses::RSTATUS_OK);
    env.Refresh(second);
    env.CheckUsage(12, true);
    checkWrite(first, NKikimrKeyValue::Statuses::RSTATUS_DISK_QUOTA_EXCEEDED, true);

    auto blockUpdates = env.Runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>([](auto& event) {
        event.Reset();
    });
    RebootTablet(env.Runtime, first, env.Edge);
    checkWrite(first, NKikimrKeyValue::Statuses::RSTATUS_DISK_QUOTA_EXCEEDED);
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
    checkWrite(second, NKikimrKeyValue::Statuses::RSTATUS_OK);

    auto blockRecoveredUpdates = env.Runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>([](auto& event) {
        event.Reset();
    });

    RebootTablet(env.Runtime, first, env.Edge);
    checkWrite(first, NKikimrKeyValue::Statuses::RSTATUS_OK);
}

Y_UNIT_TEST(EnforceExternalSubdomainQuota) {
    TEnv env({.ExternalSubdomain = true});
    const ui64 tablet = env.Tablets.front();
    const auto write = [&](const TString& value) {
        auto* request = new TEvKeyValue::TEvExecuteTransaction;
        auto* cmd = request->Record.add_commands()->mutable_write();
        cmd->set_key("k");
        cmd->set_value(value);
        cmd->set_storage_channel(NKeyValue::InlineStorageChannelInPublicApi);
        return env.Request<TEvKeyValue::TEvExecuteTransaction, TEvKeyValue::TEvExecuteTransactionResponse>(tablet, request);
    };

    auto response = write("12345678901");
    UNIT_ASSERT_C(response.status() == NKikimrKeyValue::Statuses::RSTATUS_OK, response.ShortDebugString());
    env.Refresh(tablet);
    env.CheckUsage(11, true);

    response = write("x");
    UNIT_ASSERT_C(response.status() == NKikimrKeyValue::Statuses::RSTATUS_DISK_QUOTA_EXCEEDED, response.ShortDebugString());
    auto* del = new TEvKeyValue::TEvRequest;
    del->Record.AddCmdDeleteRange()->MutableRange();
    UNIT_ASSERT_VALUES_EQUAL(env.LegacyRequest(tablet, del).GetStatus(), NMsgBusProxy::MSTATUS_OK);
    env.Refresh(tablet);
    env.CheckUsage(0, false);

    response = write("x");
    UNIT_ASSERT_C(response.status() == NKikimrKeyValue::Statuses::RSTATUS_OK, response.ShortDebugString());
}

Y_UNIT_TEST(EnforceQuotaForThePoolHoldingTheValue) {
    TEnv env({.SeparatePools = true});
    const ui64 tablet = env.Tablets.front();
    const ui32 ssdChannel = NKeyValue::MainStorageChannelInPublicApi;
    const ui32 hddChannel = ssdChannel + 1;
    const TString value(12, 'v');
    const auto writeToChannel = [&](ui32 channel) {
        auto* request = new TEvKeyValue::TEvExecuteTransaction;
        auto* write = request->Record.add_commands()->mutable_write();
        write->set_key("k");
        write->set_value(value);
        write->set_storage_channel(channel);
        const auto response = env.Request<TEvKeyValue::TEvExecuteTransaction, TEvKeyValue::TEvExecuteTransactionResponse>(tablet, request);
        UNIT_ASSERT_C(response.status() == NKikimrKeyValue::Statuses::RSTATUS_OK, response.ShortDebugString());
        env.Refresh(tablet);
    };

    writeToChannel(hddChannel);
    env.CheckUsage(value.size(), false);
    writeToChannel(ssdChannel);
    env.CheckUsage(value.size(), true);
}

Y_UNIT_TEST(ExcludeExternalChannelsFromDiskUsage) {
    TChannelsBindings bindings(4);
    bindings[0].SetStoragePoolKind("ssd");
    bindings[1].SetStoragePoolName("data");
    bindings[2].SetStoragePoolKind("hdd");
    bindings[3].SetStoragePoolName("external");
    bindings[3].MutableExternalDataSourceId()->SetOwnerId(1);
    bindings[3].MutableExternalDataSourceId()->SetLocalId(2);
    const TStoragePools pools{{"data", "ssd"}};

    NKikimrKeyValue::StorageUsageReport report;
    report.set_generation(7);
    report.set_round(9);
    auto& bytes = *report.mutable_bytes_by_channel();
    bytes[0] = 2;
    bytes[1] = 3;
    bytes[2] = 4;
    bytes[3] = 100;
    // Adopted tablets can report channels whose bindings are not known to SchemeShard.
    bytes[4] = 5;

    const auto usage = NSchemeShard::PrepareKeyValueStorageUsage(report, bindings, pools);
    UNIT_ASSERT_VALUES_EQUAL(usage.generation(), 7);
    UNIT_ASSERT_VALUES_EQUAL(usage.round(), 9);
    UNIT_ASSERT_VALUES_EQUAL(usage.total_bytes(), 14);
    UNIT_ASSERT_VALUES_EQUAL(usage.bytes_by_pool_kind().size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(usage.bytes_by_pool_kind().at("ssd"), 5);
    UNIT_ASSERT_VALUES_EQUAL(usage.bytes_by_pool_kind().at("hdd"), 4);

    bindings[3].SetStoragePoolKind("ssd");
    const auto externalWithPoolKind = NSchemeShard::PrepareKeyValueStorageUsage(report, bindings, pools);
    UNIT_ASSERT_VALUES_EQUAL(externalWithPoolKind.total_bytes(), usage.total_bytes());
    UNIT_ASSERT_VALUES_EQUAL(externalWithPoolKind.bytes_by_pool_kind().at("ssd"), 5);
}

Y_UNIT_TEST(IgnoreOldReportsAndReleasePoolQuotaOnDrop) {
    TEnv env({.SeparatePools = true});
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
