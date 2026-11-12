#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/dsproxy/mock/model.h>
#include <ydb/core/keyvalue/keyvalue_const.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <yql/essentials/public/issue/protos/issue_id.pb.h>

#include <google/protobuf/unknown_field_set.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TKeyValueStorageQuota) {

struct TSettings {
    bool SeparatePools = false;
    bool ReportStorageUsage = true;
    bool ExternalSubdomain = false;
};

struct TEnv {
    static constexpr ui32 HddGroup = 2181038080;
    TTestBasicRuntime Runtime;
    TTestEnv Env{Runtime, TTestEnvOptions().DSProxies({
        MakeIntrusive<NFake::TProxyDS>(TGroupId::Zero()),
        MakeIntrusive<NFake::TProxyDS>(TGroupId::FromValue(HddGroup)),
    })};
    TActorId Edge = Runtime.AllocateEdgeActor();
    ui64 TxId = 100;
    ui64 SchemeShard = TTestTxConfig::SchemeShard;
    TVector<ui64> Tablets;
    THashMap<ui64, ui32> DataGroups;
    THashMap<ui64, TVector<TTabletChannelInfo::THistoryEntry>> DataChannelHistory;
    bool HddGroupExternal = false;
    std::optional<ui32> HoldGroupInfo;
    TVector<TAutoPtr<IEventHandle>> HeldGroupInfo;
    TTestActorRuntime::TEventObserverHolder BootObserver;
    TTestActorRuntime::TEventObserverHolder GroupInfoObserver;

    explicit TEnv(TSettings settings = {}) {
        // FakeHive does not resolve channel bindings; keep its tablet history consistent with the pools below.
        BootObserver = Runtime.AddObserver<TEvTablet::TEvBoot>([&](auto& ev) {
            auto& info = *ev->Get()->TabletStorageInfo;
            if (info.TabletType == TTabletTypes::KeyValue) {
                auto& history = DataChannelHistory[info.TabletID];
                if (history.empty()) {
                    history.emplace_back(0, 0);
                }

                const ui32 groupId = DataGroups.Value(info.TabletID, 0);
                if (history.back().GroupID != groupId) {
                    history.emplace_back(ev->Get()->Generation, groupId);
                }

                info.Channels[2].History = history;
                info.Channels[3].History[0].GroupID = HddGroup;
            }
        });

        GroupInfoObserver = Runtime.AddObserver<TEvNodeWardenQueryGroupInfo>([&](auto& ev) {
            auto* response = new TEvNodeWardenGroupInfo;
            auto& group = *response->Record.MutableGroup();
            const ui32 groupId = ev->Get()->Record.GetGroupId();
            UNIT_ASSERT(groupId == 0 || groupId == HddGroup);
            group.SetGroupID(groupId);
            group.SetGroupGeneration(1);
            const bool external = groupId == HddGroup && HddGroupExternal;
            group.SetIsExternalStorage(external);
            if (external) {
                group.SetBlobDepotId(123);
            }

            group.SetStoragePoolName(groupId == HddGroup ? "cold" : "data");
            group.SetStoragePoolKind(groupId == HddGroup ? "hdd" : "ssd");
            auto reply = MakeHolder<IEventHandle>(ev->Sender, ev->Recipient, response, 0, ev->Cookie);
            if (HoldGroupInfo == groupId) {
                HeldGroupInfo.emplace_back(reply.Release());
            } else {
                Runtime.Send(reply.Release());
            }

            ev.Reset();
        });

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

    void Report(ui64 tablet, ui32 generation, ui64 round, std::optional<ui64> bytes,
                const TString& poolKind = "ssd") {
        auto* ev = new TEvKeyValue::TEvStorageUsageReport;
        ev->Record.set_tablet_id(tablet);
        ev->Record.set_generation(generation);
        ev->Record.set_round(round);
        if (bytes) {
            ev->Record.set_total_bytes(*bytes);
            (*ev->Record.mutable_bytes_by_pool_kind())[poolKind] = *bytes;
        }

        ForwardToTablet(Runtime, SchemeShard, Edge, ev);
        Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    }

    void Write(ui64 tablet, const TString& key, const TString& value) {
        auto* request = new TEvKeyValue::TEvExecuteTransaction;
        auto* write = request->Record.add_commands()->mutable_write();
        write->set_key(key);
        write->set_value(value);
        write->set_storage_channel(NKeyValue::MainStorageChannelInPublicApi);
        const auto response = Request<TEvKeyValue::TEvExecuteTransaction, TEvKeyValue::TEvExecuteTransactionResponse>(tablet, request);
        UNIT_ASSERT_C(response.status() == NKikimrKeyValue::Statuses::RSTATUS_OK, response.ShortDebugString());
    }

    void RebindDataChannel(ui64 tablet, ui32 groupId) {
        DataGroups[tablet] = groupId;
        RebootTablet(Runtime, tablet, Edge);
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

Y_UNIT_TEST(AccountHistoricalGroupsAcrossDiskAndS3Rebinds) {
    TEnv env;
    const ui64 tablet = env.Tablets.front();
    env.HddGroupExternal = true;
    env.Write(tablet, "disk-old", "123");
    env.Refresh(tablet);
    env.CheckUsage(3, false);

    env.RebindDataChannel(tablet, TEnv::HddGroup);
    env.Write(tablet, "external", "12345");
    env.Refresh(tablet);
    env.CheckUsage(3, false);

    env.RebindDataChannel(tablet, 0);
    env.Write(tablet, "disk-new", "1234");
    env.Refresh(tablet);
    env.CheckUsage(7, false);
    RebootTablet(env.Runtime, tablet, env.Edge);
    env.Refresh(tablet);
    env.CheckUsage(7, false);

    auto* read = new TEvKeyValue::TEvRequest;
    read->Record.AddCmdRead()->SetKey("external");
    UNIT_ASSERT_VALUES_EQUAL(env.LegacyRequest(tablet, read).GetReadResult(0).GetValue(), "12345");

    auto* request = new TEvKeyValue::TEvExecuteTransaction;
    auto* range = request->Record.add_commands()->mutable_delete_range()->mutable_range();
    range->set_from_key_inclusive("disk-old");
    range->set_to_key_inclusive("disk-old");
    const auto response = env.Request<TEvKeyValue::TEvExecuteTransaction, TEvKeyValue::TEvExecuteTransactionResponse>(tablet, request);
    UNIT_ASSERT_C(response.status() == NKikimrKeyValue::Statuses::RSTATUS_OK, response.ShortDebugString());
    env.Refresh(tablet);
    env.CheckUsage(4, false);
}

Y_UNIT_TEST(MissingMetadataKeepsUsageAndLateRepliesDoNotRestoreOldUsage) {
    TEnv env;
    const ui64 tablet = env.Tablets.front();
    env.Write(tablet, "key", "123");
    env.Refresh(tablet);
    env.CheckUsage(3, false);

    env.RebindDataChannel(tablet, TEnv::HddGroup);
    env.Write(tablet, "external", "12345");
    env.Refresh(tablet);
    env.CheckUsage(8, false);

    env.HoldGroupInfo = TEnv::HddGroup;
    env.Refresh(tablet);
    env.CheckUsage(8, false);
    UNIT_ASSERT(!env.HeldGroupInfo.empty());

    env.Write(tablet, "key", "1234");
    env.HddGroupExternal = true;
    env.HoldGroupInfo.reset();
    env.Refresh(tablet);
    env.CheckUsage(0, false);

    for (auto& reply : env.HeldGroupInfo) {
        env.Runtime.Send(reply.Release());
    }

    env.HeldGroupInfo.clear();
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    env.CheckUsage(0, false);
}

Y_UNIT_TEST(IgnoreIncompleteReportsAndAcceptExplicitZero) {
    TEnv env;
    const ui64 tablet = env.Tablets[0];
    env.Report(tablet, 100, 1, 20);
    env.CheckUsage(20, true);

    env.Report(tablet, 100, 2, std::nullopt);
    env.CheckUsage(20, true);

    auto* legacy = new TEvKeyValue::TEvStorageUsageReport;
    legacy->Record.set_tablet_id(tablet);
    legacy->Record.set_generation(101);
    legacy->Record.set_round(1);
    // Former map entry at tag 4: channel 2, usage 100. It must not be interpreted as a complete report.
    legacy->Record.GetReflection()->MutableUnknownFields(&legacy->Record)->AddLengthDelimited(4, "\x08\x02\x10\x64");
    ForwardToTablet(env.Runtime, env.SchemeShard, env.Edge, legacy);
    env.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
    env.CheckUsage(20, true);

    RebootTablet(env.Runtime, env.SchemeShard, env.Edge);
    env.CheckUsage(20, true);

    // An incomplete report must not advance the persisted sequence number.
    env.Report(tablet, 100, 2, 0);
    env.CheckUsage(0, false);
}

Y_UNIT_TEST(UseReportedStoragePoolAfterReboot) {
    TEnv env({.SeparatePools = true});
    const ui64 tablet = env.Tablets[0];
    env.Report(tablet, 100, 1, 12, "hdd");
    env.CheckUsage(12, false);
    RebootTablet(env.Runtime, env.SchemeShard, env.Edge);
    env.CheckUsage(12, false);

    env.Report(tablet, 100, 2, 12, "ssd");
    env.CheckUsage(12, true);
    RebootTablet(env.Runtime, env.SchemeShard, env.Edge);
    env.CheckUsage(12, true);

    env.Report(tablet, 100, 3, 12, "hdd");
    env.CheckUsage(12, false);
}

Y_UNIT_TEST(IgnoreOldReportsAndReleasePoolQuotaOnDrop) {
    TEnv env({.SeparatePools = true});
    const ui64 tablet = env.Tablets[0];
    env.Report(tablet, 100, 2, 20);
    env.CheckUsage(20, true);
    RebootTablet(env.Runtime, TTestTxConfig::SchemeShard, env.Edge);
    env.CheckUsage(20, true);
    env.Report(tablet, 100, 2, 20);
    env.Report(tablet, 100, 1, 4);
    env.Report(tablet, 99, 3, 1);
    env.CheckUsage(20, true);

    TestDropSolomon(env.Runtime, ++env.TxId, "/MyRoot/Tenant", "KV");
    env.Env.TestWaitNotification(env.Runtime, env.TxId);
    env.Report(tablet, 101, 1, 30);
    env.CheckUsage(0, false);
}

} // Y_UNIT_TEST_SUITE
