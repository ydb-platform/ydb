#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/mon_helpers.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

namespace {

// GET the DevUI shard-info-by-tablet-id page HTML.
TString GetShardInfoHtml(TTestActorRuntime& runtime, ui64 schemeShard, ui64 tabletId) {
    const TString query = TStringBuilder()
        << "/app?TabletID=" << schemeShard
        << "&Page=ShardInfoByTabletId"
        << "&ShardID=" << tabletId
    ;
    return SendSchemeShardMonRequest(runtime, schemeShard, query, HTTP_METHOD_GET).Body;
}

// Extract the BindedChannels table region of the shard-info page, isolating it from
// the MoveToStoragePool form (whose <select> lists every pool name).
TString GetBindedChannelsRegion(const TString& html) {
    TStringBuf text = html;
    TStringBuf region;
    if (!text.TrySplit("BindedChannels for shard idx", region, text)) {
        return {};
    }
    text.TrySplit("</table>", region, text);
    return TString(region);
}

}  // namespace

Y_UNIT_TEST_SUITE(TSchemeShardMoveTabletToStoragePool) {

    Y_UNIT_TEST_FLAGS(MoveToStoragePool, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot",
            R"(Name: "USER_0")"
        );
        TestAlterExtSubDomain(runtime, ++txId, "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "pool-kind-1"
                    }
                    StoragePools {
                        Name: "pool-2"
                        Kind: "pool-kind-2"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );
        env.TestWaitNotification(runtime, {txId, txId - 1});

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("USER_0"),
            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
        });
        UNIT_ASSERT(tenantSchemeShard != 0 && tenantSchemeShard != (ui64)-1);

        // Create a table in the subdomain so its datashard binds to a storage pool.
        TestCreateTable(runtime, tenantSchemeShard, ++txId, "/MyRoot/USER_0",
            R"(
                Name: "table"
                Columns { Name: "key"   Type: "Uint64" }
                Columns { Name: "value" Type: "Utf8" }
                KeyColumnNames: ["key"]
            )"
        );
        env.TestWaitNotification(runtime, txId, tenantSchemeShard);

        auto tableDesc = DescribePath(runtime, tenantSchemeShard, "/MyRoot/USER_0/table", true);
        const auto& partitions = tableDesc.GetPathDescription().GetTablePartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
        const ui64 datashardId = partitions.Get(0).GetDatashardId();

        // Move the datashard's storage to pool-2.
        {
            auto r = PostMoveToStoragePoolAction(runtime, tenantSchemeShard, datashardId, "pool-2");
            UNIT_ASSERT_C(r.IsJson(), "Expected JSON success reply, got: " << r.Body);
            UNIT_ASSERT_C(r.Body.Contains("OK"), "Expected Hive reply status OK, got: " << r.Body);
        }

        // The SchemeShard-side binding now points every channel at pool-2.
        {
            const TString region = GetBindedChannelsRegion(GetShardInfoHtml(runtime, tenantSchemeShard, datashardId));
            UNIT_ASSERT_C(region.Contains("pool-2"), region);
            UNIT_ASSERT_C(!region.Contains("pool-1"), region);
        }

        // The binding is persisted: it survives a SchemeShard reboot.
        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());
        {
            const TString region = GetBindedChannelsRegion(GetShardInfoHtml(runtime, tenantSchemeShard, datashardId));
            UNIT_ASSERT_C(region.Contains("pool-2"), region);
            UNIT_ASSERT_C(!region.Contains("pool-1"), region);
        }

        // Moving to the pool the shard is already on is a no-op (no Hive round-trip).
        {
            auto r = PostMoveToStoragePoolAction(runtime, tenantSchemeShard, datashardId, "pool-2");
            UNIT_ASSERT_C(!r.IsJson(), "Expected plain no-op reply, got JSON: " << r.Body);
            UNIT_ASSERT_C(r.Body.Contains("already"), r.Body);
        }

        // ...unless the confirmation flag is set: then Hive is re-notified even for the same pool.
        {
            auto r = PostMoveToStoragePoolAction(runtime, tenantSchemeShard, datashardId, "pool-2", /* confirmSamePool */ true);
            UNIT_ASSERT_C(r.IsJson(), "Expected JSON success reply, got: " << r.Body);
            UNIT_ASSERT_C(r.Body.Contains("OK"), "Expected Hive reply status OK, got: " << r.Body);
        }

        // Negative: moving to a pool not registered for the subdomain is rejected.
        {
            auto r = PostMoveToStoragePoolAction(runtime, tenantSchemeShard, datashardId, "no-such-pool");
            UNIT_ASSERT_C(!r.IsJson(), "Expected error reply, got JSON: " << r.Body);
            UNIT_ASSERT_C(r.Body.Contains("not registered"), r.Body);
        }

        // Negative: unknown shard/tablet id is rejected.
        {
            auto r = PostMoveToStoragePoolAction(runtime, tenantSchemeShard, 0, "pool-2");
            UNIT_ASSERT_C(!r.IsJson(), "Expected error reply, got JSON: " << r.Body);
            UNIT_ASSERT_C(r.Body.Contains("Cannot find"), r.Body);
        }
    }

    // A tenant's system tablet (e.g. schemeshard itself or a hive) is registered on the tenant
    // schemeshard with no channel bindings: those live only on the root schemeshard.
    // The move must be refused there and the operator pointed at the root schemeshard.
    Y_UNIT_TEST_FLAGS(SystemTabletOnTenantSchemeShard, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot",
            R"(Name: "USER_0")"
        );
        TestAlterExtSubDomain(runtime, ++txId, "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "pool-kind-1"
                    }
                    StoragePools {
                        Name: "pool-2"
                        Kind: "pool-kind-2"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );
        env.TestWaitNotification(runtime, {txId, txId - 1});

        ui64 tenantSchemeShard = 0;
        auto subdomainDesc = DescribePath(runtime, "/MyRoot/USER_0");
        TestDescribeResult(subdomainDesc, {
            NLs::PathExist,
            NLs::IsExternalSubDomain("USER_0"),
            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
        });
        UNIT_ASSERT(tenantSchemeShard != 0 && tenantSchemeShard != (ui64)-1);

        //NOTE: Right after extsubdomain creation system tablets aren't even registered
        // properly. They would be, but only after tenant schemeshard reboot.

        // Before reboot.
        {
            // The tenant schemeshard doesn't even know about itself as a shard.
            // Both ShardInfo page and MoveToStoragePool action returns error.
            {
                const TString html = GetShardInfoHtml(runtime, tenantSchemeShard, tenantSchemeShard);
                UNIT_ASSERT_C(html.Contains("No shard info for shard ID"), html);
            }
            {
                auto r = PostMoveToStoragePoolAction(runtime, tenantSchemeShard, tenantSchemeShard, "pool-2");
                UNIT_ASSERT_C(!r.IsJson(), "Expected error reply, got JSON: " << r.Body);
                UNIT_ASSERT_C(r.Body.Contains("Cannot find the specified shard"), r.Body);
            }
        }

        RebootTablet(runtime, tenantSchemeShard, runtime.AllocateEdgeActor());

        // After reboot.
        {
            // The tenant schemeshard offers no move form for the schemeshard (will be the same for any system tablet).
            // It explains the shard is a tenant system tablet and links to the root schemeshard's shard-info page.
            {
                const TString html = GetShardInfoHtml(runtime, tenantSchemeShard, tenantSchemeShard);
                UNIT_ASSERT_C(html.Contains("tenant schemeshard"), html);
                UNIT_ASSERT_C(html.Contains(TStringBuilder() << "TabletID=" << ui64(TTestTxConfig::SchemeShard)), html);
                UNIT_ASSERT_C(!html.Contains("Move tablet to storage pool"), html);
            }

            // POST is refused with a message pointing at the root schemeshard.
            {
                auto r = PostMoveToStoragePoolAction(runtime, tenantSchemeShard, tenantSchemeShard, "pool-2");
                UNIT_ASSERT_C(!r.IsJson(), "Expected error reply, got JSON: " << r.Body);
                UNIT_ASSERT_C(r.Body.Contains("no channel bindings"), r.Body);
                UNIT_ASSERT_C(r.Body.Contains("root schemeshard"), r.Body);
            }
        }
    }

    // Root schemeshard can move a tenant's system tablet (e.g. the tenant schemeshard itself)
    // to another storage pool because root maintains the channel bindings for all shards,
    // including tenant system tablets.
    Y_UNIT_TEST_FLAGS(MoveTenantSchemeShard, AlterDatabaseCreateHiveFirst, ExternalHive) {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, TTestEnvOptions().EnableAlterDatabaseCreateHiveFirst(AlterDatabaseCreateHiveFirst));
        ui64 txId = 100;

        TestCreateExtSubDomain(runtime, ++txId, "/MyRoot",
            R"(Name: "USER_0")"
        );
        TestAlterExtSubDomain(runtime, ++txId, "/MyRoot",
            Sprintf(R"(
                    Name: "USER_0"
                    ExternalSchemeShard: true
                    PlanResolution: 50
                    Coordinators: 1
                    Mediators: 1
                    TimeCastBucketsPerMediator: 2
                    StoragePools {
                        Name: "pool-1"
                        Kind: "pool-kind-1"
                    }
                    StoragePools {
                        Name: "pool-2"
                        Kind: "pool-kind-2"
                    }

                    ExternalHive: %s
                )",
                ToString(ExternalHive).c_str()
            )
        );
        env.TestWaitNotification(runtime, {txId, txId - 1});

        ui64 tenantSchemeShard = 0;
        TestDescribeResult(DescribePath(runtime, "/MyRoot/USER_0"), {
            NLs::PathExist,
            NLs::IsExternalSubDomain("USER_0"),
            NLs::ExtractTenantSchemeshard(&tenantSchemeShard),
        });
        UNIT_ASSERT(tenantSchemeShard != 0 && tenantSchemeShard != (ui64)-1);

        // Root schemeshard shows pool-1 for all channels of the tenant schemeshard.
        {
            const TString region = GetBindedChannelsRegion(GetShardInfoHtml(runtime, TTestTxConfig::SchemeShard, tenantSchemeShard));
            UNIT_ASSERT_C(region.Contains("pool-1"), region);
            UNIT_ASSERT_C(!region.Contains("pool-2"), region);
        }

        // From root schemeshard: move the tenant schemeshard to pool-2.
        // Root has bindings for the tenant SS, so the move succeeds.
        {
            auto r = PostMoveToStoragePoolAction(runtime, TTestTxConfig::SchemeShard, tenantSchemeShard, "pool-2");
            UNIT_ASSERT_C(r.IsJson(), "Expected JSON success reply, got: " << r.Body);
            UNIT_ASSERT_C(r.Body.Contains("OK"), "Expected Hive reply status OK, got: " << r.Body);
        }

        // Root schemeshard now shows pool-2 for all channels of the tenant schemeshard.
        {
            const TString region = GetBindedChannelsRegion(GetShardInfoHtml(runtime, TTestTxConfig::SchemeShard, tenantSchemeShard));
            UNIT_ASSERT_C(!region.Contains("pool-1"), region);
            UNIT_ASSERT_C(region.Contains("pool-2"), region);
        }
    }

}
