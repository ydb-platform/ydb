#include <ydb/core/base/database_kind.h>
#include <ydb/core/base/database_kind.h_serialized.h>
#include <ydb/core/protos/subdomains.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace {

constexpr ui64 RootSchemeShard = 72057594046678944ull;
constexpr ui64 DedicatedTenantSchemeShard = 72075186224037888ull;
constexpr ui64 ServerlessTenantSchemeShard = 72075186224037889ull;

constexpr ui64 RootPathId = 1;
constexpr ui64 SharedPathId = 10;
constexpr ui64 DedicatedPathId = 11;
constexpr ui64 ServerlessPathId = 12;
constexpr ui64 SubDomainPathId = 13;

void SetKey(NKikimrSubDomains::TDomainKey& key, ui64 schemeShard, ui64 pathId) {
    key.SetSchemeShard(schemeShard);
    key.SetPathId(pathId);
}

// Fills the path itself the same way schemeshard's path describer does.
void SetSelf(NKikimrSchemeOp::TPathDescription& description,
    ui64 schemeShard, ui64 pathId, NKikimrSchemeOp::EPathType pathType)
{
    auto& self = *description.MutableSelf();
    self.SetSchemeshardId(schemeShard);
    self.SetPathId(pathId);
    self.SetPathType(pathType);
    self.SetCreateFinished(true);
}

// DomainDescription always describes the subdomain the path belongs to:
// - the subdomain root key is the path-id under which that subdomain is known to the root schemeshard,
// - the resources subdomain root key is the path-id of the subdomain that provides compute and storage
//   resources for that subdomain.
void SetSubDomain(NKikimrSchemeOp::TPathDescription& description,
    ui64 subdomainRootSchemeShard, ui64 subdomainRootPathId,
    ui64 resourcesSubdomainRootSchemeShard, ui64 resourcesSubdomainRootPathId)
{
    auto& subdomain = *description.MutableDomainDescription();
    SetKey(*subdomain.MutableDomainKey(), subdomainRootSchemeShard, subdomainRootPathId);
    SetKey(*subdomain.MutableResourcesDomainKey(),
        resourcesSubdomainRootSchemeShard, resourcesSubdomainRootPathId);
}

// Marks the subdomain as having a tenant schemeshard of its own.
void SetTenantSchemeShard(NKikimrSchemeOp::TPathDescription& description, ui64 schemeShard) {
    description.MutableDomainDescription()->MutableProcessingParams()->SetSchemeShard(schemeShard);
}

} // namespace

Y_UNIT_TEST_SUITE(DatabaseKind) {

    Y_UNIT_TEST(Root) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, RootSchemeShard, RootPathId, NKikimrSchemeOp::EPathTypeDir);
        SetSubDomain(description, RootSchemeShard, RootPathId, RootSchemeShard, RootPathId);
        SetTenantSchemeShard(description, RootSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Root);
        UNIT_ASSERT(IsDatabase(description));
    }

    Y_UNIT_TEST(SubDomain) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, RootSchemeShard, SubDomainPathId, NKikimrSchemeOp::EPathTypeSubDomain);
        SetSubDomain(description, RootSchemeShard, SubDomainPathId, RootSchemeShard, SubDomainPathId);
        // No tenant schemeshard is set: the subdomain is served by the root schemeshard.

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::SubDomain);
        UNIT_ASSERT(IsDatabase(description));
    }

    // An old-style subdomain that has got a tenant schemeshard of its own is a full-fledged database.
    Y_UNIT_TEST(SubDomainWithTenantSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, RootSchemeShard, SubDomainPathId, NKikimrSchemeOp::EPathTypeSubDomain);
        SetSubDomain(description, RootSchemeShard, SubDomainPathId, RootSchemeShard, SubDomainPathId);
        SetTenantSchemeShard(description, DedicatedTenantSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Dedicated);
    }

    Y_UNIT_TEST(DedicatedByRootSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, RootSchemeShard, DedicatedPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        SetSubDomain(description, RootSchemeShard, DedicatedPathId, RootSchemeShard, DedicatedPathId);
        SetTenantSchemeShard(description, DedicatedTenantSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Dedicated);
        UNIT_ASSERT(IsDatabase(description));
    }

    // A shared database is described exactly as a dedicated one.
    Y_UNIT_TEST(SharedByRootSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, RootSchemeShard, SharedPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        SetSubDomain(description, RootSchemeShard, SharedPathId, RootSchemeShard, SharedPathId);
        SetTenantSchemeShard(description, DedicatedTenantSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Dedicated);
    }

    Y_UNIT_TEST(ServerlessByRootSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, RootSchemeShard, ServerlessPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        SetSubDomain(description, RootSchemeShard, ServerlessPathId, RootSchemeShard, SharedPathId);
        SetTenantSchemeShard(description, ServerlessTenantSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Serverless);
        UNIT_ASSERT(IsDatabase(description));
    }

    // A database described by its own tenant schemeshard: the database is the root path there
    // and it is of the plain subdomain type, while the subdomain root key still points to the path
    // on the root schemeshard.
    Y_UNIT_TEST(DedicatedByTenantSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, DedicatedTenantSchemeShard, RootPathId, NKikimrSchemeOp::EPathTypeSubDomain);
        SetSubDomain(description, RootSchemeShard, DedicatedPathId, RootSchemeShard, DedicatedPathId);
        SetTenantSchemeShard(description, DedicatedTenantSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Dedicated);
        UNIT_ASSERT(IsDatabase(description));
    }

    Y_UNIT_TEST(ServerlessByTenantSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, ServerlessTenantSchemeShard, RootPathId, NKikimrSchemeOp::EPathTypeSubDomain);
        SetSubDomain(description, RootSchemeShard, ServerlessPathId, RootSchemeShard, SharedPathId);
        SetTenantSchemeShard(description, ServerlessTenantSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Serverless);
    }

    // Database creation is not finished yet, the resources subdomain root key is not filled in.
    Y_UNIT_TEST(DedicatedWithoutResourcesDomainKey) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, RootSchemeShard, DedicatedPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        description.MutableSelf()->SetCreateFinished(false);
        auto& subdomain = *description.MutableDomainDescription();
        SetKey(*subdomain.MutableDomainKey(), RootSchemeShard, DedicatedPathId);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Dedicated);
    }

    Y_UNIT_TEST(NotDatabase) {
        // A directory inside the root database.
        NKikimrSchemeOp::TPathDescription dir;
        SetSelf(dir, RootSchemeShard, 42, NKikimrSchemeOp::EPathTypeDir);
        SetSubDomain(dir, RootSchemeShard, RootPathId, RootSchemeShard, RootPathId);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(dir), EDatabaseKind::NotDatabase);
        UNIT_ASSERT(!IsDatabase(dir));

        // A directory inside a dedicated database (the root path id of the tenant
        // schemeshard is taken by the database itself, so 42 here is just a directory).
        NKikimrSchemeOp::TPathDescription tenantDir;
        SetSelf(tenantDir, DedicatedTenantSchemeShard, 42, NKikimrSchemeOp::EPathTypeDir);
        SetSubDomain(tenantDir, RootSchemeShard, DedicatedPathId, RootSchemeShard, DedicatedPathId);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(tenantDir), EDatabaseKind::NotDatabase);

        // A table inside a serverless database.
        NKikimrSchemeOp::TPathDescription table;
        SetSelf(table, ServerlessTenantSchemeShard, 42, NKikimrSchemeOp::EPathTypeTable);
        SetSubDomain(table, RootSchemeShard, ServerlessPathId, RootSchemeShard, SharedPathId);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(table), EDatabaseKind::NotDatabase);

        // A topic right under the root database.
        NKikimrSchemeOp::TPathDescription topic;
        SetSelf(topic, RootSchemeShard, 42, NKikimrSchemeOp::EPathTypePersQueueGroup);
        SetSubDomain(topic, RootSchemeShard, RootPathId, RootSchemeShard, RootPathId);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(topic), EDatabaseKind::NotDatabase);
    }

    Y_UNIT_TEST(EmptyDescription) {
        NKikimrSchemeOp::TPathDescription empty;
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(empty), EDatabaseKind::NotDatabase);
        UNIT_ASSERT(!IsDatabase(empty));

        // No DomainDescription: the root path is indistinguishable from a plain directory.
        NKikimrSchemeOp::TPathDescription noDomain;
        SetSelf(noDomain, RootSchemeShard, RootPathId, NKikimrSchemeOp::EPathTypeDir);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(noDomain), EDatabaseKind::NotDatabase);

        // No DomainDescription: an ext subdomain is a database with a tenant schemeshard of its own
        // by definition, and there is nothing to tell serverless from dedicated by.
        NKikimrSchemeOp::TPathDescription noDomainExtSubDomain;
        SetSelf(noDomainExtSubDomain, RootSchemeShard, ServerlessPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(noDomainExtSubDomain), EDatabaseKind::Dedicated);

        // No DomainDescription: the subdomain path type on its own means the path is a database,
        // but a subdomain with a tenant schemeshard of its own cannot be told from one without it.
        NKikimrSchemeOp::TPathDescription noDomainSubDomain;
        SetSelf(noDomainSubDomain, DedicatedTenantSchemeShard, RootPathId, NKikimrSchemeOp::EPathTypeSubDomain);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(noDomainSubDomain), EDatabaseKind::SubDomain);
        UNIT_ASSERT(IsDatabase(noDomainSubDomain));
    }

    Y_UNIT_TEST(DescribeSchemeResult) {
        NKikimrScheme::TEvDescribeSchemeResult result;
        result.SetStatus(NKikimrScheme::StatusSuccess);
        result.SetPath("/Root/serverless");
        auto& description = *result.MutablePathDescription();
        SetSelf(description, RootSchemeShard, ServerlessPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        SetSubDomain(description, RootSchemeShard, ServerlessPathId, RootSchemeShard, SharedPathId);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(result), EDatabaseKind::Serverless);
        UNIT_ASSERT(IsDatabase(result));

        NKikimrScheme::TEvDescribeSchemeResult notFound;
        notFound.SetStatus(NKikimrScheme::StatusPathDoesNotExist);
        notFound.SetPath("/Root/missing");
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(notFound), EDatabaseKind::NotDatabase);
        UNIT_ASSERT(!IsDatabase(notFound));
    }

    Y_UNIT_TEST(KindToString) {
        UNIT_ASSERT_VALUES_EQUAL(ToString(EDatabaseKind::NotDatabase), "NotDatabase");
        UNIT_ASSERT_VALUES_EQUAL(ToString(EDatabaseKind::Root), "Root");
        UNIT_ASSERT_VALUES_EQUAL(ToString(EDatabaseKind::SubDomain), "SubDomain");
        UNIT_ASSERT_VALUES_EQUAL(ToString(EDatabaseKind::Dedicated), "Dedicated");
        UNIT_ASSERT_VALUES_EQUAL(ToString(EDatabaseKind::Serverless), "Serverless");
    }
}

} // namespace NKikimr
