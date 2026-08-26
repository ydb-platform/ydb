#include <ydb/core/base/database_kind.h>
#include <ydb/core/base/database_kind.h_serialized.h>
#include <ydb/core/protos/subdomains.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace {

constexpr ui64 GlobalSchemeShard = 72057594046678944ull;
constexpr ui64 DedicatedSchemeShard = 72075186224037888ull;
constexpr ui64 ServerlessSchemeShard = 72075186224037889ull;

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

// DomainDescription always describes the database the path belongs to:
// domain{SchemeShard, PathId} is the key of that database on the global schemeshard,
// resources{SchemeShard, PathId} is the key of the database that owns its resources.
void SetDomain(NKikimrSchemeOp::TPathDescription& description,
    ui64 domainSchemeShard, ui64 domainPathId,
    ui64 resourcesSchemeShard, ui64 resourcesPathId)
{
    auto& domain = *description.MutableDomainDescription();
    SetKey(*domain.MutableDomainKey(), domainSchemeShard, domainPathId);
    SetKey(*domain.MutableResourcesDomainKey(), resourcesSchemeShard, resourcesPathId);
}

void SetOwnSchemeShard(NKikimrSchemeOp::TPathDescription& description, ui64 schemeShard) {
    description.MutableDomainDescription()->MutableProcessingParams()->SetSchemeShard(schemeShard);
}

} // namespace

Y_UNIT_TEST_SUITE(DatabaseKind) {

    Y_UNIT_TEST(Root) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, GlobalSchemeShard, RootPathId, NKikimrSchemeOp::EPathTypeDir);
        SetDomain(description, GlobalSchemeShard, RootPathId, GlobalSchemeShard, RootPathId);
        SetOwnSchemeShard(description, GlobalSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Root);
        UNIT_ASSERT(IsDatabase(description));
    }

    Y_UNIT_TEST(SubDomain) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, GlobalSchemeShard, SubDomainPathId, NKikimrSchemeOp::EPathTypeSubDomain);
        SetDomain(description, GlobalSchemeShard, SubDomainPathId, GlobalSchemeShard, SubDomainPathId);
        // No own schemeshard is set.

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::SubDomain);
        UNIT_ASSERT(IsDatabase(description));
    }

    // An old-style subdomain that has got a schemeshard of its own is a full-fledged database.
    Y_UNIT_TEST(SubDomainWithOwnSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, GlobalSchemeShard, SubDomainPathId, NKikimrSchemeOp::EPathTypeSubDomain);
        SetDomain(description, GlobalSchemeShard, SubDomainPathId, GlobalSchemeShard, SubDomainPathId);
        SetOwnSchemeShard(description, DedicatedSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Dedicated);
    }

    Y_UNIT_TEST(DedicatedByGlobalSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, GlobalSchemeShard, DedicatedPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        SetDomain(description, GlobalSchemeShard, DedicatedPathId, GlobalSchemeShard, DedicatedPathId);
        SetOwnSchemeShard(description, DedicatedSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Dedicated);
        UNIT_ASSERT(IsDatabase(description));
    }

    // A shared database is described exactly as a dedicated one.
    Y_UNIT_TEST(SharedByGlobalSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, GlobalSchemeShard, SharedPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        SetDomain(description, GlobalSchemeShard, SharedPathId, GlobalSchemeShard, SharedPathId);
        SetOwnSchemeShard(description, DedicatedSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Dedicated);
    }

    Y_UNIT_TEST(ServerlessByGlobalSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, GlobalSchemeShard, ServerlessPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        SetDomain(description, GlobalSchemeShard, ServerlessPathId, GlobalSchemeShard, SharedPathId);
        SetOwnSchemeShard(description, ServerlessSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Serverless);
        UNIT_ASSERT(IsDatabase(description));
    }

    // A database described by its own schemeshard: the database is the root path there
    // and it is of the plain subdomain type, while DomainKey still points to the path
    // on the global schemeshard.
    Y_UNIT_TEST(DedicatedByOwnSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, DedicatedSchemeShard, RootPathId, NKikimrSchemeOp::EPathTypeSubDomain);
        SetDomain(description, GlobalSchemeShard, DedicatedPathId, GlobalSchemeShard, DedicatedPathId);
        SetOwnSchemeShard(description, DedicatedSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Dedicated);
        UNIT_ASSERT(IsDatabase(description));
    }

    Y_UNIT_TEST(ServerlessByOwnSchemeShard) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, ServerlessSchemeShard, RootPathId, NKikimrSchemeOp::EPathTypeSubDomain);
        SetDomain(description, GlobalSchemeShard, ServerlessPathId, GlobalSchemeShard, SharedPathId);
        SetOwnSchemeShard(description, ServerlessSchemeShard);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Serverless);
    }

    // Database creation is not finished yet, the resources key is not filled in.
    Y_UNIT_TEST(DedicatedWithoutResourcesDomainKey) {
        NKikimrSchemeOp::TPathDescription description;
        SetSelf(description, GlobalSchemeShard, DedicatedPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        description.MutableSelf()->SetCreateFinished(false);
        auto& domain = *description.MutableDomainDescription();
        SetKey(*domain.MutableDomainKey(), GlobalSchemeShard, DedicatedPathId);

        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(description), EDatabaseKind::Dedicated);
    }

    Y_UNIT_TEST(NotDatabase) {
        // A directory inside the root database.
        NKikimrSchemeOp::TPathDescription dir;
        SetSelf(dir, GlobalSchemeShard, 42, NKikimrSchemeOp::EPathTypeDir);
        SetDomain(dir, GlobalSchemeShard, RootPathId, GlobalSchemeShard, RootPathId);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(dir), EDatabaseKind::NotDatabase);
        UNIT_ASSERT(!IsDatabase(dir));

        // A directory inside a dedicated database (the root path id of the tenant
        // schemeshard is taken by the database itself, so 42 here is just a directory).
        NKikimrSchemeOp::TPathDescription tenantDir;
        SetSelf(tenantDir, DedicatedSchemeShard, 42, NKikimrSchemeOp::EPathTypeDir);
        SetDomain(tenantDir, GlobalSchemeShard, DedicatedPathId, GlobalSchemeShard, DedicatedPathId);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(tenantDir), EDatabaseKind::NotDatabase);

        // A table inside a serverless database.
        NKikimrSchemeOp::TPathDescription table;
        SetSelf(table, ServerlessSchemeShard, 42, NKikimrSchemeOp::EPathTypeTable);
        SetDomain(table, GlobalSchemeShard, ServerlessPathId, GlobalSchemeShard, SharedPathId);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(table), EDatabaseKind::NotDatabase);

        // A topic right under the root database.
        NKikimrSchemeOp::TPathDescription topic;
        SetSelf(topic, GlobalSchemeShard, 42, NKikimrSchemeOp::EPathTypePersQueueGroup);
        SetDomain(topic, GlobalSchemeShard, RootPathId, GlobalSchemeShard, RootPathId);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(topic), EDatabaseKind::NotDatabase);
    }

    Y_UNIT_TEST(EmptyDescription) {
        NKikimrSchemeOp::TPathDescription empty;
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(empty), EDatabaseKind::NotDatabase);
        UNIT_ASSERT(!IsDatabase(empty));

        // No DomainDescription: the root path is indistinguishable from a plain directory.
        NKikimrSchemeOp::TPathDescription noDomain;
        SetSelf(noDomain, GlobalSchemeShard, RootPathId, NKikimrSchemeOp::EPathTypeDir);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(noDomain), EDatabaseKind::NotDatabase);

        // No DomainDescription: a database with its own schemeshard is reported as dedicated.
        NKikimrSchemeOp::TPathDescription noDomainExtSubDomain;
        SetSelf(noDomainExtSubDomain, GlobalSchemeShard, ServerlessPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(noDomainExtSubDomain), EDatabaseKind::Dedicated);

        // No DomainDescription: the subdomain path type on its own means the path is a database,
        // but a database with a schemeshard of its own cannot be told from a subdomain without one.
        NKikimrSchemeOp::TPathDescription noDomainSubDomain;
        SetSelf(noDomainSubDomain, DedicatedSchemeShard, RootPathId, NKikimrSchemeOp::EPathTypeSubDomain);
        UNIT_ASSERT_VALUES_EQUAL(GetDatabaseKind(noDomainSubDomain), EDatabaseKind::SubDomain);
        UNIT_ASSERT(IsDatabase(noDomainSubDomain));
    }

    Y_UNIT_TEST(DescribeSchemeResult) {
        NKikimrScheme::TEvDescribeSchemeResult result;
        result.SetStatus(NKikimrScheme::StatusSuccess);
        result.SetPath("/Root/serverless");
        auto& description = *result.MutablePathDescription();
        SetSelf(description, GlobalSchemeShard, ServerlessPathId, NKikimrSchemeOp::EPathTypeExtSubDomain);
        SetDomain(description, GlobalSchemeShard, ServerlessPathId, GlobalSchemeShard, SharedPathId);

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
