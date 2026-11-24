#include <ydb/core/blobstorage/base/storage_pool_pile_override.h>
#include <library/cpp/testing/gtest/gtest.h>

namespace NKikimr {
namespace {

TEST(StoragePoolPileOverrides, SelectorsAndConflicts) {
    NKikimrConfig::TBlobStorageConfig config;
    auto *rule = config.AddStoragePoolPileOverrides();
    rule->AddPileName("dc2");
    rule->AddPileName("dc3");
    rule->AddKind("ssd");
    rule->AddKind("hdd");
    rule->SetStaticGroupOverride(true);
    rule->SetErasureSpecies("block-4-2");
    rule->MutableGeometry();
    EXPECT_FALSE(ValidateStoragePoolPileOverrides(config));
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc2", "ssd"), rule);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc2", "hdd"), rule);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc2", std::nullopt), rule);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc3", "ssd"), rule);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc3", "hdd"), rule);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc3", std::nullopt), rule);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc1", "ssd"), nullptr);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc2", "other"), nullptr);

    auto *other = config.AddStoragePoolPileOverrides();
    other->CopyFrom(*rule);
    other->ClearKind();
    other->SetStaticGroupOverride(false);
    other->AddKind("other");
    EXPECT_FALSE(ValidateStoragePoolPileOverrides(config));
    other->AddKind("ssd");
    EXPECT_TRUE(ValidateStoragePoolPileOverrides(config));
    other->ClearKind();
    other->SetAllKinds(true);
    EXPECT_TRUE(ValidateStoragePoolPileOverrides(config));
}

TEST(StoragePoolPileOverrides, StaticOnlyAndAllKinds) {
    NKikimrConfig::TBlobStorageConfig config;
    auto *rule = config.AddStoragePoolPileOverrides();
    rule->AddPileName("dc2");
    rule->AddPileName("dc3");
    rule->SetStaticGroupOverride(true);
    rule->MutableGeometry();
    EXPECT_FALSE(ValidateStoragePoolPileOverrides(config));
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc2", "static"), nullptr);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc2", ""), nullptr);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc2", std::nullopt), rule);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc3", std::nullopt), rule);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc3", "ssd"), nullptr);
    rule->SetAllKinds(true);
    EXPECT_FALSE(ValidateStoragePoolPileOverrides(config));
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc2", ""), rule);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc2", "future-kind"), rule);
    EXPECT_EQ(FindStoragePoolPileOverride(config, "dc3", "future-kind"), rule);
}

TEST(StoragePoolPileOverrides, MissingOrDuplicatePiles) {
    NKikimrConfig::TBlobStorageConfig config;
    auto *rule = config.AddStoragePoolPileOverrides();
    rule->SetAllKinds(true);
    rule->MutableGeometry();
    EXPECT_TRUE(ValidateStoragePoolPileOverrides(config));
    rule->AddPileName("dc2");
    EXPECT_FALSE(ValidateStoragePoolPileOverrides(config));
    rule->AddPileName("dc2");
    EXPECT_TRUE(ValidateStoragePoolPileOverrides(config));
}

TEST(StoragePoolPileOverrides, PartiallyOverlappingPileSelectors) {
    NKikimrConfig::TBlobStorageConfig config;
    auto *first = config.AddStoragePoolPileOverrides();
    first->AddPileName("dc1");
    first->AddPileName("dc2");
    first->AddKind("ssd");
    first->MutableGeometry();
    auto *second = config.AddStoragePoolPileOverrides();
    second->AddPileName("dc2");
    second->AddPileName("dc3");
    second->AddKind("hdd");
    second->MutableGeometry();
    EXPECT_FALSE(ValidateStoragePoolPileOverrides(config));
    second->AddKind("ssd");
    EXPECT_TRUE(ValidateStoragePoolPileOverrides(config));
    second->ClearKind();
    second->SetStaticGroupOverride(true);
    EXPECT_FALSE(ValidateStoragePoolPileOverrides(config));
    first->SetStaticGroupOverride(true);
    EXPECT_TRUE(ValidateStoragePoolPileOverrides(config));
    first->SetStaticGroupOverride(false);
    second->SetStaticGroupOverride(false);
    second->SetAllKinds(true);
    EXPECT_TRUE(ValidateStoragePoolPileOverrides(config));
    config.MutableStoragePoolPileOverrides()->SwapElements(0, 1);
    EXPECT_TRUE(ValidateStoragePoolPileOverrides(config));
}

TEST(StoragePoolPileOverrides, ReplaceAndInherit) {
    NKikimrBlobStorage::TDefineStoragePool pool;
    pool.SetName("database:ssd");
    pool.SetNumGroups(5);
    pool.SetErasureSpecies("mirror-3-dc");
    pool.MutableGeometry()->SetNumFailRealms(3);
    pool.MutableGeometry()->SetNumFailDomainsPerFailRealm(3);
    pool.MutableGeometry()->SetNumVDisksPerFailDomain(1);
    pool.AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::SSD);
    NKikimrBlobStorage::TStoragePoolPileOverride rule;
    rule.SetErasureSpecies("block-4-2");
    rule.MutableGeometry();
    ApplyStoragePoolPileOverride(pool, rule);
    EXPECT_EQ(pool.GetErasureSpecies(), "block-4-2");
    EXPECT_EQ(pool.GetGeometry().GetNumFailRealms(), 0);
    EXPECT_EQ(pool.PDiskFilterSize(), 1);
    EXPECT_EQ(pool.GetNumGroups(), 5);
    EXPECT_EQ(pool.GetName(), "database:ssd");
    rule.MutablePDiskFilters()->AddPDiskFilter()->AddProperty()->SetType(NKikimrBlobStorage::NVME);
    ApplyStoragePoolPileOverride(pool, rule);
    EXPECT_EQ(pool.PDiskFilterSize(), 1);
    EXPECT_EQ(pool.GetPDiskFilter(0).GetProperty(0).GetType(), NKikimrBlobStorage::NVME);
    rule.MutablePDiskFilters()->Clear();
    ApplyStoragePoolPileOverride(pool, rule);
    EXPECT_EQ(pool.PDiskFilterSize(), 0);

    NKikimrConfig::TSelfManagementConfig sm;
    sm.SetPDiskType(NKikimrBlobStorage::SSD);
    sm.SetAutomaticStaticGroupManagement(true);
    ApplyStoragePoolPileOverride(sm, rule);
    EXPECT_FALSE(sm.HasPDiskType());
    EXPECT_EQ(sm.GetErasureSpecies(), "block-4-2");
    EXPECT_TRUE(sm.GetAutomaticStaticGroupManagement());
}

TEST(StoragePoolPileOverrides, ErasureRequiresExplicitGeometry) {
    NKikimrConfig::TBlobStorageConfig config;
    auto *rule = config.AddStoragePoolPileOverrides();
    rule->AddPileName("dc2");
    rule->SetAllKinds(true);
    rule->SetErasureSpecies("block-4-2");
    EXPECT_TRUE(ValidateStoragePoolPileOverrides(config));
    rule->MutableGeometry();
    EXPECT_FALSE(ValidateStoragePoolPileOverrides(config));
    rule->SetErasureSpecies("unknown");
    EXPECT_TRUE(ValidateStoragePoolPileOverrides(config));
}

} // namespace
} // namespace NKikimr
