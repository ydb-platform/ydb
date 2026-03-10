#include "validators.h"

#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_base.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/table_service_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/xrange.h>

using namespace NKikimr::NConfig;

#define WITH(x) if (x; true)

namespace {

struct TVDiskLoc {
    ui32 NodeID;
    ui32 PDiskID;
    ui32 VDiskSlotID;
    ui64 PDiskGuid;
    ui32 GroupID;
    ui32 RingID;
    ui32 FailDomainID;
};

void Fill(
    TVDiskLoc loc,
    NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet,
    std::map<TPDiskKey, NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk*>& pdisks,
    std::map<TVDiskKey, NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk*>& vdisks,
    NKikimrBlobStorage::TVDiskLocation& out)
{
    out.SetNodeID(loc.NodeID);
    out.SetPDiskID(loc.PDiskID);
    out.SetVDiskSlotID(loc.VDiskSlotID);
    out.SetPDiskGuid(loc.PDiskGuid);

    TPDiskKey pdiskKey{
        .NodeId = loc.NodeID,
        .PDiskId = loc.PDiskID,
    };
    if (auto it = pdisks.find(pdiskKey); it == pdisks.end()) {
        pdisks[pdiskKey] = serviceSet.AddPDisks();
    }
    WITH(auto& pdisk = *pdisks[pdiskKey]) {
        pdisk.SetNodeID(loc.NodeID);
        pdisk.SetPDiskID(loc.PDiskID);
        pdisk.SetPDiskGuid(loc.PDiskGuid);
        pdisk.SetPath(Sprintf("/%d/%d/%lu", loc.NodeID, loc.PDiskID, loc.PDiskGuid));
        pdisk.SetPDiskCategory(5);
    }

    TVDiskKey vdiskKey{
        .NodeId = loc.NodeID,
        .PDiskId = loc.PDiskID,
        .VDiskSlotId = loc.VDiskSlotID,
    };
    if (auto it = vdisks.find(vdiskKey); it == vdisks.end()) {
        vdisks[vdiskKey] = serviceSet.AddVDisks();
    }
    WITH(auto& vdisk = *vdisks[vdiskKey]) {
        vdisk.MutableVDiskLocation()->CopyFrom(out);
        WITH(auto& id = *vdisk.MutableVDiskID()) {
            id.SetRing(loc.RingID);
            id.SetDomain(loc.FailDomainID);
            id.SetGroupID(loc.GroupID);
            id.SetVDisk(0);
        }
    }
}

void FillDefaultServiceSet(NKikimrBlobStorage::TNodeWardenServiceSet& serviceSet) {

    std::map<TPDiskKey, NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk*> pdisks;
    std::map<TVDiskKey, NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk*> vdisks;

    WITH(auto& group = *serviceSet.AddGroups()) {
        ui32 groupID = 0;
        WITH(auto& ring = *group.AddRings()) {
            ui32 ringID = 0;
            WITH(auto& failDomain = *ring.AddFailDomains()) {
                Y_UNUSED(failDomain);
            }

            WITH(auto& failDomain = *ring.AddFailDomains()) {
                ui32 failDomainID = 1;
                WITH(auto& vdiskLoc = *failDomain.AddVDiskLocations()) {
                    Fill(TVDiskLoc{
                            .NodeID = 1,
                            .PDiskID = 1,
                            .VDiskSlotID = 1,
                            .PDiskGuid = 1001,
                            .GroupID = groupID,
                            .RingID = ringID,
                            .FailDomainID = failDomainID,
                        },
                        serviceSet,
                        pdisks,
                        vdisks,
                        vdiskLoc);
                }
            }
        }
    }

    WITH(auto& group = *serviceSet.AddGroups()) {
        ui32 groupID = 1;
        WITH(auto& ring = *group.AddRings()) {
            Y_UNUSED(ring);
        }
        WITH(auto& ring = *group.AddRings()) {
            WITH(auto& failDomain = *ring.AddFailDomains()) {
                Y_UNUSED(failDomain);
            }
        }
        WITH(auto& ring = *group.AddRings()) {
            ui32 ringID = 2;
            WITH(auto& failDomain = *ring.AddFailDomains()) {
                Y_UNUSED(failDomain);
            }
            WITH(auto& failDomain = *ring.AddFailDomains()) {
                ui32 failDomainID = 1;
                WITH(auto& vdiskLoc = *failDomain.AddVDiskLocations()) {
                    Fill(TVDiskLoc{
                            .NodeID = 1,
                            .PDiskID = 1,
                            .VDiskSlotID = 2,
                            .PDiskGuid = 1001,
                            .GroupID = groupID,
                            .RingID = ringID,
                            .FailDomainID = failDomainID,
                        },
                        serviceSet,
                        pdisks,
                        vdisks,
                        vdiskLoc);
                }
                WITH(auto& vdiskLoc = *failDomain.AddVDiskLocations()) {
                    Fill(TVDiskLoc{
                            .NodeID = 2,
                            .PDiskID = 1,
                            .VDiskSlotID = 3,
                            .PDiskGuid = 2001,
                            .GroupID = groupID,
                            .RingID = ringID,
                            .FailDomainID = failDomainID,
                        },
                        serviceSet,
                        pdisks,
                        vdisks,
                        vdiskLoc);
                }
                WITH(auto& vdiskLoc = *failDomain.AddVDiskLocations()) {
                    Fill(TVDiskLoc{
                            .NodeID = 3,
                            .PDiskID = 2,
                            .VDiskSlotID = 4,
                            .PDiskGuid = 3001,
                            .GroupID = groupID,
                            .RingID = ringID,
                            .FailDomainID = failDomainID,
                        },
                        serviceSet,
                        pdisks,
                        vdisks,
                        vdiskLoc);
                }
                WITH(auto& vdiskLoc = *failDomain.AddVDiskLocations()) {
                    Fill(TVDiskLoc{
                            .NodeID = 4,
                            .PDiskID = 4,
                            .VDiskSlotID = 5,
                            .PDiskGuid = 4004,
                            .GroupID = groupID,
                            .RingID = ringID,
                            .FailDomainID = failDomainID,
                        },
                        serviceSet,
                        pdisks,
                        vdisks,
                        vdiskLoc);
                }
            }
            WITH(auto& failDomain = *ring.AddFailDomains()) {
                Y_UNUSED(failDomain);
            }
        }
    }
}

std::pair<NKikimrConfig::TAppConfig, NKikimrConfig::TAppConfig> PrepareStaticStorageTest() {
    NKikimrConfig::TAppConfig cur;
    NKikimrConfig::TAppConfig proposed;
    auto& curServiceSet = *cur.MutableBlobStorageConfig()->MutableServiceSet();
    auto& proposedServiceSet = *proposed.MutableBlobStorageConfig()->MutableServiceSet();

    FillDefaultServiceSet(curServiceSet);
    FillDefaultServiceSet(proposedServiceSet);

    return {cur, proposed};
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(ConfigValidation) {
    Y_UNIT_TEST(SameStaticGroup) {
        auto [cur, proposed] = PrepareStaticStorageTest();

        std::vector<TString> err;
        auto res = ValidateStaticGroup(cur, proposed, err);
        UNIT_ASSERT_VALUES_EQUAL(err.size(), 0);
        UNIT_ASSERT_EQUAL(res, EValidationResult::Ok);
    }

    Y_UNIT_TEST(StaticGroupSizesGrow) {
        {
            auto [cur, proposed] = PrepareStaticStorageTest();

            WITH(auto& groups = *proposed.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups()) {
                groups.erase(--groups.end());
            }

            std::vector<TString> err;
            auto res = ValidateStaticGroup(cur, proposed, err);
            UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(err[0], "Group either added or removed");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Warn);
        }

        {
            auto [cur, proposed] = PrepareStaticStorageTest();

            WITH(auto& group = *proposed.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(1)) {
                WITH(auto& rings = *group.MutableRings()) {
                    rings.erase(++rings.begin());
                }
            }

            std::vector<TString> err;
            auto res = ValidateStaticGroup(cur, proposed, err);
            UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(err[0], "Ring sizes must be the same");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Error);

        }

        {
            auto [cur, proposed] = PrepareStaticStorageTest();

            WITH(auto& group = *proposed.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(1)) {
                WITH(auto& ring = *group.MutableRings(2)) {
                    WITH(auto& failDomains = *ring.MutableFailDomains()) {
                        failDomains.erase(failDomains.begin());
                    }
                }
            }

            std::vector<TString> err;
            auto res = ValidateStaticGroup(cur, proposed, err);
            UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(err[0], "FailDomain sizes must be the same");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Error);

        }

        {
            auto [cur, proposed] = PrepareStaticStorageTest();

            WITH(auto& group = *proposed.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(1)) {
                WITH(auto& ring = *group.MutableRings(2)) {
                    WITH(auto& failDomain = *ring.MutableFailDomains(1)) {
                        WITH(auto& vdiskLocs = *failDomain.MutableVDiskLocations()) {
                            vdiskLocs.erase(++++vdiskLocs.begin());
                        }
                    }
                }
            }

            std::vector<TString> err;
            auto res = ValidateStaticGroup(cur, proposed, err);
            UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(err[0], "VDiskLocation sizes must be the same");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
        }
    }

    Y_UNIT_TEST(StaticGroupSizesShrink) {
        {
            auto [cur, proposed] = PrepareStaticStorageTest();

            WITH(auto& groups = *cur.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups()) {
                groups.erase(--groups.end());
            }

            std::vector<TString> err;
            auto res = ValidateStaticGroup(cur, proposed, err);
            UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(err[0], "Group either added or removed");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Warn);
        }

        {
            auto [cur, proposed] = PrepareStaticStorageTest();

            WITH(auto& group = *cur.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(1)) {
                WITH(auto& rings = *group.MutableRings()) {
                    rings.erase(++rings.begin());
                }
            }

            std::vector<TString> err;
            auto res = ValidateStaticGroup(cur, proposed, err);
            UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(err[0], "Ring sizes must be the same");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Error);

        }

        {
            auto [cur, proposed] = PrepareStaticStorageTest();

            WITH(auto& group = *cur.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(1)) {
                WITH(auto& ring = *group.MutableRings(2)) {
                    WITH(auto& failDomains = *ring.MutableFailDomains()) {
                        failDomains.erase(failDomains.begin());
                    }
                }
            }

            std::vector<TString> err;
            auto res = ValidateStaticGroup(cur, proposed, err);
            UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(err[0], "FailDomain sizes must be the same");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Error);

        }

        {
            auto [cur, proposed] = PrepareStaticStorageTest();

            WITH(auto& group = *cur.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(1)) {
                WITH(auto& ring = *group.MutableRings(2)) {
                    WITH(auto& failDomain = *ring.MutableFailDomains(1)) {
                        WITH(auto& vdiskLocs = *failDomain.MutableVDiskLocations()) {
                            vdiskLocs.erase(++++vdiskLocs.begin());
                        }
                    }
                }
            }

            std::vector<TString> err;
            auto res = ValidateStaticGroup(cur, proposed, err);
            UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(err[0], "VDiskLocation sizes must be the same");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
        }
    }

    Y_UNIT_TEST(VDiskChanged) {
        auto [cur, proposed] = PrepareStaticStorageTest();

        WITH(auto& group = *cur.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(1)) {
            WITH(auto& ring = *group.MutableRings(2)) {
                WITH(auto& failDomain = *ring.MutableFailDomains(1)) {
                    WITH(auto& vdiskLoc = *failDomain.MutableVDiskLocations(1)) {
                        vdiskLoc.SetNodeID(33); // replaced one disk
                        vdiskLoc.SetPDiskID(2);
                        vdiskLoc.SetVDiskSlotID(33);
                        vdiskLoc.SetPDiskGuid(33001);
                    }
                }
            }
        }

        std::vector<TString> err;
        auto res = ValidateStaticGroup(cur, proposed, err);
        UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(err[0], "VDiskLocation changed");
        UNIT_ASSERT_EQUAL(res, EValidationResult::Warn);
    }

    Y_UNIT_TEST(TooManyVDiskChanged) {
        auto [cur, proposed] = PrepareStaticStorageTest();

        WITH(auto& group = *cur.MutableBlobStorageConfig()->MutableServiceSet()->MutableGroups(1)) {
            WITH(auto& ring = *group.MutableRings(2)) {
                WITH(auto& failDomain = *ring.MutableFailDomains(1)) {
                    WITH(auto& vdiskLoc = *failDomain.MutableVDiskLocations(0)) {
                        vdiskLoc.SetNodeID(36); // replaced one disk
                        vdiskLoc.SetPDiskID(3);
                        vdiskLoc.SetVDiskSlotID(36);
                        vdiskLoc.SetPDiskGuid(36001);
                    }
                    WITH(auto& vdiskLoc = *failDomain.MutableVDiskLocations(1)) {
                        vdiskLoc.SetNodeID(33); // replaced one disk
                        vdiskLoc.SetPDiskID(2);
                        vdiskLoc.SetVDiskSlotID(33);
                        vdiskLoc.SetPDiskGuid(33001);
                    }
                }
            }
        }

        std::vector<TString> err;
        auto res = ValidateStaticGroup(cur, proposed, err);
        UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(err[0], "Too many VDiskLocation changes");
        UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
    }
}

Y_UNIT_TEST_SUITE(DatabaseConfigValidation) {
    Y_UNIT_TEST(AllowedFields) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnablePgSyntax(true);
        config.MutableTableServiceConfig()->SetEnableStreamWrite(true);

        std::vector<TString> err;
        auto res = ValidateDatabaseConfig(config, err);
        UNIT_ASSERT_VALUES_EQUAL(err.size(), 0);
        UNIT_ASSERT_EQUAL(res, EValidationResult::Ok);
    }

    Y_UNIT_TEST(NotAllowedFields) {
        auto [config, _] = PrepareStaticStorageTest();

        std::vector<TString> err;
        auto res = ValidateDatabaseConfig(config, err);
        UNIT_ASSERT_VALUES_EQUAL(err.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(err[0], "'blob_storage_config' is not allowed to be used in the database configuration");
        UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
    }
}

Y_UNIT_TEST_SUITE(StateStorageConfigValidation) {

    void FillRing(NKikimrConfig::TDomainsConfig::TStateStorage::TRing* ring, ui32 ringsCnt = 8) {
        ring->SetNToSelect(5);
        ui32 nodeId = 0;
        for(ui32 _ : xrange(ringsCnt)) {
            auto* r = ring->AddRing();
            for (ui32 _ : xrange(5, 13)) {
                r->AddNode(nodeId++);
            }
        }
    }

    Y_UNIT_TEST(Empty) {
        auto res = ValidateStateStorageConfig("StateStorage", {}, {});
        UNIT_ASSERT_EQUAL(res, "New StateStorage configuration is not filled in");
    }

    Y_UNIT_TEST(Good) {
        NKikimrConfig::TDomainsConfig::TStateStorage proposed;
        FillRing(proposed.MutableRing());
        auto res = ValidateStateStorageConfig("StateStorage", {}, proposed);
        UNIT_ASSERT(res.empty());
    }

    Y_UNIT_TEST(NToSelect) {
        NKikimrConfig::TDomainsConfig::TStateStorage proposed;
        FillRing(proposed.MutableRing());
        proposed.MutableRing()->SetNToSelect(0);
        auto res = ValidateStateStorageConfig("StateStorage", {}, proposed);
        UNIT_ASSERT_EQUAL(res, "New StateStorage configuration NToSelect has invalid value");
        proposed.MutableRing()->SetNToSelect(10);
        res = ValidateStateStorageConfig("StateStorage", {}, proposed);
        UNIT_ASSERT_EQUAL(res, "New StateStorage configuration NToSelect has invalid value");
    }

    Y_UNIT_TEST(WriteOnly) {
        NKikimrConfig::TDomainsConfig::TStateStorage proposed;
        FillRing(proposed.AddRingGroups());
        FillRing(proposed.AddRingGroups());
        proposed.MutableRingGroups(0)->SetWriteOnly(true);
        auto res = ValidateStateStorageConfig("StateStorage", {}, proposed);
        UNIT_ASSERT_EQUAL(res, "New StateStorage configuration first ring group is WriteOnly");
    }

    Y_UNIT_TEST(Disabled) {
        NKikimrConfig::TDomainsConfig::TStateStorage proposed;
        FillRing(proposed.MutableRing(), 5);
        proposed.MutableRing()->MutableRing(0)->SetIsDisabled(true);
        proposed.MutableRing()->MutableRing(1)->SetIsDisabled(true);
        auto res = ValidateStateStorageConfig("StateStorage", {}, proposed);
        UNIT_ASSERT_EQUAL(res, "New StateStorage configuration disabled too many rings");
    }

    Y_UNIT_TEST(DisabledGood) {
        NKikimrConfig::TDomainsConfig::TStateStorage proposed;
        FillRing(proposed.MutableRing());
        proposed.MutableRing()->MutableRing(0)->SetIsDisabled(true);
        auto res = ValidateStateStorageConfig("StateStorage", {}, proposed);
        UNIT_ASSERT(res.empty());
    }

    Y_UNIT_TEST(CanDisableAndChange) {
        NKikimrConfig::TDomainsConfig::TStateStorage cur;
        FillRing(cur.MutableRing());
        NKikimrConfig::TDomainsConfig::TStateStorage proposed;
        FillRing(proposed.MutableRing());
        proposed.MutableRing()->MutableRing(0)->SetIsDisabled(true);
        proposed.MutableRing()->MutableRing(0)->AddNode(100);
        auto res = ValidateStateStorageConfig("StateStorage", cur, proposed);
        UNIT_ASSERT(res.empty());
    }

    Y_UNIT_TEST(CanChangeDisabled) {
        NKikimrConfig::TDomainsConfig::TStateStorage cur;
        FillRing(cur.MutableRing());
        cur.MutableRing()->MutableRing(0)->SetIsDisabled(true);
        NKikimrConfig::TDomainsConfig::TStateStorage proposed;
        FillRing(proposed.MutableRing());
        proposed.MutableRing()->MutableRing(0)->AddNode(100);
        auto res = ValidateStateStorageConfig("StateStorage", cur, proposed);
        UNIT_ASSERT(res.empty());
    }

    Y_UNIT_TEST(ChangesNotAllowed) {
        NKikimrConfig::TDomainsConfig::TStateStorage cur;
        FillRing(cur.MutableRing());
        NKikimrConfig::TDomainsConfig::TStateStorage proposed;
        FillRing(proposed.MutableRing());
        proposed.MutableRing()->MutableRing(0)->AddNode(100);
        auto res = ValidateStateStorageConfig("StateStorage", cur, proposed);
        UNIT_ASSERT(res.StartsWith("StateStorage ring #0differs from# Ring"));
    }

    Y_UNIT_TEST(ValidateConfigSelfManagement) {
        NKikimrConfig::TAppConfig proposed;
        proposed.MutableSelfManagementConfig()->SetEnabled(true);
        std::vector<TString> err;
        auto res = ValidateConfig(proposed, err);
        UNIT_ASSERT_EQUAL(err.size(), 0);
        UNIT_ASSERT_EQUAL(res, EValidationResult::Ok);
    }

    Y_UNIT_TEST(ValidateConfigDomainEmpty) {
        NKikimrConfig::TAppConfig proposed;
        auto* domains = proposed.MutableDomainsConfig();
        domains->AddStateStorage();
        std::vector<TString> err;
        auto res = ValidateConfig(proposed, err);
        UNIT_ASSERT_EQUAL(err.size(), 1);
        UNIT_ASSERT_EQUAL(err[0], "Domains is not defined in DomainsConfig");
        UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
    }

    Y_UNIT_TEST(ValidateConfigSSId) {
        NKikimrConfig::TAppConfig proposed;
        auto* domains = proposed.MutableDomainsConfig();
        domains->AddDomain()->AddSSId(10);
        auto* ss = domains->AddStateStorage();
        ss->SetSSId(1);
        std::vector<TString> err;
        auto res = ValidateConfig(proposed, err);
        UNIT_ASSERT_EQUAL(err.size(), 1);
        UNIT_ASSERT_EQUAL(err[0], "State storage config is not defined in DomainsConfig section");
        UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
    }

    Y_UNIT_TEST(ValidateConfigBad) {
        NKikimrConfig::TAppConfig proposed;
        auto* domains = proposed.MutableDomainsConfig();
        domains->AddDomain()->AddSSId(1);
        auto* ss = domains->AddStateStorage();
        ss->SetSSId(1);
        std::vector<TString> err;
        auto res = ValidateConfig(proposed, err);
        UNIT_ASSERT_EQUAL(err.size(), 1);
        UNIT_ASSERT_EQUAL(err[0], "New StateStorage configuration is not filled in");
        UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
    }

    Y_UNIT_TEST(ValidateConfigValidatesStateStorage) {
        NKikimrConfig::TAppConfig proposed;
        auto* domains = proposed.MutableDomainsConfig();
        domains->AddDomain()->AddSSId(1);
        auto* ss = domains->AddStateStorage();
        ss->SetSSId(1);
        FillRing(ss->MutableRing());
        ss->MutableRing()->SetNToSelect(10);
        std::vector<TString> err;
        auto res = ValidateConfig(proposed, err);
        UNIT_ASSERT_EQUAL(err.size(), 1);
        UNIT_ASSERT_EQUAL(err[0], "New StateStorage configuration NToSelect has invalid value");
        UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
    }

    Y_UNIT_TEST(ValidateConfigGood) {
        NKikimrConfig::TAppConfig proposed;
        auto* domains = proposed.MutableDomainsConfig();
        domains->AddDomain()->AddSSId(1);
        auto* ss = domains->AddStateStorage();
        ss->SetSSId(1);
        FillRing(ss->MutableRing());
        std::vector<TString> err;
        auto res = ValidateConfig(proposed, err);
        UNIT_ASSERT_VALUES_EQUAL(err.size(), 0);
        UNIT_ASSERT_EQUAL(res, EValidationResult::Ok);
    }

    Y_UNIT_TEST(ValidateConfigExplicitGood) {
        NKikimrConfig::TAppConfig proposed;
        auto* domains = proposed.MutableDomainsConfig();
        FillRing(domains->MutableExplicitStateStorageConfig()->MutableRing());
        FillRing(domains->MutableExplicitStateStorageBoardConfig()->MutableRing());
        FillRing(domains->MutableExplicitSchemeBoardConfig()->MutableRing());
        std::vector<TString> err;
        auto res = ValidateConfig(proposed, err);
        UNIT_ASSERT_VALUES_EQUAL(err.size(), 0);
        UNIT_ASSERT_EQUAL(res, EValidationResult::Ok);
    }

    Y_UNIT_TEST(ValidateConfigExplicitBad) {
        NKikimrConfig::TAppConfig proposed;
        auto* domains = proposed.MutableDomainsConfig();
        FillRing(domains->MutableExplicitStateStorageConfig()->MutableRing());
        FillRing(domains->MutableExplicitSchemeBoardConfig()->MutableRing());
        std::vector<TString> err;
        auto res = ValidateConfig(proposed, err);
        UNIT_ASSERT_EQUAL(err.size(), 1);
        UNIT_ASSERT_EQUAL(err[0], "Domains is not defined in DomainsConfig");
        UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
    }
}

Y_UNIT_TEST_SUITE(MonitoringConfigValidation) {
    Y_UNIT_TEST(RequireCountersAuthentication) {
        { // Without security config
            NKikimrConfig::TAppConfig config;
            config.MutableMonitoringConfig()->SetRequireCountersAuthentication(true);
            std::vector<TString> msg;
            auto res = ValidateMonitoringConfig(config, msg);
            UNIT_ASSERT_VALUES_EQUAL(msg.size(), 1);
            UNIT_ASSERT_EQUAL(msg[0], "Setting EnforceUserTokenRequirement is disabled, but RequireCountersAuthentication is enabled");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
        }
        { // With EnforceUserTokenRequirement disabled
            NKikimrConfig::TAppConfig config;
            config.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(false);
            config.MutableMonitoringConfig()->SetRequireCountersAuthentication(true);
            std::vector<TString> msg;
            auto res = ValidateMonitoringConfig(config, msg);
            UNIT_ASSERT_VALUES_EQUAL(msg.size(), 1);
            UNIT_ASSERT_EQUAL(msg[0], "Setting EnforceUserTokenRequirement is disabled, but RequireCountersAuthentication is enabled");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
        }
        { // With EnforceUserTokenRequirement enabled
            NKikimrConfig::TAppConfig config;
            config.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
            config.MutableMonitoringConfig()->SetRequireCountersAuthentication(true);
            std::vector<TString> msg;
            auto res = ValidateMonitoringConfig(config, msg);
            UNIT_ASSERT_VALUES_EQUAL(msg.size(), 0);
            UNIT_ASSERT_EQUAL(res, EValidationResult::Ok);
        }
    }

    Y_UNIT_TEST(RequireHealthcheckAuthentication) {
        { // Without security config
            NKikimrConfig::TAppConfig config;
            config.MutableMonitoringConfig()->SetRequireHealthcheckAuthentication(true);
            std::vector<TString> msg;
            auto res = ValidateMonitoringConfig(config, msg);
            UNIT_ASSERT_VALUES_EQUAL(msg.size(), 1);
            UNIT_ASSERT_EQUAL(msg[0], "Setting EnforceUserTokenRequirement is disabled, but RequireHealthcheckAuthentication is enabled");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
        }
        { // With EnforceUserTokenRequirement disabled
            NKikimrConfig::TAppConfig config;
            config.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(false);
            config.MutableMonitoringConfig()->SetRequireHealthcheckAuthentication(true);
            std::vector<TString> msg;
            auto res = ValidateMonitoringConfig(config, msg);
            UNIT_ASSERT_VALUES_EQUAL(msg.size(), 1);
            UNIT_ASSERT_EQUAL(msg[0], "Setting EnforceUserTokenRequirement is disabled, but RequireHealthcheckAuthentication is enabled");
            UNIT_ASSERT_EQUAL(res, EValidationResult::Error);
        }
        { // With EnforceUserTokenRequirement enabled
            NKikimrConfig::TAppConfig config;
            config.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
            config.MutableMonitoringConfig()->SetRequireHealthcheckAuthentication(true);
            std::vector<TString> msg;
            auto res = ValidateMonitoringConfig(config, msg);
            UNIT_ASSERT_VALUES_EQUAL(msg.size(), 0);
            UNIT_ASSERT_EQUAL(res, EValidationResult::Ok);
        }
    }
}
