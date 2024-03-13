#include "validators.h"

#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/blobstorage_disk.pb.h>

#include <library/cpp/testing/unittest/registar.h>

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
