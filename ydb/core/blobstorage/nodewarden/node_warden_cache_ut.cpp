#include "node_warden.h"
#include "node_warden_impl.h"

#include <ydb/core/protos/blobstorage_base.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NStorage {
namespace {

    using TServiceSet = NKikimrBlobStorage::TNodeWardenServiceSet;

    TServiceSet::TPDisk& AddPDisk(TServiceSet& serviceSet, ui32 nodeId, ui32 pdiskId,
            NKikimrBlobStorage::EEntityStatus status = NKikimrBlobStorage::EEntityStatus::CREATE) {
        auto *pdisk = serviceSet.AddPDisks();
        pdisk->SetNodeID(nodeId);
        pdisk->SetPDiskID(pdiskId);
        pdisk->SetEntityStatus(status);
        return *pdisk;
    }

    TServiceSet::TVDisk& AddVDisk(TServiceSet& serviceSet, ui32 nodeId, ui32 pdiskId, ui32 vslotId,
            TString storagePoolName,
            NKikimrBlobStorage::EEntityStatus status = NKikimrBlobStorage::EEntityStatus::CREATE,
            bool doDestroy = false) {
        auto *vdisk = serviceSet.AddVDisks();
        auto *location = vdisk->MutableVDiskLocation();
        location->SetNodeID(nodeId);
        location->SetPDiskID(pdiskId);
        location->SetVDiskSlotID(vslotId);
        vdisk->SetStoragePoolName(std::move(storagePoolName));
        vdisk->SetEntityStatus(status);
        vdisk->SetDoDestroy(doDestroy);
        return *vdisk;
    }

    void UpdateCache(TServiceSet& cache, const TServiceSet& update, bool comprehensive) {
        auto config = MakeIntrusive<TNodeWardenConfig>(TIntrusivePtr<IPDiskServiceFactory>{});
        TNodeWarden nodeWarden(config);
        nodeWarden.UpdateServiceSet(update, comprehensive, [] {})(&cache)();
    }

    Y_UNIT_TEST_SUITE(TNodeWardenCacheTest) {
        Y_UNIT_TEST(IncrementalUpdateReplacesExistingVDisk) {
            TServiceSet cache;
            TServiceSet initial;
            AddVDisk(initial, 1, 10, 100, "old-pool");
            UpdateCache(cache, initial, true);

            TServiceSet update;
            AddVDisk(update, 1, 10, 100, "new-pool", NKikimrBlobStorage::EEntityStatus::RESTART);
            UpdateCache(cache, update, false);

            UNIT_ASSERT_VALUES_EQUAL(cache.VDisksSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(cache.GetVDisks(0).GetStoragePoolName(), "new-pool");
            UNIT_ASSERT(cache.GetVDisks(0).GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::RESTART);
        }

        Y_UNIT_TEST(IncrementalUpdateRemovesVDisk) {
            TServiceSet initial;
            AddVDisk(initial, 1, 10, 100, "old-pool");
            AddVDisk(initial, 1, 10, 101, "old-pool");

            TServiceSet cache;
            UpdateCache(cache, initial, true);

            TServiceSet update;
            AddVDisk(update, 1, 10, 100, {}, NKikimrBlobStorage::EEntityStatus::DESTROY);
            AddVDisk(update, 1, 10, 101, {}, NKikimrBlobStorage::EEntityStatus::INITIAL, true);
            UpdateCache(cache, update, false);

            UNIT_ASSERT_VALUES_EQUAL(cache.VDisksSize(), 0);
        }

        Y_UNIT_TEST(IncrementalUpdateRemovesVDiskWithDestroyedPDisk) {
            TServiceSet initial;
            AddPDisk(initial, 1, 10);
            AddVDisk(initial, 1, 10, 100, "old-pool");

            TServiceSet cache;
            UpdateCache(cache, initial, true);

            TServiceSet update;
            AddPDisk(update, 1, 10, NKikimrBlobStorage::EEntityStatus::DESTROY);
            AddVDisk(update, 1, 10, 100, {}, NKikimrBlobStorage::EEntityStatus::DESTROY);
            UpdateCache(cache, update, false);

            UNIT_ASSERT_VALUES_EQUAL(cache.PDisksSize(), 0);
            UNIT_ASSERT_VALUES_EQUAL(cache.VDisksSize(), 0);
        }
    }

} // namespace
} // namespace NKikimr::NStorage
