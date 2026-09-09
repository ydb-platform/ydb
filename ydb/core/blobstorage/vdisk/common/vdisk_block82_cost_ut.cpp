#include "vdisk_context.h"
#include "vdisk_costmodel.h"
#include "vdisk_hugeblobctx.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
Y_UNIT_TEST_SUITE(VDiskBlock82Cost) {
    Y_UNIT_TEST(ImmutableHeaderPolicy) {
        for (const auto species : {TErasureType::Erasure4Plus2Block, TErasureType::Erasure8Plus2Block}) {
            const auto info = MakeIntrusive<TBlobStorageGroupInfo>(species);
            for (bool configured : {false, true}) {
                const auto counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
                const auto ctx = MakeIntrusive<TVDiskContext>(TActorId(), info->PickTopology(), counters,
                    info->GetVDiskId(0), nullptr, NPDisk::DEVICE_TYPE_UNKNOWN, false,
                    nullptr, nullptr, nullptr, nullptr, configured);
                UNIT_ASSERT_VALUES_EQUAL(ctx->EffectiveAddHeader,
                    configured && species == TErasureType::Erasure4Plus2Block);
            }
        }
    }

    Y_UNIT_TEST(HugeThresholdAndWireRoundTrip) {
        for (const auto species : {TErasureType::Erasure4Plus2Block, TErasureType::Erasure8Plus2Block}) {
            const TBlobStorageGroupType type(species);
            for (bool configured : {false, true}) {
                const bool addHeader = configured && type.CanUseLegacyHeader();
                const ui32 overhead = addHeader ? TDiskBlob::HeaderSize : 0;
                const THugeBlobCtx huge(nullptr, addHeader);
                for (const auto crc : {TErasureType::CrcModeNone, TErasureType::CrcModeWholePart}) {
                    for (const ui32 size : {1u, 255u, 256u, 257u, 8192u, 8193u}) {
                        const TLogoBlobID id(1, 1, 1, 0, size, 0, 0, crc);
                        const ui32 payload = type.MaxPartSize(id);
                        for (const ui32 threshold : {0u, 1u, 4u, 5u, payload + overhead,
                                payload + overhead + 1}) {
                            const TCostModel local(1000, 1000000, 1000000, 4096, 4096,
                                threshold, type, configured);
                            UNIT_ASSERT_VALUES_EQUAL(local.MinHugeBlobInBytes,
                                threshold > overhead ? threshold - overhead : 0);
                            NKikimrBlobStorage::TVDiskCostSettings settings;
                            local.FillInSettings(settings);
                            const TCostModel remote(settings, type);
                            UNIT_ASSERT_VALUES_EQUAL(remote.MinHugeBlobInBytes, local.MinHugeBlobInBytes);
                            for (const auto handle : {NKikimrBlobStorage::TabletLog,
                                    NKikimrBlobStorage::AsyncBlob, NKikimrBlobStorage::UserData}) {
                                TEvBlobStorage::TEvVPut put(TLogoBlobID(id, 1), TRope(TString(payload, 'x')),
                                    TVDiskID(0, 1, 0, 0, 0), false, nullptr, TInstant::Max(), handle);
                                bool localLog = false, remoteLog = false;
                                const ui64 localCost = local.GetCost(put, &localLog);
                                UNIT_ASSERT_VALUES_EQUAL(localCost, remote.GetCost(put, &remoteLog));
                                UNIT_ASSERT_VALUES_EQUAL(localLog, remoteLog);
                                UNIT_ASSERT_VALUES_EQUAL(!localLog, huge.IsHugeBlob(type, id, threshold));
                                const TCostModel::TMessageCostEssence essence(put);
                                UNIT_ASSERT_VALUES_EQUAL(local.CalculateCost(essence), localCost);
                                UNIT_ASSERT_VALUES_EQUAL(remote.CalculateCost(essence), localCost);
                            }
                        }
                    }
                }
            }
        }
    }
}
}
