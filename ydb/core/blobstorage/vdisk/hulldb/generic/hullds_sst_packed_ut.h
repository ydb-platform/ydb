#pragma once

#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>

#include <util/random/fast.h>

// Test data for the packed LogoBlob SST index form
namespace NKikimr::NSstIndexTest {

    namespace {

        using TRec = TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>;

        const TBlobStorageGroupType GType(TErasureType::Erasure4Plus2Block);

        // An SST as compaction writes it: groups of steps per (tablet, channel, generation); inline blobs placed one
        // after another in data chunks, huge blobs in slots, records without data, and a few oddities. A record
        // starts a new group with probability 3 / groupSwitch: about 5 records per group by default, the production
        // average of about 500 at groupSwitch = 1500. Ingresses are random 64-bit values, or drawn from
        // `ingressValues` random ones when that is set (production SSTs see a handful of distinct ingresses).
        TTrackableVector<TRec> MakeIndex(TTestContexts& ctxs, ui32 records, ui32 seed, ui32 groupSwitch = 16,
                ui32 ingressValues = 0) {
            TReallyFastRng32 rng(seed);
            TVector<ui64> ingresses;
            for (ui32 i = 0; i < ingressValues; ++i) {
                ingresses.push_back(ui64(rng()) << 32 | rng());
            }
            TTrackableVector<TRec> index(TMemoryConsumer(ctxs.GetVCtx()->SstIndex));
            ui64 tablet = 72075186224037888ull;
            ui32 channel = 0, gen = 1, step = 1;
            ui32 dataChunk = 10, dataOffset = 0;
            ui32 manyHugeIndex = 0;
            while (index.size() < records) {
                switch (rng() % groupSwitch) {
                    case 0: tablet += 1 + rng() % 1000; channel = rng() % 3; gen = 1 + rng() % 10; step = rng() % 100; break;
                    case 1: ++channel; gen = 1 + rng() % 10; step = rng() % 100; break;
                    case 2: gen += 1 + rng() % 3; step = rng() % 100; break;
                    default: step += rng() % 4; break;
                }
                const ui32 blobSize = rng() % 4 ? 100 + rng() % 4000 : 1 + rng() % (8 << 20);
                const ui32 cookie = rng() % 8 ? 0 : rng() % 1000;
                const ui32 partId = 1 + rng() % 6;
                const TLogoBlobID id(tablet, gen, step, channel, blobSize, cookie, partId);
                if (!index.empty() && !(index.back().GetKey() < TKeyLogoBlob(id))) {
                    continue;
                }

                const ui64 ingress = ingressValues ? ingresses[rng() % ingressValues]
                    : (ui64(rng()) << 32 | rng()) & (rng() % 2 ? 0xFFFF000000000000ull : ~0ull);
                TMemRecLogoBlob memRec{TIngress(ingress)};
                const NMatrix::TVectorType parts = TIngress(ingress).LocalParts(GType);
                const ui32 size = TDiskBlob::CalculateBlobSize(GType, id.FullID(), parts,
                    rng() % 8 ? EBlobHeaderMode::XXH3_64BIT_HEADER : EBlobHeaderMode::OLD_HEADER)
                    + (rng() % 50 ? 0 : 1 + rng() % 100);
                switch (rng() % 10) {
                    case 0: case 1: case 2:
                        break; // no data
                    case 3: case 4: case 5: case 6:
                        if (dataOffset + size > (128 << 20) || rng() % 200 == 0) {
                            ++dataChunk;
                            dataOffset = rng() % 2 ? 0 : 4096;
                        }
                        memRec.SetDiskBlob(TDiskPart(dataChunk, dataOffset, size));
                        dataOffset += AlignUp<ui32>(size, 4);
                        break;
                    case 7: case 8:
                        memRec.SetHugeBlob(TDiskPart(1000 + rng() % 50, (rng() % 300) * (512 << 10), size));
                        break;
                    default:
                        if (rng() % 2) {
                            const ui32 num = 2 + rng() % 3;
                            memRec.SetManyHugeBlobs(manyHugeIndex, num, size);
                            manyHugeIndex += num;
                        } else {
                            memRec.SetMemBlob(ui64(rng()) << 20 | rng(), size);
                        }
                        break;
                }
                index.emplace_back(TKeyLogoBlob(id), memRec);
            }
            return index;
        }

    } // anonymous

} // NKikimr::NSstIndexTest
