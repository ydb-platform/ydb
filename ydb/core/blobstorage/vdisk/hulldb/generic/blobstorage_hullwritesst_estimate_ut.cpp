#include "blobstorage_hullwritesst.h"
#include "blobstorage_hullrecmerger.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_output_estimate.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>

namespace NKikimr {

    // TFreshOutputEstimate claims to bound what the Fresh compaction writer emits, but it is a hand
    // derivation of TWriter's packing rules and can drift from them. This pins it against the real
    // writer: a random Fresh segment is charged record by record, then merged and written exactly as
    // Fresh compaction does, and the chunks the writer actually used must never exceed the estimate.
    Y_UNIT_TEST_SUITE(TFreshOutputEstimateVsWriter) {

        using TWriter = TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>::TWriter;
        using TMerger = TCompactRecordMerger<TKeyLogoBlob, TMemRecLogoBlob>;

        struct TConfig {
            ui32 ChunkSize = 1 << 20;
            ui32 AppendBlockSize = 4 << 10;
            ui32 WriteBlockSize = 16 << 10;
            ui32 ChunksPerSst = 1;
            // Upper limit on a blob's size; its parts, which is what Fresh stores, are smaller.
            ui32 MaxBlobSize = 64 << 10;
            ui32 Keys = 20000;
            // Shares of the keys whose parts are inline, huge, or index-only; the rest is inline.
            ui32 HugePercent = 20;
            ui32 IndexOnlyPercent = 20;
            // Keep every part this disk may hold, rather than a random subset: the largest merged
            // records are the ones that waste the most at an SST boundary.
            bool AllParts = false;
            ui64 Seed = 1;
        };

        struct TResult {
            ui64 Estimated = 0;
            ui64 Actual = 0;
            ui64 Ssts = 0;
            ui64 Charge = 0;
        };

        // One Fresh record as TFreshIndexAndData::PutLogoBlobWithData() and Put() would store it.
        struct TFreshRecord {
            TMemRecLogoBlob MemRec;
            TRope Data; // inline blobs only
        };

        TResult Run(const TConfig& cfg) {
            TTestContexts ctx(cfg.ChunkSize);
            const TVDiskContextPtr vctx = ctx.GetVCtx();
            const TBlobStorageGroupType gtype = vctx->Top->GType;
            TRopeArena arena(&TRopeArenaBackend::Allocate);
            TReallyFastRng32 rng(cfg.Seed);

            const TFreshOutputGeometry geometry{
                .ChunkSize = cfg.ChunkSize,
                .AppendBlockSize = cfg.AppendBlockSize,
                .ChunksPerSst = cfg.ChunksPerSst,
                .TotalPartCount = gtype.TotalPartCount(),
                .IndexRecordBytes = sizeof(TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>),
            };
            TFreshOutputEstimate estimate;

            TDeque<TChunkIdx> reservedChunks;
            for (TChunkIdx chunkIdx = 1; chunkIdx < 100000; ++chunkIdx) {
                reservedChunks.push_back(chunkIdx);
            }

            TResult result;
            auto makeWriter = [&] {
                return std::make_unique<TWriter>(vctx, EWriterDataType::Fresh, cfg.ChunksPerSst, 1, 1,
                    cfg.ChunkSize, cfg.AppendBlockSize, cfg.WriteBlockSize, result.Ssts, false, reservedChunks, arena,
                    EBlobHeaderMode::OLD_HEADER);
            };
            auto drain = [](TWriter& writer) {
                while (writer.GetPendingMessage()) {
                }
            };
            auto finishSst = [&](std::unique_ptr<TWriter>& writer) {
                while (!writer->FlushNext(0, Max<ui64>(), 1)) {
                    drain(*writer);
                }
                drain(*writer);
                result.Actual += writer->GetConclusion().UsedChunks.size();
                ++result.Ssts;
            };

            std::unique_ptr<TWriter> writer = makeWriter();
            TMerger merger(gtype, EBlobHeaderMode::OLD_HEADER);
            ui64 lsn = 1;
            bool anythingWritten = false;

            for (ui32 step = 1; step <= cfg.Keys; ++step) {
                const ui32 blobSize = 1 + rng.Uniform(cfg.MaxBlobSize);
                const TLogoBlobID id(1, 1, step, 0, blobSize, 0);
                const TKeyLogoBlob key(id);
                const ui32 kind = rng.Uniform(100);
                const bool huge = kind < cfg.HugePercent;
                const bool indexOnly = !huge && kind < cfg.HugePercent + cfg.IndexOnlyPercent;

                // Build the Fresh records for this key, charging each one as Fresh would.
                TVector<TFreshRecord> records;
                if (indexOnly) {
                    TMaybe<TIngress> ingress = TIngress::CreateIngressWOLocal(vctx->Top.get(), vctx->ShortSelfVDisk, id);
                    if (!ingress) {
                        continue;
                    }
                    records.push_back({TMemRecLogoBlob(*ingress), {}});
                    estimate.AddIndexOnly();
                } else {
                    for (ui32 partId = 1; partId <= gtype.TotalPartCount(); ++partId) {
                        if (!cfg.AllParts && rng.Uniform(2)) {
                            continue;
                        }
                        const TLogoBlobID partIdx(id, partId);
                        TMaybe<TIngress> ingress = TIngress::CreateIngressWithLocal(vctx->Top.get(),
                            vctx->ShortSelfVDisk, partIdx);
                        if (!ingress) {
                            continue; // this disk cannot hold that part
                        }
                        TFreshRecord& record = records.emplace_back(TFreshRecord{TMemRecLogoBlob(*ingress), {}});
                        const ui32 partSize = gtype.PartSize(partIdx);
                        if (huge) {
                            record.MemRec.SetHugeBlob(TDiskPart(1, 0, partSize));
                            estimate.AddHuge();
                        } else {
                            record.Data = TDiskBlob::Create(id.BlobSize(), partId, gtype.TotalPartCount(),
                                TRope(TString(partSize, 'x')), arena, EBlobHeaderMode::OLD_HEADER, std::nullopt);
                            record.MemRec.SetMemBlob(0, record.Data.GetSize());
                            estimate.AddInline(record.Data.GetSize());
                        }
                    }
                }
                if (records.empty()) {
                    continue;
                }

                // Merge and write them the way Fresh compaction does.
                merger.Clear();
                for (const TFreshRecord& record : records) {
                    const bool inline_ = record.MemRec.GetType() == TBlobType::MemBlob;
                    merger.AddFromFresh(record.MemRec, inline_ ? &record.Data : nullptr, key, lsn++);
                }
                merger.Finish(false, true);

                auto push = [&] {
                    TDiskPart preallocated;
                    const TMemRecLogoBlob& memRec = merger.GetMemRec();
                    if (!writer->PushIndexOnly(key, memRec, &merger.GetDataMerger(), &preallocated)) {
                        return false;
                    }
                    if (memRec.GetType() == TBlobType::DiskBlob && memRec.DataSize()) {
                        const TDiskPart written = writer->PushDataOnly(merger.GetDataMerger().CreateDiskBlob(arena));
                        UNIT_ASSERT_C(written == preallocated, "written# " << written.ToString()
                            << " preallocated# " << preallocated.ToString());
                    }
                    return true;
                };
                if (!push()) {
                    // Exactly what Fresh compaction does on ETryProcessItemStatus::FinishSST.
                    finishSst(writer);
                    writer = makeWriter();
                    UNIT_ASSERT_C(push(), "a single record does not fit an empty SST");
                }
                drain(*writer);
                anythingWritten = true;
            }
            if (anythingWritten) {
                finishSst(writer);
            }

            result.Charge = estimate.GetCharge(geometry);
            result.Estimated = estimate.GetChunks(geometry);
            return result;
        }

        void Check(const TConfig& cfg) {
            const TResult r = Run(cfg);
            Cerr << "ChunkSize# " << cfg.ChunkSize << " ChunksPerSst# " << cfg.ChunksPerSst
                << " MaxBlobSize# " << cfg.MaxBlobSize << " AllParts# " << cfg.AllParts << " Seed# " << cfg.Seed
                << " Charge# " << r.Charge << " Ssts# " << r.Ssts
                << " Actual# " << r.Actual << " Estimated# " << r.Estimated << Endl;
            UNIT_ASSERT_C(r.Actual, "nothing was written, the test exercises nothing");
            UNIT_ASSERT_C(r.Estimated >= r.Actual, "the estimate must bound the writer: Actual# " << r.Actual
                << " Estimated# " << r.Estimated << " Seed# " << cfg.Seed);
        }

        Y_UNIT_TEST(SmallBlobsOneChunkPerSst) {
            for (ui64 seed = 1; seed <= 20; ++seed) {
                Check({.MaxBlobSize = 4 << 10, .Seed = seed});
            }
        }

        Y_UNIT_TEST(LargeBlobsOneChunkPerSst) {
            // Items close to a sizeable fraction of the chunk make next-fit waste as large as it gets.
            for (ui64 seed = 1; seed <= 20; ++seed) {
                Check({.MaxBlobSize = 256 << 10, .Keys = 2000, .Seed = seed});
            }
        }

        Y_UNIT_TEST(MergedPartsOneChunkPerSst) {
            // Every part the disk may hold, so records merge into the largest items possible.
            for (ui64 seed = 1; seed <= 20; ++seed) {
                Check({.MaxBlobSize = 128 << 10, .Keys = 4000, .HugePercent = 0, .IndexOnlyPercent = 0,
                    .AllParts = true, .Seed = seed});
            }
        }

        Y_UNIT_TEST(IndexOnlyOneChunkPerSst) {
            // Index records alone: the index region, its linkers and the placeholder are all there is.
            for (ui64 seed = 1; seed <= 5; ++seed) {
                Check({.ChunkSize = 256 << 10, .Keys = 50000, .HugePercent = 50, .IndexOnlyPercent = 50,
                    .Seed = seed});
            }
        }

        Y_UNIT_TEST(MultiChunkSsts) {
            for (ui32 chunksPerSst : {2, 4}) {
                for (ui64 seed = 1; seed <= 10; ++seed) {
                    Check({.ChunksPerSst = chunksPerSst, .MaxBlobSize = 128 << 10, .Keys = 6000, .Seed = seed});
                }
            }
        }

        Y_UNIT_TEST(EstimateIsNotWildlyPessimistic) {
            // An upper bound that reserves several times what is written would be correct and useless.
            // Small blobs keep the per-boundary slack small, so the estimate has to stay close.
            const TResult r = Run({.MaxBlobSize = 4 << 10, .Keys = 50000, .Seed = 7});
            UNIT_ASSERT_GE(r.Estimated, r.Actual);
            UNIT_ASSERT_C(r.Estimated <= r.Actual + 1 + r.Actual / 4, "Actual# " << r.Actual
                << " Estimated# " << r.Estimated);
        }
    }

} // NKikimr
