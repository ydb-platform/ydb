#include "blobstorage_hullwritesst.h"
#include "blobstorage_hullrecmerger.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_arena.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>

namespace NKikimr {

    // A planned compaction counts the SSTs it will write by replaying TSstSpaceModel, and reserves that many; it
    // stops if it ever needs more. So the model has to decide exactly as TWriter does, record by record: this feeds
    // both the same records and requires the same answer every time.
    Y_UNIT_TEST_SUITE(TSstSpaceModelVsWriter) {

        using TWriter = TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>::TWriter;
        using TModel = TSstSpaceModel<TKeyLogoBlob, TMemRecLogoBlob>;
        using TMerger = TCompactRecordMerger<TKeyLogoBlob, TMemRecLogoBlob>;

        struct TConfig {
            ui32 ChunkSize = 1 << 20;
            ui32 AppendBlockSize = 4 << 10;
            ui32 WriteBlockSize = 16 << 10;
            ui32 ChunksPerSst = 1;
            ui32 MaxBlobSize = 64 << 10;
            ui32 Keys = 20000;
            ui32 HugePercent = 20;
            ui32 IndexOnlyPercent = 20;
            bool AllParts = false;
            ui64 Seed = 1;
        };

        struct TFreshRecord {
            TMemRecLogoBlob MemRec;
            TRope Data;
        };

        // Returns the number of SSTs written.
        ui32 Run(const TConfig& cfg) {
            TTestContexts ctx(cfg.ChunkSize);
            const TVDiskContextPtr vctx = ctx.GetVCtx();
            const TBlobStorageGroupType gtype = vctx->Top->GType;
            TRopeArena arena(&TRopeArenaBackend::Allocate);
            TReallyFastRng32 rng(cfg.Seed);

            TDeque<TChunkIdx> reservedChunks;
            for (TChunkIdx chunkIdx = 1; chunkIdx < 100000; ++chunkIdx) {
                reservedChunks.push_back(chunkIdx);
            }

            ui32 ssts = 0;
            ui32 usedChunks = 0;
            auto makeWriter = [&] {
                return std::make_unique<TWriter>(vctx, EWriterDataType::Comp, cfg.ChunksPerSst, 1, 1,
                    cfg.ChunkSize, cfg.AppendBlockSize, cfg.WriteBlockSize, ssts, false, reservedChunks, arena,
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
                const ui32 used = writer->GetConclusion().UsedChunks.size();
                UNIT_ASSERT_C(used <= cfg.ChunksPerSst, "used# " << used);
                usedChunks += used;
                ++ssts;
            };

            std::unique_ptr<TWriter> writer = makeWriter();
            std::optional<TModel> model;
            model.emplace(cfg.ChunkSize, cfg.AppendBlockSize, cfg.ChunksPerSst);
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

                TVector<TFreshRecord> records;
                if (indexOnly) {
                    TMaybe<TIngress> ingress = TIngress::CreateIngressWOLocal(vctx->Top.get(), vctx->ShortSelfVDisk, id);
                    if (!ingress) {
                        continue;
                    }
                    records.push_back({TMemRecLogoBlob(*ingress), {}});
                } else {
                    for (ui32 partId = 1; partId <= gtype.TotalPartCount(); ++partId) {
                        if (!cfg.AllParts && rng.Uniform(2)) {
                            continue;
                        }
                        const TLogoBlobID partIdx(id, partId);
                        TMaybe<TIngress> ingress = TIngress::CreateIngressWithLocal(vctx->Top.get(),
                            vctx->ShortSelfVDisk, partIdx);
                        if (!ingress) {
                            continue;
                        }
                        TFreshRecord& record = records.emplace_back(TFreshRecord{TMemRecLogoBlob(*ingress), {}});
                        const ui32 partSize = gtype.PartSize(partIdx);
                        if (huge) {
                            record.MemRec.SetHugeBlob(TDiskPart(1, 0, partSize));
                        } else {
                            record.Data = TDiskBlob::Create(id.BlobSize(), partId, gtype.TotalPartCount(),
                                TRope(TString(partSize, 'x')), arena, EBlobHeaderMode::OLD_HEADER, std::nullopt);
                            record.MemRec.SetMemBlob(0, record.Data.GetSize());
                        }
                    }
                }
                if (records.empty()) {
                    continue;
                }

                merger.Clear();
                for (const TFreshRecord& record : records) {
                    const bool inline_ = record.MemRec.GetType() == TBlobType::MemBlob;
                    merger.AddFromFresh(record.MemRec, inline_ ? &record.Data : nullptr, key, lsn++);
                }
                merger.Finish(false, true);

                const TMemRecLogoBlob& memRec = merger.GetMemRec();
                // exactly what THullCompactionWorker::PlanItem() feeds the model
                const ui32 inplacedDataSize = memRec.GetType() == TBlobType::DiskBlob ? memRec.DataSize() : 0;
                const ui32 numAddedOuts = merger.GetDataMerger().GetSavedHugeBlobs().size();

                auto push = [&] {
                    TDiskPart preallocated;
                    const bool pushed = writer->PushIndexOnly(key, memRec, &merger.GetDataMerger(), &preallocated);
                    const bool modelled = model->Push(inplacedDataSize, numAddedOuts);
                    UNIT_ASSERT_VALUES_EQUAL_C(pushed, modelled, "step# " << step << " inplacedDataSize# "
                        << inplacedDataSize << " numAddedOuts# " << numAddedOuts << " Seed# " << cfg.Seed);
                    if (!pushed) {
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
                    finishSst(writer);
                    writer = makeWriter();
                    model.emplace(cfg.ChunkSize, cfg.AppendBlockSize, cfg.ChunksPerSst);
                    UNIT_ASSERT_C(push(), "a single record does not fit an empty SST");
                }
                drain(*writer);
                anythingWritten = true;
            }
            if (anythingWritten) {
                finishSst(writer);
            }
            UNIT_ASSERT_C(ssts, "nothing was written, the test exercises nothing");
            Cerr << "ChunkSize# " << cfg.ChunkSize << " ChunksPerSst# " << cfg.ChunksPerSst
                << " MaxBlobSize# " << cfg.MaxBlobSize << " Seed# " << cfg.Seed
                << " Ssts# " << ssts << " UsedChunks# " << usedChunks << Endl;
            return ssts;
        }

        Y_UNIT_TEST(SmallBlobs) {
            for (ui64 seed = 1; seed <= 10; ++seed) {
                Run({.MaxBlobSize = 4 << 10, .Seed = seed});
            }
        }

        Y_UNIT_TEST(LargeBlobs) {
            for (ui64 seed = 1; seed <= 10; ++seed) {
                Run({.MaxBlobSize = 256 << 10, .Keys = 2000, .Seed = seed});
            }
        }

        Y_UNIT_TEST(MergedParts) {
            for (ui64 seed = 1; seed <= 10; ++seed) {
                Run({.MaxBlobSize = 128 << 10, .Keys = 4000, .HugePercent = 0, .IndexOnlyPercent = 0,
                    .AllParts = true, .Seed = seed});
            }
        }

        Y_UNIT_TEST(HugeAndIndexOnly) {
            // several huge parts of one blob go to the outbound area, a single one does not
            for (ui64 seed = 1; seed <= 5; ++seed) {
                Run({.ChunkSize = 256 << 10, .Keys = 50000, .HugePercent = 50, .IndexOnlyPercent = 50,
                    .AllParts = seed % 2 == 1, .Seed = seed});
            }
        }

        Y_UNIT_TEST(MultiChunkSsts) {
            for (ui32 chunksPerSst : {2, 3, 4}) {
                for (ui64 seed = 1; seed <= 5; ++seed) {
                    Run({.ChunksPerSst = chunksPerSst, .MaxBlobSize = 128 << 10, .Keys = 6000, .Seed = seed});
                }
            }
        }
    }

} // NKikimr
