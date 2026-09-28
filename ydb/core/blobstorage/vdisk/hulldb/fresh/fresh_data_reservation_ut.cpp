#include "fresh_data.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_settings.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    // Chunks reserved in advance for compacting Fresh: a record is admitted only once Cur holds enough to
    // compact everything already in it, everything in flight, and the record itself; and Cur never rotates
    // out while anything is in flight, so every record lands in the segment its chunks were held for.
    Y_UNIT_TEST_SUITE(TFreshDataReservation) {

        using TFreshData = ::NKikimr::TFreshData<TKeyLogoBlob, TMemRecLogoBlob>;

        struct TEnv {
            TTestContexts Ctx;
            TLevelIndexSettings Settings;
            std::shared_ptr<TRopeArena> Arena = std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);
            TFreshData Fresh;
            ui64 Lsn = 1;
            ui32 Step = 1;
            TChunkIdx NextChunk = 100;

            // Small chunks, so a segment fills up and wants rotating after a few dozen records.
            explicit TEnv(bool useDreg = false, ui32 chunkSize = 64 << 10)
                : Ctx(chunkSize)
                , Settings(Ctx.GetHullCtx(), 8u, 64u << 20u, 0, TDuration::Minutes(10), 10u, useDreg, false)
                , Fresh(Settings, CreateDefaultTimeProvider(), Arena)
            {}

            // A record as the skeleton would charge it before admitting it, and the Put that lands it.
            struct TRecord {
                TLogoBlobID Id;
                TString Data;
                TFreshOutputEstimate Charge;
            };

            TRecord MakeRecord(ui32 blobSize = 4000) {
                const TLogoBlobID id(1, 1, Step++, 0, blobSize, 0);
                const auto& gtype = Ctx.GetHullCtx()->VCtx->Top->GType;
                TRecord r{id, TString(gtype.PartSize(TLogoBlobID(id, 1)), 'x'), {}};
                const auto mode = Ctx.GetHullCtx()->VCfg->BlobHeaderMode;
                r.Charge.AddInline(TDiskBlob::GetBlobHeaderSize(mode) + r.Data.size());
                return r;
            }

            void Put(const TRecord& r) {
                Fresh.PutLogoBlobWithData(Lsn++, TKeyLogoBlob(r.Id), 1, TIngress(), TRope(r.Data), std::nullopt);
            }

            // Admission as the skeleton does it: reserve what is missing, then take the record in flight.
            void Admit(const TRecord& r) {
                if (const ui64 shortfall = Fresh.GetCurReservationShortfall(r.Charge)) {
                    TVector<TChunkIdx> chunks;
                    for (ui64 i = 0; i < shortfall; ++i) {
                        chunks.push_back(NextChunk++);
                    }
                    Fresh.AddCurReservedChunks(chunks);
                }
                UNIT_ASSERT_VALUES_EQUAL(Fresh.GetCurReservationShortfall(r.Charge), 0);
                Fresh.AdmitInFlight(r.Charge);
            }

            // The log write completed: put the record, then land it, in that order. Landing may let a pending
            // rotation happen, which must not move the record into a segment nothing was reserved for.
            void Replay(const TRecord& r) {
                Put(r);
                Fresh.LandInFlight(r.Charge);
            }
        };

        Y_UNIT_TEST(FirstRecordNeedsAChunk) {
            TEnv env;
            const auto r = env.MakeRecord();
            UNIT_ASSERT_VALUES_EQUAL(env.Fresh.GetCurReservationShortfall(r.Charge), 1);
            env.Fresh.AddCurReservedChunks({1});
            UNIT_ASSERT_VALUES_EQUAL(env.Fresh.GetCurReservationShortfall(r.Charge), 0);
        }

        // A record in flight is still owed: a second one is judged against both.
        Y_UNIT_TEST(InFlightCountsAgainstTheReservation) {
            TEnv env;
            env.Fresh.AddCurReservedChunks({1});
            ui32 admitted = 0;
            while (!env.Fresh.GetCurReservationShortfall(env.MakeRecord().Charge)) {
                env.Fresh.AdmitInFlight(env.MakeRecord().Charge);
                ++admitted;
                UNIT_ASSERT_C(admitted < 1000, "one chunk never filled up");
            }
            UNIT_ASSERT_C(admitted > 1, "admitted# " << admitted);
        }

        // Landing moves a record from in flight into the segment; what is owed stays the same.
        Y_UNIT_TEST(LandingMovesChargeIntoSegment) {
            TEnv env;
            const auto first = env.MakeRecord();
            env.Admit(first);
            const auto probe = env.MakeRecord();
            const ui64 before = env.Fresh.GetCurReservationShortfall(probe.Charge);
            env.Replay(first);
            UNIT_ASSERT(env.Fresh.GetInFlight().Empty());
            UNIT_ASSERT_VALUES_EQUAL(env.Fresh.GetCurReservationShortfall(probe.Charge), before);
        }

        // A compaction that would rotate Cur out waits for the records in flight.
        Y_UNIT_TEST(CompactionRotationWaitsForInFlight) {
            TEnv env;
            const auto r = env.MakeRecord();
            env.Admit(r);
            UNIT_ASSERT(!env.Fresh.NeedsCompaction(0, true));
            UNIT_ASSERT(env.Fresh.IsRotationPending());

            env.Replay(r);
            UNIT_ASSERT(env.Fresh.NeedsCompaction(0, true));
            UNIT_ASSERT(!env.Fresh.IsRotationPending());
        }

        // A record that never lands must not leave the rotation waiting.
        Y_UNIT_TEST(AbandonedRecordReleasesRotation) {
            TEnv env;
            const auto r = env.MakeRecord();
            env.Admit(r);
            UNIT_ASSERT(!env.Fresh.NeedsCompaction(0, true));
            env.Fresh.LandInFlight(r.Charge); // no Put: the write was abandoned
            UNIT_ASSERT(env.Fresh.NeedsCompaction(0, true));
        }

        // The segment that rotates out keeps what compacting it needs; the new Cur takes the rest,
        // instead of it going back to PDisk only to be reserved again.
        Y_UNIT_TEST(RotationHandsSurplusToNewCur) {
            TEnv env;
            const auto r = env.MakeRecord();
            env.Admit(r);
            env.Replay(r);
            env.Fresh.AddCurReservedChunks({500, 501, 502}); // well beyond one small record's needs

            UNIT_ASSERT(env.Fresh.NeedsCompaction(0, true));
            auto old = env.Fresh.FindSegmentForCompaction();
            const ui64 kept = old->GetReservedChunks().size();
            UNIT_ASSERT_VALUES_EQUAL(kept, old->GetOutputChunks());
            UNIT_ASSERT_VALUES_EQUAL(kept, 1);

            // The new Cur holds the rest, so the next records need nothing more from PDisk.
            const auto next = env.MakeRecord();
            UNIT_ASSERT_VALUES_EQUAL(env.Fresh.GetCurReservationShortfall(next.Charge), 0);

            // The compaction takes Old's chunks to write into, and nothing stays behind with the segment.
            UNIT_ASSERT_VALUES_EQUAL(old->TakeReservedChunks().size(), kept);
            env.Fresh.CompactionSstCreated(std::move(old));
        }

        // An aborted compaction retries the same Old, and TFreshData leaves whatever chunks Old holds alone. (In the
        // VDisk there are none left by then: the compaction took them as it started and forgot them on abort.)
        Y_UNIT_TEST(AbortedCompactionKeepsOldChunks) {
            TEnv env;
            const auto r = env.MakeRecord();
            env.Admit(r);
            env.Replay(r);
            UNIT_ASSERT(env.Fresh.NeedsCompaction(0, true));
            auto old = env.Fresh.FindSegmentForCompaction();
            const auto chunks = old->GetReservedChunks();
            UNIT_ASSERT(!chunks.empty());

            env.Fresh.CompactionAborted();
            // A retry rotates nothing, so a record in flight does not hold it back.
            env.Admit(env.MakeRecord());
            UNIT_ASSERT(env.Fresh.NeedsCompaction(0, false));
            auto retried = env.Fresh.FindSegmentForCompaction();
            UNIT_ASSERT_EQUAL(retried.Get(), old.Get());
            UNIT_ASSERT_EQUAL(retried->GetReservedChunks(), chunks);
        }

        // With Dreg, a full Cur is swapped out on Put. That rotation waits for records in flight as well,
        // and happens as soon as the last of them lands.
        Y_UNIT_TEST(DregSwapWaitsForInFlight) {
            TEnv env(true);
            const auto inFlight = env.MakeRecord();
            env.Admit(inFlight);
            // Fill Cur past its threshold with records that bypass admission, as internal writers do.
            for (ui32 i = 0; !env.Fresh.IsRotationPending(); ++i) {
                UNIT_ASSERT_C(i < 10000, "Cur never filled up");
                env.Put(env.MakeRecord());
            }
            UNIT_ASSERT(!env.Fresh.GetInFlight().Empty());

            env.Fresh.LandInFlight(inFlight.Charge);
            UNIT_ASSERT(!env.Fresh.IsRotationPending());
            UNIT_ASSERT(env.Fresh.GetInFlight().Empty());
            // The swap did happen: there is now a Dreg for the log cut to want compacted, while the new,
            // empty Cur holds nothing it could want.
            UNIT_ASSERT(env.Fresh.NeedsCompaction(Max<ui64>(), false));
        }

        // Fills Cur with admitted, landed records up to the one that would push it past one SST, and returns it.
        static TEnv::TRecord FillUpToOneSst(TEnv& env) {
            TEnv::TRecord next = env.MakeRecord();
            for (ui32 guard = 0; !env.Fresh.WouldOutgrowSst(next.Charge); ++guard) {
                UNIT_ASSERT_C(guard < 10000, "Cur never filled up");
                env.Admit(next);
                env.Replay(next);
                next = env.MakeRecord();
            }
            return next;
        }

        // Rather than grow past one SST, Cur rotates out, and compacts into exactly one chunk.
        Y_UNIT_TEST(SizeRotationKeepsSegmentToOneSst) {
            TEnv env;
            const auto next = FillUpToOneSst(env);
            UNIT_ASSERT(env.Fresh.CanRotateCur());
            env.Fresh.RequestSizeRotation();
            UNIT_ASSERT(env.Fresh.NeedsCompaction(0, false));
            auto old = env.Fresh.FindSegmentForCompaction();
            UNIT_ASSERT_VALUES_EQUAL(old->GetOutputChunks(), 1);
            // The record that did not fit goes into the new Cur...
            UNIT_ASSERT(!env.Fresh.WouldOutgrowSst(next.Charge));
            // ...which cannot rotate again while Old is compacting, and grows instead.
            UNIT_ASSERT(!env.Fresh.CanRotateCur());
        }

        // Like every rotation, the one at one SST waits for records in flight.
        Y_UNIT_TEST(SizeRotationWaitsForInFlight) {
            TEnv env;
            TEnv::TRecord last = env.MakeRecord();
            env.Admit(last);
            TEnv::TRecord next = env.MakeRecord();
            for (ui32 guard = 0; !env.Fresh.WouldOutgrowSst(next.Charge); ++guard) {
                UNIT_ASSERT_C(guard < 10000, "Cur never filled up");
                env.Replay(last); // the previous record lands...
                last = next;
                env.Admit(last); // ...while this one stays in flight
                next = env.MakeRecord();
            }
            env.Fresh.RequestSizeRotation();
            UNIT_ASSERT(!env.Fresh.NeedsCompaction(0, false));
            UNIT_ASSERT(env.Fresh.IsRotationPending());

            env.Replay(last);
            UNIT_ASSERT(env.Fresh.NeedsCompaction(0, false));
            auto old = env.Fresh.FindSegmentForCompaction();
            UNIT_ASSERT_VALUES_EQUAL(old->GetOutputChunks(), 1);
        }

        // With Dreg free, Cur rotates into it on the spot.
        Y_UNIT_TEST(SizeRotationIntoDregIsImmediate) {
            TEnv env(true);
            const auto next = FillUpToOneSst(env);
            UNIT_ASSERT(env.Fresh.CanRotateCur());
            env.Fresh.RequestSizeRotation();
            UNIT_ASSERT(!env.Fresh.IsRotationPending());
            UNIT_ASSERT(!env.Fresh.WouldOutgrowSst(next.Charge)); // Cur is new and empty
            UNIT_ASSERT(env.Fresh.NeedsCompaction(Max<ui64>(), false)); // and the full one is now Dreg
        }
    }

} // NKikimr
