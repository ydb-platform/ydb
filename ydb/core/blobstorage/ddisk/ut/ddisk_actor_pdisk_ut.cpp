#include "ddisk_actor_pdisk_common_ut.h"
#include <ydb/library/pdisk_io/uring_test_support.h>

#include <array>

namespace NKikimr {

namespace {

void WaitNativeGate(TTestContext& ctx, size_t count = 1) {
    const auto deadline = TInstant::Now() + TDuration::Seconds(30);
    for (;;) {
        auto state = ctx.SendAndGrab<TEvGateState>(new TEvControlGate());
        if (state->Get()->Held >= count) {
            return;
        }
        UNIT_ASSERT_C(TInstant::Now() < deadline, "native completion did not reach event gate");
        Sleep(TDuration::MilliSeconds(1));
    }
}

enum class EPayloadLayout {
    Unaligned,
    FragmentedUnaligned,
    FragmentedAligned,
};

void TestShutdownReleasesReservations(NDDisk::TDDiskConfig config, bool abandonReservations = false) {
    TTestContext ctx(config, NLog::PRI_ERROR, 1, std::nullopt, /*probeReservations=*/true, abandonReservations);
    for (ui32 cycle = 0; cycle < 3; ++cycle) {
        const auto creds = Connect(ctx, 701, 1);
        ctx.WaitForReservationsSettled();
        // The previous incarnation's unused reserve must not accumulate across restarts.
        UNIT_ASSERT_VALUES_EQUAL(ctx.UncommittedChunks(), cycle == 0 ? 4 : 0);
        const auto readBack = [&](ui32 chunk) {
            AssertReadResult(ctx.SendAndGrab<NDDisk::TEvReadResult>(
                new NDDisk::TEvRead(creds, {chunk, 0, MinBlockSize}, {true})),
                MakeData('A' + chunk, MinBlockSize), config.EnableChecksums);
        };
        for (ui32 chunk = 0; chunk < cycle; ++chunk) {
            readBack(chunk);
        }
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(cycle, 0, MinBlockSize), NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(MakeAlignedRope(MakeData('A' + cycle, MinBlockSize)));
        AssertStatus<NDDisk::TEvWriteResult>(ctx.SendAndGrab<NDDisk::TEvWriteResult>(write.release()), TReplyStatus::OK);
        readBack(cycle);
        ctx.WaitForReservationsSettled();
        UNIT_ASSERT_C(ctx.UncommittedChunks() > 0, "test requires unused live PDisk reservations");
        const auto reserved = ctx.UncommittedChunks();
        ctx.StopDDisk(0);
        if (abandonReservations) {
            UNIT_ASSERT_VALUES_EQUAL(ctx.UncommittedChunks(), reserved);
        } else {
            ctx.WaitForReservationsReleased();
            UNIT_ASSERT_VALUES_EQUAL(ctx.UncommittedChunks(), 0);
        }
        ctx.StartDDisk(0);
    }
    const auto creds = Connect(ctx, 701, 1);
    for (ui32 chunk = 0; chunk < 3; ++chunk) {
        AssertReadResult(ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {chunk, 0, MinBlockSize}, {true})),
            MakeData('A' + chunk, MinBlockSize), config.EnableChecksums);
    }
    ctx.WaitForReservationsSettled();
    UNIT_ASSERT_VALUES_EQUAL(ctx.UncommittedChunks(), 0);
    ctx.StopDDisk(0);
    if (!abandonReservations) {
        ctx.WaitForReservationsReleased();
    }
}

void TestWriteAndReadPayloadLayout(NDDisk::TDDiskConfig config, EPayloadLayout layout) {
    config.CheckChecksumBeforeWrite = true;
    TTestContext ctx(std::move(config), NLog::PRI_INFO);
    const auto creds = Connect(ctx, 32, 1);
    const TString expected = MakeData('A', MinBlockSize) + MakeData('B', MinBlockSize);
    TRope payload;
    if (layout == EPayloadLayout::Unaligned) {
        payload = MakeAlignedRope(TString("!") + expected);
        payload.EraseFront(1);
        UNIT_ASSERT_VALUES_EQUAL(payload.Begin().ContiguousSize(), expected.size());
        UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(payload.Begin().ContiguousData()) % MinBlockSize, 1u);
    } else {
        // Split inside an integrity block to exercise streaming checksum validation as well as copying.
        const ui32 split = layout == EPayloadLayout::FragmentedUnaligned ? MinBlockSize - 1 : MinBlockSize;
        payload = MakeAlignedRope(expected.substr(0, split));
        payload.Insert(payload.End(), MakeAlignedRope(expected.substr(split)));
        UNIT_ASSERT_VALUES_EQUAL(payload.Begin().ContiguousSize(), split);
        UNIT_ASSERT_C(payload.Begin().ContiguousSize() < payload.size(), "payload must remain fragmented");
    }

    // Exercise both initial allocation and an overwrite of an existing chunk.
    for (ui32 attempt = 0; attempt < 2; ++attempt) {
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(7, MinBlockSize, expected.size()), NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(TRope(payload));
        AssertStatus<NDDisk::TEvWriteResult>(
            ctx.SendAndGrab<NDDisk::TEvWriteResult>(write.release()), TReplyStatus::OK);

        auto read = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {7, MinBlockSize, static_cast<ui32>(expected.size())}, {true}));
        AssertReadResult(read, expected);
    }
}

void TestDevNullWriteAndRead(bool checksums) {
    NDDisk::TDDiskConfig config;
    config.DevNullMode = true;
    config.EnableChecksums = checksums;
    config.CheckChecksumBeforeWrite = true;
    config.CheckChecksumWhenRead = true;
    TTestContext ctx(config);
    const auto creds = Connect(ctx, 702, 1);
    const TString zeros(2 * MinBlockSize, '\0');
    // With checksums enabled, zero writes seed resident metadata for used blocks.
    // Nonzero writes without checksums demonstrate that the payload is discarded.
    const TString payload = checksums ? zeros : MakeData('D', zeros.size());
    for (ui32 attempt = 0; attempt < 2; ++attempt) {
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(7, MinBlockSize, payload.size()), NDDisk::TWriteInstruction(0));
        if (checksums) {
            write->AddPayloadThenChecksum(MakeAlignedRope(payload));
        } else {
            write->AddPayload(MakeAlignedRope(payload));
        }
        AssertStatus<NDDisk::TEvWriteResult>(
            ctx.SendAndGrab<NDDisk::TEvWriteResult>(write.release()), TReplyStatus::OK);
        auto read = ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {7, MinBlockSize, static_cast<ui32>(zeros.size())}, {true}));
        AssertReadResult(read, zeros, checksums);
    }
    ctx.StopDDisk(0);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDDiskActorPDiskTest) {
    Y_UNIT_TEST(SectorMapRejectsMultiplePDisks) {
        auto sectors = MakeIntrusive<NPDisk::TSectorMap>(64_GB);
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TTestContext({}, NLog::PRI_ERROR, 2, ChunkSize, false, false, sectors),
            yexception, "SectorMap supports exactly one PDisk");

        TTestContext ctx({.ForcePDiskFallback = true}, NLog::PRI_ERROR, 1,
            ChunkSize, false, false, sectors);
        UNIT_ASSERT_EXCEPTION_CONTAINS(ctx.AddPDisk(), yexception,
            "SectorMap supports exactly one PDisk");
        UNIT_ASSERT_EXCEPTION_CONTAINS(ctx.AddDisk(), yexception,
            "SectorMap supports exactly one PDisk");
        UNIT_ASSERT_VALUES_EQUAL(ctx.PDisks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ctx.Disks.size(), 1);

        // Rejected additions must leave the existing PDisk usable.
        const auto creds = Connect(ctx, 703, 1);
        const TString data = MakeData('S', MinBlockSize);
        auto write = std::make_unique<NDDisk::TEvWrite>(creds,
            NDDisk::TBlockSelector(0, 0, MinBlockSize), NDDisk::TWriteInstruction(0));
        write->AddPayloadThenChecksum(MakeAlignedRope(data));
        AssertStatus<NDDisk::TEvWriteResult>(ctx.SendAndGrab<NDDisk::TEvWriteResult>(write.release()), TReplyStatus::OK);
        AssertReadResult(ctx.SendAndGrab<NDDisk::TEvReadResult>(
            new NDDisk::TEvRead(creds, {0, 0, MinBlockSize}, {true})), data);
    }

    Y_UNIT_TEST(DevNullDiscardsNonzeroWritesWithoutChecksums_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestDevNullWriteAndRead(false);
    }

    Y_UNIT_TEST(DevNullVerifiesZeroWritesAndReadsWithChecksums_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestDevNullWriteAndRead(true);
    }

    Y_UNIT_TEST(CoroutineParkedWriteSessionReplacement_Uring) {
        if (!NPDisk::RequireUring()) {
            return;
        }
        for (bool generation : {false, true}) {
            TTestContext ctx({}, NLog::PRI_ERROR, 1, std::nullopt, true);
            auto creds = Connect(ctx, 994, 1);

            auto write = [&](const auto& credentials, ui64 chunk, ui64 cookie, ui32 block = 2) {
                auto request = std::make_unique<NDDisk::TEvWrite>(credentials,
                    NDDisk::TBlockSelector(chunk, block * MinBlockSize, MinBlockSize), NDDisk::TWriteInstruction(0));
                request->AddPayloadThenChecksum(MakeAlignedRope(MakeData('A', MinBlockSize)));
                ctx.Send(request.release(), cookie);
            };
            write(creds, 0, 500);
            AssertStatus<NDDisk::TEvWriteResult>(ctx.Grab<NDDisk::TEvWriteResult>(), TReplyStatus::OK);
            ctx.WaitForReservationsSettled();
            auto initial = ctx.SendAndGrab<TEvGateState>(new TEvControlGate(NPDisk::TEvChunkReserveResult::EventType));
            const auto reserved = initial->Get()->Reserved;
            UNIT_ASSERT(reserved > 0);
            for (size_t i = 0; i < reserved; ++i) {
                write(creds, i + 1, 500);
                AssertStatus<NDDisk::TEvWriteResult>(ctx.Grab<NDDisk::TEvWriteResult>(), TReplyStatus::OK);
            }
            WaitNativeGate(ctx);
            auto empty = ctx.SendAndGrab<TEvGateState>(new TEvControlGate());
            UNIT_ASSERT_VALUES_EQUAL(empty->Get()->Reserved, 0);
            write(creds, 100, 501);
            // The write parks until a reserve arrives; the read of its unpublished chunk
            // answers zeroes without waiting for that allocation.
            ctx.Send(new NDDisk::TEvRead(creds, {100, 0, MinBlockSize}, {true}), 502);
            auto unpublished = ctx.Grab<NDDisk::TEvReadResult>();
            AssertStatus<NDDisk::TEvReadResult>(unpublished, TReplyStatus::OK);
            UNIT_ASSERT_VALUES_EQUAL(unpublished->Cookie, 502);
            UNIT_ASSERT_VALUES_EQUAL(unpublished->Get()->GetPayload(0).ConvertToString(),
                TString(MinBlockSize, '\0'));
            auto fresh = NDDisk::TQueryCredentials::ToDDisk(994, generation ? 2 : 1,
                generation ? 0 : 1, std::nullopt, 0);
            auto connected = ctx.SendAndGrab<NDDisk::TEvConnectResult>(new NDDisk::TEvConnect(fresh));
            AssertStatus<NDDisk::TEvConnectResult>(connected, TReplyStatus::OK);
            fresh.DDiskInstanceGuid = connected->Get()->Record.GetDDiskInstanceGuid();
            fresh.ConnectionToken.emplace(connected->Get()->Record.GetConnectionToken());
            write(fresh, 100, 503, 3);
            ctx.SendAndGrab<TEvGateState>(new TEvControlGate(0, true));
            std::set<ui64> cookies;
            for (ui32 i = 0; i < 2; ++i) {
                auto result = ctx.Grab<NDDisk::TEvWriteResult>();
                UNIT_ASSERT(cookies.insert(result->Cookie).second);
                AssertStatus<NDDisk::TEvWriteResult>(result, result->Cookie == 501 ? TReplyStatus::SESSION_MISMATCH : TReplyStatus::OK);
            }
            auto completed = ctx.SendAndGrab<TEvGateState>(new TEvControlGate());
            UNIT_ASSERT(completed->Get()->Router);
            UNIT_ASSERT_VALUES_EQUAL(completed->Get()->Held, 0);
            // Batches that complete inline emit no mailbox event. Verify the revoked
            // write left its block untouched and the replacement wrote its own block.
            AssertReadResult(ctx.SendAndGrab<NDDisk::TEvReadResult>(
                new NDDisk::TEvRead(fresh, {100, 2 * MinBlockSize, MinBlockSize}, {true})),
                TString(MinBlockSize, '\0'));
            AssertReadResult(ctx.SendAndGrab<NDDisk::TEvReadResult>(
                new NDDisk::TEvRead(fresh, {100, 3 * MinBlockSize, MinBlockSize}, {true})),
                MakeData('A', MinBlockSize));
            ctx.StopDDisk(0);
        }
    }

    Y_UNIT_TEST(DisjointNativeWritesAcrossConnectionReplacement_Uring) {
        if (!NPDisk::RequireUring()) {
            return;
        }
        TTestContext ctx({}, NLog::PRI_ERROR, 1, std::nullopt, true);
        auto creds = Connect(ctx, 991, 1);

        auto write = [&](const auto& credentials, ui32 block, char value, ui64 cookie) {
            auto request = std::make_unique<NDDisk::TEvWrite>(credentials,
                NDDisk::TBlockSelector(0, block * MinBlockSize, MinBlockSize), NDDisk::TWriteInstruction(0));
            request->AddPayloadThenChecksum(MakeAlignedRope(MakeData(value, MinBlockSize)));
            ctx.Send(request.release(), cookie);
        };
        write(creds, 0, 'I', 500);
        AssertStatus<NDDisk::TEvWriteResult>(ctx.Grab<NDDisk::TEvWriteResult>(), TReplyStatus::OK);
        ctx.WaitForReservationsSettled();
        ctx.SendAndGrab<TEvGateState>(new TEvControlGate(TEvents::TEvResumeRunnable::EventType));
        write(creds, 1, 'A', 501);
        WaitNativeGate(ctx);
        write(creds, 2, 'S', 502);
        // The second writer waits for the first writer to release their shared metadata
        // pair before admitting anything. The first writer is already accepted.
        auto fresh = Connect(ctx, 991, 2);
        write(fresh, 3, 'B', 503);
        write(fresh, 4, 'C', 504);
        const auto held = ctx.SendAndGrab<TEvGateState>(new TEvControlGate());
        UNIT_ASSERT(held->Get()->Held >= 1);
        UNIT_ASSERT(held->Get()->Router);
        ctx.SendAndGrab<TEvGateState>(new TEvControlGate(0, true));
        // The writer which was parked before admission sees the replaced session; the
        // accepted writer and both writers on the fresh connection complete.
        std::set<ui64> cookies;
        for (ui32 i = 0; i < 4; ++i) {
            auto result = ctx.Grab<NDDisk::TEvWriteResult>();
            UNIT_ASSERT(cookies.insert(result->Cookie).second);
            AssertStatus<NDDisk::TEvWriteResult>(result,
                result->Cookie == 502 ? TReplyStatus::SESSION_MISMATCH : TReplyStatus::OK);
        }
        UNIT_ASSERT(cookies == (std::set<ui64>{501, 502, 503, 504}));
        const auto state = ctx.SendAndGrab<TEvGateState>(new TEvControlGate());
        UNIT_ASSERT_VALUES_EQUAL(state->Get()->Held, 0);
        UNIT_ASSERT(state->Get()->IoCompletions >= 4);
        for (const auto& [block, value] : std::array<std::pair<ui32, char>, 4>{{
                {1, 'A'}, {2, '\0'}, {3, 'B'}, {4, 'C'}}}) {
            AssertReadResult(ctx.SendAndGrab<NDDisk::TEvReadResult>(
                new NDDisk::TEvRead(fresh, {0, block * MinBlockSize, MinBlockSize}, {true})),
                MakeData(value, MinBlockSize));
        }
        ctx.StopDDisk(0);
    }

    Y_UNIT_TEST(CoroutineDeletionInterruption_Uring) {
        if (!NPDisk::RequireUring()) {
            return;
        }
        for (bool broken : {false, true}) for (ui32 phase : {0u, 1u, 2u}) {
            TTestContext ctx({.EnableChecksums = phase != 0}, NLog::PRI_ERROR, 1, std::nullopt, true);
            const auto creds = Connect(ctx, 992, 1);
            auto write = std::make_unique<NDDisk::TEvWrite>(creds,
                NDDisk::TBlockSelector(0, 0, MinBlockSize), NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(MakeAlignedRope(MakeData('A', MinBlockSize)));
            AssertStatus<NDDisk::TEvWriteResult>(ctx.SendAndGrab<NDDisk::TEvWriteResult>(write.release()), TReplyStatus::OK);
            ctx.WaitForReservationsSettled();
            auto before = ctx.SendAndGrab<TEvGateState>(new TEvControlGate(NPDisk::TEvLogResult::EventType));
            UNIT_ASSERT(before->Get()->Router && before->Get()->IoCompletions > 0);
            ctx.Send(new NDDisk::TEvDeleteTabletChunks(creds), 501);
            WaitNativeGate(ctx);
            if (phase == 2) {
                // Release phase one and arm phase two in the same activation.
                ctx.SendAndGrab<TEvGateState>(new TEvControlGate(NPDisk::TEvLogResult::EventType, true));
                WaitNativeGate(ctx);
            }
            if (broken) {
                ctx.SendAndGrab<TEvGateState>(new TEvControlGate(0, false, true));
            }
            else {
                ctx.Send(new NPDisk::TEvLogResult(NKikimrProto::INVALID_ROUND, 0, "native session loss", 0));
            }
            auto reply = ctx.Grab<NDDisk::TEvDeleteTabletChunksResult>();
            AssertStatus<NDDisk::TEvDeleteTabletChunksResult>(reply, broken ? TReplyStatus::ERROR : TReplyStatus::SESSION_MISMATCH);
            UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, 501);
            ctx.SendAndGrab<TEvGateState>(new TEvControlGate(0, true));
            // A same-type reply barrier catches a duplicate completion without a timed quiet window.
            ctx.Send(new NDDisk::TEvDeleteTabletChunks(creds), 502);
            auto barrier = ctx.Grab<NDDisk::TEvDeleteTabletChunksResult>();
            UNIT_ASSERT_VALUES_EQUAL(barrier->Cookie, 502);
            AssertStatus<NDDisk::TEvDeleteTabletChunksResult>(barrier, broken ? TReplyStatus::ERROR : TReplyStatus::SESSION_MISMATCH);
            ctx.StopDDisk(0);
        }
    }

    Y_UNIT_TEST(DisjointNativeSyncSourcesRetainCompletePayloads_Uring) {
        if (!NPDisk::RequireUring()) {
            return;
        }
        for (bool checksums : {false, true}) for (bool partial : {false, true}) {
            TTestContext ctx({.EnableChecksums = checksums}, NLog::PRI_ERROR, 1, std::nullopt, true);
            ctx.AddDisk();
            const auto creds = Connect(ctx, 993, 1);
            const auto source = ConnectTo(ctx, 1, 993, 1);
            const TString payload = MakeData('S', 3 * MinBlockSize);
            auto write = std::make_unique<NDDisk::TEvWrite>(source,
                NDDisk::TBlockSelector(0, 0, payload.size()), NDDisk::TWriteInstruction(0));
            write->AddPayloadThenChecksum(MakeAlignedRope(payload));
            AssertStatus<NDDisk::TEvWriteResult>(ctx.SendToAndGrab<NDDisk::TEvWriteResult>(1, write.release()), TReplyStatus::OK);
            auto sourceState = ctx.SendToAndGrab<TEvGateState>(1, new TEvControlGate());
            UNIT_ASSERT(sourceState->Get()->Router && sourceState->Get()->IoCompletions > 0);
            ctx.SendAndGrab<TEvGateState>(new TEvControlGate(NDDisk::TEvReadResult::EventType));

            auto sync = [&](ui32 offset, ui32 size, ui64 cookie) {
                auto request = std::make_unique<NDDisk::TEvSync>(creds);
                request->AddSegmentFromDDisk({ctx.NodeId, ctx.Disks[1].PDiskId, ctx.Disks[1].SlotId},
                    *source.DDiskInstanceGuid, {0, offset, size});
                ctx.Send(request.release(), cookie);
            };
            sync(0, payload.size(), 501);
            WaitNativeGate(ctx);
            const ui32 secondSize = partial ? MinBlockSize : 2 * MinBlockSize;
            const TString newer = MakeData('N', secondSize);
            auto overwrite = std::make_unique<NDDisk::TEvWrite>(source,
                NDDisk::TBlockSelector(0, 3 * MinBlockSize, newer.size()), NDDisk::TWriteInstruction(0));
            overwrite->AddPayloadThenChecksum(MakeAlignedRope(newer));
            AssertStatus<NDDisk::TEvWriteResult>(ctx.SendToAndGrab<NDDisk::TEvWriteResult>(1, overwrite.release()), TReplyStatus::OK);
            sync(3 * MinBlockSize, secondSize, 502);
            WaitNativeGate(ctx, 2);
            ctx.SendAndGrab<TEvGateState>(new TEvControlGate(0, true));
            std::set<ui64> cookies;
            for (ui32 i = 0; i < 2; ++i) {
                auto result = ctx.Grab<NDDisk::TEvSyncResult>();
                AssertStatus<NDDisk::TEvSyncResult>(result, TReplyStatus::OK);
                UNIT_ASSERT(cookies.insert(result->Cookie).second);
                UNIT_ASSERT(result->Get()->Record.GetSegmentResults(0).GetStatus() == TReplyStatus::OK);
            }
            AssertReadResult(ctx.SendAndGrab<NDDisk::TEvReadResult>(
                new NDDisk::TEvRead(creds, {0, 0, static_cast<ui32>(payload.size() + newer.size())}, {true})),
                payload + newer, checksums);
            auto state = ctx.SendAndGrab<TEvGateState>(new TEvControlGate());
            UNIT_ASSERT(state->Get()->Router && state->Get()->IoCompletions > 0);
            UNIT_ASSERT_VALUES_EQUAL(state->Get()->Held, 0);
            ctx.StopDDisk(0);
            ctx.StopDDisk(1);
        }
    }

    Y_UNIT_TEST(StartupRepairsAbandonedReservations_PDiskFallback) {
        TestShutdownReleasesReservations({.ForcePDiskFallback = true}, true);
    }

    Y_UNIT_TEST(StartupRepairsAbandonedReservationsWithoutChecksums_PDiskFallback) {
        TestShutdownReleasesReservations({.ForcePDiskFallback = true, .EnableChecksums = false}, true);
    }

    Y_UNIT_TEST(StartupRepairsAbandonedReservations_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestShutdownReleasesReservations({}, true);
    }

    Y_UNIT_TEST(StartupRepairsAbandonedReservationsWithoutChecksums_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestShutdownReleasesReservations({.EnableChecksums = false}, true);
    }

    Y_UNIT_TEST(ShutdownReleasesReservations_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestShutdownReleasesReservations({});
    }

    Y_UNIT_TEST(ShutdownReleasesReservations_PDiskFallback) {
        TestShutdownReleasesReservations({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ShutdownReleasesReservationsWithoutChecksums_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestShutdownReleasesReservations({.EnableChecksums = false});
    }

    Y_UNIT_TEST(ShutdownReleasesReservationsWithoutChecksums_PDiskFallback) {
        TestShutdownReleasesReservations({.ForcePDiskFallback = true, .EnableChecksums = false});
    }
    Y_UNIT_TEST(WriteAndReadUnalignedPayload_Uring) {
        TestWriteAndReadPayloadLayout({}, EPayloadLayout::Unaligned);
    }

    Y_UNIT_TEST(WriteAndReadUnalignedPayload_PDiskFallback) {
        TestWriteAndReadPayloadLayout({.ForcePDiskFallback = true}, EPayloadLayout::Unaligned);
    }

    Y_UNIT_TEST(WriteAndReadFragmentedUnalignedPayload_Uring) {
        TestWriteAndReadPayloadLayout({}, EPayloadLayout::FragmentedUnaligned);
    }

    Y_UNIT_TEST(WriteAndReadFragmentedUnalignedPayload_PDiskFallback) {
        TestWriteAndReadPayloadLayout({.ForcePDiskFallback = true}, EPayloadLayout::FragmentedUnaligned);
    }

    Y_UNIT_TEST(WriteAndReadFragmentedAlignedPayload_Uring) {
        TestWriteAndReadPayloadLayout({}, EPayloadLayout::FragmentedAligned);
    }

    Y_UNIT_TEST(WriteAndReadFragmentedAlignedPayload_PDiskFallback) {
        TestWriteAndReadPayloadLayout({.ForcePDiskFallback = true}, EPayloadLayout::FragmentedAligned);
    }

    Y_UNIT_TEST(WriteAndRead_4KiB_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestWriteAndRead({}, 4_KB);
    }

    Y_UNIT_TEST(WriteAndRead_4KiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 4_KB);
    }

    Y_UNIT_TEST(WriteAndRead_8KiB_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestWriteAndRead({}, 8_KB);
    }

    Y_UNIT_TEST(WriteAndRead_8KiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 8_KB);
    }

    Y_UNIT_TEST(WriteAndRead_1MiB_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestWriteAndRead({}, 1_MB);
    }

    Y_UNIT_TEST(WriteAndRead_1MiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 1_MB);
    }

    Y_UNIT_TEST(WriteAndReadWithoutChecksums_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestWriteAndReadWithoutChecksums({});
    }

    Y_UNIT_TEST(WriteAndReadWithoutChecksums_PDiskFallback) {
        TestWriteAndReadWithoutChecksums({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(CheckVChunksArePerTablet_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestCheckVChunksArePerTablet({});
    }

    Y_UNIT_TEST(CheckVChunksArePerTablet_PDiskFallback) {
        TestCheckVChunksArePerTablet({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(OverwriteSameOffset_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestOverwrite({});
    }

    Y_UNIT_TEST(OverwriteSameOffset_PDiskFallback) {
        TestOverwrite({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ReadUnallocatedChunk_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestReadUnallocatedChunk({});
    }

    Y_UNIT_TEST(ReadUnallocatedChunk_PDiskFallback) {
        TestReadUnallocatedChunk({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ManyVChunksPerTablet_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestManyVChunks({});
    }

    Y_UNIT_TEST(ManyVChunksPerTablet_PDiskFallback) {
        TestManyVChunks({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWrites_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestMultiTabletInterleaved({});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWrites_PDiskFallback) {
        TestMultiTabletInterleaved({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWritesWithDDiskRestart_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestMultiTabletInterleavedWritesWithDDiskRestart({});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWritesWithDDiskRestart_PDiskFallback) {
        TestMultiTabletInterleavedWritesWithDDiskRestart({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(MultipleRestarts_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestMultipleRestarts({});
    }

    Y_UNIT_TEST(MultipleRestarts_PDiskFallback) {
        TestMultipleRestarts({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(OverwriteAfterRestart_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestOverwriteAfterRestart({});
    }

    Y_UNIT_TEST(OverwriteAfterRestart_PDiskFallback) {
        TestOverwriteAfterRestart({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(EmptyRestart_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestEmptyRestart({});
    }

    Y_UNIT_TEST(EmptyRestart_PDiskFallback) {
        TestEmptyRestart({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ConnectionTokenAcrossRestart) {
        TestConnectionTokenAcrossRestart();
    }

    Y_UNIT_TEST(RestartAfterCutLog_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestRestartAfterCutLog({});
    }

    Y_UNIT_TEST(RestartAfterCutLog_PDiskFallback) {
        TestRestartAfterCutLog({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ReadWithoutConnect_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestReadWithoutConnect({});
    }

    Y_UNIT_TEST(ReadWithoutConnect_PDiskFallback) {
        TestReadWithoutConnect({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(PDiskRestartWithReservedChunks_DDiskZombie_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestPDiskRestartWithReservedChunks({}, /*restartDDisk=*/false);
    }

    Y_UNIT_TEST(PDiskRestartWithReservedChunks_DDiskZombie_PDiskFallback) {
        TestPDiskRestartWithReservedChunks({.ForcePDiskFallback = true}, /*restartDDisk=*/false);
    }

    Y_UNIT_TEST(PDiskRestartWithReservedChunks_DDiskRestart_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestPDiskRestartWithReservedChunks({}, /*restartDDisk=*/true);
    }

    Y_UNIT_TEST(PDiskRestartWithReservedChunks_DDiskRestart_PDiskFallback) {
        TestPDiskRestartWithReservedChunks({.ForcePDiskFallback = true}, /*restartDDisk=*/true);
    }

    Y_UNIT_TEST(Smoke_2Tablets_2VChunks_1Segment) {
        TestSync(2, 2, 8, 1);
    }

    Y_UNIT_TEST(PhysicalChunkSizeFullChunkIo_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestPhysicalChunkSizeFullChunkIo({});
    }

    Y_UNIT_TEST(PhysicalChunkSizeFullChunkIo_PDiskFallback) {
        TestPhysicalChunkSizeFullChunkIo({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(DeleteTabletChunks_Uring) {
        if (!NPDisk::RequireUring()) { return; }
        TestDeleteTabletChunks({});
    }

    Y_UNIT_TEST(DeleteTabletChunks_PDiskFallback) {
        TestDeleteTabletChunks({.ForcePDiskFallback = true});
    }
}

} // NKikimr
