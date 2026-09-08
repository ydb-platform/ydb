#include "ddisk_actor_pdisk_common_ut.h"

namespace NKikimr {

namespace {

enum class EPayloadLayout {
    Unaligned,
    FragmentedUnaligned,
    FragmentedAligned,
};

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

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDDiskActorPDiskTest) {
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
        TestWriteAndRead({}, 4_KB);
    }

    Y_UNIT_TEST(WriteAndRead_4KiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 4_KB);
    }

    Y_UNIT_TEST(WriteAndRead_8KiB_Uring) {
        TestWriteAndRead({}, 8_KB);
    }

    Y_UNIT_TEST(WriteAndRead_8KiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 8_KB);
    }

    Y_UNIT_TEST(WriteAndRead_1MiB_Uring) {
        TestWriteAndRead({}, 1_MB);
    }

    Y_UNIT_TEST(WriteAndRead_1MiB_PDiskFallback) {
        TestWriteAndRead({.ForcePDiskFallback = true}, 1_MB);
    }

    Y_UNIT_TEST(WriteAndReadWithoutChecksums_Uring) {
        TestWriteAndReadWithoutChecksums({});
    }

    Y_UNIT_TEST(WriteAndReadWithoutChecksums_PDiskFallback) {
        TestWriteAndReadWithoutChecksums({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(CheckVChunksArePerTablet_Uring) {
        TestCheckVChunksArePerTablet({});
    }

    Y_UNIT_TEST(CheckVChunksArePerTablet_PDiskFallback) {
        TestCheckVChunksArePerTablet({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(OverwriteSameOffset_Uring) {
        TestOverwrite({});
    }

    Y_UNIT_TEST(OverwriteSameOffset_PDiskFallback) {
        TestOverwrite({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ReadUnallocatedChunk_Uring) {
        TestReadUnallocatedChunk({});
    }

    Y_UNIT_TEST(ReadUnallocatedChunk_PDiskFallback) {
        TestReadUnallocatedChunk({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ManyVChunksPerTablet_Uring) {
        TestManyVChunks({});
    }

    Y_UNIT_TEST(ManyVChunksPerTablet_PDiskFallback) {
        TestManyVChunks({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWrites_Uring) {
        TestMultiTabletInterleaved({});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWrites_PDiskFallback) {
        TestMultiTabletInterleaved({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWritesWithDDiskRestart_Uring) {
        TestMultiTabletInterleavedWritesWithDDiskRestart({});
    }

    Y_UNIT_TEST(MultiTabletInterleavedWritesWithDDiskRestart_PDiskFallback) {
        TestMultiTabletInterleavedWritesWithDDiskRestart({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(MultipleRestarts_Uring) {
        TestMultipleRestarts({});
    }

    Y_UNIT_TEST(MultipleRestarts_PDiskFallback) {
        TestMultipleRestarts({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(OverwriteAfterRestart_Uring) {
        TestOverwriteAfterRestart({});
    }

    Y_UNIT_TEST(OverwriteAfterRestart_PDiskFallback) {
        TestOverwriteAfterRestart({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(EmptyRestart_Uring) {
        TestEmptyRestart({});
    }

    Y_UNIT_TEST(EmptyRestart_PDiskFallback) {
        TestEmptyRestart({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ConnectionTokenAcrossRestart) {
        TestConnectionTokenAcrossRestart();
    }

    Y_UNIT_TEST(RestartAfterCutLog_Uring) {
        TestRestartAfterCutLog({});
    }

    Y_UNIT_TEST(RestartAfterCutLog_PDiskFallback) {
        TestRestartAfterCutLog({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(ReadWithoutConnect_Uring) {
        TestReadWithoutConnect({});
    }

    Y_UNIT_TEST(ReadWithoutConnect_PDiskFallback) {
        TestReadWithoutConnect({.ForcePDiskFallback = true});
    }

    Y_UNIT_TEST(PDiskRestartWithReservedChunks_DDiskZombie_Uring) {
        TestPDiskRestartWithReservedChunks({}, /*restartDDisk=*/false);
    }

    Y_UNIT_TEST(PDiskRestartWithReservedChunks_DDiskZombie_PDiskFallback) {
        TestPDiskRestartWithReservedChunks({.ForcePDiskFallback = true}, /*restartDDisk=*/false);
    }

    Y_UNIT_TEST(PDiskRestartWithReservedChunks_DDiskRestart_Uring) {
        TestPDiskRestartWithReservedChunks({}, /*restartDDisk=*/true);
    }

    Y_UNIT_TEST(PDiskRestartWithReservedChunks_DDiskRestart_PDiskFallback) {
        TestPDiskRestartWithReservedChunks({.ForcePDiskFallback = true}, /*restartDDisk=*/true);
    }

    Y_UNIT_TEST(Smoke_2Tablets_2VChunks_1Segment) {
        TestSync(2, 2, 8, 1);
    }

    Y_UNIT_TEST(DeleteTabletChunks_Uring) {
        TestDeleteTabletChunks({});
    }

    Y_UNIT_TEST(DeleteTabletChunks_PDiskFallback) {
        TestDeleteTabletChunks({.ForcePDiskFallback = true});
    }
}

} // NKikimr
