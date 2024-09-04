#include "defs.h"

#include "blobstorage_pdisk_chunk_id_formatter.h"
#include "blobstorage_pdisk_data.h"
#include "blobstorage_pdisk_driveestimator.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_sectorrestorator.h"
#include "blobstorage_pdisk_state.h"
#include "blobstorage_pdisk_tools.h"
#include "blobstorage_pdisk_ut_defs.h"
#include "blobstorage_pdisk_util_atomicblockcounter.h"
#include "blobstorage_pdisk_util_sector.h"

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/pdisk_io/wcache.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/null.h>
#include <util/system/tempfile.h>
#include <cstring>

namespace NKikimr { namespace NPDisk {

Y_UNIT_TEST_SUITE(TPDiskUtil) {

    Y_UNIT_TEST(AtomicBlockCounterFunctional) {
        TAtomicBlockCounter counter;
        UNIT_ASSERT_EQUAL(counter.Get(), 0);
        UNIT_ASSERT_EQUAL(counter.Increment(), 1);
        UNIT_ASSERT_EQUAL(counter.Add(20), 21);
        UNIT_ASSERT_EQUAL(counter.Decrement(), 20);
        UNIT_ASSERT_EQUAL(counter.Sub(10), 10);
        counter.BlockA();
        UNIT_ASSERT_EQUAL(counter.IsBlocked(), true);
        UNIT_ASSERT_EQUAL(counter.Sub(5), 5);
        UNIT_ASSERT_EQUAL(counter.Increment(), 0);
        UNIT_ASSERT_EQUAL(counter.Add(5), 0);
        counter.UnblockB();
        UNIT_ASSERT_EQUAL(counter.Add(5), 0);
        counter.BlockB();
        UNIT_ASSERT_EQUAL(counter.Add(5), 0);
        counter.UnblockA();
        UNIT_ASSERT_EQUAL(counter.Add(5), 0);
        counter.UnblockB();
        UNIT_ASSERT_EQUAL(counter.Add(5), 10);
    }

    Y_UNIT_TEST(AtomicBlockCounterSeqno) {
        TAtomicBlockCounter counter;
        TAtomicBlockCounter::TResult res;
        counter.BlockA(res);
        ui16 s = res.Seqno;

        // Block functional
        counter.BlockA(res);
        UNIT_ASSERT_EQUAL(res.Seqno, s);
        counter.UnblockA(res);
        UNIT_ASSERT_EQUAL(res.Seqno, ++s);
        counter.UnblockA(res);
        UNIT_ASSERT_EQUAL(res.Seqno, s);
        counter.UnblockB(res);
        UNIT_ASSERT_EQUAL(res.Seqno, s);
        counter.BlockB(res);
        UNIT_ASSERT_EQUAL(res.Seqno, ++s);
        counter.BlockB(res);
        UNIT_ASSERT_EQUAL(res.Seqno, s);
        counter.UnblockB(res);
        UNIT_ASSERT_EQUAL(res.Seqno, ++s);

        // Threshold Ops functional
        counter.ThresholdAdd(10, 100, res);
        UNIT_ASSERT_EQUAL(res.Seqno, ++s);
        counter.ThresholdSub(2, 100, res);
        UNIT_ASSERT_EQUAL(res.Seqno, ++s);

        // Seqno overflow
        ui16 se = s - 2;
        int i = 0;
        while (s != se) {
            if (i % 2 == 0) {
                counter.BlockB(res);
            } else {
                counter.UnblockB(res);
            }
            UNIT_ASSERT_EQUAL(res.Seqno, ++s);
            i++;
        }
    }

    Y_UNIT_TEST(Light) {
        TLight l;
        TIntrusivePtr<::NMonitoring::TDynamicCounters> c(new ::NMonitoring::TDynamicCounters());
        l.Initialize(c, "l");
        auto state = c->GetCounter("l_state");
        auto count = c->GetCounter("l_count");

        // functional
        l.Set(true, 1);
        UNIT_ASSERT_EQUAL(state->Val(), 1);
        UNIT_ASSERT_EQUAL(count->Val(), 1);
        l.Set(false, 2);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 1);
        l.Set(false, 3);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 1);
        l.Set(true, 4);
        UNIT_ASSERT_EQUAL(state->Val(), 1);
        UNIT_ASSERT_EQUAL(count->Val(), 2);
        l.Set(true, 5);
        UNIT_ASSERT_EQUAL(state->Val(), 1);
        UNIT_ASSERT_EQUAL(count->Val(), 2);

        // Single reordering
        l.Set(true, 7);
        UNIT_ASSERT_EQUAL(state->Val(), 1);
        UNIT_ASSERT_EQUAL(count->Val(), 2);
        l.Set(false, 6);
        UNIT_ASSERT_EQUAL(state->Val(), 1);
        UNIT_ASSERT_EQUAL(count->Val(), 3);

        // Multiple reorderings
        l.Set(false, 8);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 3);
        l.Set(true, 12);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 3);
        l.Set(false, 13);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 3);
        l.Set(true, 14);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 3);
        l.Set(false, 11);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 3);
        l.Set(true, 10);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 3);
        l.Set(true, 9);
        UNIT_ASSERT_EQUAL(state->Val(), 1);
        UNIT_ASSERT_EQUAL(count->Val(), 6);

        // Multiple reorderings with pause
        l.Set(false, 15);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 6);
        l.Set(false, 17);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 6);
        l.Set(true, 18);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 6);
        l.Set(true, 20);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 6);
        l.Set(true, 22);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 6);
        l.Set(false, 21);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 6);
        l.Set(true, 16);
        UNIT_ASSERT_EQUAL(state->Val(), 1);
        UNIT_ASSERT_EQUAL(count->Val(), 8);
        l.Set(false, 19);
        UNIT_ASSERT_EQUAL(state->Val(), 1);
        UNIT_ASSERT_EQUAL(count->Val(), 10);

        // Resizing
        l.Set(false, 23);
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 10);
        ui16 seqno = 24; // skip one
        for (int i = 0; i < 199; i++) {
            l.Set(i % 2 == 1 , ++seqno);
            UNIT_ASSERT_EQUAL(count->Val(), 10);
        }
        l.Set(true, 24); // place missed one
        UNIT_ASSERT_EQUAL(state->Val(), 0);
        UNIT_ASSERT_EQUAL(count->Val(), 110);

        { // Seqno overflow
            ui64 N = 3ull << 16ull;
            i64 cnt = count->Val();
            for (ui64 i = 0; i < N; i++) {
                bool st = (i % 3 == 0);
                l.Set(st, ++seqno);
                if (st) {
                    cnt++;
                }
                UNIT_ASSERT_EQUAL(count->Val(), cnt);
            }
            UNIT_ASSERT_EQUAL(state->Val(), 0);
        }

        { // Seqno overflow and reorderings with max possible size
            i64 cnt = count->Val();
            i64 cntStart = cnt;
            ui16 missedSeqno = ++seqno;
            bool st = false;
            UNIT_ASSERT(missedSeqno > 10);
            while (seqno != missedSeqno - 2) { // one for missed and one for initial
                bool stPrev = st;
                st = (seqno % 5 == 0);
                l.Set(st, ++seqno);
                if (st && !stPrev) {
                    cnt++;
                }
                UNIT_ASSERT_EQUAL(count->Val(), cntStart);
            }
            l.Set(false, missedSeqno); // place missed one
            UNIT_ASSERT_EQUAL((bool)state->Val(), st);
            UNIT_ASSERT_EQUAL(count->Val(), cnt);
        }
    }

    Y_UNIT_TEST(LightOverflow) {
        TLight l;
        TIntrusivePtr<::NMonitoring::TDynamicCounters> c(new ::NMonitoring::TDynamicCounters());
        l.Initialize(c, "l");
        auto state = c->GetCounter("l_state");
        auto count = c->GetCounter("l_count");

        { // Seqno overflow
            ui64 seqno = 0;
            ui64 N = 3ull << 16ull;
            for (ui64 i = 0; i < N; i++) {
                l.Set(false, seqno);
                ++seqno;
            }
            UNIT_ASSERT_EQUAL(state->Val(), 0);
        }
    }

    Y_UNIT_TEST(DriveEstimator) {
        TTempFileHandle file;
        file.Resize(1 << 30);
        TDriveEstimator estimator(file.Name());
        TDriveModel model = estimator.EstimateDriveModel();
        UNIT_ASSERT_UNEQUAL(model.Speed(TDriveModel::OP_TYPE_AVG), 0);
        UNIT_ASSERT_UNEQUAL(model.SeekTimeNs(), 0);
    }

void TestOffset(ui64 offset, ui64 size, ui64 expectedFirstSector, ui64 expectedLastSector,
        ui64 expectedSectorOffset) {
    TDiskFormat format;
    format.Clear();
    format.SectorSize = 4096;
    format.FormatFlags &= ~EFormatFlags::FormatFlagErasureEncodeUserChunks;

    ui64 firstSector;
    ui64 lastSector;
    ui64 sectorOffset;
    bool isOk = ParseSectorOffset(format, nullptr, 0, offset, size, firstSector, lastSector, sectorOffset);
    UNIT_ASSERT_C(isOk && firstSector == expectedFirstSector && lastSector == expectedLastSector &&
            sectorOffset == expectedSectorOffset,
            "isOk# " << isOk << "\n"
            "offset# " << offset << " size# " << size << "\n"
            "firstSector# " << firstSector << " expectedFirstSector# " << expectedFirstSector << "\n"
            "lastSector# " << lastSector << " expectedLastSector# " << expectedLastSector << "\n"
            "sectorOffset# " << sectorOffset << " expectedSectorOffset# " << expectedSectorOffset << "\n"
            );
}

    Y_UNIT_TEST(OffsetParsingCorrectness) {
        TDiskFormat format;
        format.Clear();
        format.SectorSize = 4096;
        const ui64 sectorPayload = format.SectorPayloadSize();

        TestOffset(0, sectorPayload*15, 0, 14, 0);

        TestOffset(0, sectorPayload*15, 0, 14, 0);

        const ui64 size = sectorPayload * LogErasureDataParts;
        for (ui64 offset = 1; offset < size; ++offset) {
            const ui64 lastSector = (offset + size + sectorPayload - 1) / sectorPayload - 1;
            TestOffset(offset, size, offset / sectorPayload, lastSector, offset % sectorPayload);
        }

        TestOffset(4123, 4012*13, 1, 13, 59);

        TestOffset(4123, sectorPayload*14, 1, 15, 59);

        TestOffset(4123, 4063*13, 1, 14, 59);
    }

void TestPayloadOffset(ui64 firstSector, ui64 lastSector, ui64 currentSector, ui64 expectedPayloadSize,
        ui64 expectedPayloadOffset) {
    TDiskFormat format;
    format.Clear();
    format.SectorSize = 4096;
    format.FormatFlags &= ~EFormatFlags::FormatFlagErasureEncodeUserChunks;

    ui64 payloadSize;
    ui64 payloadOffset;
    ParsePayloadFromSectorOffset(format, firstSector, lastSector, currentSector, &payloadSize, &payloadOffset);
    UNIT_ASSERT_C(payloadSize == expectedPayloadSize && payloadOffset == expectedPayloadOffset,
            "firstSector# " << firstSector << " lastSector# " << lastSector << " currentSector# " << currentSector << "\n"
            "payloadSize# " << payloadSize << " expectedPayloadSize# " << expectedPayloadSize << "\n"
            "payloadOffset# " << payloadOffset << " expectedPayloadOffset# " << expectedPayloadOffset << "\n"
            );
}

    Y_UNIT_TEST(PayloadParsingTest) {
        TDiskFormat format;
        format.Clear();
        format.SectorSize = 4096;
        const ui64 sectorPayload = format.SectorPayloadSize();

        TestPayloadOffset(1, 1, 1, sectorPayload, 0);
        TestPayloadOffset(1, 2, 2, sectorPayload, sectorPayload);

        TestPayloadOffset(0, 15, 0, 16 * sectorPayload, 0);
        TestPayloadOffset(0, 15, 0, 16 * sectorPayload, 0);

        TestPayloadOffset(0, 15, 14, 2 * sectorPayload, 14 * sectorPayload);
        TestPayloadOffset(0, 15, 15, sectorPayload, 15 * sectorPayload);

        TestPayloadOffset(13, 15, 13, 3 * sectorPayload, 0);
        TestPayloadOffset(13, 15, 14, 2 * sectorPayload, sectorPayload);
    }

    Y_UNIT_TEST(SectorRestorator) {
        TDiskFormat format;
        format.Clear();
        TSectorsWithData sectors(format.SectorSize, LogErasureDataParts + 1);
        constexpr ui64 magic = 0x123951924;
        ui64 nonce = 1;
        for (ui32 i = 0; i < LogErasureDataParts + 1; ++i) {
            memset(sectors[i].Begin(), 0, sectors[i].Size());
            sectors[i].SetCanary();
            auto *footer = sectors[i].GetDataFooter();
            footer->Version = PDISK_DATA_VERSION;
            footer->Nonce = nonce++;
            NPDisk::TPDiskHashCalculator hasher;
            if (i < LogErasureDataParts) {
                ui64 offset = format.SectorSize * i;
                footer->Hash = hasher.HashSector(offset, magic, sectors[i].Begin(), sectors[i].Size());
            }
        }
        TSectorRestorator restorator(false, LogErasureDataParts, true, format);
        restorator.Restore(sectors.Data(), 0, magic, 0, 0);
        UNIT_ASSERT_C(restorator.GoodSectorCount == LogErasureDataParts + 1,
                "restorator.GoodSectorCount# " << restorator.GoodSectorCount);
    }

    Y_UNIT_TEST(SectorRestoratorOldNewHash) {
        TDiskFormat format;
        format.Clear();
        TSectorsWithData sectors(format.SectorSize, 3);
        const ui64 magic = 0x123951924;
        const ui64 offset = format.SectorSize * 17;
        ui64 nonce = 1;
        for (ui32 i = 1; i < sectors.Size(); ++i) {
            memset(sectors[i].Begin(), 13, sectors[i].Size());
            sectors[i].SetCanary();
            auto *footer = sectors[i].GetDataFooter();
            footer->Version = PDISK_DATA_VERSION;
            footer->Nonce = nonce++;
            NPDisk::TPDiskHashCalculator hasher;
            switch (i) {
            case 1:
                footer->Hash = hasher.T1ha0HashSector<TT1ha0NoAvxHasher>(offset, magic, sectors[i].Begin(), sectors[i].Size());
                break;
            case 2:
                footer->Hash = hasher.HashSector(offset, magic, sectors[i].Begin(), sectors[i].Size());
                break;
            default:
                UNIT_ASSERT(false);
            }
            TSectorRestorator restorator(false, 1, false, format);
            restorator.Restore(sectors[i].Begin(), offset, magic, 0, 0);
            UNIT_ASSERT_C(restorator.GoodSectorCount == 1, "i# " << i
                    << " GoodSectorCount# " << restorator.GoodSectorCount);
        }
    }

    Y_UNIT_TEST(SectorPrint) {
        TSectorsWithData sectors(97, 1);
        memset(sectors[0].Begin(), 0, sectors[0].Size());
        sectors[0][0] = 12;
        sectors[0][1] = 9;
        sectors[0].SetCanary();
        Cnull << sectors[0].ToString();
    }

    Y_UNIT_TEST(TChunkIdFormatter) {
        auto test = [] (const TDeque<ui32>& in, const TString& expect) {
            TStringStream ss;
            TChunkIdFormatter(ss).PrintBracedChunksList(in);
            UNIT_ASSERT_EQUAL_C(ss.Str(), expect, " got# " << ss.Str().Quote() << " expect# " << expect.Quote());
        };

        test({1}, "{1}");
        test({1, 2}, "{1, 2}");
        test({1, 2, 3}, "{1..3}");
        test({1, 3, 5}, "{1, 3, 5}");
        test({1, 2, 3, 5, 6, 7}, "{1..3, 5..7}");
    }

    Y_UNIT_TEST(TOwnerPrintTest) {
        TStringStream ss;
        ss << TOwner(0) << " " << TOwner(3) << " " << TOwner(124) << " " << TOwner(231);
        TString expect =  "0 3 124 231";
        UNIT_ASSERT_EQUAL_C(ss.Str(), expect, " got# " << ss.Str().Quote() << " expect# " << expect.Quote());
    }

    Y_UNIT_TEST(TChunkStateEnumPrintTest) {
        TStringStream ss;
        ss << TChunkState::DATA_RESERVED;
        TString expect = "DATA_RESERVED";
        UNIT_ASSERT_EQUAL_C(ss.Str(), expect, " got# " << ss.Str().Quote() << " expect# " << expect.Quote());
    }

    Y_UNIT_TEST(TIoResultEnumPrintTest) {
        TStringStream ss;
        ss << EIoResult::Ok << " ";
        ss << EIoResult::TryAgain << " ";
        ss << EIoResult::FileLockError;
        TString expect = "Ok TryAgain FileLockError";
        UNIT_ASSERT_EQUAL_C(ss.Str(), expect, " got# " << ss.Str().Quote() << " expect# " << expect.Quote());
    }

    Y_UNIT_TEST(TIoTypeEnumPrintTest) {
        TStringStream ss;
        ss << IAsyncIoOperation::EType::PRead << " ";
        ss << IAsyncIoOperation::EType::PWrite << " ";
        ss << IAsyncIoOperation::EType::PTrim;
        TString expect = "PRead PWrite PTrim";
        UNIT_ASSERT_EQUAL_C(ss.Str(), expect, " got# " << ss.Str().Quote() << " expect# " << expect.Quote());
    }

    Y_UNIT_TEST(TestNVMeSerial) {
        TString path = "/dev/disk/by-partlabel/kikimr_nvme_01";
        TStringStream details;
        if (std::optional<NPDisk::TDriveData> data = NPDisk::GetDriveData(path, &details)) {
            Cout << "data# " << data->ToString(false) << Endl;
        } else {
            Cout << "error, details# " << details.Str() << Endl;
        }
    }

    Y_UNIT_TEST(TestDeviceList) {
        TStringStream details;
        for(const NPDisk::TDriveData& data : ListDevicesWithPartlabel(details)) {
            Cout << "data# " << data.ToString(false) << Endl;
        }
    }

    Y_UNIT_TEST(TestBufferPool) {
        TBufferPoolCommon pool(512, 10, TBufferPool::TPDiskParams{});

        for (ui32 i = 0; i < 100; ++i) {
            TBuffer::TPtr buffers{pool.Pop()};
        }

        std::vector<TBuffer::TPtr> buffers;
        for (ui32 i = 0; i < 20; ++i) {
            buffers.emplace_back(pool.Pop());
        }
    }

    Y_UNIT_TEST(SectorMap) {
        TIntrusivePtr<TSectorMap> sectorMap(new TSectorMap(1024*1024));
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));
        TActorSystemCreator creator;
        THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDeviceWithDefaults("SectorMap:123", *mon,
                    NPDisk::TDeviceMode::LockFile, sectorMap, creator.GetActorSystem()));
        ui32 size = 4096;
        TAlignedData data(size);
        TAlignedData readData(size);
        memset(data.Get(), 1, size);
        device->PwriteSync(data.Get(), size, 0, {}, {});
        device->PreadSync(readData.Get(), size, 0, {}, {});
        UNIT_ASSERT(memcmp(data.Get(), readData.Get(), size) == 0);
    }

    Y_UNIT_TEST(FormatSectorMap) {
        TIntrusivePtr<TSectorMap> sectorMap(new TSectorMap(1024*1024*1024));
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;

        NPDisk::TKey chunkKey{};
        NPDisk::TKey logKey{};
        NPDisk::TKey sysLogKey{};

        TPDiskConfig cfg("SectorMap:1024042", 12345, 0, {});
        FormatPDisk(cfg.Path, 0, 4096, MIN_CHUNK_SIZE, cfg.PDiskGuid, chunkKey, logKey, sysLogKey,
                YdbDefaultPDiskSequence, TString(), false, false, sectorMap);
    }

    Y_UNIT_TEST(SectorMapStoreLoadFromFile) {
        TIntrusivePtr<TSectorMap> sectorMap(new TSectorMap(1024*1024));
        TTempFileHandle tmp;

        ui32 size = 1024*1024;
        TAlignedData data(size);
        for (ui32 i = 0; i < size; ++i) {
            data.Get()[i] = i % 139;
        }
        memset(data.Get(), 0x23, size);
        sectorMap->Write(data.Get(), size, 4096);

        sectorMap->StoreToFile(tmp.Name());
        sectorMap->LoadFromFile(tmp.Name());

        TAlignedData readData(size);
        sectorMap->Read(readData.Get(), size, 4096);
        UNIT_ASSERT(memcmp(data.Get(), readData.Get(), size) == 0);
    }
}

}} // namespace NKikimr // namespace NPDisk
