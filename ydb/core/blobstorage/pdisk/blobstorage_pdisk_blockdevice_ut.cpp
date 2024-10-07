#include "defs.h"

#include "blobstorage_pdisk_blockdevice.h"
#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_actorsystem_creator.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_ut_defs.h"
#include "blobstorage_pdisk_ut_helpers.h"

#include <ydb/core/control/immediate_control_board_wrapper.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/condvar.h>
#include <util/string/printf.h>
#include <util/system/file.h>
#include <util/system/sanitizers.h>

namespace NKikimr {

class TWriter : public NPDisk::TCompletionAction {
    NPDisk::IBlockDevice &Device;
    NPDisk::TBuffer *Buffer;
    const i32 GenerationsToSpawn;
    TAtomic *Counter;
public:
    TWriter(NPDisk::IBlockDevice &device, NPDisk::TBuffer *data, const i32 generationsToSpawn, TAtomic *counter)
        : Device(device)
        , Buffer(data)
        , GenerationsToSpawn(generationsToSpawn)
        , Counter(counter)
    {}

    void Exec(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        AtomicIncrement(*Counter);
        if (GenerationsToSpawn > 0) {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(Buffer->Data(), Buffer->Size());
            Device.PwriteAsync(Buffer->Data(), Buffer->Size(), 0, new TWriter(Device, Buffer, GenerationsToSpawn - 1, Counter),
                    NPDisk::TReqId(NPDisk::TReqId::Test1, 0), {});
            Device.PwriteAsync(Buffer->Data(), Buffer->Size(), 0, new TWriter(Device, Buffer, GenerationsToSpawn - 1, Counter),
                    NPDisk::TReqId(NPDisk::TReqId::Test2, 0), {});
        }
        delete this;
    }

    void Release(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        delete this;
    }
};

class TFlusher : public NPDisk::TCompletionAction {
    NPDisk::IBlockDevice &Device;
    const i32 GenerationsToSpawn;
    TAtomic *Counter;
public:
    TFlusher(NPDisk::IBlockDevice &device, const i32 generationsToSpawn, TAtomic *counter)
        : Device(device)
        , GenerationsToSpawn(generationsToSpawn)
        , Counter(counter)
    {}

    void Exec(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        AtomicIncrement(*Counter);
        if (GenerationsToSpawn > 0) {
            Device.FlushAsync(new TFlusher(Device, GenerationsToSpawn - 1, Counter), NPDisk::TReqId(NPDisk::TReqId::Test0, 0));
            Device.FlushAsync(new TFlusher(Device, GenerationsToSpawn - 1, Counter), NPDisk::TReqId(NPDisk::TReqId::Test0, 0));
        }
        delete this;
    }

    void Release(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        delete this;
    }
};

class TRabbit : public NPDisk::TCompletionAction {
    NPDisk::IBlockDevice &Device;
    NPDisk::TBuffer *Buffer;
    const i32 GenerationsToSpawn;
    TAtomic *Counter;
public:
    TRabbit(NPDisk::IBlockDevice &device, NPDisk::TBuffer *data, const i32 generationsToSpawn, TAtomic *counter)
        : Device(device)
        , Buffer(data)
        , GenerationsToSpawn(generationsToSpawn)
        , Counter(counter)
    {}

    void Exec(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        AtomicIncrement(*Counter);
        if (GenerationsToSpawn > 0) {
            REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(Buffer->Data(), Buffer->Size());
            Device.PwriteAsync(Buffer->Data(), Buffer->Size(), 0, new TRabbit(Device, Buffer, GenerationsToSpawn - 1, Counter),
                    NPDisk::TReqId(NPDisk::TReqId::Test0, 0), {});
            Device.FlushAsync(new TRabbit(Device, Buffer, GenerationsToSpawn - 1, Counter), NPDisk::TReqId(NPDisk::TReqId::Test1, 0));
            Device.PreadAsync(Buffer->Data(), Buffer->Size(), 0, new TRabbit(Device, Buffer, GenerationsToSpawn - 1, Counter),
                    NPDisk::TReqId(NPDisk::TReqId::Test3, 0), {});
        }
        delete this;
    }

    void Release(TActorSystem *actorSystem) override {
        Y_UNUSED(actorSystem);
        delete this;
    }
};

class TCompletionWorkerWithCounter : public NPDisk::TCompletionAction {
public:
    TCompletionWorkerWithCounter(TAtomic& counter, TDuration workTime = TDuration::Zero())
        : Counter(counter)
        , WorkTime(workTime)
    {}

    void Exec(TActorSystem *) override {
        Sleep(WorkTime);
        AtomicIncrement(Counter);
        delete this;
    }

    void Release(TActorSystem *) override {
        AtomicIncrement(Counter);
        delete this;
    }

private:
    TAtomic& Counter;
    TDuration WorkTime;
};

TString CreateFile(const char *baseDir, ui32 dataSize) {
    TString databaseDirectory = MakeDatabasePath(baseDir);
    MakeDirIfNotExist(databaseDirectory.c_str());
    TString path = MakePDiskPath(baseDir);
    {
        TFile file(path.c_str(), OpenAlways | RdWr | Seq | Direct);
        file.Resize(dataSize);
        file.Close();
    }
    UNIT_ASSERT_EQUAL_C(NFs::Exists(path), true, "File " << path << " does not exist.");
    return path;
}

constexpr TDuration TIMEOUT = NSan::PlainOrUnderSanitizer(TDuration::Seconds(120), TDuration::Seconds(360));

void WaitForValue(TAtomic *counter, TDuration maxDuration, TAtomicBase expectedValue) {
    TInstant finishTime = TInstant::Now() + maxDuration;
    while (TInstant::Now() < finishTime) {
        for (int i = 0; i < 100; ++i) {
            Sleep(TDuration::MilliSeconds(50));
            TAtomicBase resultingCounter = AtomicGet(*counter);
            if (resultingCounter >= expectedValue) {
                return;
            }
        }
    }
}

void RunTestDestructionWithMultipleFlushesFromCompletionAction() {
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
    THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));
    const ui32 dataSize = 4 << 10;
    const i32 generations = 8;
    TAtomic counter = 0;

    TTempDir tempDir;
    TString path = CreateFile(tempDir().c_str(), dataSize);

    TActorSystemCreator creator;
    THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDevice(path, 0, *mon, 0, 0, 4,
                NPDisk::TDeviceMode::LockFile, 1, 2 << generations, nullptr));
    device->Initialize(creator.GetActorSystem(), {});

    (new TFlusher(*device, generations, &counter))->Exec(nullptr);
    device->Stop();
    for (int i = 0; i < 10000; ++i) {
        (new TFlusher(*device, generations, &counter))->Exec(nullptr);
    }
    device.Destroy();

    Ctest << "Done" << Endl;
}

void RunWriteTestWithSectorMap(NPDisk::NSectorMap::EDiskMode diskMode, ui32 diskSize, ui32 bufferSize, bool sequential = true) {
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
    THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));

    TActorSystemCreator creator;
    TIntrusivePtr<NPDisk::TSectorMap> sectorMap = new NPDisk::TSectorMap(diskSize, diskMode);
    THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDeviceWithDefaults(
            /* path can be empty when sector map is used */ "", *mon, NPDisk::TDeviceMode::None, sectorMap, creator.GetActorSystem()));

    NPDisk::TAlignedData data(bufferSize);
    memset(data.Get(), 0, data.Size());

    TAtomic completedWrites = 0;

    ui32 offsetIncrement = sequential ? bufferSize : 2 * bufferSize;
    TAtomic expectedWrites = diskSize / (offsetIncrement);

    for (ui64 offset = 0; offset < diskSize; offset += offsetIncrement) {
        device->PwriteAsync(data.Get(), data.Size(), offset, new TCompletionWorkerWithCounter(completedWrites), NPDisk::TReqId(NPDisk::TReqId::Test1, 0), {});
    }

    WaitForValue(&completedWrites, TIMEOUT, expectedWrites);

    device.Destroy();
}

Y_UNIT_TEST_SUITE(TBlockDeviceTest) {

    Y_UNIT_TEST(TestDestructionWithMultipleFlushesFromCompletionAction) {
        RunTestDestructionWithMultipleFlushesFromCompletionAction();
    }

    Y_UNIT_TEST(TestDeviceWithSubmitGetThread) {
        const TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));
        const ui32 fileSize = 4 << 20;
        const ui32 dataSize = 4 << 10;
        NPDisk::TAlignedData data(dataSize);

        TTempDir tempDir;
        TString path = CreateFile(tempDir().c_str(), fileSize);

        TActorSystemCreator creator;
        THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDeviceWithDefaults(path, *mon,
                    NPDisk::TDeviceMode::LockFile | NPDisk::TDeviceMode::UseSubmitGetThread, nullptr, creator.GetActorSystem()));

        device->PreadSync(data.Get(), data.Size(), 0, {}, nullptr);
        device->PwriteSync(data.Get(), data.Size(), 0, {}, nullptr);

        device.Destroy();
        Ctest << "Done" << Endl;
    }

    Y_UNIT_TEST(TestWriteWithNoneSectorMap2GbDisk8MbBuffer) {
        ui32 diskSize = 2 * 1024 * 1024 * 1024u;
        ui32 bufferSize = 8 * 1024 * 1024u;
        RunWriteTestWithSectorMap(NPDisk::NSectorMap::DM_NONE, diskSize, bufferSize);
        Ctest << "Done" << Endl;
    }

    Y_UNIT_TEST(TestWriteWithNoneSectorMap2GbDisk8MbBufferNonSequential) {
        ui32 diskSize = 2 * 1024 * 1024 * 1024u;
        ui32 bufferSize = 8 * 1024 * 1024u;
        bool sequential = false;
        RunWriteTestWithSectorMap(NPDisk::NSectorMap::DM_NONE, diskSize, bufferSize, sequential);
        Ctest << "Done" << Endl;
    }

    Y_UNIT_TEST(TestWriteWithNoneSectorMap2GbDisk32KbBuffer) {
        ui32 diskSize = 2 * 1024 * 1024 * 1024u;
        ui32 bufferSize = 32 * 1024u;
        RunWriteTestWithSectorMap(NPDisk::NSectorMap::DM_NONE, diskSize, bufferSize);
        Ctest << "Done" << Endl;
    }

    Y_UNIT_TEST(TestWriteWithNoneSectorMap2GbDisk32KbBufferNonSequential) {
        ui32 diskSize = 2 * 1024 * 1024 * 1024u;
        ui32 bufferSize = 32 * 1024u;
        bool sequential = false;
        RunWriteTestWithSectorMap(NPDisk::NSectorMap::DM_NONE, diskSize, bufferSize, sequential);
        Ctest << "Done" << Endl;
    }

    Y_UNIT_TEST(TestWriteWithHddSectorMap2GbDisk8MbBuffer) {
        ui32 diskSize = 2 * 1024 * 1024 * 1024u;
        ui32 bufferSize = 8 * 1024 * 1024u;
        RunWriteTestWithSectorMap(NPDisk::NSectorMap::DM_HDD, diskSize, bufferSize);
        Ctest << "Done" << Endl;
    }

    Y_UNIT_TEST(TestWriteWithHddSectorMap2GbDisk8MbBufferNonSequential) {
        ui32 diskSize = 2 * 1024 * 1024 * 1024u;
        ui32 bufferSize = 8 * 1024 * 1024u;
        bool sequential = false;
        RunWriteTestWithSectorMap(NPDisk::NSectorMap::DM_HDD, diskSize, bufferSize, sequential);
        Ctest << "Done" << Endl;
    }

    Y_UNIT_TEST(TestWriteWithHddSectorMap2GbDisk32KbBuffer) {
        ui32 diskSize = 2 * 1024 * 1024 * 1024u;
        ui32 bufferSize = 32 * 1024u;
        RunWriteTestWithSectorMap(NPDisk::NSectorMap::DM_HDD, diskSize, bufferSize);
        Ctest << "Done" << Endl;
    }

    Y_UNIT_TEST(WriteReadRestart) {
        using namespace NPDisk;

        TActorSystemCreator creator;
        auto start = TMonotonic::Now();
        while ((TMonotonic::Now() - start).Seconds() < 5) {
            for (auto completionThreadsCount : {0, 1, 2, 3}) {
                const TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
                THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));

                ui32 buffSize = 64_KB;
                auto randomData = PrepareData(buffSize);
                ui32 bufferPoolSize = 512;
                THolder<NPDisk::TBufferPool> bufferPool(NPDisk::CreateBufferPool(buffSize, bufferPoolSize, false, {}));
                ui64 inFlight = 128;
                ui32 maxQueuedCompletionActions = bufferPoolSize / 2;
                ui64 diskSize = 32_GB;

                TIntrusivePtr<NPDisk::TSectorMap> sectorMap = new NPDisk::TSectorMap(diskSize, NSectorMap::DM_NONE);
                THolder<NPDisk::IBlockDevice> device(CreateRealBlockDevice("", 0, *mon, 0, 0, inFlight, TDeviceMode::None,
                        maxQueuedCompletionActions, completionThreadsCount, sectorMap));
                device->Initialize(creator.GetActorSystem(), {});

                TAtomic counter = 0;
                const i64 totalRequests = 500;
                for (i64 i = 0; i < totalRequests; i++) {
                    auto *completion = new TCompletionWorkerWithCounter(counter, TDuration::MicroSeconds(100));
                    NPDisk::TBuffer::TPtr buffer(bufferPool->Pop());
                    buffer->FlushAction = completion;
                    auto* data = buffer->Data();
                    switch (RandomNumber<ui32>(3)) {
                    case 0:
                        device->PreadAsync(data, 32_KB, 0, buffer.Release(), TReqId(), nullptr);
                        break;
                    case 1:
                        memcpy(data, randomData.data(), 32_KB);
                        device->PwriteAsync(data, 32_KB, 0, buffer.Release(), TReqId(), nullptr);
                        break;
                    case 2:
                        device->FlushAsync(completion, TReqId());
                        buffer->FlushAction = nullptr;
                        break;
                    default:
                        break;
                    }
                }

                Ctest << AtomicGet(counter)  << Endl;
                device.Destroy();
                UNIT_ASSERT(AtomicGet(counter) == totalRequests);
            }
        }
    }

    /*
    Y_UNIT_TEST(TestRabbitCompletionAction) {
        const TIntrusivePtr<::NMonitoring::TDynamicCounters> counters = new ::NMonitoring::TDynamicCounters;
        THolder<TPDiskMon> mon(new TPDiskMon(counters, 0, nullptr));
        const ui32 dataSize = 4 << 10;
        const i32 generations = 8;
        TAtomic counter = 0;

        TTempDir tempDir;
        TString path = CreateFile(tempDir().c_str(), dataSize);
        {
            NPDisk::TAlignedData alignedBuffer;
            alignedBuffer.Resize(dataSize);
            memset(alignedBuffer.Get(), 0, dataSize);
            THolder<NPDisk::IBlockDevice> device(NPDisk::CreateRealBlockDevice(path, 0, *mon));
            device->Initialize(nullptr);

            (new TRabbit(*device, alignedBuffer, generations, &counter))->Exec(nullptr);

            TAtomicBase expectedCounter = 0;
            TAtomicBase generationSize = 1;
            for (int i = 0; i <= generations; ++i) {
                expectedCounter += generationSize;
                generationSize *= 3;
            }

            WaitForValue(&counter, 60000, expectedCounter);

            TAtomicBase resultingCounter = AtomicGet(counter);

            UNIT_ASSERT_EQUAL_C(resultingCounter, expectedCounter, "restultingCounter = " << resultingCounter <<
                " while " << expectedCounter << " expected.");

            device.Destroy();
        }
        Ctest << "Done" << Endl;
    }
    */
}

} // namespace NKikimr
