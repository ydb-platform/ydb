#include <ydb/core/blobstorage/ddisk/ut/ddisk_actor_pdisk_common_ut.h>

#include <util/datetime/base.h>

namespace NKikimr {
    namespace {

        void TestFillAndRestart() {
            constexpr ui64 capacity = 32_GB;
            ui64 records = 0;
            ui64 lastLsn = 0;
            bool full = false;
            constexpr ui32 window = 256;
            NDDisk::TPersistentBufferFormat format;
            format.MaxChunks = capacity / ChunkSize;
            format.PerTabletStorageLimit = 64_GB;
            format.MaxInMemoryCache = 0;
            auto sectors = MakeIntrusive<NPDisk::TSectorMap>(64_GB);
            TTestContext ctx({.ForcePDiskFallback = true}, NLog::PRI_ERROR, 1,
                             ChunkSize, false, false, sectors, format);
            Connect(ctx, 1201, 1);
            ctx.DDiskServiceId = MakeBlobStoragePersistentBufferId(ctx.NodeId, PDiskId, SlotId);

            auto connect = [&] {
                auto creds = NDDisk::TQueryCredentials::ToPersistentBuffer(1201, 1, std::nullopt, 0);
                auto result = ctx.SendAndGrab<NDDisk::TEvConnectResult>(new NDDisk::TEvConnect(creds));
                AssertStatus<NDDisk::TEvConnectResult>(result, TReplyStatus::OK);
                creds.DDiskInstanceGuid = result->Get()->Record.GetDDiskInstanceGuid();
                creds.ConnectionToken.emplace(result->Get()->Record.GetConnectionToken());
                return creds;
            };
            auto creds = connect();
            auto token = ctx.SendAndGrab<NDDisk::TEvGetPersistentBufferRegistrationTokenResult>(
                new NDDisk::TEvGetPersistentBufferRegistrationToken(creds));
            AssertStatus<NDDisk::TEvGetPersistentBufferRegistrationTokenResult>(token, TReplyStatus::OK);
            AssertStatus<NDDisk::TEvRegisterPersistentBufferResult>(
                ctx.SendAndGrab<NDDisk::TEvRegisterPersistentBufferResult>(
                    new NDDisk::TEvRegisterPersistentBuffer(creds, token->Get()->Record.GetToken())), TReplyStatus::OK);

            const TString payload(MinBlockSize, '\0');
            const auto writeStart = TMonotonic::Now();
            for (ui64 base = 0; !full; base += window) {
                for (ui64 i = base; i < base + window; ++i) {
                    auto request = std::make_unique<NDDisk::TEvWritePersistentBuffer>(creds,
                                                                                      NDDisk::TBlockSelector(i / (ChunkSize / MinBlockSize),
                                                                                                             (i % (ChunkSize / MinBlockSize)) * MinBlockSize, MinBlockSize),
                                                                                      i + 1, NDDisk::TWriteInstruction(0));
                    request->AddPayloadThenChecksum(MakeAlignedRope(payload));
                    ctx.Send(request.release(), i + 1);
                }
                for (ui64 i = base; i < base + window; ++i) {
                    auto result = ctx.Grab<NDDisk::TEvWritePersistentBufferResult>();
                    if (result->Get()->Record.GetStatus() == TReplyStatus::OVERFILL) {
                        full = true;
                    } else {
                        AssertStatus<NDDisk::TEvWritePersistentBufferResult>(result, TReplyStatus::OK);
                        UNIT_ASSERT(result->Cookie > base && result->Cookie <= base + window);
                        ++records;
                        lastLsn = std::max(lastLsn, result->Cookie);
                    }
                }
                if ((base + window) % (1_GB / MinBlockSize) == 0) {
                    Cerr << "PB written bytes=" << records * MinBlockSize
                         << " elapsed=" << TMonotonic::Now() - writeStart << Endl;
                }
            }
            UNIT_ASSERT(records > 0);
            const ui64 bytes = records * MinBlockSize;
            const auto writeTime = TMonotonic::Now() - writeStart;
            UNIT_ASSERT_C(writeTime <= TDuration::Seconds(70), "Filling PersistentBuffer took " << writeTime << "; limit is 70 seconds");
            Cerr << "PB filled: bytes=" << bytes << " records=" << records << " write=" << writeTime << Endl;
            auto verifyInfo = [&] {
                auto info = ctx.SendAndGrab<NDDisk::TEvPersistentBufferInfo>(
                    new NDDisk::TEvGetPersistentBufferInfo(true, true));
                UNIT_ASSERT_VALUES_EQUAL(info->Get()->FreeSpace.size(), format.MaxChunks);
                UNIT_ASSERT_VALUES_EQUAL(info->Get()->TabletInfos.size(), 1);
                const auto& tablet = info->Get()->TabletInfos.front();
                UNIT_ASSERT_VALUES_EQUAL(tablet.LsnsCount, records);
                UNIT_ASSERT_VALUES_EQUAL(tablet.Size, bytes);
                UNIT_ASSERT_VALUES_EQUAL(tablet.FirstLsn, 1);
                UNIT_ASSERT_VALUES_EQUAL(tablet.LastLsn, lastLsn);
            };
            verifyInfo();
            ctx.StopDDisk(0);
            const auto restoreStart = TMonotonic::Now();
            ctx.StartDDisk(0);
            ctx.DDiskServiceId = ctx.Disks[0].DDiskServiceId;
            Connect(ctx, 1201, 1);
            ctx.DDiskServiceId = MakeBlobStoragePersistentBufferId(ctx.NodeId, PDiskId, SlotId);
            creds = connect();
            ctx.Send(new NDDisk::TEvReadPersistentBuffer(creds, {0, 0, MinBlockSize}, 1, 1, {true}));
            auto read = ctx.Grab<NDDisk::TEvReadPersistentBufferResult>(TDuration::Seconds(40));
            AssertStatus<NDDisk::TEvReadPersistentBufferResult>(read, TReplyStatus::OK);
            const auto restoreTime = TMonotonic::Now() - restoreStart;
            UNIT_ASSERT_C(restoreTime <= TDuration::Seconds(40), "Restoring PersistentBuffer took " << restoreTime << "; limit is 40 seconds");
            UNIT_ASSERT_VALUES_EQUAL(read->Get()->GetPayload(0).ConvertToString(), payload);
            verifyInfo();
            Cerr << "PB benchmark: bytes=" << bytes << " records=" << records
                 << " write=" << writeTime << " restore=" << restoreTime << Endl;
        }

    } // anonymous namespace

    Y_UNIT_TEST_SUITE(TPersistentBufferBenchmark) {
        Y_UNIT_TEST(Zero4KiBFill32GiBAndRestartSectorMap) {
            TestFillAndRestart();
        }
    } // Y_UNIT_TEST_SUITE(TPersistentBufferBenchmark)

} // namespace NKikimr
