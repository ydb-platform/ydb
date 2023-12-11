#include "reader.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_ut_env.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr {

    class TReaderWrapperActor: public TActorBootstrapped<TReaderWrapperActor> {
        std::shared_ptr<TReader> Reader;
    public:
        TReaderWrapperActor(std::shared_ptr<TReader> reader)
            : Reader(reader)
        {
        }

        void Bootstrap() {
            Become(&TThis::StateFunc);
        }

        void DoJobQuant(const TActorContext &ctx) {
            Reader->DoJobQuant(ctx);
        }

        STRICT_STFUNC(StateFunc,
            CFunc(NActors::TEvents::TEvWakeup::EventType, DoJobQuant)
            hFunc(NPDisk::TEvChunkReadResult, Reader->Handle)
        );
    };

    Y_UNIT_TEST_SUITE(ReaderTestSuite) {

        Y_UNIT_TEST(SimpleTest) {
            TActorTestContext testCtx({ false });
            auto* runtime = testCtx.GetRuntime();

            TVDiskMock mock(&testCtx);
            mock.InitFull();
            TQueue<TPartInfo> parts;
            auto dsk = MakeIntrusive<TPDiskParams>(
                mock.PDiskParams->Owner,
                mock.PDiskParams->OwnerRound,
                0U, 0U, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL
            );
            auto pDiskId = testCtx.GetPDisk()->PDiskActor;
            auto pdiskCtx = std::make_shared<TPDiskCtx>(dsk, pDiskId, "");

            mock.ReserveChunk();
            const ui32 chunk = *mock.Chunks[EChunkState::RESERVED].begin();
            TString data{"asdfg"};
            TString dataCopy = data;
            testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
            chunk, 0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(dataCopy), nullptr, false, 0),
            NKikimrProto::OK);
            mock.CommitReservedChunks();
            parts.push(
                TPartInfo{.Key=TLogoBlobID(), .PartData=TDiskPart(chunk, 0, data.size())}
            );
            UNIT_ASSERT_VALUES_EQUAL(data.size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(std::get<TDiskPart>(parts.front().PartData).Size, 5);

            auto reader = std::make_shared<TReader>(
                1, pdiskCtx, parts, std::make_shared<TReplQuoter>(10)
            );
            const TActorId readerActorId = runtime->Register(new TReaderWrapperActor(reader));
            UNIT_ASSERT(!reader->TryGetResults().has_value());
            runtime->Send(new IEventHandle(readerActorId, testCtx.Sender, new NActors::TEvents::TEvWakeup()));
            Sleep(TDuration::Seconds(1));
            UNIT_ASSERT(reader->TryGetResults().has_value());
        }

    }

} // NKikimr
