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
            auto dsk = MakeIntrusive<TPDiskParams>(
                mock.PDiskParams->Owner,
                mock.PDiskParams->OwnerRound,
                0U, 0U, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL
            );
            auto pDiskId = testCtx.GetPDisk()->PDiskActor;
            auto pdiskCtx = std::make_shared<TPDiskCtx>(dsk, pDiskId, "");

            TVector<std::pair<TString, bool>> inputData{{"asdfg", true}, {"qwer", false}, {"jsdfnj", true}, {"jlsnfgj", true}, {"lsdmn", true}};
            TQueue<TPartInfo> parts;
            for (const auto& [data, isDiskPart]: inputData) {
                if (isDiskPart) {
                    mock.ReserveChunk();
                    const ui32 chunkIdx = *mock.Chunks[EChunkState::RESERVED].begin();
                    TString dataCopy = data;
                    testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                    chunkIdx, 0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(dataCopy), nullptr, false, 0),
                    NKikimrProto::OK);
                    mock.CommitReservedChunks();
                    parts.push(TPartInfo{.Key=TLogoBlobID(), .PartData=TDiskPart(chunkIdx, 0, data.size())});
                } else {
                    parts.push(TPartInfo{.Key=TLogoBlobID(), .PartData=TRope(data)});
                }
            }

            ui32 batchSize = 2;
            auto reader = std::make_shared<TReader>(
                batchSize, pdiskCtx, parts, std::make_shared<TReplQuoter>(10)
            );
            const TActorId readerActorId = runtime->Register(new TReaderWrapperActor(reader));
            UNIT_ASSERT(!reader->TryGetResults().has_value());
            for (ui32 i = 0; i < inputData.size(); i += batchSize) {
                runtime->Send(new IEventHandle(readerActorId, testCtx.Sender, new NActors::TEvents::TEvWakeup()));
                Sleep(TDuration::Seconds(1));
                auto res = reader->TryGetResults();
                UNIT_ASSERT(res.has_value());
                UNIT_ASSERT_VALUES_EQUAL(res->size(), Min(batchSize, static_cast<ui32>(inputData.size() - i)));
                for (ui32 k = i; k < i + batchSize && k < inputData.size(); ++k) {
                    UNIT_ASSERT_VALUES_EQUAL(res->at(k - i).PartData.ConvertToString(), inputData[k].first);
                }
                UNIT_ASSERT(!reader->TryGetResults().has_value());
            }
        }

    }

} // NKikimr
