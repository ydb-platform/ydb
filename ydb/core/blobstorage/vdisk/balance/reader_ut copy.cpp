#include "reader.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

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
            TEnvironmentSetup Env({
                .NodeCount = 8,
                .VDiskReplPausedAtStart = false,
                .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            });
            Env.CreateBoxAndPool(1, 1);
            Env.Sim(TDuration::Minutes(1));

            auto groups = Env.GetGroups();
            UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
            auto GroupInfo = Env.GetGroupInfo(groups.front());
            const TVDiskID& someVDisk = GroupInfo->GetVDiskId(0);

            auto baseConfig = Env.FetchBaseConfig();
            const auto& somePDisk = baseConfig.GetPDisk(0);
            const auto& state = Env.PDiskMockStates[{somePDisk.GetNodeId(), somePDisk.GetPDiskId()}];
            auto nodeId = somePDisk.GetNodeId();
            auto pDiskId = Env.PDiskActors[0];
            auto ownerId = *state->GetOwnerId(someVDisk);
            auto ownerRound = *state->GetOwnerRound(someVDisk);

            auto quoter = std::make_shared<TReplQuoter>(10);

            auto dsk = MakeIntrusive<TPDiskParams>(
                ownerId,
                ownerRound,
                0U, 0U, 0UL, 0UL, 0UL, 0UL, 0UL, 0UL
            );


            ui32 numParts = 5;
            TQueue<TPartInfo> parts;
            // parts.push(TPartInfo{.Key=TLogoBlobID(), .PartData=TDiskPart(1, 2, 3)});

            const TActorId sender = Env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            Env.Runtime->WrapInActorContext(sender, [&] {
                Env.Runtime->Send(new IEventHandle(
                    pDiskId,
                    sender,
                    new NPDisk::TEvChunkReserve(ownerId, ownerRound, numParts)
                ));
            });
            {
                auto res = Env.WaitForEdgeActorEvent<NPDisk::TEvChunkReserveResult>(sender, false);
                for (auto chunkId: res->Get()->ChunkIds) {
                    Env.Runtime->WrapInActorContext(sender, [&] {
                        Env.Runtime->Send(new IEventHandle(pDiskId, sender, new NPDisk::TEvChunkWrite(
                            ownerId, ownerRound, chunkId, 0, 
                        )));
                    });
                }
                // UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.GetStatus(), 1);
            }


            auto pdiskCtx = std::make_shared<TPDiskCtx>(
                dsk,
                pDiskId,
                ""
            );

            auto reader = std::make_shared<TReader>(
                2,
                pdiskCtx,
                parts,
                quoter
            );
            // 
            const TActorId readerActorId = Env.Runtime->Register(new TReaderWrapperActor(reader), nodeId);
            UNIT_ASSERT(!reader->TryGetResults().has_value());

            // const TActorId sender = Env.Runtime->AllocateEdgeActor(nodeId, __FILE__, __LINE__);
            Env.Runtime->WrapInActorContext(sender, [&] {
                Env.Runtime->Send(new IEventHandle(readerActorId, sender, new NActors::TEvents::TEvWakeup()));
            });
            Env.Sim(TDuration::Seconds(10));
            UNIT_ASSERT(reader->TryGetResults().has_value());

            // {
            //     const size_t BatchSize;
            //     TPDiskCtxPtr PDiskCtx;
            //     TQueue<TPartInfo> Parts;
            //     TReplQuoter::TPtr Quoter;

            //     TVector<TPart> Result;
            //     ui32 Responses;
            //     ui32 ExpectedResponses;

            // }

        }

    }

} // NKikimr
