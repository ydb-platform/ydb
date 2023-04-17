#include "blobstorage_pdisk_ut.h"

#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_ut_env.h"

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <util/system/hp_timer.h>
#include <util/random/random.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPDiskRaces) {
    void TestKillOwnerWhileDeletingChunk(bool usePDiskMock, ui32 timeLimit, ui32 inflight, ui32 reservedChunks, ui32 vdisksNum) {
        THPTimer timer;
        while (timer.Passed() < timeLimit) {
            TActorTestContext testCtx({ false, usePDiskMock });
            const TString data = PrepareData(4096);

            auto logNoTest = [&](TVDiskMock& mock, NPDisk::TCommitRecord rec) {
                auto evLog = MakeHolder<NPDisk::TEvLog>(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound, 0, TRcBuf(PrepareData(1)),
                        mock.GetLsnSeg(), nullptr);
                evLog->Signature.SetCommitRecord();
                evLog->CommitRecord = std::move(rec);
                testCtx.Send(evLog.Release());
            };

            TVDiskMock mock(&testCtx);
            mock.Init();

            std::vector<TVDiskMock> mocks;
            for (ui32 i = 0; i < vdisksNum; ++i) {
                mocks.push_back(TVDiskMock(&testCtx));
                mocks[i].Init();
            }

            for (ui32 i = 0; i < reservedChunks; ++i) {
                mock.ReserveChunk();
            }
            mock.CommitReservedChunks();
            TVector<TChunkIdx> chunkIds(mock.Chunks[EChunkState::COMMITTED].begin(), mock.Chunks[EChunkState::COMMITTED].end());

            while (mock.Chunks[EChunkState::COMMITTED].size() > 0) {
                auto it = mock.Chunks[EChunkState::COMMITTED].begin();
                for (ui32 i = 0; i < inflight; ++i) {
                    TString dataCopy = data;
                    testCtx.Send(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        *it, 0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(dataCopy), nullptr, false, 0));
                }
                NPDisk::TCommitRecord rec;
                rec.DeleteChunks.push_back(*it);
                logNoTest(mock, rec);
                mock.Chunks[EChunkState::COMMITTED].erase(it);
            }
            mock.Chunks[EChunkState::COMMITTED].clear();

            testCtx.Send(new NPDisk::TEvHarakiri(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound));

            for (ui32 c = 0, i = 0; i < 300; c = (c + 1) % mocks.size(), ++i) {
                testCtx.Send(new NPDisk::TEvChunkReserve(mocks[c].PDiskParams->Owner, mocks[c].PDiskParams->OwnerRound, 1));
                THolder<NPDisk::TEvChunkReserveResult> evRes = testCtx.Recv<NPDisk::TEvChunkReserveResult>();
                if (!evRes || evRes->Status != NKikimrProto::OK) {
                    break;
                }
                const ui32 reservedChunk = evRes->ChunkIds.front();
                auto& reservedChunks = mocks[c].Chunks[EChunkState::RESERVED];
                reservedChunks.emplace(reservedChunk);
                
                NPDisk::TCommitRecord rec;
                rec.CommitChunks.push_back(*reservedChunks.begin());
                logNoTest(mocks[c], rec);
                reservedChunks.clear();
            }
            testCtx.Recv<NPDisk::TEvHarakiriResult>();
        }
    }

    Y_UNIT_TEST(KillOwnerWhileDeletingChunk) {
        TestKillOwnerWhileDeletingChunk(false, 20, 0, 10, 100);
    }

    Y_UNIT_TEST(KillOwnerWhileDeletingChunkWithInflight) {
        TestKillOwnerWhileDeletingChunk(false, 20, 50, 10, 100);
    }

    Y_UNIT_TEST(KillOwnerWhileDeletingChunkWithInflightMock) {
        TestKillOwnerWhileDeletingChunk(true, 20, 50, 10, 100);
    }
    
    void TestDecommit(bool usePDiskMock, ui32 timeLimit, ui32 inflight, ui32 reservedChunks) {
        THPTimer timer;
        while (timer.Passed() < timeLimit) {
            TActorTestContext testCtx({ false, usePDiskMock });
            const TString data = PrepareData(4096);

            auto logNoTest = [&](TVDiskMock& mock, NPDisk::TCommitRecord rec) {
                auto evLog = MakeHolder<NPDisk::TEvLog>(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound, 0, TRcBuf(PrepareData(1)),
                        mock.GetLsnSeg(), nullptr);
                evLog->Signature.SetCommitRecord();
                evLog->CommitRecord = std::move(rec);
                testCtx.Send(evLog.Release());
            };

            auto sendManyReads = [&](TVDiskMock& mock, TChunkIdx chunk, ui32 number, ui64& cookie) {
                for (ui32 i = 0; i < number; ++i) {
                    testCtx.Send(new NPDisk::TEvChunkRead(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        chunk, 0, data.size(), 0, (void*)(cookie++)));
                }
            };
            
            auto sendManyWrites = [&](TVDiskMock& mock, TChunkIdx chunk, ui32 number, ui64& cookie) {
                for (ui32 i = 0; i < number; ++i) {
                    TString dataCopy = data;
                    testCtx.Send(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        chunk, 0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(dataCopy), (void*)(cookie++), false, 0));
                }
            };

            TVDiskMock mock(&testCtx);
            mock.Init();

            for (ui32 i = 0; i < reservedChunks; ++i) {
                mock.ReserveChunk();
            }

            {
                auto& chunkIds = mock.Chunks[EChunkState::COMMITTED];
                for (auto it = chunkIds.begin(); it != chunkIds.end(); ++it) {
                    TString dataCopy = data;
                    testCtx.TestResponse<NPDisk::TEvChunkWriteResult>(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        *it, 0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(dataCopy), (void*)10, false, 0),
                        NKikimrProto::OK);
                }
            }

            mock.CommitReservedChunks();

            auto& chunkIds = mock.Chunks[EChunkState::COMMITTED];

            ui64 cookie = 0;
            for (auto it = chunkIds.begin(); it != chunkIds.end(); ++it) {
                sendManyWrites(mock, *it, inflight, cookie);
                sendManyReads(mock, *it, inflight, cookie);
                NPDisk::TCommitRecord rec;
                rec.DeleteChunks.push_back(*it);
                rec.DeleteToDecommitted = true;
                logNoTest(mock, rec);
                sendManyWrites(mock, *it, inflight, cookie);
                sendManyReads(mock, *it, inflight, cookie);
            }
            mock.Chunks[EChunkState::COMMITTED].clear();

            for (ui32 i = 0; i < inflight * 2 * reservedChunks; ++i) {
                {
                    auto res = testCtx.Recv<NPDisk::TEvChunkReadResult>();
                    UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
                    if (res->Data.IsReadable()) {
                        UNIT_ASSERT_VALUES_EQUAL(res->Data.ToString(), data);
                    }
                }
                {
                    auto res = testCtx.Recv<NPDisk::TEvChunkWriteResult>();
                    UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
                }
            } 
            for (ui32 i = 0; i < reservedChunks; ++i) {
                testCtx.TestResponse<NPDisk::TEvChunkForgetResult>(new NPDisk::TEvChunkForget(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound),
                        NKikimrProto::OK);
            }
        }
    }

    Y_UNIT_TEST(Decommit) {
        TestDecommit(false, 20, 0, 10);
    }

    Y_UNIT_TEST(DecommitWithInflight) {
        TestDecommit(false, 20, 50, 10);
    }

    Y_UNIT_TEST(DecommitWithInflightMock) {
        TestDecommit(true, 20, 50, 10);
    }

    void TestKillOwnerWhileDecommitting(bool usePDiskMock, ui32 timeLimit, ui32 inflight, ui32 reservedChunks, ui32 vdisksNum) {
        THPTimer timer;
        while (timer.Passed() < timeLimit) {
            TActorTestContext testCtx({ false, usePDiskMock });
            const TString data = PrepareData(4096);

            auto logNoTest = [&](TVDiskMock& mock, NPDisk::TCommitRecord rec) {
                auto evLog = MakeHolder<NPDisk::TEvLog>(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound, 0, TRcBuf(PrepareData(1)),
                        mock.GetLsnSeg(), nullptr);
                evLog->Signature.SetCommitRecord();
                evLog->CommitRecord = std::move(rec);
                testCtx.Send(evLog.Release());
            };

            TVDiskMock mock(&testCtx);
            mock.Init();

            std::vector<TVDiskMock> mocks;
            for (ui32 i = 0; i < vdisksNum; ++i) {
                mocks.push_back(TVDiskMock(&testCtx));
                mocks[i].Init();
            }

            for (ui32 i = 0; i < reservedChunks; ++i) {
                mock.ReserveChunk();
            }
            mock.CommitReservedChunks();
            TVector<TChunkIdx> chunkIds(mock.Chunks[EChunkState::COMMITTED].begin(), mock.Chunks[EChunkState::COMMITTED].end());
            
            while (mock.Chunks[EChunkState::COMMITTED].size() > 0) {
                auto it = mock.Chunks[EChunkState::COMMITTED].begin();
                for (ui32 i = 0; i < inflight; ++i) {
                    TString dataCopy = data;
                    testCtx.Send(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        *it, 0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(dataCopy), nullptr, false, 0));
                }
                NPDisk::TCommitRecord rec;
                rec.DeleteChunks.push_back(*it);
                rec.DeleteToDecommitted = true;
                logNoTest(mock, rec);
                mock.Chunks[EChunkState::COMMITTED].erase(it);
            }
            mock.Chunks[EChunkState::COMMITTED].clear();

            testCtx.Send(new NPDisk::TEvHarakiri(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound));

            for (ui32 c = 0, i = 0; i < 300; c = (c + 1) % mocks.size(), ++i) {
                testCtx.Send(new NPDisk::TEvChunkReserve(mocks[c].PDiskParams->Owner, mocks[c].PDiskParams->OwnerRound, 1));
                THolder<NPDisk::TEvChunkReserveResult> evRes = testCtx.Recv<NPDisk::TEvChunkReserveResult>();
                if (!evRes || evRes->Status != NKikimrProto::OK) {
                    break;
                }
                const ui32 reservedChunk = evRes->ChunkIds.front();
                auto& reservedChunks = mocks[c].Chunks[EChunkState::RESERVED];
                reservedChunks.emplace(reservedChunk);
                
                NPDisk::TCommitRecord rec;
                rec.CommitChunks.push_back(*reservedChunks.begin());
                logNoTest(mocks[c], rec);
                reservedChunks.clear();
            } 
            testCtx.Recv<NPDisk::TEvHarakiriResult>();
        }
    }

    Y_UNIT_TEST(KillOwnerWhileDecommitting) {
        TestKillOwnerWhileDecommitting(false, 20, 0, 10, 100);
    }

    Y_UNIT_TEST(KillOwnerWhileDecommittingWithInflight) {
        TestKillOwnerWhileDecommitting(false, 20, 0, 10, 100);
    }

    Y_UNIT_TEST(KillOwnerWhileDecommittingWithInflightMock) {
        TestKillOwnerWhileDecommitting(true, 20, 0, 10, 100);
    }

    void OwnerRecreationRaces(bool usePDiskMock, ui32 timeLimit, ui32 vdisksNum) {
        TActorTestContext testCtx({ false, usePDiskMock });

        std::vector<TVDiskMock> mocks;
        enum EMockState {
            Empty,
            InitStarted, 
            InitFinished,
            KillStarted,
            KillFinished
        };
        std::vector<EMockState> mockState(vdisksNum, EMockState::Empty);
        for (ui32 i = 0; i < vdisksNum; ++i) {
            mocks.push_back(TVDiskMock(&testCtx));
        }

        THPTimer timer;
        while (timer.Passed() < timeLimit) {
            ui32 i = RandomNumber(vdisksNum);
            ui32 action = RandomNumber<ui32>(10);
            if (action != 0 && mocks[i].PDiskParams) {
                auto evLog = MakeHolder<NPDisk::TEvLog>(mocks[i].PDiskParams->Owner, mocks[i].PDiskParams->OwnerRound, 0, TRcBuf(PrepareData(1)),
                        mocks[i].GetLsnSeg(), nullptr);
                evLog->Signature = TLogSignature::SignatureLogoBlobOpt;
                testCtx.Send(evLog.Release());
            } else {
                switch (mockState[i]) {
                case EMockState::InitStarted:
                    {
                        auto res = testCtx.Recv<NPDisk::TEvYardInitResult>();
                        UNIT_ASSERT_VALUES_EQUAL(res->Status, NKikimrProto::OK);
                        mocks[i].PDiskParams.Reset(res->PDiskParams);
                        mocks[i].OwnerRound = res->PDiskParams->OwnerRound;
                        mockState[i] = EMockState::InitFinished;
                    }
                    break;
                case EMockState::InitFinished:
                    {
                        auto evSlay = MakeHolder<NPDisk::TEvSlay>(mocks[i].VDiskID, mocks[i].OwnerRound++, 0, 0);
                        testCtx.Send(evSlay.Release());
                        mockState[i] = EMockState::KillStarted;
                    }
                    break;
                case EMockState::KillStarted:
                    {
                        testCtx.Recv<NPDisk::TEvSlayResult>();
                        mockState[i] = EMockState::KillFinished;
                    }
                    break;
                case EMockState::Empty: case EMockState::KillFinished:
                    {
                        auto evInit = MakeHolder<NPDisk::TEvYardInit>(mocks[i].OwnerRound++, mocks[i].VDiskID, testCtx.TestCtx.PDiskGuid);
                        testCtx.Send(evInit.Release());
                        mockState[i] = EMockState::InitStarted;
                    }
                    break;
                }
            }
        }
    }

    Y_UNIT_TEST(OwnerRecreationRaces) {
        OwnerRecreationRaces(false, 20, 1);
    }
}

}
