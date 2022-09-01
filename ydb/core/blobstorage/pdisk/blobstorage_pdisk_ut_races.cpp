#include "blobstorage_pdisk_ut.h"

#include "blobstorage_pdisk_abstract.h"
#include "blobstorage_pdisk_impl.h"
#include "blobstorage_pdisk_ut_env.h"

#include <ydb/core/blobstorage/crypto/default.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <util/system/hp_timer.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TPDiskRaces) {
    Y_UNIT_TEST(KillOwnerWhileDeletingChunk) {
        THPTimer timer;
        ui32 timeLimit = 20;
        while (timer.Passed() < timeLimit) {
            TActorTestContext testCtx(false);

            auto logNoTest = [&](TVDiskMock& mock, NPDisk::TCommitRecord rec) {
                auto evLog = MakeHolder<NPDisk::TEvLog>(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound, 0, PrepareData(1),
                        mock.GetLsnSeg(), nullptr);
                evLog->Signature.SetCommitRecord();
                evLog->CommitRecord = std::move(rec);
                testCtx.Send(evLog.Release());
            };

            TVDiskMock mock(&testCtx);
            mock.Init();

            ui32 vdisksNum = 100;
            std::vector<TVDiskMock> mocks;
            for (ui32 i = 0; i < vdisksNum; ++i) {
                mocks.push_back(TVDiskMock(&testCtx));
                mocks[i].Init();
            }

            ui32 reservedChunks = 10;

            for (ui32 i = 0; i < reservedChunks; ++i) {
                mock.ReserveChunk();
            }
            mock.CommitReservedChunks();

            while (mock.Chunks[EChunkState::COMMITTED].size() > 0) {
                auto it = mock.Chunks[EChunkState::COMMITTED].begin();
                NPDisk::TCommitRecord rec;
                rec.DeleteChunks.push_back(*it);
                logNoTest(mock, rec);
                mock.Chunks[EChunkState::COMMITTED].erase(it);
            }

            testCtx.Send(new NPDisk::TEvHarakiri(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound));

            for (ui32 c = 0; ; c = (c + 1) % mocks.size()) {
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

    Y_UNIT_TEST(KillOwnerWhileDeletingChunkWithInflight) {
        THPTimer timer;
        ui32 timeLimit = 20;
        while (timer.Passed() < timeLimit) {
            TActorTestContext testCtx(false);

            auto logNoTest = [&](TVDiskMock& mock, NPDisk::TCommitRecord rec) {
                auto evLog = MakeHolder<NPDisk::TEvLog>(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound, 0, PrepareData(1),
                        mock.GetLsnSeg(), nullptr);
                evLog->Signature.SetCommitRecord();
                evLog->CommitRecord = std::move(rec);
                testCtx.Send(evLog.Release());
            };

            TVDiskMock mock(&testCtx);
            mock.Init();

            ui32 vdisksNum = 100;
            std::vector<TVDiskMock> mocks;
            for (ui32 i = 0; i < vdisksNum; ++i) {
                mocks.push_back(TVDiskMock(&testCtx));
                mocks[i].Init();
            }

            ui32 reservedChunks = 10;
            for (ui32 i = 0; i < reservedChunks; ++i) {
                mock.ReserveChunk();
            }
            mock.CommitReservedChunks();
            TVector<TChunkIdx> chunkIds(mock.Chunks[EChunkState::COMMITTED].begin(), mock.Chunks[EChunkState::COMMITTED].end());
            
            ui32 inflight = 50;

            while (mock.Chunks[EChunkState::COMMITTED].size() > 0) {
                auto it = mock.Chunks[EChunkState::COMMITTED].begin();
                for (ui32 i = 0; i < inflight; ++i) {
                    TString data = "HATE. LET ME TELL YOU HOW MUCH I'VE COME TO HATE YOU SINCE I BEGAN TO LIVE...";
                    testCtx.Send(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        *it, 0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(data), nullptr, false, 0));
                }
                NPDisk::TCommitRecord rec;
                rec.DeleteChunks.push_back(*it);
                logNoTest(mock, rec);
                mock.Chunks[EChunkState::COMMITTED].erase(it);
            }
            mock.Chunks[EChunkState::COMMITTED].clear();

            testCtx.Send(new NPDisk::TEvHarakiri(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound));

            for (ui32 c = 0; ; c = (c + 1) % mocks.size()) {
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

    Y_UNIT_TEST(DecommitWithInflight) {
        THPTimer timer;
        ui32 timeLimit = 20;
        while (timer.Passed() < timeLimit) {
            TActorTestContext testCtx(false);
            ui32 dataSize = 1024;

            auto logNoTest = [&](TVDiskMock& mock, NPDisk::TCommitRecord rec) {
                auto evLog = MakeHolder<NPDisk::TEvLog>(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound, 0, PrepareData(1),
                        mock.GetLsnSeg(), nullptr);
                evLog->Signature.SetCommitRecord();
                evLog->CommitRecord = std::move(rec);
                testCtx.Send(evLog.Release());
            };

            auto sendManyReads = [&](TVDiskMock& mock, TChunkIdx chunk, ui32 number, ui64& cookie) {
                for (ui32 i = 0; i < number; ++i) {
                    testCtx.Send(new NPDisk::TEvChunkRead(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        chunk, 0, dataSize, 0, (void*)(cookie++)));
                }
            };
            
            auto sendManyWrites = [&](TVDiskMock& mock, TChunkIdx chunk, ui32 number, ui64& cookie) {
                for (ui32 i = 0; i < number; ++i) {
                    TString data = PrepareData(dataSize);
                    testCtx.Send(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        chunk, 0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(data), (void*)(cookie++), false, 0));
                }
            };

            TVDiskMock mock(&testCtx);
            mock.Init();

            ui32 reservedChunks = 10;
            for (ui32 i = 0; i < reservedChunks; ++i) {
                mock.ReserveChunk();
            }

            {
                auto& chunkIds = mock.Chunks[EChunkState::COMMITTED];
                for (auto it = chunkIds.begin(); it != chunkIds.end(); ++it) {
                    TString data = PrepareData(dataSize);
                    testCtx.TestResponce<NPDisk::TEvChunkWriteResult>(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        *it, 0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(data), (void*)10, false, 0),
                        NKikimrProto::OK);
                }
            }

            mock.CommitReservedChunks();

            ui32 inflight = 50;
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
                }
                {
                    auto res = testCtx.Recv<NPDisk::TEvChunkWriteResult>();
                    UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
                }
            } 
            for (ui32 i = 0; i < reservedChunks; ++i) {
                testCtx.TestResponce<NPDisk::TEvChunkForgetResult>(new NPDisk::TEvChunkForget(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound),
                        NKikimrProto::OK);
            }
        }
    }

    Y_UNIT_TEST(KillOwnerWhileDecommittingChunksWithInflight) {
        THPTimer timer;
        ui32 timeLimit = 20;
        while (timer.Passed() < timeLimit) {
            TActorTestContext testCtx(false);

            auto logNoTest = [&](TVDiskMock& mock, NPDisk::TCommitRecord rec) {
                auto evLog = MakeHolder<NPDisk::TEvLog>(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound, 0, PrepareData(1),
                        mock.GetLsnSeg(), nullptr);
                evLog->Signature.SetCommitRecord();
                evLog->CommitRecord = std::move(rec);
                testCtx.Send(evLog.Release());
            };

            TVDiskMock mock(&testCtx);
            mock.Init();

            ui32 vdisksNum = 100;
            std::vector<TVDiskMock> mocks;
            for (ui32 i = 0; i < vdisksNum; ++i) {
                mocks.push_back(TVDiskMock(&testCtx));
                mocks[i].Init();
            }

            ui32 reservedChunks = 10;
            for (ui32 i = 0; i < reservedChunks; ++i) {
                mock.ReserveChunk();
            }
            mock.CommitReservedChunks();
            TVector<TChunkIdx> chunkIds(mock.Chunks[EChunkState::COMMITTED].begin(), mock.Chunks[EChunkState::COMMITTED].end());
            
            ui32 inflight = 50;

            while (mock.Chunks[EChunkState::COMMITTED].size() > 0) {
                auto it = mock.Chunks[EChunkState::COMMITTED].begin();
                for (ui32 i = 0; i < inflight; ++i) {
                    TString data = "HATE. LET ME TELL YOU HOW MUCH I'VE COME TO HATE YOU SINCE I BEGAN TO LIVE...";
                    testCtx.Send(new NPDisk::TEvChunkWrite(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound,
                        *it, 0, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(data), nullptr, false, 0));
                }
                NPDisk::TCommitRecord rec;
                rec.DeleteChunks.push_back(*it);
                rec.DeleteToDecommitted = true;
                logNoTest(mock, rec);
                mock.Chunks[EChunkState::COMMITTED].erase(it);
            }
            mock.Chunks[EChunkState::COMMITTED].clear();

            testCtx.Send(new NPDisk::TEvHarakiri(mock.PDiskParams->Owner, mock.PDiskParams->OwnerRound));

            for (ui32 c = 0; ; c = (c + 1) % mocks.size()) {
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
}

}
