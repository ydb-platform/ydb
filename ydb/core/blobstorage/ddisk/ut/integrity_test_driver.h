#pragma once

#include <ydb/core/blobstorage/ddisk/integrity_manager.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NDDisk::NIntegrityTest {

// Captures physical I/O; native handles own logical operations and notifications.
class TFixture : public TIntegrityManager {
public:
    struct TWriteIo : TWriteSubmission {
        std::shared_ptr<TMetadataWrite> Context;
    };

    struct TReadIo : TMetadataRead {
        std::shared_ptr<TMetadataWrite> Context;
    };

    struct TActions {
        std::vector<ui64> Allocations;
        std::vector<TWriteIo> Writes;
        std::vector<TReadIo> Reads;

        size_t size() const {
            return Allocations.size() + Writes.size() + Reads.size();
        }

        bool empty() const {
            return !size();
        }
    };

    TActions Submissions;
    std::vector<TChunkIdx> Returned;

    using TIntegrityManager::TIntegrityManager;

    TActions TakeActions() {
        return std::exchange(Submissions, {});
    }

    void Submit(TWork work) {
        Submissions.Allocations.insert(Submissions.Allocations.end(), work.Allocations.begin(), work.Allocations.end());
        Returned.insert(Returned.end(), work.ReturnedChunks.begin(), work.ReturnedChunks.end());
        for (auto& write : work.Writes) {
            Submissions.Writes.push_back({std::move(write), {}});
        }
    }

    TExtent StartExtent(TDataChunkKey key, TChunkIdx chunk) {
        auto [extent, work] = TIntegrityManager::StartExtent(key, chunk);
        Submit(std::move(work));
        return extent;
    }

    void CompleteAllocation(ui64 token, TChunkIdx chunk) {
        Submit(TIntegrityManager::CompleteAllocation(token, chunk));
        NotifyCompleted();
    }

    void SubmitMetadataReads(TMetadataReads reads) {
        for (auto& read : reads) {
            Submissions.Reads.push_back({std::move(read), {}});
        }
    }

    void SubmitWrite(TWriteOperation& operation, TConstArrayRef<ui64> checksums) {
        UNIT_ASSERT(operation.IsReady());
        UNIT_ASSERT(!operation.GetResult());
        auto context = PrepareMetadataWrite(operation, checksums);
        if (context->ReadSize) {
            Submissions.Reads.push_back({{0, context->ChunkIdx, context->ReadOffset, context->ReadSize},
                std::move(context)});
        } else {
            SubmitMetadataWrite(std::move(context));
        }
    }

    TWriteOperation Write(TDataChunkKey key, ui32 offset, ui32 size, const std::vector<ui64>& checksums) {
        auto operation = PrepareWrite(key, offset, size);
        SubmitWrite(operation, checksums);
        return operation;
    }

    void CompleteWrite(TWriteIo& io, bool ok = true) {
        if (io.Context) {
            CompleteMetadataWrite(std::exchange(io.Context, {}), ok);
        } else {
            UNIT_ASSERT(io.Id);
            Submit(TIntegrityManager::CompleteWrite(std::exchange(io.Id, 0), ok));
        }
        NotifyCompleted();
    }

    void CompleteRead(TReadIo& io, TRope data, bool ok = true) {
        if (io.Context) {
            auto context = std::exchange(io.Context, {});
            if (!ok || Stopped || !context->Transform(TReadPayload(std::move(data)))) {
                CompleteMetadataWrite(context, false);
            } else {
                SubmitMetadataWrite(std::move(context));
            }
        } else {
            UNIT_ASSERT(io.Id);
            TMetadataReadResult result{std::exchange(io.Id, 0), TIoResult{ok, std::move(data)}};
            CompleteMetadataReads(TConstArrayRef<TMetadataReadResult>(&result, 1));
        }
        NotifyCompleted();
    }

    void CommitTabletChunksDeletion(ui64 tablet) {
        TIntegrityManager::CommitTabletChunksDeletion(tablet);
        NotifyCompleted();
    }

    std::vector<TChunkIdx> TakeReleasableIntegrityChunks() {
        auto work = TIntegrityManager::TakeReleasableIntegrityChunks();
        auto chunks = std::exchange(work.ReturnedChunks, {});
        Submit(std::move(work));
        NotifyCompleted();
        return chunks;
    }

    void Stop() {
        Stopped = true;
        TIntegrityManager::Stop();
        NotifyCompleted();
    }

private:
    void SubmitMetadataWrite(std::shared_ptr<TMetadataWrite> context) {
        Submissions.Writes.push_back({{0, context->ChunkIdx, context->WriteOffset, context->WriteImage},
            std::move(context)});
    }

    bool Stopped = false;
};

}
