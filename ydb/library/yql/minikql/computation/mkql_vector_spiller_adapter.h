#pragma once

#include <queue>

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/computation/mkql_spiller.h>
#include <ydb/library/actors/util/rope.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

namespace NKikimr::NMiniKQL {

///Stores long vectors of any type (excepting pointers). Tested on int and chars.
///The adapter places moved vectors into buffer and flushes buffer on disk when sizeLimit is reached.
///Returns vectors in FIFO order.
template<class T, class Alloc>
class TVectorSpillerAdapter {
public:
    enum class EState {
        AcceptingData,
        SpillingData,
        AcceptingDataRequests,
        RestoringData,
        DataReady
    };

    TVectorSpillerAdapter(ISpiller::TPtr spiller, size_t sizeLimit)
        : Spiller(spiller)
        , SizeLimit(sizeLimit)
    {
    }

    ///Returns current stete of the adapter
    EState GetState() const {
        return State;
    }

    ///Is adapter ready to spill next vector via AddData method.
    ///Returns false in case when there are async operations in progress.
    bool IsAcceptingData() {
        return State == EState::AcceptingData;
    }

    ///When data is ready ExtractVector() is expected to be called.
    bool IsDataReady() {
        return State == EState::DataReady;
    }

    ///Adapter is ready to accept requests for vectors.
    bool IsAcceptingDataRequests() {
        return State == EState::AcceptingDataRequests;
    }

    ///Adds new vector to storage. Will not launch real disk operation if case of small vectors
    ///(if inner buffer is not full).
    void AddData(std::vector<T, Alloc>&& vec) {
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        MKQL_ENSURE(State == EState::AcceptingData, "Internal logic error");

        StoredChunksElementsCount.push(vec.size());
        CurrentVector = std::move(vec);
        NextVectorPositionToSave = 0;

        SaveNextPartOfVector();
    }

    ///Should be used to update async operatrions statuses. 
    ///For SpillingData state it will try to spill more content of inner buffer.
    ///ForRestoringData state it will try to load more content of requested vector.
    void Update() {
        switch (State) {
            case EState::SpillingData:
                MKQL_ENSURE(WriteOperation.has_value(), "Internal logic error");
                if (!WriteOperation->HasValue()) return;

                StoredChunks.push(WriteOperation->ExtractValue());
                WriteOperation = std::nullopt;
                if (IsFinalizing) {
                    State = EState::AcceptingDataRequests;
                    return;
                }

                if (CurrentVector.empty()) {
                    State = EState::AcceptingData;
                    return;
                }

                SaveNextPartOfVector();
                return;
            case EState::RestoringData:
                MKQL_ENSURE(ReadOperation.has_value(), "Internal logic error");
                if (!ReadOperation->HasValue()) return;
                Buffer = std::move(ReadOperation->ExtractValue().value());
                ReadOperation = std::nullopt;
                StoredChunks.pop();

                LoadNextVector();
                return;
            default:
                return;
        }
    }

    ///Get requested vector.
    std::vector<T, Alloc>&& ExtractVector() {
        StoredChunksElementsCount.pop();
        State = EState::AcceptingDataRequests;
        return std::move(CurrentVector);
    }

    ///Start restoring next vector. If th eentire contents of the vector are in memory
    ///State will be changed to DataREady without any async read operation. ExtractVector is expected
    ///to be called immediately.
    void RequestNextVector() {
        MKQL_ENSURE(State == EState::AcceptingDataRequests, "Internal logic error");
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        MKQL_ENSURE(!StoredChunksElementsCount.empty(), "Internal logic error");

        CurrentVector.reserve(StoredChunksElementsCount.front());
        State = EState::RestoringData;

        LoadNextVector();
    }

    ///Finalize will spill all the contents of inner buffer if any.
    ///Is case if buffer is not ready async write operation will be started.
    void Finalize() {
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        if (Buffer.empty()) {
            State = EState::AcceptingDataRequests;
            return;
        }

        SaveBuffer();
        IsFinalizing = true;
    }

private:

    void CopyRopeToTheEndOfVector(std::vector<T, Alloc>& vec, TRope& rope) {
        for (auto it = rope.begin(); it != rope.end(); ++it) {
            const T* data = reinterpret_cast<const T*>(it.ContiguousData());
            vec.insert(vec.end(), data, data + it.ContiguousSize() / sizeof(T)); // size is always multiple of sizeof(T)
        }
    }

    void LoadNextVector() {
        auto requestedVectorSize= StoredChunksElementsCount.front();
        MKQL_ENSURE(requestedVectorSize >= CurrentVector.size(), "Internal logic error");
        size_t sizeToLoad = (requestedVectorSize - CurrentVector.size()) * sizeof(T);

        if (Buffer.size() >= sizeToLoad) {
            // if all the data for requested vector is ready
            TRope remainingPartOfVector = Buffer.Extract(Buffer.Position(0), Buffer.Position(sizeToLoad));
            CopyRopeToTheEndOfVector(CurrentVector, remainingPartOfVector);
            State = EState::DataReady;
        } else {
            CopyRopeToTheEndOfVector(CurrentVector, Buffer);
            ReadOperation = Spiller->Extract(StoredChunks.front());
        }
    }

    void SaveBuffer() {
        State = EState::SpillingData;
        WriteOperation = Spiller->Put(std::move(Buffer));
    }

    void AddDataToRope(const T* data, size_t count) {
        TRope tmp = TRope::Uninitialized(count * sizeof(T));
        TRopeUtils::Memcpy(tmp.begin(), reinterpret_cast<const char*>(data), count * sizeof(T));
        Buffer.Insert(Buffer.End(), std::move(tmp));
    }

    void SaveNextPartOfVector() {
        size_t maxFittingElemets = (SizeLimit - Buffer.size()) / sizeof(T);
        size_t remainingElementsInVector = CurrentVector.size() - NextVectorPositionToSave;
        size_t elementsToCopyFromVector = std::min(maxFittingElemets, remainingElementsInVector);

        AddDataToRope(CurrentVector.data() + NextVectorPositionToSave, elementsToCopyFromVector);

        NextVectorPositionToSave += elementsToCopyFromVector;
        if (NextVectorPositionToSave >= CurrentVector.size()) {
            CurrentVector.resize(0);
            NextVectorPositionToSave = 0;
        }

        if (SizeLimit - Buffer.size() < sizeof(T)) {
            SaveBuffer();
            return;
        }

        State = EState::AcceptingData;
    }

private:
    EState State = EState::AcceptingData;
     
    ISpiller::TPtr Spiller;
    const size_t SizeLimit;
    TRope Buffer;

    // Used to store vector while spilling and also used while restoring the data
    std::vector<T, Alloc> CurrentVector;
    size_t NextVectorPositionToSave = 0;

    std::queue<ISpiller::TKey> StoredChunks; 
    std::queue<size_t> StoredChunksElementsCount;

    std::optional<NThreading::TFuture<ISpiller::TKey>> WriteOperation = std::nullopt;
    std::optional<NThreading::TFuture<std::optional<TRope>>> ReadOperation = std::nullopt;

    bool IsFinalizing = false;
};

}//namespace NKikimr::NMiniKQL