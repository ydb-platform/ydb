#pragma once
#include "mkql_spiller.h"
#include <queue>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>


namespace NKikimr::NMiniKQL {

///Stores and loads very long sequences of TMultiType UVs
///Can split sequences into chunks
///Sends chunks to ISplitter and keeps assigned keys
///When all data is written switches to read mode. Switching back to writing mode is not supported
///Provides an interface for sequential read (like forward iterator)
///When interaction with ISpiller is required, Write and Read operations return a Future
template<class T>
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

    EState GetState() const {
        return State;
    }

    bool IsAcceptingData() {
        return State == EState::AcceptingData;
    }

    bool IsDataReady() {
        return State == EState::DataReady;
    }

    bool IsAcceptingDataRequests() {
        return State == EState::AcceptingDataRequests;
    }

    
    void AddData(std::vector<T>&& vec) {
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        MKQL_ENSURE(!WriteOperation.has_value(), "Internal logic error");

        MKQL_ENSURE(State == EState::AcceptingData, "Internal logic error");

        CurrentVector = vec;
        StoredChunksSizes.push(vec.size() * sizeof(T));
        NextPositionToSave = 0;

        SaveNextChunk();
    }

    void Update() {
        if (State == EState::SpillingData) {
            if (CurrentVector.empty() && !WriteOperation.has_value()) return;
            if (!WriteOperation->HasValue()) return;

            StoredChunks.push(WriteOperation->ExtractValue());
            WriteOperation = std::nullopt;
            if (IsFinalizing) {
                State = EState::AcceptingDataRequests;
                return;
            }

            SaveNextChunk();
            return;
        }

        if (State == EState::RestoringData) {
            if (!ReadOperation.has_value() || !ReadOperation->HasValue()) return;

            CurrentChunk = std::move(ReadOperation->ExtractValue().value());
            ReadOperation = std::nullopt;

            auto sizeToBeCopiedToVector = CurrentChunk.size();

            LoadNextVector();

            StoredChunksSizes.front() -= sizeToBeCopiedToVector;
            
        }
    }

    std::vector<T> ExtractVector() {
        StoredChunksSizes.pop();
        State = EState::AcceptingDataRequests;
        return std::move(CurrentVector);
    }

    void RequestNextVector() {
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        MKQL_ENSURE(State == EState::AcceptingDataRequests, "Internal logic error");
        MKQL_ENSURE(!StoredChunksSizes.empty(), "Internal logic error");

        CurrentVector.reserve(StoredChunksSizes.front());
        State = EState::RestoringData;
        LoadNextVector();

    }

    void Finalize() {
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        MKQL_ENSURE(!WriteOperation.has_value(), "Internal logic error");
        if (CurrentChunk.empty()) {
            State = EState::AcceptingDataRequests;
            return;
        }

        SaveCurrentChunk();
        IsFinalizing = true;
    }

private:

    void LoadNextVector() {
        if (ReadOperation.has_value()) return;

        auto& requestedVectorSize = StoredChunksSizes.front();

        // if vector is fully loaded to memory now
        if (CurrentChunk.size() >= requestedVectorSize) {
            auto data = CurrentChunk.GetContiguousSpan();
            const char* from = data.SubSpan(0, requestedVectorSize).Data();
            const char* to = from + requestedVectorSize;
            CurrentVector.insert(CurrentVector.end(), (T*)from, (T*)to);
            CurrentChunk = TRope(TString(data.Data() + requestedVectorSize, data.size() - requestedVectorSize));
            State = EState::DataReady;
        } else {
            const char* from = CurrentChunk.GetContiguousSpan().Data();
            const char* to = from + CurrentChunk.GetContiguousSpan().Size();
            CurrentVector.insert(CurrentVector.end(), (T*)from, (T*)to);
            ReadOperation = Spiller->Extract(StoredChunks.front());
            StoredChunks.pop();
        }
    }

    void SaveCurrentChunk() {
        State = EState::SpillingData;
        WriteOperation = Spiller->Put(std::move(CurrentChunk));
        CurrentChunk = TRope();
    }

    bool IsDataFittingInCurrentChunk() {
        return (SizeLimit - CurrentChunk.size()) >= (CurrentVector.size() - NextPositionToSave) * sizeof(T);
    }

    void AddDataToRope(T* data, size_t count) {
        CurrentChunk.Insert(CurrentChunk.End(), TRope(TString(reinterpret_cast<const char*>(data), count * sizeof(T))));
    }

    // TODO check empty vector in the middle
    void SaveNextChunk() {
        if (CurrentVector.empty()) {
            State = EState::AcceptingData;
            return;
        }

        if (IsDataFittingInCurrentChunk()) {
            AddDataToRope(CurrentVector.data() + NextPositionToSave,  CurrentVector.size() - NextPositionToSave);
            CurrentVector.clear();
            NextPositionToSave = 0;
        } else {
            size_t elementsToAdd = (SizeLimit - CurrentChunk.size()) / sizeof(T);
            AddDataToRope(CurrentVector.data() + NextPositionToSave, elementsToAdd);
            NextPositionToSave += elementsToAdd;
        }

        if (SizeLimit - CurrentChunk.size() < sizeof(T)) {
            SaveCurrentChunk();
            return;
        }

        State = EState::AcceptingData;

        return;
    }

private:
    EState State = EState::AcceptingData;
    ISpiller::TPtr Spiller;
    const size_t SizeLimit;
    size_t RemainingSize = 0;
    char* SavingDataPtr = nullptr;
    TRope CurrentChunk;

    std::vector<T> CurrentVector;
    size_t NextPositionToSave = 0;

    std::queue<ISpiller::TKey> StoredChunks; 
    std::queue<size_t> StoredChunksSizes;

    std::optional<NThreading::TFuture<ISpiller::TKey>> WriteOperation;
    std::optional<NThreading::TFuture<std::optional<TRope>>> ReadOperation;

    bool IsFinalizing = false;
};

}//namespace NKikimr::NMiniKQL
