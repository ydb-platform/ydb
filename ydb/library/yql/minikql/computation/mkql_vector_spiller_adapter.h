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
        return CurrentVector.empty() && !WriteOperation.has_value();
    }

    
    bool AddData(std::vector<T>&& vec) {
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        MKQL_ENSURE(!WriteOperation.has_value(), "Internal logic error");

        MKQL_ENSURE(State == EState::AcceptingData, "Internal logic error");

        CurrentVector = vec;
        StoredChunksSizes.push(vec.size());
        NextPositionToSave = 0;

        return SaveNextChunk();
    }

    void Update() {
        if (State == EState::SpillingData) {
            if (CurrentVector.empty() && !WriteOperation.has_value()) return;
            if (!WriteOperation->HasValue()) return;

            StoredChunks.push(WriteOperation->ExtractValue());
            WriteOperation = std::nullopt;

            SaveNextChunk();
            return;
        }

        if (State == EState::RestoringData) {
            if (!ReadOperation.has_value() || !ReadOperation->HasValue()) return;

            CurrentChunk = std::move(ReadOperation->ExtractValue().value());
            ReadOperation = std::nullopt;

            LoadNextVector();
            
        }
    }

    std::vector<T> ExtractVector() {
        StoredChunksSizes.pop();
        return std::move(CurrentVector);
    }

    void RequestNextVector() {
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        MKQL_ENSURE(State == EState::AcceptindDataRequests, "Internal logic error");
        MKQL_ENSURE(!StoredChunksSizes.empty(), "Internal logic error");

        CurrentVector.reserve(StoredChunks.front());

        LoadNextVector();

    }

    bool Finalize() {
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        MKQL_ENSURE(!WriteOperation.has_value(), "Internal logic error");
        if (CurrentChunk.empty()) return true;

        SaveCurrentChunk();
        return false;
    }

private:

    bool LoadNextVector() {
        if (ReadOperation.has_value()) return false;

        auto requestedVectorSize = StoredChunksSizes.front();

        // if vector is fully loaded to memory now
        if (CurrentChunk.size() >= requestedVectorSize) {
            auto data = CurrentChunk.GetContiguousSpan();
            CurrentVector.insert(CurrentVector.End(), data.Data(), requestedVectorSize);
            CurrentChunk = TRope(TString(data.Data() + requestedVectorSize, data.size() - requestedVectorSize));
            State = EState::DataReady;
        } else {
            CurrentVector.insert(CurrentVector.End(), CurrentChunk.GetContiguousSpan());
            ReadOperation = Spiller->Extract(StoredChunks.front());
            StoredChunks.pop();
        }
    }

    void SaveCurrentChunk() {
        State = EState::SpillingData;
        WriteOperation = Spiller->Put(std::move(CurrentChunk));
    }

    bool IsDataFittingInCurrentChunk() {
        return (SizeLimit - CurrentChunk.size()) >= (CurrentVector.size() - NextPositionToSave) * sizeof(T);
    }

    void AddDataToRope(T* data, size_t count) {
        CurrentChunk.End(), TRope(TString(reinterpret_cast<const char*>(data, count * sizeof(T))));
    }

    // TODO check empty vector in the middle
    bool SaveNextChunk() {
        MKQL_ENSURE(!CurrentVector.empty(), "Internal logic error");

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
            return false;
        }

        return true;
    }

    bool LoadNextChunk() {
        MKQL_ENSURE(State == EState::RestoringData, "Internal logic error");
        MKQL_ENSURE(!ReadOperation.has_value(), "Internal logic error");

        auto nextId = StoredChunks.front();
        StoredChunks.pop();

        ReadOperation = Spiller->Extract(nextId);
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
};

}//namespace NKikimr::NMiniKQL
