#pragma once

#include <queue>

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/computation/mkql_spiller.h>

namespace NKikimr::NMiniKQL {

///Stores long vectors of any type (excepting pointers). Tested on int and chars.
///The adapter places moved vectors into buffer and flushes buffer on disk when sizeLimit is reached.
///Returns vectors in FIFO order.
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
        MKQL_ENSURE(State == EState::AcceptingData, "Internal logic error");

        CurrentVector = vec;
        StoredChunksElementsCount.push(vec.size());
        NextPositionToSave = 0;

        SaveNextPartOfVector();
    }

    void Update() {
        switch (State) {
            case EState::SpillingData:
                if (!WriteOperation.HasValue()) return;

                StoredChunks.push(WriteOperation.ExtractValue());
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
                if (!ReadOperation.HasValue()) return;
                // TODO: check spilling error here: YQL-17970
                CurrentChunk = std::move(ReadOperation.ExtractValue().value());

                LoadNextVector();
                return;
            default:
                return;
        }

    }

    std::vector<T> ExtractVector() {
        StoredChunksElementsCount.pop();
        State = EState::AcceptingDataRequests;
        return CurrentVector;
    }

    void RequestNextVector() {
        MKQL_ENSURE(State == EState::AcceptingDataRequests, "Internal logic error");
        MKQL_ENSURE(!StoredChunksElementsCount.empty(), "Internal logic error");

        CurrentVector.resize(0);
        CurrentVector.reserve(StoredChunksElementsCount.front());
        State = EState::RestoringData;

        LoadNextVector();
    }

    void Finalize() {
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        if (CurrentChunk.empty()) {
            State = EState::AcceptingDataRequests;
            return;
        }

        SaveCurrentChunk();
        IsFinalizing = true;
    }

private:

    void LoadNextVector() {
        auto requestedVectorSize= StoredChunksElementsCount.front();
        MKQL_ENSURE(requestedVectorSize >= CurrentVector.size(), "Internal logic error");
        size_t sizeToLoad = (requestedVectorSize - CurrentVector.size()) * sizeof(T);

        // if vector is fully loaded to memory now
        if (CurrentChunk.size() >= sizeToLoad) {
            auto data = CurrentChunk.GetContiguousSpan();
            const char* from = data.SubSpan(0, sizeToLoad).Data();
            const char* to = from + sizeToLoad;
            CurrentVector.insert(CurrentVector.end(), (T*)from, (T*)to);
            CurrentChunk = TRope(TString(data.Data() + sizeToLoad, data.size() - sizeToLoad));
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

    void SaveNextPartOfVector() {
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

        CurrentVector.resize(0);
        State = EState::AcceptingData;

        return;
    }

private:
    EState State = EState::AcceptingData;
    ISpiller::TPtr Spiller;
    const size_t SizeLimit;
    TRope CurrentChunk;

    // Used to store vector while spilling and also used while restoring the data
    std::vector<T> CurrentVector;
    size_t NextPositionToSave = 0;

    std::queue<ISpiller::TKey> StoredChunks; 
    std::queue<size_t> StoredChunksElementsCount;

    NThreading::TFuture<ISpiller::TKey> WriteOperation;
    NThreading::TFuture<std::optional<TRope>> ReadOperation;

    bool IsFinalizing = false;
};

}//namespace NKikimr::NMiniKQL
