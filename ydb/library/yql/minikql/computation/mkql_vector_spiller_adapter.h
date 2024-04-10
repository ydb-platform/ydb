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
    void AddData(std::vector<T>&& vec) {
        MKQL_ENSURE(CurrentVector.empty(), "Internal logic error");
        MKQL_ENSURE(State == EState::AcceptingData, "Internal logic error");

        CurrentVector = vec;
        StoredChunksElementsCount.push(vec.size());
        NextVectorPositionToSave = 0;

        SaveNextPartOfVector();
    }

    ///Should be used to update async operatrions statuses. 
    ///For SpillingData state it will try to spill more content of inner buffer.
    ///ForRestoringData state it will try to load more content of requested vector.
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
                Buffer = std::move(ReadOperation.ExtractValue().value());

                LoadNextVector();
                return;
            default:
                return;
        }
    }

    ///Get requested vector.
    std::vector<T> ExtractVector() {
        StoredChunksElementsCount.pop();
        State = EState::AcceptingDataRequests;
        return CurrentVector;
    }

    ///Start restoring next vector. If th eentire contents of the vector are in memory
    ///State will be changed to DataREady without any async read operation. ExtractVector is expected
    ///to be called immediately.
    void RequestNextVector() {
        MKQL_ENSURE(State == EState::AcceptingDataRequests, "Internal logic error");
        MKQL_ENSURE(!StoredChunksElementsCount.empty(), "Internal logic error");

        CurrentVector.resize(0);
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

    void LoadNextVector() {
        auto requestedVectorSize= StoredChunksElementsCount.front();
        MKQL_ENSURE(requestedVectorSize >= CurrentVector.size(), "Internal logic error");
        size_t sizeToLoad = (requestedVectorSize - CurrentVector.size()) * sizeof(T);

        // if vector is fully loaded to memory now
        if (Buffer.size() >= sizeToLoad) {
            auto data = Buffer.GetContiguousSpan();
            const char* from = data.SubSpan(0, sizeToLoad).Data();
            const char* to = from + sizeToLoad;
            CurrentVector.insert(CurrentVector.end(), (T*)from, (T*)to);
            Buffer = TRope(TString(data.Data() + sizeToLoad, data.size() - sizeToLoad));
            State = EState::DataReady;
        } else {
            const char* from = Buffer.GetContiguousSpan().Data();
            const char* to = from + Buffer.GetContiguousSpan().Size();
            CurrentVector.insert(CurrentVector.end(), (T*)from, (T*)to);
            ReadOperation = Spiller->Extract(StoredChunks.front());
            StoredChunks.pop();
        }
    }

    void SaveBuffer() {
        State = EState::SpillingData;
        WriteOperation = Spiller->Put(std::move(Buffer));
    }

    bool IsRestOfVectorFittingIntoBuffer() {
        return (SizeLimit - Buffer.size()) >= (CurrentVector.size() - NextVectorPositionToSave) * sizeof(T);
    }

    void AddDataToRope(T* data, size_t count) {
        Buffer.Insert(Buffer.End(), TRope(TString(reinterpret_cast<const char*>(data), count * sizeof(T))));
    }

    void SaveNextPartOfVector() {

        if (IsRestOfVectorFittingIntoBuffer()) {
            AddDataToRope(CurrentVector.data() + NextVectorPositionToSave,  CurrentVector.size() - NextVectorPositionToSave);
            CurrentVector.clear();
            NextVectorPositionToSave = 0;
        } else {
            size_t elementsToAdd = (SizeLimit - Buffer.size()) / sizeof(T);
            AddDataToRope(CurrentVector.data() + NextVectorPositionToSave, elementsToAdd);
            NextVectorPositionToSave += elementsToAdd;
        }

        if (SizeLimit - Buffer.size() < sizeof(T)) {
            SaveBuffer();
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
    TRope Buffer;

    // Used to store vector while spilling and also used while restoring the data
    std::vector<T> CurrentVector;
    size_t NextVectorPositionToSave = 0;

    std::queue<ISpiller::TKey> StoredChunks; 
    std::queue<size_t> StoredChunksElementsCount;

    NThreading::TFuture<ISpiller::TKey> WriteOperation;
    NThreading::TFuture<std::optional<TRope>> ReadOperation;

    bool IsFinalizing = false;
};

}//namespace NKikimr::NMiniKQL
