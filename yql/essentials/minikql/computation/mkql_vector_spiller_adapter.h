#pragma once

#include <queue>

#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/computation/mkql_spiller.h>
#include <yql/essentials/minikql/mkql_alloc.h>

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
        : Spiller_(spiller)
        , SizeLimit_(sizeLimit)
    {
    }

    ///Returns current stete of the adapter
    EState GetState() const {
        return State_;
    }

    ///Is adapter ready to spill next vector via AddData method.
    ///Returns false in case when there are async operations in progress.
    bool IsAcceptingData() {
        return State_ == EState::AcceptingData;
    }

    ///When data is ready ExtractVector() is expected to be called.
    bool IsDataReady() {
        return State_ == EState::DataReady;
    }

    ///Adapter is ready to accept requests for vectors.
    bool IsAcceptingDataRequests() {
        return State_ == EState::AcceptingDataRequests;
    }

    ///Adds new vector to storage. Will not launch real disk operation if case of small vectors
    ///(if inner buffer is not full).
    void AddData(std::vector<T, Alloc>&& vec) {
        MKQL_ENSURE(CurrentVector_.empty(), "Internal logic error");
        MKQL_ENSURE(State_ == EState::AcceptingData, "Internal logic error");

        StoredChunksElementsCount_.push(vec.size());
        CurrentVector_ = std::move(vec);
        NextVectorPositionToSave_ = 0;

        SaveNextPartOfVector();
    }

    ///Should be used to update async operatrions statuses.
    ///For SpillingData state it will try to spill more content of inner buffer.
    ///ForRestoringData state it will try to load more content of requested vector.
    void Update() {
        switch (State_) {
            case EState::SpillingData:
                MKQL_ENSURE(WriteOperation_.has_value(), "Internal logic error");
                if (!WriteOperation_->HasValue()) return;

                StoredChunks_.push(WriteOperation_->ExtractValue());
                WriteOperation_ = std::nullopt;
                if (IsFinalizing_) {
                    State_ = EState::AcceptingDataRequests;
                    return;
                }

                if (CurrentVector_.empty()) {
                    State_ = EState::AcceptingData;
                    return;
                }

                SaveNextPartOfVector();
                return;
            case EState::RestoringData:
                MKQL_ENSURE(ReadOperation_.has_value(), "Internal logic error");
                if (!ReadOperation_->HasValue()) return;
                Buffer_ = std::move(ReadOperation_->ExtractValue().value());
                ReadOperation_ = std::nullopt;
                StoredChunks_.pop();

                LoadNextVector();
                return;
            default:
                return;
        }
    }

    ///Get requested vector.
    std::vector<T, Alloc>&& ExtractVector() {
        StoredChunksElementsCount_.pop();
        State_ = EState::AcceptingDataRequests;
        return std::move(CurrentVector_);
    }

    ///Start restoring next vector. If th eentire contents of the vector are in memory
    ///State will be changed to DataREady without any async read operation. ExtractVector is expected
    ///to be called immediately.
    void RequestNextVector() {
        MKQL_ENSURE(State_ == EState::AcceptingDataRequests, "Internal logic error");
        MKQL_ENSURE(CurrentVector_.empty(), "Internal logic error");
        MKQL_ENSURE(!StoredChunksElementsCount_.empty(), "Internal logic error");

        CurrentVector_.reserve(StoredChunksElementsCount_.front());
        State_ = EState::RestoringData;

        LoadNextVector();
    }

    ///Finalize will spill all the contents of inner buffer if any.
    ///Is case if buffer is not ready async write operation will be started.
    void Finalize() {
        MKQL_ENSURE(CurrentVector_.empty(), "Internal logic error");
        if (Buffer_.Empty()) {
            State_ = EState::AcceptingDataRequests;
            return;
        }

        SaveBuffer();
        IsFinalizing_ = true;
    }

private:
    class TVectorStream : public IOutputStream {
    public:
        explicit TVectorStream(std::vector<T, Alloc>& vec)
            : Dst_(vec)
        {
        }
    private:
        virtual void DoWrite(const void* buf, size_t len) override {
            MKQL_ENSURE(len % sizeof(T) == 0, "size should always by multiple of sizeof(T)");
            const T* data = reinterpret_cast<const T*>(buf);
            Dst_.insert(Dst_.end(), data, data + len / sizeof(T));
        }
        std::vector<T, Alloc>& Dst_;
    };

    void CopyRopeToTheEndOfVector(std::vector<T, Alloc>& vec, const NYql::TChunkedBuffer& rope, size_t toCopy = std::numeric_limits<size_t>::max()) {
        TVectorStream out(vec);
        rope.CopyTo(out, toCopy);
    }

    void LoadNextVector() {
        auto requestedVectorSize= StoredChunksElementsCount_.front();
        MKQL_ENSURE(requestedVectorSize >= CurrentVector_.size(), "Internal logic error");
        size_t sizeToLoad = (requestedVectorSize - CurrentVector_.size()) * sizeof(T);

        if (Buffer_.Size() >= sizeToLoad) {
            // if all the data for requested vector is ready
            CopyRopeToTheEndOfVector(CurrentVector_, Buffer_, sizeToLoad);
            Buffer_.Erase(sizeToLoad);
            State_ = EState::DataReady;
        } else {
            CopyRopeToTheEndOfVector(CurrentVector_, Buffer_);
            ReadOperation_ = Spiller_->Extract(StoredChunks_.front());
        }
    }

    void SaveBuffer() {
        State_ = EState::SpillingData;
        WriteOperation_ = Spiller_->Put(std::move(Buffer_));
    }

    void AddDataToRope(const T* data, size_t count) {
        auto owner = std::make_shared<std::vector<T>>(data, data + count);
        TStringBuf buf(reinterpret_cast<const char *>(owner->data()), count * sizeof(T));
        Buffer_.Append(buf, owner);
    }

    void SaveNextPartOfVector() {
        size_t maxFittingElemets = (SizeLimit_ - Buffer_.Size()) / sizeof(T);
        size_t remainingElementsInVector = CurrentVector_.size() - NextVectorPositionToSave_;
        size_t elementsToCopyFromVector = std::min(maxFittingElemets, remainingElementsInVector);

        AddDataToRope(CurrentVector_.data() + NextVectorPositionToSave_, elementsToCopyFromVector);

        NextVectorPositionToSave_ += elementsToCopyFromVector;
        if (NextVectorPositionToSave_ >= CurrentVector_.size()) {
            CurrentVector_.resize(0);
            NextVectorPositionToSave_ = 0;
        }

        if (SizeLimit_ - Buffer_.Size() < sizeof(T)) {
            SaveBuffer();
            return;
        }

        State_ = EState::AcceptingData;
    }

private:
    EState State_ = EState::AcceptingData;

    ISpiller::TPtr Spiller_;
    const size_t SizeLimit_;
    NYql::TChunkedBuffer Buffer_;

    // Used to store vector while spilling and also used while restoring the data
    std::vector<T, Alloc> CurrentVector_;
    size_t NextVectorPositionToSave_ = 0;

    std::queue<ISpiller::TKey> StoredChunks_;
    std::queue<size_t> StoredChunksElementsCount_;

    std::optional<NThreading::TFuture<ISpiller::TKey>> WriteOperation_ = std::nullopt;
    std::optional<NThreading::TFuture<std::optional<NYql::TChunkedBuffer>>> ReadOperation_ = std::nullopt;

    bool IsFinalizing_ = false;
};

}//namespace NKikimr::NMiniKQL
