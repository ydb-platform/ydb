#pragma once
#include "mkql_spiller.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_pack.h>

namespace NKikimr::NMiniKQL {

/// Stores and loads very long sequences of TMultiType UVs
/// Can split sequences into chunks
/// Sends chunks to ISplitter and keeps assigned keys
/// When all data is written switches to read mode. Switching back to writing mode is not supported
/// Provides an interface for sequential read (like forward iterator)
/// When interaction with ISpiller is required, Write and Read operations return a Future
class TWideUnboxedValuesSpillerAdapter {
public:
    TWideUnboxedValuesSpillerAdapter(ISpiller::TPtr spiller, const TMultiType* type, size_t sizeLimit, ui64 minMemorySizeToReport = 10_KB)
        : Spiller_(spiller)
        , ItemType_(type)
        , SizeLimit_(sizeLimit)
        , Packer_(type, EValuePackerVersion::V1)
        , MinMemorySizeToReport_(minMemorySizeToReport)
    {
    }

    /// Write wide UV item
    /// \returns
    ///  - nullopt, if thee values are accumulated
    ///  - TFeature, if the values are being stored asynchronously and a caller must wait until async operation ends
    ///    In this case a caller must wait operation completion and call StoreCompleted.
    ///    Design note: not using Subscribe on a Future here to avoid possible race condition
    std::optional<NThreading::TFuture<ISpiller::TKey>> WriteWideItem(const TArrayRef<NUdf::TUnboxedValuePod>& wideItem) {
        Packer_.AddWideItem(wideItem.data(), wideItem.size());
        return FlushPacker(false);
    }

    std::optional<NThreading::TFuture<ISpiller::TKey>> FinishWriting() {
        return FlushPacker(true);
    }

    void AsyncWriteCompleted(ISpiller::TKey key) {
        StoredChunks_.push_back(key);
        ReportPackerFreed();
    }

    // Extracting interface
    bool Empty() const {
        return StoredChunks_.empty() && !CurrentBatch_;
    }
    std::optional<NThreading::TFuture<std::optional<NYql::TChunkedBuffer>>> ExtractWideItem(const TArrayRef<NUdf::TUnboxedValue>& wideItem) {
        MKQL_ENSURE(!Empty(), "Internal logic error");
        if (CurrentBatch_) {
            auto row = CurrentBatch_->Head();
            for (size_t i = 0; i != wideItem.size(); ++i) {
                wideItem[i] = row[i];
            }
            CurrentBatch_->Pop();
            if (CurrentBatch_->empty()) {
                CurrentBatch_ = std::nullopt;
            }
            return std::nullopt;
        } else {
            auto r = Spiller_->Get(StoredChunks_.front());
            StoredChunks_.pop_front();
            return r;
        }
    }

    void AsyncReadCompleted(NYql::TChunkedBuffer&& rope, const THolderFactory& holderFactory) {
        // Implementation detail: deserialization is performed in a processing thread
        TUnboxedValueBatch batch(ItemType_);
        Packer_.UnpackBatch(std::move(rope), holderFactory, batch);
        CurrentBatch_ = std::move(batch);
    }

private:
    void ReportPackerSize(ui64 currentSize, bool forced) {
        if (currentSize < ReportedPackerSize_) {
            Y_DEBUG_ABORT("Packer size should always grow");
            return;
        }
        ui64 sizeDiff = currentSize - ReportedPackerSize_;
        if (sizeDiff > MinMemorySizeToReport_ || forced) {
            Spiller_->ReportAlloc(sizeDiff);
            ReportedPackerSize_ += sizeDiff;
        }
    }

    void ReportPackerFreed() {
        Spiller_->ReportFree(ReportedPackerSize_);
        ReportedPackerSize_ = 0;
    }

    std::optional<NThreading::TFuture<ISpiller::TKey>> FlushPacker(bool forced) {
        if (Packer_.IsEmpty()) {
            return std::nullopt;
        }

        ui64 estimatedPackedSize = Packer_.PackedSizeEstimate();
        ReportPackerSize(estimatedPackedSize, forced);
        if (estimatedPackedSize > SizeLimit_ || forced) {
            return Spiller_->Put(std::move(Packer_.Finish()));
        }

        return std::nullopt;
    }

    ISpiller::TPtr Spiller_;
    const TMultiType* const ItemType_;
    const size_t SizeLimit_;
    TValuePackerTransport<false> Packer_;
    std::deque<ISpiller::TKey> StoredChunks_;
    std::optional<TUnboxedValueBatch> CurrentBatch_;
    ui64 ReportedPackerSize_ = 0;
    const ui64 MinMemorySizeToReport_;
};

} // namespace NKikimr::NMiniKQL
