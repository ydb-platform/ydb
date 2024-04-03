#pragma once
#include "mkql_spiller.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>


namespace NKikimr::NMiniKQL {

///Stores and loads very long sequences of TMultiType UVs
///Can split sequences into chunks
///Sends chunks to ISplitter and keeps assigned keys
///When all data is written switches to read mode. Switching back to writing mode is not supported
///Provides an interface for sequential read (like forward iterator)
///When interaction with ISpiller is required, Write and Read operations return a Future
class TWideUnboxedValuesSpillerAdapter {
public:
    TWideUnboxedValuesSpillerAdapter(ISpiller::TPtr spiller, const TMultiType* type, size_t sizeLimit)
        : Spiller(spiller)
        , ItemType(type)
        , SizeLimit(sizeLimit)
        , Packer(type)
    {
    }

    /// Write wide UV item
    /// \returns
    ///  - nullopt, if thee values are accumulated
    ///  - TFeature, if the values are being stored asynchronously and a caller must wait until async operation ends
    ///    In this case a caller must wait operation completion and call StoreCompleted.
    ///    Design note: not using Subscribe on a Future here to avoid possible race condition
    std::optional<NThreading::TFuture<ISpiller::TKey>> WriteWideItem(const TArrayRef<NUdf::TUnboxedValuePod>& wideItem) {
       Packer.AddWideItem(wideItem.data(), wideItem.size());
       if(Packer.PackedSizeEstimate() > SizeLimit) {
           return Spiller->Put(std::move(Packer.Finish()));
       } else {
           return std::nullopt;
       }
    }

    std::optional<NThreading::TFuture<ISpiller::TKey>> FinishWriting() {
        if (Packer.IsEmpty())
            return std::nullopt;
        return Spiller->Put(std::move(Packer.Finish()));
    }

    void AsyncWriteCompleted(ISpiller::TKey key) {
        StoredChunks.push_back(key);
    }

    //Extracting interface
    bool Empty() const {
       return StoredChunks.empty() && !CurrentBatch;
    }
    std::optional<NThreading::TFuture<std::optional<TRope>>> ExtractWideItem(const TArrayRef<NUdf::TUnboxedValue>& wideItem) {
        MKQL_ENSURE(!Empty(), "Internal logic error");
        if (CurrentBatch) {
            auto row = CurrentBatch->Head();
            for (size_t i = 0; i != wideItem.size(); ++i) {
                wideItem[i] = row[i];
            }
            CurrentBatch->Pop();
            if (CurrentBatch->empty()) {
                CurrentBatch = std::nullopt;
            }
            return std::nullopt;
        } else {
            auto r = Spiller->Get(StoredChunks.front());
            StoredChunks.pop_front();
            return r;
        }
    }

    void AsyncReadCompleted(TRope&& rope,const THolderFactory& holderFactory ) {
        //Implementation detail: deserialization is performed in a processing thread
        TUnboxedValueBatch batch(ItemType);
        Packer.UnpackBatch(std::move(rope), holderFactory, batch);
        CurrentBatch = std::move(batch);
    }

private:
    ISpiller::TPtr Spiller;
    const TMultiType* const ItemType;
    const size_t SizeLimit;
    TValuePackerTransport<false> Packer;
    std::deque<ISpiller::TKey> StoredChunks;
    std::optional<TUnboxedValueBatch> CurrentBatch;
};

}//namespace NKikimr::NMiniKQL
