#include "internal.h"

namespace NKikimr::NPQ {

    TRequestedBlob::TRequestedBlob(ui64 offset, ui16 partNo, ui32 count, ui16 internalPartsCount, ui32 size, TString value, const TKey& key, ui64 creationUnixTime)
        : Value(std::move(value))
        , Offset(offset)
        , PartNo(partNo)
        , Count(count)
        , InternalPartsCount(internalPartsCount)
        , Size(size)
        , Cached(false)
        , Key(key)
        , CreationUnixTime(creationUnixTime)
        , Batches(nullptr)
    {
    }

    bool TRequestedBlob::Empty() const {
        return !Batches || Batches->empty();
    }

    void TRequestedBlob::ExtractBatches() {
        if (!Empty()) {
            return;
        }

        Batches = std::make_shared<TVector<TBatch>>();
        for (TBlobIterator it(Key, Value); it.IsValid(); it.Next()) {
            auto batch = it.GetBatch();
            batch.Unpack();
            Batches->push_back(std::move(batch));
        }
    }

    void TRequestedBlob::Clear() {
        Value.clear();
        Batches.reset();
    }

    void TRequestedBlob::SetValue(const TString& value) {
        Value = value;
    }

    void TRequestedBlob::Verify() const {
        ui32 size = 0;
        for (const auto& batch : *Batches) {
            size += batch.GetUnpackedSize();
        }

        AFL_ENSURE(size <= Size)
            ("size", size)
            ("Size", Size)
            ("key", Key.ToString());
        // Do we need this check?
        // TClientBlob::CheckBlob(Key, Value);
    }
}

