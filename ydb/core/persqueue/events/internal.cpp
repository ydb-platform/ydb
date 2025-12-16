#include "internal.h"

namespace NKikimr::NPQ {

    TRequestedBlob::TRequestedBlob(ui64 offset, ui16 partNo, ui32 count, ui16 internalPartsCount, ui32 size, TString value, const TKey& key, ui64 creationUnixTime)
        : Offset(offset)
        , PartNo(partNo)
        , Count(count)
        , InternalPartsCount(internalPartsCount)
        , Size(size)
        , PackedSize(value.size())
        , UnpackedSize(0)
        , Cached(false)
        , Key(key)
        , CreationUnixTime(creationUnixTime)
        , Batches(std::make_shared<TVector<TBatch>>())
    {        
        if (value.empty()) {
            return;
        }

        for (TBlobIterator it(Key, value); it.IsValid(); it.Next()) {
            auto batch = it.GetBatch();
            batch.Unpack();
            UnpackedSize += batch.GetUnpackedSize();
            Batches->push_back(std::move(batch));
        }
        AFL_ENSURE(PackedSize <= Size)
            ("packedSize", PackedSize)
            ("Size", Size)
            ("key", Key.ToString());
    }

    bool TRequestedBlob::Empty() const {
        return !Batches || Batches->empty();
    }

    void TRequestedBlob::Clear() {
        Batches.reset();
        UnpackedSize = 0;
    }

    void TRequestedBlob::SetValue(const TString& value) {
        Batches = std::make_shared<TVector<TBatch>>();
        PackedSize = value.size();
        
        for (TBlobIterator it(Key, value); it.IsValid(); it.Next()) {
            auto batch = it.GetBatch();
            batch.Unpack();
            UnpackedSize += batch.GetUnpackedSize();
            Batches->push_back(std::move(batch));
        }

        AFL_ENSURE(PackedSize <= Size)
            ("packedSize", PackedSize)
            ("Size", Size)
            ("key", Key.ToString());
    }
}

