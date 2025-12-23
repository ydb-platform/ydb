#include "internal.h"

namespace NKikimr::NPQ {

    TRequestedBlob::TRequestedBlob(ui64 offset, ui16 partNo, ui32 count, ui16 internalPartsCount, ui32 size, TString value, const TKey& key, ui64 creationUnixTime)
        : Key(key)
        , RawValue(std::move(value))
        , Offset(offset)
        , PartNo(partNo)
        , Count(count)
        , InternalPartsCount(internalPartsCount)
        , Size(size)
        , Cached(false)
        , CreationUnixTime(creationUnixTime)
        , Batches(nullptr)
    {        
        AFL_ENSURE(RawValue.size() <= Size)
            ("RawValue.size()", RawValue.size())
            ("Size", Size)
            ("key", Key.ToString());
    }

    bool TRequestedBlob::Empty() const {
        return !Batches && (RawValue.is_null() || RawValue.empty());
    }

    void TRequestedBlob::Clear() {
        if (Batches) {
            Batches.reset();
        }

        if (RawValue) {
            RawValue.clear();
        }
    }

    std::shared_ptr<TVector<TBatch>> TRequestedBlob::GetBatches() const {
        if (Batches) {
            return Batches;
        }

        Batches = std::make_shared<TVector<TBatch>>(GetUnpackedBatches(Key, RawValue));
        RawValue.clear();
        return Batches;
    }
}

