#pragma once

#include "defs.h"

namespace NKikimr {

#pragma pack(push, 4)
template <class TKey, class TMemRec>
struct TIndexRecord {
private:
    TKey Key;
    TMemRec MemRec;

public:
    TIndexRecord() = default;

    TIndexRecord(const TKey& key)
        : Key(key)
        , MemRec()
    {}

    TIndexRecord(const TKey& key, const TMemRec& memRec)
        : Key(key)
        , MemRec(memRec)
    {}

    TKey GetKey() const {
        return ReadUnaligned<TKey>(&Key);
    }

    TMemRec GetMemRec() const {
        return ReadUnaligned<TMemRec>(&MemRec);
    }

    bool operator <(const TKey& key) const {
        return GetKey() < key;
    }

    bool operator <(const TIndexRecord& rec) const {
        return GetKey() < rec.GetKey();
    }

    bool operator ==(const TIndexRecord& rec) const {
        return GetKey() == rec.GetKey();
    }

    using TVec = TVector<TIndexRecord>;
};
#pragma pack(pop)

} // NKikimr
