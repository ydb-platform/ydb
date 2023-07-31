#pragma once

#include "header.h"

#include <library/cpp/packedtypes/longs.h>

#include <util/generic/typetraits.h>

#include <library/cpp/microbdb/noextinfo.pb.h>

inline bool operator<(const TNoExtInfo&, const TNoExtInfo&) {
    return false;
}

namespace NMicroBDB {
    Y_HAS_MEMBER(TExtInfo);

    template <class, bool>
    struct TSelectExtInfo;

    template <class T>
    struct TSelectExtInfo<T, false> {
        typedef TNoExtInfo TExtInfo;
    };

    template <class T>
    struct TSelectExtInfo<T, true> {
        typedef typename T::TExtInfo TExtInfo;
    };

    template <class T>
    class TExtInfoType {
    public:
        static const bool Exists = THasTExtInfo<T>::value;
        typedef typename TSelectExtInfo<T, Exists>::TExtInfo TResult;
    };

    Y_HAS_MEMBER(MakeExtKey);

    template <class, class, bool>
    struct TSelectMakeExtKey;

    template <class TVal, class TKey>
    struct TSelectMakeExtKey<TVal, TKey, false> {
        static inline void Make(TKey* to, typename TExtInfoType<TKey>::TResult*, const TVal* from, const typename TExtInfoType<TVal>::TResult*) {
            *to = *from;
        }
    };

    template <class TVal, class TKey>
    struct TSelectMakeExtKey<TVal, TKey, true> {
        static inline void Make(TKey* to, typename TExtInfoType<TKey>::TResult* toExt, const TVal* from, const typename TExtInfoType<TVal>::TResult* fromExt) {
            TVal::MakeExtKey(to, toExt, from, fromExt);
        }
    };

    template <typename T>
    inline size_t SizeOfExt(const T* rec, size_t* /*out*/ extLenSize = nullptr, size_t* /*out*/ extSize = nullptr) {
        if (!TExtInfoType<T>::Exists) {
            if (extLenSize)
                *extLenSize = 0;
            if (extSize)
                *extSize = 0;
            return SizeOf(rec);
        } else {
            size_t sz = SizeOf(rec);
            i64 l;
            int els = in_long(l, (const char*)rec + sz);
            if (extLenSize)
                *extLenSize = static_cast<size_t>(els);
            if (extSize)
                *extSize = static_cast<size_t>(l);
            return sz;
        }
    }

    template <class T>
    bool GetExtInfo(const T* rec, typename TExtInfoType<T>::TResult* extInfo) {
        Y_VERIFY(TExtInfoType<T>::Exists, "GetExtInfo should only be used with extended records");
        if (!rec)
            return false;
        size_t els;
        size_t es;
        size_t s = SizeOfExt(rec, &els, &es);
        const ui8* raw = (const ui8*)rec + s + els;
        return extInfo->ParseFromArray(raw, es);
    }

    template <class T>
    const ui8* GetExtInfoRaw(const T* rec, size_t* len) {
        Y_VERIFY(TExtInfoType<T>::Exists, "GetExtInfo should only be used with extended records");
        if (!rec) {
            *len = 0;
            return nullptr;
        }
        size_t els;
        size_t es;
        size_t s = SizeOfExt(rec, &els, &es);
        *len = els + es;
        return (const ui8*)rec + s;
    }

    // Compares serialized extInfo (e.g. for stable sort)
    template <class T>
    int CompareExtInfo(const T* a, const T* b) {
        Y_VERIFY(TExtInfoType<T>::Exists, "CompareExtInfo should only be used with extended records");
        size_t elsA, esA;
        size_t elsB, esB;
        SizeOfExt(a, &elsA, &esA);
        SizeOfExt(a, &elsB, &esB);
        if (esA != esB)
            return esA - esB;
        else
            return memcmp((const ui8*)a + elsA, (const ui8*)b + elsB, esA);
    }

}

using NMicroBDB::TExtInfoType;

template <class TVal, class TKey>
struct TMakeExtKey {
    static const bool Exists = NMicroBDB::THasMakeExtKey<TVal>::value;
    static inline void Make(TKey* to, typename TExtInfoType<TKey>::TResult* toExt, const TVal* from, const typename TExtInfoType<TVal>::TResult* fromExt) {
        NMicroBDB::TSelectMakeExtKey<TVal, TKey, Exists>::Make(to, toExt, from, fromExt);
    }
};
