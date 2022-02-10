#pragma once

#include <util/generic/array_ref.h>
#include <util/memory/blob.h>
#include <util/memory/tempbuf.h>
#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/typetraits.h>

#include <array>
#include <string>
#include <utility>

template <typename T>
struct TMemoryTraits {
    enum {
        SimpleMemory = std::is_arithmetic<T>::value,
        ContinuousMemory = SimpleMemory,
        OwnsMemory = SimpleMemory,
    };

    using TElementType = T;
};

template <typename T, size_t n>
struct TMemoryTraits<T[n]> {
    enum {
        SimpleMemory = TMemoryTraits<T>::SimpleMemory,
        ContinuousMemory = SimpleMemory,
        OwnsMemory = SimpleMemory,
    };

    using TElementType = T;
};

template <typename T, size_t n>
struct TMemoryTraits<std::array<T, n>> {
    enum {
        SimpleMemory = TMemoryTraits<T>::SimpleMemory,
        ContinuousMemory = SimpleMemory,
        OwnsMemory = SimpleMemory,
    };

    using TElementType = T;
};

template <typename A, typename B>
struct TMemoryTraits<std::pair<A, B>> {
    enum {
        SimpleMemory = TMemoryTraits<A>::SimpleMemory && TMemoryTraits<B>::SimpleMemory,
        ContinuousMemory = SimpleMemory,
        OwnsMemory = SimpleMemory,
    };

    using TElementType = std::pair<A, B>;
};

template <>
struct TMemoryTraits<TBuffer> {
    enum {
        SimpleMemory = false,
        ContinuousMemory = true,
        OwnsMemory = true,
    };

    using TElementType = char;
};

template <>
struct TMemoryTraits<TTempBuf> {
    enum {
        SimpleMemory = false,
        ContinuousMemory = true,
        OwnsMemory = true,
    };

    using TElementType = char;
};

template <>
struct TMemoryTraits< ::TBlob> {
    enum {
        SimpleMemory = false,
        ContinuousMemory = true,
        OwnsMemory = true,
    };

    using TElementType = char;
};

template <typename T>
struct TElementDependentMemoryTraits {
    enum {
        SimpleMemory = false,
        ContinuousMemory = TMemoryTraits<T>::SimpleMemory,
    };

    using TElementType = T;
};

template <typename T, typename TAlloc>
struct TMemoryTraits<std::vector<T, TAlloc>>: public TElementDependentMemoryTraits<T> {
    enum {
        OwnsMemory = TMemoryTraits<T>::OwnsMemory
    };
};

template <typename T, typename TAlloc>
struct TMemoryTraits<TVector<T, TAlloc>>: public TMemoryTraits<std::vector<T, TAlloc>> {
};

template <typename T>
struct TMemoryTraits<TTempArray<T>>: public TElementDependentMemoryTraits<T> {
    enum {
        OwnsMemory = TMemoryTraits<T>::OwnsMemory
    };
};

template <typename T, typename TCharTraits, typename TAlloc>
struct TMemoryTraits<std::basic_string<T, TCharTraits, TAlloc>>: public TElementDependentMemoryTraits<T> {
    enum {
        OwnsMemory = TMemoryTraits<T>::OwnsMemory
    };
};

template <>
struct TMemoryTraits<TString>: public TElementDependentMemoryTraits<char> {
    enum {
        OwnsMemory = true
    };
};

template <>
struct TMemoryTraits<TUtf16String>: public TElementDependentMemoryTraits<wchar16> {
    enum {
        OwnsMemory = true
    };
};

template <typename T>
struct TMemoryTraits<TArrayRef<T>>: public TElementDependentMemoryTraits<T> {
    enum {
        OwnsMemory = false
    };
};

template <typename TCharType, typename TCharTraits>
struct TMemoryTraits<TBasicStringBuf<TCharType, TCharTraits>>: public TElementDependentMemoryTraits<TCharType> {
    enum {
        OwnsMemory = false
    };
};

template <>
struct TMemoryTraits<TStringBuf>: public TElementDependentMemoryTraits<char> {
    enum {
        OwnsMemory = false
    };
};

template <>
struct TMemoryTraits<TWtringBuf>: public TElementDependentMemoryTraits<wchar16> {
    enum {
        OwnsMemory = false
    };
};
