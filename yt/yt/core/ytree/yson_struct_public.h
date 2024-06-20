#pragma once

#include <library/cpp/yt/memory/serialize.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NYTree {

///////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnrecognizedStrategy,
    (Drop)
    (Keep)
    (KeepRecursive)
    (Throw)
    (ThrowRecursive)
);

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CExternalizedYsonStructTraits = requires {
    typename T::TExternalSerializer;
};

template <class T>
concept CExternallySerializable = requires (T* t) {
    { GetExternalizedYsonStructTraits(t) } -> CExternalizedYsonStructTraits;
};

template <CExternallySerializable T>
using TGetExternalizedYsonStructTraits = decltype(GetExternalizedYsonStructTraits(std::declval<T*>()));

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

template <class T>
struct TYsonSourceTraits;

} // namespace NPrivate

template <class T>
concept CYsonStructSource = NPrivate::TYsonSourceTraits<T>::IsValid;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
