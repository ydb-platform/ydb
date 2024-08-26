#ifndef PHOENIX_INL_H_
#error "Direct inclusion of this file is not allowed, include phoenix.h"
// For the sake of sane code completion.
#include "phoenix.h"
#endif

#include <type_traits>

namespace NYT::NPhoenix {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void* TProfiler::DoInstantiate()
{
    using TFactory = typename TFactoryTraits<T>::TFactory;
    using TBase = typename TPolymorphicTraits<T>::TBase;

    T* ptr = TFactory::template Instantiate<T>();
    TBase* basePtr = static_cast<TBase*>(ptr);
    return basePtr;
}

template <class T>
void TProfiler::Register(ui32 tag)
{
    using TIdClass = typename TIdClass<T>::TType;

    auto pair = TagToEntry_.emplace(tag, TEntry());
    YT_VERIFY(pair.second);
    auto& entry = pair.first->second;
    entry.Tag = tag;
    entry.TypeInfo = &typeid(TIdClass);
    entry.Factory = std::bind(&DoInstantiate<T>);
    YT_VERIFY(TypeInfoToEntry_.emplace(entry.TypeInfo, &entry).second);
}

template <class T>
T* TProfiler::Instantiate(ui32 tag)
{
    using TBase = typename TPolymorphicTraits<T>::TBase;
    TBase* basePtr = static_cast<TBase*>(GetEntry(tag).Factory());
    return dynamic_cast<T*>(basePtr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix
