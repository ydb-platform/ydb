#ifndef DESCRIPTORS_INL_H_
#error "Direct inclusion of this file is not allowed, include descriptors.h"
// For the sake of sane code completion.
#include "descriptors.h"
#endif

#include "polymorphic.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T* TTypeDescriptor::TryConstruct() const
{
    if constexpr(NDetail::TPolymorphicTraits<T>::Polymorphic) {
        return PolymorphicConstructor_ ? dynamic_cast<T*>(PolymorphicConstructor_()) : nullptr;
    } else {
        return ConcreteConstructor_ ? static_cast<T*>(ConcreteConstructor_()) : nullptr;
    }
}

template <class T>
T* TTypeDescriptor::ConstructOrThrow() const
{
    auto* instance = TryConstruct<T>();
    if (!instance) {
        THROW_ERROR_EXCEPTION("Cannot instantiate object of type %v",
            Name_);
    }
    return instance;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2
