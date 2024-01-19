#pragma once

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Specialized trait is assumed to have alias for Serializable.
//! Semanticallly TSerializer should have some way of (de-)serialization.
template <class T>
struct TSerializationTraits
{
    static constexpr bool IsSerializable = false;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CSerializableByTraits = TSerializationTraits<T>::IsSerializable;

////////////////////////////////////////////////////////////////////////////////

//! NB: Template class specialization is only allowed in namespaces enclosing the original one,
//! thus you can only use this macro inside namespaces "::", "::NYT", "::NYT::NYTree".
#define ASSIGN_EXTERNAL_YSON_SERIALIZER(Type, SerializerType) \
    template <> \
    struct ::NYT::NYTree::TSerializationTraits<Type> \
    { \
        static constexpr bool IsSerializable = true; \
        using TSerializer = SerializerType; \
    }; \

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
