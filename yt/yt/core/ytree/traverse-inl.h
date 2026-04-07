#ifndef TRAVERSE_INL_H_
#error "Direct inclusion of this file is not allowed, include traverse.h"
// For the sake of sane code completion.
#include "traverse.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived T>
void TraverseYsonStruct(const TYsonStructParameterVisitor& visitor, const NYPath::TYPath& path)
{
    auto meta = TYsonStructRegistry::Get()->GetMeta<T>();
    YT_VERIFY(meta);
    meta->Traverse(visitor, path);
}

template <CStrongTypedef T>
void TraverseYsonStruct(const TYsonStructParameterVisitor& visitor, const NYPath::TYPath& path)
{
    TraverseYsonStruct<typename T::TUnderlying>(visitor, path);
}

template <CList T>
void TraverseYsonStruct(const TYsonStructParameterVisitor& visitor, const NYPath::TYPath& path)
{
    TraverseYsonStruct<std::remove_cvref_t<decltype(*std::begin(T{}))>>(visitor, path + "/*");
}

template <CDict T>
void TraverseYsonStruct(const TYsonStructParameterVisitor& visitor, const NYPath::TYPath& path)
{
    TraverseYsonStruct<typename T::mapped_type>(visitor, path + "/*");;
}

template <CTuple T>
void TraverseYsonStruct(const TYsonStructParameterVisitor& visitor, const NYPath::TYPath& path)
{
    [&]<size_t ...I> (std::index_sequence<I...>) {
        (TraverseYsonStruct<std::tuple_element_t<I, T>>(visitor, path + "/" + ToString(I)), ...);
    } (std::make_index_sequence<std::tuple_size_v<T>>());
}

template <CNullable T>
void TraverseYsonStruct(const TYsonStructParameterVisitor& visitor, const NYPath::TYPath& path)
{
    TraverseYsonStruct<std::remove_cvref_t<decltype(*T())>>(visitor, path);
}

template <class T>
void TraverseYsonStruct(const TYsonStructParameterVisitor& /*visitor*/, const NYPath::TYPath& /*path*/)
{
    return;
}

template <class T>
void TraverseYsonStruct(const TYsonStructParameterVisitor& visitor)
{
    TraverseYsonStruct<T>(visitor, {});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
