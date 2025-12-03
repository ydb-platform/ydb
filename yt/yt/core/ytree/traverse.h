#pragma once

#include "public.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TraverseYsonStruct(const TYsonStructParameterVisitor& visitor, const NYPath::TYPath& path);

template <class T>
void TraverseYsonStruct(const TYsonStructParameterVisitor& visitor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define TRAVERSE_INL_H_
#include "traverse-inl.h"
#undef TRAVERSE_INL_H_
