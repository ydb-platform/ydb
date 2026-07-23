#pragma once

#include <util/generic/strbuf.h>

namespace NYT::NYaml {

////////////////////////////////////////////////////////////////////////////////

//! Parses a YAML string and deserializes the result into type T
//! using the standard YsonStruct deserialization machinery.
template <class T>
T ConvertFromYaml(TStringBuf yaml);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYaml

#define YAML_CONVERT_INL_H_
#include "convert-inl.h"
#undef YAML_CONVERT_INL_H_
