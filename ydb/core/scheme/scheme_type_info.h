#pragma once

#include <ydb/core/scheme_types/scheme_type_info.h>

namespace NKikimr::NScheme {

::TString TypeName(const TTypeInfo typeInfo, const ::TString& typeMod = {});

} // NKikimr::NScheme
