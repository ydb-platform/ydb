#pragma once

#include "udf_string_ref.h"
#include "udf_type_size_check.h"
#include "udf_version.h"

namespace NYql {
namespace NUdf {

//////////////////////////////////////////////////////////////////////////////
// TPgTypeDescription
//////////////////////////////////////////////////////////////////////////////

struct TPgTypeDescription1 {
    TStringRef Name;
    ui32 TypeId = 0;
    i32 Typelen = 0;
    ui32 ArrayTypeId = 0;
    ui32 ElementTypeId = 0;
    bool PassByValue = false;
};

using TPgTypeDescription = TPgTypeDescription1;

UDF_ASSERT_TYPE_SIZE(TPgTypeDescription1, 40);

} // namspace NUdf
} // namspace NYql
