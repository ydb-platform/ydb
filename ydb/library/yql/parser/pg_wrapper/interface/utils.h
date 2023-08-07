#pragma once

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <util/generic/maybe.h>

namespace NYql {

TMaybe<ui32> ConvertToPgType(NKikimr::NUdf::EDataSlot slot);
TMaybe<NKikimr::NUdf::EDataSlot> ConvertFromPgType(ui32 typeId);

bool ParsePgIntervalModifier(const TString& str, i32& ret);

std::unique_ptr<NUdf::IPgBuilder> CreatePgBuilder();
bool HasPgKernel(ui32 procOid);

} // NYql
