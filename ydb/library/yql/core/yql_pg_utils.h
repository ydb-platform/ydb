#pragma once
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <util/generic/maybe.h>

namespace NYql {

TMaybe<ui32> ConvertToPgType(NKikimr::NUdf::EDataSlot slot);
TMaybe<NKikimr::NUdf::EDataSlot> ConvertFromPgType(ui32 typeId);

}
