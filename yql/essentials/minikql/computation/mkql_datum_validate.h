#pragma once

#include <yql/essentials/public/udf/udf_validate.h>

#include <util/generic/fwd.h>

#include <arrow/datum.h>

namespace NKikimr::NMiniKQL {

void ValidateDatum(arrow::Datum datum, TMaybe<arrow::ValueDescr> expectedDescription, NYql::NUdf::EValidateDatumMode validateMode);

} // namespace NKikimr::NMiniKQL

#if !defined(NDEBUG)
    #define VALIDATE_DATUM_ARROW_BLOCK_CONSTRUCTOR(datum) ValidateDatum((datum), Nothing(), NYql::NUdf::EValidateDatumMode::Cheap);
#else //! defined(NDEBUG)
    #define VALIDATE_DATUM_ARROW_BLOCK_CONSTRUCTOR(datum)
#endif // !defined(NDEBUG)
