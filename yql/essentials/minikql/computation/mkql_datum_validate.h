#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

#include <util/generic/fwd.h>

#include <arrow/datum.h>

namespace NKikimr::NMiniKQL {

void ValidateDatum(arrow::Datum datum, TMaybe<arrow::ValueDescr> expectedDescription, const TType* type, NYql::EDatumValidationMode validateMode);

} // namespace NKikimr::NMiniKQL

#if !defined(NDEBUG)
    #define VALIDATE_DATUM_ARROW_BLOCK_CONSTRUCTOR(datum) ValidateDatum((datum), Nothing(), nullptr, NYql::EDatumValidationMode::Cheap);
#else //! defined(NDEBUG)
    #define VALIDATE_DATUM_ARROW_BLOCK_CONSTRUCTOR(datum)
#endif // !defined(NDEBUG)
