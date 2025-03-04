#include "assign_const.h"
#include "collection.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>

namespace NKikimr::NArrow::NSSA {

TConclusionStatus TConstProcessor::DoExecute(const std::shared_ptr<TAccessorsCollection>& resources, const TProcessorContext& /*context*/) const {
    AFL_VERIFY(GetInput().empty());
    resources->AddConstantVerified(GetOutputColumnIdOnce(), ScalarConstant);
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NArrow::NSSA
