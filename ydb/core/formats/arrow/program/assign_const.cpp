#include "assign_const.h"
#include "collection.h"
#include "execution.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>

namespace NKikimr::NArrow::NSSA {

TConclusion<IResourceProcessor::EExecutionResult> TConstProcessor::DoExecute(
    const TProcessorContext& context, const TExecutionNodeContext& /*nodeContext*/) const {
    AFL_VERIFY(GetInput().empty());
    context.GetResources()->AddConstantVerified(GetOutputColumnIdOnce(), ScalarConstant);
    return IResourceProcessor::EExecutionResult::Success;
}

NJson::TJsonValue TConstProcessor::DoDebugJson() const {
    NJson::TJsonValue result = NJson::JSON_MAP;
    AFL_VERIFY(ScalarConstant);
    result.InsertValue("v", ScalarConstant->ToString());
    return result;
}

}   // namespace NKikimr::NArrow::NSSA
