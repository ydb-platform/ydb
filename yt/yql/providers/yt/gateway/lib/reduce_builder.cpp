#include "reduce_builder.h"

#include <yql/essentials/core/yql_type_helpers.h>

namespace NYql {

void TReduceJobBuilder::SetInputType(TYqlUserJobBase* reduceJob, NNodes::TYtReduce reduce) {
    const auto inputTypeSet = NYql::GetSetting(reduce.Settings().Ref(), EYtSettingType::ReduceInputType);
    TString inputType = NCommon::WriteTypeToYson(inputTypeSet
        ? inputTypeSet->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()
        : GetSequenceItemType(reduce.Reducer().Args().Arg(0), true)
    );
    reduceJob->SetInputType(inputType);
}

} // namespace NYql
