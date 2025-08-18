#include "map_builder.h"
#include <yql/essentials/core/yql_type_helpers.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

using namespace NYT;
using namespace NNodes;
using namespace NThreading;

TMapJobBuilder::TMapJobBuilder(const TString& jobPrefix)
    : Prefix_(jobPrefix)
{
}

void TMapJobBuilder::SetBlockInput(TYqlUserJobBase* mapJob, NNodes::TYtMap map) {
    const bool blockInput = NYql::HasSetting(map.Settings().Ref(), EYtSettingType::BlockInputApplied);
    mapJob->SetUseBlockInput(blockInput);
}

void TMapJobBuilder::SetBlockOutput(TYqlUserJobBase* mapJob, NNodes::TYtMap map) {
    const bool blockOutput = NYql::HasSetting(map.Settings().Ref(), EYtSettingType::BlockOutputApplied);
    mapJob->SetUseBlockOutput(blockOutput);
}

void TMapJobBuilder::SetInputType(TYqlUserJobBase* mapJob, NNodes::TYtMap map) {
    TString inputType = NCommon::WriteTypeToYson(GetSequenceItemType(map.Input().Size() == 1U ? TExprBase(map.Input().Item(0)) : TExprBase(map.Mapper().Args().Arg(0)), true));
    mapJob->SetInputType(inputType);
}

} // namespace NYql
