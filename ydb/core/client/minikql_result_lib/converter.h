#pragma once

#include "objects.h"

#include <ydb/public/lib/deprecated/kicli/kicli.h>


namespace NKikimrMiniKQL {
class TValue;
class TType;
} // namespace NKikimrMiniKQL


namespace NKikimr {
namespace NResultLib {

TStruct ConvertResult(const NKikimrMiniKQL::TValue& value, const NKikimrMiniKQL::TType& type);

// convert C++ API result
inline TStruct ConvertResult(const NKikimr::NClient::TQueryResult& apiResult) {
    const NKikimrClient::TResponse& response = apiResult.GetResult<NKikimrClient::TResponse>();
    Y_ABORT_UNLESS(response.HasExecutionEngineEvaluatedResponse());
    const auto& result = response.GetExecutionEngineEvaluatedResponse();
    // TODO: type caching
    return ConvertResult(result.GetValue(), result.GetType());
}

} // namespace NResultLib
} // namespace NKikimr
