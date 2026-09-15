#include "wrap_rpc_error.h"

#include <library/cpp/yson/node/node.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

TYtError ToYtError(const TError& err)
{
    TNode::TMapType attributes;
    for (const auto& [key, value] : err.Attributes().ListPairs()) {
        attributes.emplace(key, value);
    }

    const auto& innerErrors = err.InnerErrors();

    TVector<TYtError> errors;
    errors.reserve(innerErrors.size());
    for (const auto& inner : innerErrors) {
        errors.push_back(ToYtError(inner));
    }
    return {err.GetCode(), TString(err.GetMessage()), std::move(errors), std::move(attributes)};
}

TErrorResponse ToErrorResponse(TErrorException ex)
{
    auto error = std::move(ex.Error());
    const auto ytError = ToYtError(error);
    const auto requestId = error.Attributes().Find<TString>("request_id").value_or("0-0-0-0");
    throw TErrorResponse(std::move(ytError), requestId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
