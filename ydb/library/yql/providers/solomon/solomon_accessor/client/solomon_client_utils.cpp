#include "solomon_client_utils.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NSo {

template <typename T>
TSolomonClientResponse<T>::TSolomonClientResponse()
    : Status(STATUS_RETRIABLE_ERROR) {}

template <typename T>
TSolomonClientResponse<T>::TSolomonClientResponse(const TString& error)
    : Status(STATUS_FATAL_ERROR)
    , Error(error) {}

template <typename T>
TSolomonClientResponse<T>::TSolomonClientResponse(T&& result)
    : Status(STATUS_OK)
    , Result(std::move(result)) {}

template class TSolomonClientResponse<TGetLabelsResult>;
template class TSolomonClientResponse<TListMetricsResult>;
template class TSolomonClientResponse<TGetPointsCountResult>;
template class TSolomonClientResponse<TGetDataResult>;

} // namespace NYql::NSo
