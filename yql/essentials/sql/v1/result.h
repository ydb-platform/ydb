#pragma once

#include <yql/essentials/utils/yql_panic.h>

#include <expected>
#include <variant>

namespace NSQLTranslationV1 {

template <class TPtr>
class TNonNull final: public TPtr {
public:
    explicit TNonNull(TPtr ptr)
        : TPtr(std::move(ptr))
    {
        YQL_ENSURE(*this);
    }
};

enum class ESQLError {
    Basic,
    UnsupportedYqlSelect,
};

template <class T>
using TSQLResult = std::expected<T, ESQLError>;

using TSQLStatus = TSQLResult<std::monostate>;

template <class T>
bool IsUnwrappable(const TSQLResult<T>& result) {
    return result || result.error() == ESQLError::Basic;
}

template <class T>
void EnsureUnwrappable(const TSQLResult<T>& result) {
    YQL_ENSURE(
        IsUnwrappable(result),
        "Expected "
            << "at most " << ESQLError::Basic << " error, "
            << "but got " << result.error());
}

bool Unwrap(TSQLStatus status);

} // namespace NSQLTranslationV1
