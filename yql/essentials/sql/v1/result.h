#pragma once

#include <yql/essentials/utils/yql_panic.h>

#include <expected>
#include <variant>

namespace NSQLTranslationV1 {

class TContext;

template <class TPtr>
class TNullable final: public TPtr {
public:
    // Every pointer is nullable
    // NOLINTNEXTLINE(google-explicit-constructor)
    TNullable(TPtr ptr)
        : TPtr(std::move(ptr))
    {
    }
};

template <class TPtr>
class TNonNull final: public TPtr {
public:
    explicit TNonNull(TPtr ptr)
        : TPtr(std::move(ptr))
    {
        YQL_ENSURE(*static_cast<TPtr*>(this));
    }

    operator bool() = delete;
};

enum class ESQLError {
    Basic,
    UnsupportedYqlSelect,
};

std::unexpected<ESQLError> UnsupportedYqlSelect(TContext& ctx, TStringBuf message);

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
