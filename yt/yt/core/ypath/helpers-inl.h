#ifndef HELPERS_INL_H
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "token.h"

#include <library/cpp/yt/string/string_builder.h>
#include <library/cpp/yt/string/format.h>

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <typename ...TArgs>
void YPathJoinImpl(TStringBuilder* builder, TArgs&&... literals);

template <>
inline void YPathJoinImpl(TStringBuilder* /*builder*/)
{ }

template <typename TFirstArg, typename ...TArgs>
void YPathJoinImpl(TStringBuilder* builder, TFirstArg&& firstLiteral, TArgs&&... literals)
{
    builder->AppendChar('/');
    AppendYPathLiteral(builder, std::forward<TFirstArg>(firstLiteral));
    YPathJoinImpl(builder, std::forward<TArgs>(literals)...);
}

} // namespace NDetail

template <typename ...TArgs>
TYPath YPathJoin(const TYPath& path, TArgs&&... literals)
{
    TStringBuilder builder;

    auto estimateLength = [] (const auto& literal) {
        if constexpr (requires { literal.length(); }) {
            return literal.length();
        } else {
            return 1;
        }
    };
    builder.Reserve(path.length() + (sizeof...(literals) + ... + estimateLength(literals)));

    builder.AppendString(path);
    NDetail::YPathJoinImpl(&builder, literals...);
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
