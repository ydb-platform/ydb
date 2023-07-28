#pragma once
#ifndef ARCADIA_ENUM_INL_H_
#error "Direct inclusion of this file is not allowed, include arcadia_enum.h"
// For the sake of sane code completion.
#include "arcadia_enum.h"
#endif

#include <util/system/type_name.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TArcadiaEnumTraitsImpl
{
    static constexpr bool IsBitEnum = false;
    static constexpr bool IsStringSerializableEnum = false;

    static TStringBuf GetTypeName()
    {
        static const auto Result = TypeName<T>();
        return Result;
    }

    static std::optional<TStringBuf> FindLiteralByValue(T value)
    {
        auto names = GetEnumNames<T>();
        auto it = names.find(value);
        return it == names.end() ? std::nullopt : std::make_optional(TStringBuf(it->second));
    }

    static std::optional<T> FindValueByLiteral(TStringBuf literal)
    {
        static const auto LiteralToValue = [] {
            THashMap<TString, T> result;
            for (const auto& [value, name] : GetEnumNames<T>()) {
                result.emplace(name, value);
            }
            return result;
        }();
        auto it = LiteralToValue.find(literal);
        return it == LiteralToValue.end() ? std::nullopt : std::make_optional(it->second);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
