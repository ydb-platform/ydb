#pragma once

#include <util/generic/serialized_enum.h>
#include <util/generic/vector.h>

namespace NUnifiedAgent {
    namespace NPrivate {
        using TEnumNames = TVector<const TString*>;

        template <typename TEnum>
        TEnumNames* BuildEnumNames() {
            const auto names = GetEnumNames<TEnum>();
            TEnumNames* result = new TEnumNames(names.size());
            size_t index = 0;
            for (const auto& p: names) {
                Y_ABORT_UNLESS(static_cast<size_t>(p.first) == index);
                (*result)[index++] = &p.second;
            }
            return result;
        }

        template <typename TEnum>
        const TEnumNames& EnumNamesVec() {
            static const TEnumNames* EnumNames = BuildEnumNames<TEnum>();
            return *EnumNames;
        }
    }

    template <typename TEnum, typename = std::enable_if_t<std::is_enum_v<TEnum>>>
    inline const TString& NameOf(TEnum val) noexcept {
        return *NPrivate::EnumNamesVec<TEnum>()[static_cast<size_t>(val)];
    }
}
