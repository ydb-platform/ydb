#pragma once
#include <util/stream/output.h>
#include <util/string/printf.h>
#include <util/generic/string.h>
namespace NKikimr::NOlap {
namespace NDetail{
template<typename... Ts>
TString BetterSprintf(const char* format, Ts... ts) {
    return ::Sprintf(format, [](auto& value){
        // support printing TString, TStringBuf, std::string, std::string_view
        if constexpr ( requires (decltype(value) value) { value.c_str(); } ) {
            return value.c_str();
        } else {
            return value;
        }
    }(ts)...);
}
} // NDetail

template<typename... Ts>
void Errs(const char* format, Ts... ts) {
    Cerr << NDetail::BetterSprintf(format, std::forward<Ts>(ts)...);
}

} // NKikimr::NOlap
