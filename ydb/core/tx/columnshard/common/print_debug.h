#pragma once
#include <util/stream/output.h>
#include <util/string/printf.h>
// #include <util/string
#include <util/generic/string.h>
namespace NKikimr::NOlap {

template<typename... Ts>
TString MySprintf(const char* format, Ts... ts) {
    return ::Sprintf(format, [](auto& value){
        // support printing TString, TStringBuf, std::string, std::string_view
        if constexpr ( requires (decltype(value) value) { value.c_str(); } ) {
            return value.c_str();
        } else {
            return value;
        }
    }(ts)...);
}

template<typename... Ts>
void Errs(const char* format, Ts... ts) {
    Cerr << MySprintf(format, std::forward<Ts>(ts)...);
}

} // 
