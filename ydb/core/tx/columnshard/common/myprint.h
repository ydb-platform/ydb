#pragma once
#include <util/string/printf.h>
// #include <util/string
#include <util/generic/string.h>
namespace NKikimr::NOlap {

template<typename... Ts>
TString MySprintf(const char* format, Ts... ts) {
    return ::Sprintf(format, [](auto& value){
        if constexpr ( requires (decltype(value) value) { value.c_str(); } ) {
            return value.c_str();
        } else {
            return value;
        }
    }(ts)...);
}

} // 
