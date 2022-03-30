#pragma once

#include <util/digest/city.h>
#include <util/string/join.h>

namespace NKikimr::NSQS {

template <class... TArgs>
ui64 GetKeysHash(TArgs&&... args) {
    return CityHash64(Join("#", args...));
}    
    
} // namespace NKikimr::NSQS
