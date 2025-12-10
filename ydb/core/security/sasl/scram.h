#pragma once

#include <string>

#include <util/system/types.h>

namespace NKikimr::NSasl {

bool GenerateScramSecrets(const std::string& hashType,
    const std::string& password, const std::string& salt, ui32 iterationsCount,
    std::string& storedKey, std::string& serverKey, std::string& errorText);

} // namespace NKikimr::NSasl
