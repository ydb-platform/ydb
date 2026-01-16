#pragma once

#include <string>

#include <util/system/types.h>

namespace NLogin::NSasl {

bool GenerateScramSecrets(const std::string& hashType,
    const std::string& password, const std::string& salt, ui32 iterationsCount,
    std::string& storedKey, std::string& serverKey, std::string& errorText);

bool ComputeServerKey(const std::string& hashType,
    const std::string& password, const std::string& salt, ui32 iterationsCount,
    std::string& serverKey, std::string& errorText);

std::string PrepareSaslPlainAuthMsg(const std::string& authenticationId, const std::string& password,
    const std::string& authorizationId = "");

} // namespace NLogin::NSasl
