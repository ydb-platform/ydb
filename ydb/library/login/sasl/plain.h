#pragma once

#include <string>

namespace NLogin::NSasl {

std::string BuildSaslPlainAuthMsg(const std::string& authenticationId, const std::string& password,
    const std::string& authorizationId = "");

}
