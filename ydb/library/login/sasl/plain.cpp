#include "plain.h"

namespace NLogin::NSasl {

std::string BuildSaslPlainAuthMsg(const std::string& authenticationId, const std::string& password,
    const std::string& authorizationId)
{
    std::string res;
    res.reserve(authenticationId.size() + password.size() + authorizationId.size() + 2);
    res += authorizationId;
    res.push_back('\0');
    res += authenticationId;
    res.push_back('\0');
    res += password;
    return res;
}

}
