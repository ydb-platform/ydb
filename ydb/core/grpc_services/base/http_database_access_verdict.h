#pragma once

namespace NKikimr::NGRpcService {

enum class EHttpDatabaseAccessVerdict {
    Ok /* "ok" */,
    EmptyDatabase /* "empty_database" */,
    NotADatabase /* "not_a_database" */,
    NoConnectRight /* "no_connect_right" */,
    NoSecurityObject /* "no_security_object" */,
};

} // namespace NKikimr::NGRpcService
