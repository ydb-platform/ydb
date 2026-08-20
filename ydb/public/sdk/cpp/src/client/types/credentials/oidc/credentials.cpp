#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>

namespace NYdb::inline Dev {

    bool TOidcToken::IsUsable(TInstant now, TDuration skew) const {
        return !Token.empty() && (!ExpiresAt.has_value() || ExpiresAt.value() > now + skew);
    }

    bool TOidcTokenSet::IsUsable(TInstant now, TDuration skew) const {
        return AccessToken.IsUsable(now, skew) || (RefreshToken.has_value() && RefreshToken->IsUsable(now, skew));
    }

} // namespace NYdb::inline Dev
