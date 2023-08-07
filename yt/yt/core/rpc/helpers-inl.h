#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif

#include "authentication_identity.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void WriteAuthenticationIdentityToProto(T* proto, const TAuthenticationIdentity& identity)
{
    if (identity.User == RootUserName) {
        proto->clear_user();
    } else {
        proto->set_user(identity.User);
    }
    if (identity.UserTag == identity.User) {
        proto->clear_user_tag();
    } else {
        proto->set_user_tag(identity.UserTag);
    }
}

template <class T>
TAuthenticationIdentity ParseAuthenticationIdentityFromProto(const T& proto)
{
    TAuthenticationIdentity identity;
    identity.User = proto.has_user() ? proto.user() : RootUserName;
    identity.UserTag = proto.has_user_tag() ? proto.user_tag () : identity.User;
    return identity;
}

////////////////////////////////////////////////////////////////////////////////

template <class E>
int FeatureIdToInt(E featureId)
{
    static_assert(
        std::is_same_v<int, std::underlying_type_t<E>>,
        "Feature set enum must have `int` as its underlying type.");
    return ToUnderlying(featureId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
