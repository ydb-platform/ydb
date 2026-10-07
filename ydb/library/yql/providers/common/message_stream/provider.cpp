#include "provider.h"
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

namespace NFq::NMessageStream {
using namespace NYql;
const TStructExprType* MakeRawRowType(TExprContext& ctx) {
    return ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
        ctx.MakeType<TItemExprType>("Data", ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String))});
}
TString ComposeAuthToken(const THashMap<TString, TString>& properties,
    const TString& fallbackToken, const TString& serviceAccountId, const TString& serviceAccountSignature) {
    const auto method = properties.Value("authMethod", "");
    if (method == "TOKEN") {
        return ComposeStructuredTokenJsonForTokenAuthWithSecret(properties.Value("tokenReference", ""), properties.Value("token", ""));
    }
    if (method == "BASIC") {
        return ComposeStructuredTokenJsonForBasicAuthWithSecret(properties.Value("login", ""), properties.Value("passwordReference", ""), properties.Value("password", ""));
    }
    if (method == "IAM") {
        return ComposeStructuredTokenJsonForIamAuth(properties.Value("iamServiceAccountId", ""), properties.Value("iamResourceId", ""));
    }
    if (const auto it = properties.find("transient_token"); it != properties.end()) {
        return ComposeStructuredTokenJsonForTransientTokenAuth(it->second);
    }
    return ComposeStructuredTokenJsonForServiceAccount(serviceAccountId, serviceAccountSignature, fallbackToken);
}
}
