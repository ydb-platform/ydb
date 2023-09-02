#include "secret_vault_service.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilder* builder,
    const ISecretVaultService::TSecretSubrequest& subrequest,
    TStringBuf /*spec*/)
{
    builder->AppendFormat("%v:%v:%v:%v",
        subrequest.SecretId,
        subrequest.SecretVersion,
        subrequest.DelegationToken,
        subrequest.Signature);
}

TString ToString(const ISecretVaultService::TSecretSubrequest& subrequest)
{
    return ToStringViaBuilder(subrequest);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

