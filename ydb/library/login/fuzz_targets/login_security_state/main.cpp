#include <ydb/library/login/fuzz_targets/login_fuzz_common.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

#include <ydb/library/login/login.h>

namespace {

NLogin::TLoginProvider::TLoginUserResponse MintToken(NLogin::TLoginProvider& provider) {
    NLogin::TLoginProvider::TLoginUserRequest request;
    request.User = "stateuser";
    request.Options.WithUserGroups = true;
    request.HashToValidate.emplace();
    request.HashToValidate->AuthMech = NLoginProto::ESaslAuthMech::Plain;
    request.HashToValidate->HashType = NLoginProto::EHashType::Argon;
    request.HashToValidate->Hash = NAuthSecurityFuzz::ValidArgonHashValue();
    return provider.LoginUser(request);
}

void AddStructuredSid(FuzzedDataProvider& fdp, NLoginProto::TSecurityState& state) {
    auto* sid = state.AddSids();
    sid->SetType(fdp.ConsumeBool() ? NLoginProto::ESidType::USER : NLoginProto::ESidType::GROUP);
    sid->SetName(NAuthSecurityFuzz::ConsumeName(fdp, 16, sid->GetType() == NLoginProto::ESidType::USER));
    sid->SetIsEnabled(fdp.ConsumeBool());
    if (fdp.ConsumeBool()) {
        sid->SetPasswordHashes(NAuthSecurityFuzz::ValidPasswordHashes());
    } else {
        sid->SetPasswordHashes(fdp.ConsumeRandomLengthString(256));
    }
    for (size_t i = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, 4); i < count; ++i) {
        sid->AddMembers(NAuthSecurityFuzz::ConsumeName(fdp, 16, false));
    }
    sid->SetCreatedAt(fdp.ConsumeIntegral<i64>());
    sid->SetFailedLoginAttemptCount(fdp.ConsumeIntegral<ui32>());
    sid->SetLastFailedLogin(fdp.ConsumeIntegral<i64>());
    sid->SetLastSuccessfulLogin(fdp.ConsumeIntegral<i64>());
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    NLogin::TLoginProvider signer;
    signer.Audience = "state-audience";
    signer.RotateKeys();
    NAuthSecurityFuzz::EnsureHashedUser(signer, "stateuser");
    NLogin::TLoginProvider::TLoginUserResponse loginResponse;
    try {
        loginResponse = MintToken(signer);
    } catch (...) {
    }

    try {
        const auto cleanState = signer.GetSecurityState();
        NLogin::TLoginProvider restored;
        restored.UpdateSecurityState(cleanState);
        if (!loginResponse.Token.empty()) {
            NLogin::TLoginProvider::TValidateTokenRequest request;
            request.Token = loginResponse.Token;
            (void)restored.ValidateToken(request);
        }
        (void)restored.GetSecurityState();
    } catch (...) {
    }

    NLoginProto::TSecurityState parsedState;
    if (parsedState.ParseFromArray(data, static_cast<int>(size))) {
        try {
            NLogin::TLoginProvider provider;
            provider.UpdateSecurityState(parsedState);
            (void)provider.GetSecurityState();
            NLogin::TLoginProvider::TValidateTokenRequest request;
            if (fdp.ConsumeBool()) {
                request.Token = loginResponse.Token;
            } else {
                request.Token = fdp.ConsumeRandomLengthString(1024);
            }
            (void)provider.ValidateToken(request);
        } catch (...) {
        }
    }

    NLoginProto::TSecurityState structured;
    structured.SetAudience(fdp.ConsumeBool() ? TString("state-audience") : NAuthSecurityFuzz::ConsumeToken(fdp, 16));
    if (!signer.Keys.empty() && fdp.ConsumeBool()) {
        const auto& key = signer.Keys.back();
        auto* publicKey = structured.AddPublicKeys();
        publicKey->SetKeyId(key.KeyId);
        publicKey->SetKeyDataPEM(key.PublicKey);
        publicKey->SetExpiresAt(std::chrono::duration_cast<std::chrono::milliseconds>(key.ExpiresAt.time_since_epoch()).count());
    }
    for (size_t i = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, 8); i < count; ++i) {
        AddStructuredSid(fdp, structured);
    }

    try {
        NLogin::TLoginProvider provider;
        provider.UpdateSecurityState(structured);
        const auto exported = provider.GetSecurityState();
        NLogin::TLoginProvider restored;
        restored.UpdateSecurityState(exported);
        if (!loginResponse.Token.empty()) {
            NLogin::TLoginProvider::TValidateTokenRequest request;
            request.Token = loginResponse.Token;
            (void)restored.ValidateToken(request);
        }
    } catch (...) {
    }

    return 0;
}
