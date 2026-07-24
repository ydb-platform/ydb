#include <ydb/library/login/fuzz_targets/login_fuzz_common.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

#include <ydb/library/login/login.h>

#include <algorithm>
#include <vector>

namespace {

void AddUnique(std::vector<TString>& values, const TString& value) {
    if (!value.empty() && std::find(values.begin(), values.end(), value) == values.end()) {
        values.push_back(value);
    }
}

void EraseValue(std::vector<TString>& values, const TString& value) {
    values.erase(std::remove(values.begin(), values.end(), value), values.end());
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    NLogin::TLoginProvider provider;
    provider.Audience = NAuthSecurityFuzz::ConsumeToken(fdp, 12);
    provider.RotateKeys();

    std::vector<TString> users;
    std::vector<TString> groups;
    std::vector<TString> tokens;

    NAuthSecurityFuzz::EnsureHashedUser(provider, "seeduser");
    AddUnique(users, "seeduser");

    NLogin::TLoginProvider::TCreateGroupRequest seedGroup;
    seedGroup.Group = "seedgroup";
    provider.CreateGroup(seedGroup);
    AddUnique(groups, "seedgroup");

    const size_t operations = fdp.ConsumeIntegralInRange<size_t>(1, 64);
    for (size_t i = 0; i < operations; ++i) {
        switch (fdp.ConsumeIntegralInRange<int>(0, 11)) {
        case 0: {
            NLogin::TLoginProvider::TCreateUserRequest request;
            request.User = NAuthSecurityFuzz::PickKnownOrNew(fdp, users, true);
            if (fdp.ConsumeBool()) {
                NAuthSecurityFuzz::PopulateValidHashedUser(request, fdp.ConsumeBool());
            } else {
                request.Password = fdp.ConsumeRandomLengthString(256);
                if (fdp.ConsumeBool()) {
                    request.HashedPassword = fdp.ConsumeRandomLengthString(256);
                }
                request.CanLogin = fdp.ConsumeBool();
            }

            try {
                const auto response = provider.CreateUser(request);
                if (response.Error.empty()) {
                    AddUnique(users, request.User);
                }
            } catch (...) {
            }
            break;
        }
        case 1: {
            NLogin::TLoginProvider::TModifyUserRequest request;
            request.User = NAuthSecurityFuzz::PickKnownOrNew(fdp, users, true);
            if (fdp.ConsumeBool()) {
                request.HashedPassword = NAuthSecurityFuzz::ValidPasswordHashes();
            } else if (fdp.ConsumeBool()) {
                request.Password = fdp.ConsumeRandomLengthString(256);
            }
            if (fdp.ConsumeBool()) {
                if (fdp.ConsumeBool()) {
                    request.HashedPassword = NAuthSecurityFuzz::ValidPasswordHashes();
                } else {
                    request.HashedPassword = fdp.ConsumeRandomLengthString(256);
                }
            }
            if (fdp.ConsumeBool()) {
                request.CanLogin = fdp.ConsumeBool();
            }

            try {
                (void)provider.ModifyUser(request);
            } catch (...) {
            }
            break;
        }
        case 2: {
            const TString user = NAuthSecurityFuzz::PickKnownOrNew(fdp, users, true);
            try {
                const auto response = provider.RemoveUser(user);
                if (response.Error.empty()) {
                    EraseValue(users, user);
                }
            } catch (...) {
            }
            break;
        }
        case 3: {
            NLogin::TLoginProvider::TCreateGroupRequest request;
            request.Options.StrongCheckName = fdp.ConsumeBool();
            request.Group = NAuthSecurityFuzz::PickKnownOrNew(fdp, groups, request.Options.StrongCheckName);

            try {
                const auto response = provider.CreateGroup(request);
                if (response.Error.empty()) {
                    AddUnique(groups, request.Group);
                }
            } catch (...) {
            }
            break;
        }
        case 4: {
            NLogin::TLoginProvider::TAddGroupMembershipRequest request;
            request.Group = NAuthSecurityFuzz::PickKnownOrNew(fdp, groups, fdp.ConsumeBool());
            request.Member = fdp.ConsumeBool()
                ? NAuthSecurityFuzz::PickKnownOrNew(fdp, users, true)
                : NAuthSecurityFuzz::PickKnownOrNew(fdp, groups, false);
            try {
                (void)provider.AddGroupMembership(request);
            } catch (...) {
            }
            break;
        }
        case 5: {
            NLogin::TLoginProvider::TRemoveGroupMembershipRequest request;
            request.Group = NAuthSecurityFuzz::PickKnownOrNew(fdp, groups, false);
            request.Member = fdp.ConsumeBool()
                ? NAuthSecurityFuzz::PickKnownOrNew(fdp, users, true)
                : NAuthSecurityFuzz::PickKnownOrNew(fdp, groups, false);
            try {
                (void)provider.RemoveGroupMembership(request);
            } catch (...) {
            }
            break;
        }
        case 6: {
            NLogin::TLoginProvider::TRenameGroupRequest request;
            request.Options.StrongCheckName = fdp.ConsumeBool();
            request.Group = NAuthSecurityFuzz::PickKnownOrNew(fdp, groups, request.Options.StrongCheckName);
            request.NewName = NAuthSecurityFuzz::ConsumeName(fdp, 16, request.Options.StrongCheckName);
            try {
                const auto response = provider.RenameGroup(request);
                if (response.Error.empty()) {
                    EraseValue(groups, request.Group);
                    AddUnique(groups, request.NewName);
                }
            } catch (...) {
            }
            break;
        }
        case 7: {
            const TString group = NAuthSecurityFuzz::PickKnownOrNew(fdp, groups, false);
            try {
                const auto response = provider.RemoveGroup(group);
                if (response.Error.empty()) {
                    EraseValue(groups, group);
                }
            } catch (...) {
            }
            break;
        }
        case 8: {
            NLogin::TLoginProvider::TLoginUserRequest request;
            request.User = NAuthSecurityFuzz::PickKnownOrNew(fdp, users, true);
            request.Options.WithUserGroups = fdp.ConsumeBool();

            const int mode = fdp.ConsumeIntegralInRange<int>(0, 3);
            if (mode == 0) {
                request.HashToValidate.emplace();
                request.HashToValidate->AuthMech = NLoginProto::ESaslAuthMech::Plain;
                request.HashToValidate->HashType = NLoginProto::EHashType::Argon;
                if (fdp.ConsumeBool()) {
                    request.HashToValidate->Hash = NAuthSecurityFuzz::ValidArgonHashValue();
                } else {
                    request.HashToValidate->Hash = fdp.ConsumeRandomLengthString(96);
                }
            } else if (mode == 1) {
                request.HashToValidate.emplace();
                request.HashToValidate->AuthMech = NLoginProto::ESaslAuthMech::Plain;
                request.HashToValidate->HashType = NLoginProto::EHashType::ScramSha256;
                if (fdp.ConsumeBool()) {
                    request.HashToValidate->Hash = NAuthSecurityFuzz::ValidScramServerKey();
                } else {
                    request.HashToValidate->Hash = fdp.ConsumeRandomLengthString(96);
                }
            } else if (mode == 2) {
                request.HashToValidate.emplace();
                request.HashToValidate->AuthMech = NLoginProto::ESaslAuthMech::Scram;
                request.HashToValidate->HashType = NLoginProto::EHashType::ScramSha256;
                if (fdp.ConsumeBool()) {
                    request.HashToValidate->Hash = NAuthSecurityFuzz::ValidScramClientProof();
                } else {
                    request.HashToValidate->Hash = fdp.ConsumeRandomLengthString(96);
                }
                if (fdp.ConsumeBool()) {
                    request.HashToValidate->AuthMessage = NAuthSecurityFuzz::ValidScramAuthMessage();
                } else {
                    request.HashToValidate->AuthMessage = fdp.ConsumeRandomLengthString(256);
                }
            } else {
                request.ExternalAuth = NAuthSecurityFuzz::ConsumeToken(fdp, 12);
            }

            try {
                const auto response = provider.LoginUser(request);
                if (!response.Token.empty()) {
                    AddUnique(tokens, response.Token);
                }
            } catch (...) {
            }
            break;
        }
        case 9: {
            NLogin::TLoginProvider::TCheckLockOutRequest request;
            request.User = NAuthSecurityFuzz::PickKnownOrNew(fdp, users, true);
            try {
                (void)provider.CheckLockOutUser(request);
            } catch (...) {
            }
            break;
        }
        case 10: {
            try {
                (void)provider.GetGroupsMembership(
                    fdp.ConsumeBool()
                        ? NAuthSecurityFuzz::PickKnownOrNew(fdp, users, true)
                        : NAuthSecurityFuzz::PickKnownOrNew(fdp, groups, false));
            } catch (...) {
            }
            break;
        }
        case 11: {
            NLogin::TLoginProvider::TValidateTokenRequest request;
            if (tokens.empty() || !fdp.ConsumeBool()) {
                request.Token = fdp.ConsumeRandomLengthString(1024);
            } else {
                request.Token = tokens[fdp.ConsumeIntegralInRange<size_t>(0, tokens.size() - 1)];
            }
            try {
                (void)provider.ValidateToken(request);
            } catch (...) {
            }
            break;
        }
        }
    }

    try {
        const auto state = provider.GetSecurityState();
        NLogin::TLoginProvider restored;
        restored.UpdateSecurityState(state);
        for (const auto& token : tokens) {
            NLogin::TLoginProvider::TValidateTokenRequest request;
            request.Token = token;
            (void)restored.ValidateToken(request);
        }
        (void)restored.GetSecurityState();
    } catch (...) {
    }

    return 0;
}
