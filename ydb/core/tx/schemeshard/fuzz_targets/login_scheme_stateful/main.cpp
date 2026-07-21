#include <ydb/library/login/login.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <array>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace {

constexpr std::array<TStringBuf, 4> Users = {
    "user0",
    "user1",
    "user2",
    "user3",
};

constexpr std::array<TStringBuf, 3> Groups = {
    "group0",
    "group1",
    "group2",
};

TString PickUser(FuzzedDataProvider& fdp) {
    return TString(Users[fdp.ConsumeIntegralInRange<size_t>(0, Users.size() - 1)]);
}

TString PickGroup(FuzzedDataProvider& fdp) {
    return TString(Groups[fdp.ConsumeIntegralInRange<size_t>(0, Groups.size() - 1)]);
}

TString Password(const TString& user) {
    return "Password-" + user + "-xA1!";
}

struct TModel {
    TSet<TString> Users;
    TSet<TString> Groups;
    TSet<std::pair<TString, TString>> Memberships;
    THashMap<TString, bool> CanLogin;
    TVector<std::pair<TString, TString>> Tokens;
};

void RemoveUserFromModel(TModel& model, const TString& user) {
    model.Users.erase(user);
    model.CanLogin.erase(user);
    for (auto it = model.Memberships.begin(); it != model.Memberships.end();) {
        if (it->first == user) {
            it = model.Memberships.erase(it);
        } else {
            ++it;
        }
    }
}

void RemoveGroupFromModel(TModel& model, const TString& group) {
    model.Groups.erase(group);
    for (auto it = model.Memberships.begin(); it != model.Memberships.end();) {
        if (it->second == group) {
            it = model.Memberships.erase(it);
        } else {
            ++it;
        }
    }
}

void CheckToken(NLogin::TLoginProvider& provider, const TString& token, const TString& expectedUser) {
    const auto result = provider.ValidateToken({.Token = token});
    Y_ABORT_UNLESS(!result.Error);
    Y_ABORT_UNLESS(result.User == expectedUser);
}

void CheckSavedTokens(NLogin::TLoginProvider& provider, const TModel& model, FuzzedDataProvider& fdp) {
    if (model.Tokens.empty()) {
        return;
    }

    const ui32 checks = fdp.ConsumeIntegralInRange<ui32>(0, Min<ui32>(8, model.Tokens.size()));
    for (ui32 i = 0; i < checks; ++i) {
        const auto& [token, user] = model.Tokens[fdp.ConsumeIntegralInRange<size_t>(0, model.Tokens.size() - 1)];
        const auto result = provider.ValidateToken({.Token = token});
        if (!result.Error) {
            Y_ABORT_UNLESS(result.User == user);
        }
    }
}

void RoundTripSecurityState(NLogin::TLoginProvider& provider, const TModel& model, FuzzedDataProvider& fdp) {
    NLogin::TLoginProvider mirror;
    mirror.UpdateSecurityState(provider.GetSecurityState());
    CheckSavedTokens(mirror, model, fdp);
    provider.UpdateSecurityState(mirror.GetSecurityState());
}

void FuzzLogin(FuzzedDataProvider& fdp) {
    NLogin::TLoginProvider provider;
    provider.RotateKeys();

    TModel model;

    const ui32 steps = fdp.ConsumeIntegralInRange<ui32>(1, 96);
    for (ui32 step = 0; step < steps && fdp.remaining_bytes(); ++step) {
        const TString user = PickUser(fdp);
        const TString group = PickGroup(fdp);

        switch (fdp.ConsumeIntegralInRange<ui8>(0, 10)) {
            case 0:
                if (!model.Users.contains(user)) {
                    const auto result = provider.CreateUser({.User = user, .Password = Password(user)});
                    if (!result.Error) {
                        model.Users.insert(user);
                        model.CanLogin[user] = true;
                    }
                }
                break;
            case 1:
                if (model.Users.contains(user)) {
                    const auto result = provider.RemoveUser(user);
                    if (!result.Error) {
                        RemoveUserFromModel(model, user);
                    }
                }
                break;
            case 2:
                if (model.Users.contains(user)) {
                    const bool canLogin = fdp.ConsumeBool();
                    const auto result = provider.ModifyUser({
                        .User = user,
                        .Password = fdp.ConsumeBool() ? std::make_optional(Password(user)) : std::nullopt,
                        .CanLogin = canLogin,
                    });
                    if (!result.Error) {
                        model.CanLogin[user] = canLogin;
                    }
                }
                break;
            case 3:
                if (!model.Groups.contains(group)) {
                    const auto result = provider.CreateGroup({.Group = group});
                    if (!result.Error) {
                        model.Groups.insert(group);
                    }
                }
                break;
            case 4:
                if (model.Groups.contains(group)) {
                    const auto result = provider.RemoveGroup(group);
                    if (!result.Error) {
                        RemoveGroupFromModel(model, group);
                    }
                }
                break;
            case 5:
                if (model.Users.contains(user) && model.Groups.contains(group) && !model.Memberships.contains({user, group})) {
                    const auto result = provider.AddGroupMembership({.Group = group, .Member = user});
                    if (!result.Error) {
                        model.Memberships.insert({user, group});
                    }
                }
                break;
            case 6:
                if (model.Memberships.contains({user, group})) {
                    const auto result = provider.RemoveGroupMembership({.Group = group, .Member = user});
                    if (!result.Error) {
                        model.Memberships.erase({user, group});
                    }
                }
                break;
            case 7:
                if (model.Users.contains(user)) {
                    NLogin::TLoginProvider::TLoginUserRequest request;
                    request.User = user;
                    request.Options.WithUserGroups = true;
                    NLogin::TLoginProvider::TPasswordCheckResult checkResult;
                    checkResult.Status = NLogin::TLoginProvider::TLoginUserResponse::EStatus::SUCCESS;
                    const auto result = provider.LoginUser(request, checkResult);
                    if (model.CanLogin[user]) {
                        Y_ABORT_UNLESS(!result.Error);
                        Y_ABORT_UNLESS(result.Token);
                        model.Tokens.push_back({result.Token, user});
                        CheckToken(provider, result.Token, user);
                    } else {
                        Y_ABORT_UNLESS(result.Error);
                    }
                }
                break;
            case 8:
                if (model.Users.contains(user)) {
                    NLogin::TLoginProvider::TLoginUserRequest request;
                    request.User = user;
                    NLogin::TLoginProvider::TPasswordCheckResult checkResult;
                    checkResult.FillInvalidPassword();
                    const auto result = provider.LoginUser(request, checkResult);
                    Y_ABORT_UNLESS(result.Error);
                }
                break;
            case 9:
                RoundTripSecurityState(provider, model, fdp);
                break;
            default:
                provider.ValidateToken({.Token = fdp.ConsumeRandomLengthString(128)});
                CheckSavedTokens(provider, model, fdp);
                break;
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 16 * 1024) {
        return 0;
    }

    try {
        FuzzedDataProvider fdp(data, size);
        FuzzLogin(fdp);
    } catch (...) {
    }

    return 0;
}
