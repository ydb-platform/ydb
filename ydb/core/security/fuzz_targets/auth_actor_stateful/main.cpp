#include <ydb/library/login/login.h>
#include <ydb/library/login/sasl/scram.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <array>
#include <util/generic/set.h>
#include <util/string/cast.h>

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

TString Pick(FuzzedDataProvider& fdp, const std::array<TStringBuf, 4>& values) {
    return TString(values[fdp.ConsumeIntegralInRange<size_t>(0, values.size() - 1)]);
}

TString Pick(FuzzedDataProvider& fdp, const std::array<TStringBuf, 3>& values) {
    return TString(values[fdp.ConsumeIntegralInRange<size_t>(0, values.size() - 1)]);
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

void CheckToken(NLogin::TLoginProvider& provider, const TString& token, const TString& expectedUser) {
    const auto result = provider.ValidateToken({.Token = token});
    Y_ABORT_UNLESS(!result.Error);
    Y_ABORT_UNLESS(result.User == expectedUser);
}

void ValidateSavedTokens(NLogin::TLoginProvider& provider, const TModel& model, FuzzedDataProvider& fdp) {
    const ui32 checks = Min<ui32>(model.Tokens.size(), fdp.ConsumeIntegralInRange<ui32>(0, 8));
    for (ui32 i = 0; i < checks; ++i) {
        const auto& [token, user] = model.Tokens[fdp.ConsumeIntegralInRange<size_t>(0, model.Tokens.size() - 1)];
        const auto result = provider.ValidateToken({.Token = token});
        if (!result.Error) {
            Y_ABORT_UNLESS(result.User == user);
        }
    }
}

void ExerciseScram(FuzzedDataProvider& fdp) {
    using namespace NLogin::NSasl;

    const std::string nonce = "nonce" + ToString(fdp.ConsumeIntegralInRange<ui32>(0, 16));
    const std::string salt = "salt" + ToString(fdp.ConsumeIntegralInRange<ui32>(0, 16));
    const ui32 iterations = fdp.ConsumeIntegralInRange<ui32>(1, 64);
    const std::string iterationsString = ToString(iterations);

    const std::string firstServer = BuildFirstServerMsg(nonce, salt, iterationsString);
    TFirstServerMsg parsedFirstServer;
    if (ParseFirstServerMsg(firstServer, parsedFirstServer) == EParseMsgReturnCodes::Success) {
        Y_ABORT_UNLESS(parsedFirstServer.Nonce == nonce);
        Y_ABORT_UNLESS(parsedFirstServer.Salt == salt);
        Y_ABORT_UNLESS(parsedFirstServer.IterationsCount == iterations);
    }

    const std::string finalServer = BuildFinalServerMsg("signature" + ToString(iterations));
    TFinalServerMsg parsedFinalServer;
    if (ParseFinalServerMsg(finalServer, parsedFinalServer) == EParseMsgReturnCodes::Success) {
        Y_ABORT_UNLESS(parsedFinalServer.Error.empty());
    }

    TFirstClientMsg firstClient;
    ParseFirstClientMsg(fdp.ConsumeRandomLengthString(64), firstClient);
    TFinalClientMsg finalClient;
    ParseFinalClientMsg(fdp.ConsumeRandomLengthString(64), finalClient);

    std::string storedKey;
    std::string serverKey;
    std::string error;
    if (GenerateScramSecrets("SCRAM-SHA-256", "password", salt, iterations, storedKey, serverKey, error)) {
        std::string recomputedServerKey;
        Y_ABORT_UNLESS(ComputeServerKey("SCRAM-SHA-256", "password", salt, iterations, recomputedServerKey, error));
        Y_ABORT_UNLESS(recomputedServerKey == serverKey);

        const std::string authMessage = "n=user,r=client,r=clientsrv,s=" + salt + ",i=" + iterationsString + ",c=biws,r=clientsrv";
        std::string proof;
        if (ComputeClientProof("SCRAM-SHA-256", "password", salt, iterations, authMessage, proof, error)) {
            Y_ABORT_UNLESS(VerifyClientProof("SCRAM-SHA-256", proof, storedKey, authMessage, error));
        }
    }
}

void FuzzAuth(FuzzedDataProvider& fdp) {
    NLogin::TLoginProvider provider;
    provider.RotateKeys();
    TModel model;

    const ui32 steps = fdp.ConsumeIntegralInRange<ui32>(1, 64);
    for (ui32 step = 0; step < steps && fdp.remaining_bytes(); ++step) {
        const TString user = Pick(fdp, Users);
        const TString group = Pick(fdp, Groups);

        switch (fdp.ConsumeIntegralInRange<ui8>(0, 11)) {
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
                    provider.RemoveUser(user);
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
                    provider.RemoveGroup(group);
                    model.Groups.erase(group);
                    for (auto it = model.Memberships.begin(); it != model.Memberships.end();) {
                        if (it->second == group) {
                            it = model.Memberships.erase(it);
                        } else {
                            ++it;
                        }
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
            case 9: {
                NLogin::TLoginProvider mirror;
                mirror.UpdateSecurityState(provider.GetSecurityState());
                ValidateSavedTokens(mirror, model, fdp);
                provider.UpdateSecurityState(mirror.GetSecurityState());
                break;
            }
            case 10: {
                provider.RotateKeys();
                ValidateSavedTokens(provider, model, fdp);
                break;
            }
            default: {
                const TString randomToken = fdp.ConsumeRandomLengthString(128);
                provider.ValidateToken({.Token = randomToken});
                ExerciseScram(fdp);
                break;
            }
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
        FuzzAuth(fdp);
    } catch (...) {
    }

    return 0;
}
