#include <contrib/libs/jwt-cpp/include/jwt-cpp/jwt.h>
#include <library/cpp/digest/argonish/argon2.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/rand.h>

#include <util/string/builder.h>

#include <deque>

#include <ydb/library/login/password_checker/password_checker.h>

#include "login.h"

namespace NLogin {

struct TLoginProvider::TImpl {
public:
    TLruCache SuccessPasswordsCache;
    TLruCache WrongPasswordsCache;
    std::function<bool()> IsCacheUsed = [] () {return false;};
    static const THolder<const NArgonish::IArgon2Base> ArgonHasher;

public:
    TImpl() : TImpl([] () {return false;}, {}) {}

    TImpl(const std::function<bool()>& isCacheUsed, const TLoginProvider::TCacheSettings& cacheSettings)
        : SuccessPasswordsCache(cacheSettings.SuccessPasswordsCacheCapacity)
        , WrongPasswordsCache(cacheSettings.WrongPasswordsCacheCapacity)
        , IsCacheUsed(isCacheUsed)
    {}

    void GenerateKeyPair(TString& publicKey, TString& privateKey) const ;
    TString GenerateHash(const TString& password) const;
    static bool VerifyHash(const TString& password, const TString& hash);
    bool VerifyHashWithCache(const TLruCache::TKey& key);
    bool NeedVerifyHash(const TLruCache::TKey& key, TPasswordCheckResult* checkResult);
    void UpdateCache(const TLruCache::TKey& key, const bool isSuccessVerifying);

    void UpdateCacheSettings(const TLoginProvider::TCacheSettings& cacheSettings);

private:
    void ClearCache();
};

const THolder<const NArgonish::IArgon2Base> TLoginProvider::TImpl::ArgonHasher = Default<NArgonish::TArgon2Factory>().Create(
    NArgonish::EArgon2Type::Argon2id, // Mixed version of Argon2
    2, // 2-pass computation
    (1<<11), // 2 mebibytes memory usage (in KiB)
    1 // number of threads and lanes
);

TLoginProvider::TLoginProvider()
    : Impl(new TImpl())
    , PasswordChecker(TPasswordComplexity())
    , AccountLockout()
{}

TLoginProvider::TLoginProvider(const TAccountLockout::TInitializer& accountLockoutInitializer)
    : TLoginProvider(TPasswordComplexity(), accountLockoutInitializer, [] () {return false;}, {})
{}

TLoginProvider::TLoginProvider(const TPasswordComplexity& passwordComplexity,
    const TAccountLockout::TInitializer& accountLockoutInitializer,
    const std::function<bool()>& isCacheUsed,
    const TCacheSettings& cacheSettings)
    : Impl(new TImpl(isCacheUsed, cacheSettings))
    , PasswordChecker(passwordComplexity)
    , AccountLockout(accountLockoutInitializer)
{}

TLoginProvider::~TLoginProvider()
{}

bool TLoginProvider::StrongCheckAllowedName(const TString& name) {
    return !name.empty() && name.find_first_not_of("abcdefghijklmnopqrstuvwxyz0123456789") == std::string::npos;
}

bool TLoginProvider::BasicCheckAllowedName(const TString& name) {
    return !name.empty() && name.find_first_not_of("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_") == std::string::npos;
}

bool TLoginProvider::CheckGroupNameAllowed(const bool strongCheckName, const TString& groupName) {
    if (strongCheckName) {
        return StrongCheckAllowedName(groupName);
    }
    return BasicCheckAllowedName(groupName);
}

bool TLoginProvider::CheckPasswordOrHash(bool IsHashedPassword, const TString& user, const TString& password, TString& error) const {
    if (IsHashedPassword) {
        auto hashCheckResult = HashChecker.Check(password);
        if (!hashCheckResult.Success) {
            error = hashCheckResult.Error;
            return false;
        }
    } else {
        auto passwordCheckResult = PasswordChecker.Check(user, password);
        if (!passwordCheckResult.Success) {
            error = passwordCheckResult.Error;
            return false;
        }
    }

    return true;
}

TLoginProvider::TBasicResponse TLoginProvider::CreateUser(const TCreateUserRequest& request) {
    TBasicResponse response;

    if (!StrongCheckAllowedName(request.User)) {
        response.Error = "Name is not allowed";
        return response;
    }

    if (!CheckPasswordOrHash(request.IsHashedPassword, request.User, request.Password, response.Error)) {
        return response;
    }

    auto itUserCreate = Sids.emplace(request.User, TSidRecord{.Type = NLoginProto::ESidType::USER});
    if (!itUserCreate.second) {
        if (itUserCreate.first->second.Type == ESidType::USER) {
            response.Error = "User already exists";
        } else {
            response.Error = "Account already exists";
        }
        return response;
    }

    TSidRecord& user = itUserCreate.first->second;
    user.Name = request.User;
    user.PasswordHash = request.IsHashedPassword ? request.Password : Impl->GenerateHash(request.Password);
    user.CreatedAt = std::chrono::system_clock::now();
    user.IsEnabled = request.CanLogin;
    return response;
}

bool TLoginProvider::CheckSubjectExists(const TString& name, const ESidType::SidType& type) {
    auto itSidModify = Sids.find(name);
    return itSidModify != Sids.end() && itSidModify->second.Type == type;
}

bool TLoginProvider::CheckUserExists(const TString& user) {
    return CheckSubjectExists(user, ESidType::USER);
}

bool TLoginProvider::CheckGroupExists(const TString& group) {
    return CheckSubjectExists(group, ESidType::GROUP);
}

TLoginProvider::TBasicResponse TLoginProvider::ModifyUser(const TModifyUserRequest& request) {
    TBasicResponse response;

    auto itUserModify = Sids.find(request.User);
    if (itUserModify == Sids.end() || itUserModify->second.Type != ESidType::USER) {
        response.Error = "User not found";
        return response;
    }

    TSidRecord& user = itUserModify->second;

    if (request.Password.has_value()) {
        if (!CheckPasswordOrHash(request.IsHashedPassword, request.User, request.Password.value(), response.Error)) {
            return response;
        }

        user.PasswordHash = request.IsHashedPassword ? request.Password.value() : Impl->GenerateHash(request.Password.value());
    }

    if (request.CanLogin.has_value()) {
        user.IsEnabled = request.CanLogin.value();

        if (user.IsEnabled && CheckLockoutByAttemptCount(user)) {
            ResetFailedLoginAttemptCount(&user);
        }
    }

    return response;
}

TLoginProvider::TRemoveUserResponse TLoginProvider::RemoveUser(const TString& user) {
    TRemoveUserResponse response;

    auto itUserModify = Sids.find(user);
    if (itUserModify == Sids.end() || itUserModify->second.Type != ESidType::USER) {
        response.Error = "User not found";
        return response;
    }

    auto itChildToParentIndex = ChildToParentIndex.find(user);
    if (itChildToParentIndex != ChildToParentIndex.end()) {
        for (const TString& parent : itChildToParentIndex->second) {
            auto itGroup = Sids.find(parent);
            if (itGroup != Sids.end()) {
                response.TouchedGroups.emplace_back(itGroup->first);
                itGroup->second.Members.erase(user);
            }
        }
        ChildToParentIndex.erase(itChildToParentIndex);
    }

    Sids.erase(itUserModify);

    return response;
}

TLoginProvider::TBasicResponse TLoginProvider::CreateGroup(const TCreateGroupRequest& request) {
    TBasicResponse response;

    if (!CheckGroupNameAllowed(request.Options.StrongCheckName, request.Group)) {
        response.Error = "Name is not allowed";
        return response;
    }
    auto itGroupCreate = Sids.emplace(request.Group, TSidRecord{.Type = ESidType::GROUP});
    if (!itGroupCreate.second) {
        if (itGroupCreate.first->second.Type == ESidType::GROUP) {
            response.Error = "Group already exists";
        } else {
            response.Error = "Account already exists";
        }
        return response;
    }

    TSidRecord& group = itGroupCreate.first->second;
    group.Name = request.Group;
    group.CreatedAt = std::chrono::system_clock::now();

    return response;
}

TLoginProvider::TBasicResponse TLoginProvider::AddGroupMembership(const TAddGroupMembershipRequest& request) {
    TBasicResponse response;

    auto itGroupModify = Sids.find(request.Group);
    if (itGroupModify == Sids.end() || itGroupModify->second.Type != ESidType::GROUP) {
        response.Error = "Group not found";
        return response;
    }

    if (Sids.count(request.Member) == 0) {
        response.Error = "Member account not found";
        return response;
    }

    TSidRecord& group = itGroupModify->second;

    if (group.Members.count(request.Member)) {
        response.Notice = TStringBuilder() << "Role \"" << request.Member << "\" is already a member of role \"" << group.Name << "\"";
    } else {
        group.Members.insert(request.Member);
    }

    ChildToParentIndex[request.Member].insert(request.Group);

    return response;
}

TLoginProvider::TBasicResponse TLoginProvider::RemoveGroupMembership(const TRemoveGroupMembershipRequest& request) {
    TBasicResponse response;

    auto itGroupModify = Sids.find(request.Group);
    if (itGroupModify == Sids.end() || itGroupModify->second.Type != ESidType::GROUP) {
        response.Error = "Group not found";
        return response;
    }

    TSidRecord& group = itGroupModify->second;

    if (!group.Members.count(request.Member)) {
        response.Warning = TStringBuilder() << "Role \"" << request.Member << "\" is not a member of role \"" << group.Name << "\"";
    } else {
        group.Members.erase(request.Member);
    }

    ChildToParentIndex[request.Member].erase(request.Group);

    return response;
}

TLoginProvider::TRenameGroupResponse TLoginProvider::RenameGroup(const TRenameGroupRequest& request) {
    TRenameGroupResponse response;

    if (!CheckGroupNameAllowed(request.Options.StrongCheckName, request.Group)) {
        response.Error = "Name is not allowed";
        return response;
    }

    auto itGroupModify = Sids.find(request.Group);
    if (itGroupModify == Sids.end() || itGroupModify->second.Type != ESidType::GROUP) {
        response.Error = "Group not found";
        return response;
    }

    auto itGroupCreate = Sids.emplace(request.NewName, TSidRecord{.Type = ESidType::GROUP});
    if (!itGroupCreate.second) {
        if (itGroupCreate.first->second.Type == ESidType::GROUP) {
            response.Error = "Group already exists";
        } else {
            response.Error = "Account already exists";
        }
        return response;
    }

    TSidRecord& group = itGroupCreate.first->second;
    group.Name = request.NewName;

    auto itChildToParentIndex = ChildToParentIndex.find(request.Group);
    if (itChildToParentIndex != ChildToParentIndex.end()) {
        ChildToParentIndex[request.NewName] = itChildToParentIndex->second;
        for (const TString& parent : ChildToParentIndex[request.NewName]) {
            auto itGroup = Sids.find(parent);
            if (itGroup != Sids.end()) {
                response.TouchedGroups.emplace_back(itGroup->first);
                itGroup->second.Members.erase(request.Group);
                itGroup->second.Members.insert(request.NewName);
            }
        }
        ChildToParentIndex.erase(itChildToParentIndex);
    }

    for (const TString& member : itGroupModify->second.Members) {
        ChildToParentIndex[member].erase(request.Group);
        ChildToParentIndex[member].insert(request.NewName);
    }

    Sids.erase(itGroupModify);

    return response;
}

TLoginProvider::TRemoveGroupResponse TLoginProvider::RemoveGroup(const TString& group) {
    TRemoveGroupResponse response;

    auto itGroupModify = Sids.find(group);
    if (itGroupModify == Sids.end() || itGroupModify->second.Type != ESidType::GROUP) {
        response.Error = "Group not found";
        return response;
    }

    auto itChildToParentIndex = ChildToParentIndex.find(group);
    if (itChildToParentIndex != ChildToParentIndex.end()) {
        for (const TString& parent : itChildToParentIndex->second) {
            auto itGroup = Sids.find(parent);
            if (itGroup != Sids.end()) {
                response.TouchedGroups.emplace_back(itGroup->first);
                itGroup->second.Members.erase(group);
            }
        }
        ChildToParentIndex.erase(itChildToParentIndex);
    }

    for (const TString& member : itGroupModify->second.Members) {
        ChildToParentIndex[member].erase(group);
    }

    Sids.erase(itGroupModify);

    return response;
}

std::vector<TString> TLoginProvider::GetGroupsMembership(const TString& member) const {
    std::vector<TString> groups;
    std::unordered_set<TString> visited;
    std::deque<TString> queue;
    queue.push_back(member);
    while (!queue.empty()) {
        TString member = queue.front();
        queue.pop_front();
        auto itChildToParentIndex = ChildToParentIndex.find(member);
        if (itChildToParentIndex != ChildToParentIndex.end()) {
            for (const TString& parent : itChildToParentIndex->second) {
                if (visited.insert(parent).second) {
                    queue.push_back(parent);
                    groups.push_back(parent);
                }
            }
        }
    }
    return groups;
}

bool TLoginProvider::CheckLockoutByAttemptCount(const TSidRecord& sid) const {
    return AccountLockout.AttemptThreshold != 0 && sid.FailedLoginAttemptCount >= AccountLockout.AttemptThreshold;
}

bool TLoginProvider::CheckLockout(const TSidRecord& sid) const {
    return !sid.IsEnabled || CheckLockoutByAttemptCount(sid);
}

void TLoginProvider::ResetFailedLoginAttemptCount(TSidRecord* sid) {
    sid->FailedLoginAttemptCount = 0;
}

void TLoginProvider::UnlockAccount(TSidRecord* sid) {
    ResetFailedLoginAttemptCount(sid);
}

bool TLoginProvider::ShouldResetFailedAttemptCount(const TSidRecord& sid) const {
    if (sid.FailedLoginAttemptCount == 0) {
        return false;
    }

    if (AccountLockout.AttemptResetDuration == std::chrono::system_clock::duration::zero()) {
        return false;
    }

    return sid.LastFailedLogin + AccountLockout.AttemptResetDuration < std::chrono::system_clock::now();
}

bool TLoginProvider::ShouldUnlockAccount(const TSidRecord& sid) const {
    return sid.IsEnabled && ShouldResetFailedAttemptCount(sid);
}

bool TLoginProvider::IsLockedOut(const TSidRecord& user) const {
    Y_ABORT_UNLESS(user.Type == NLoginProto::ESidType::USER);

    if (AccountLockout.AttemptThreshold == 0) {
        return false;
    }

    if (user.FailedLoginAttemptCount < AccountLockout.AttemptThreshold) {
        return false;
    }

    if (ShouldResetFailedAttemptCount(user)) {
        return false;
    }

    return true;
}

TLoginProvider::TCheckLockOutResponse TLoginProvider::CheckLockOutUser(const TCheckLockOutRequest& request) {
    TCheckLockOutResponse response;
    auto itUser = Sids.find(request.User);
    if (itUser == Sids.end()) {
        response.Status = TCheckLockOutResponse::EStatus::INVALID_USER;
        response.Error = TStringBuilder() << "Cannot find user: " << request.User;
        return response;
    } else if (itUser->second.Type != ESidType::USER) {
        response.Status = TCheckLockOutResponse::EStatus::INVALID_USER;
        response.Error = TStringBuilder() << request.User << " is a group";
        return response;
    }

    TSidRecord& sid = itUser->second;
    if (CheckLockout(sid)) {
        if (ShouldUnlockAccount(sid)) {
            UnlockAccount(&sid);
            response.Status = TCheckLockOutResponse::EStatus::RESET;
        } else {
            response.Status = TCheckLockOutResponse::EStatus::SUCCESS;

            if (!sid.IsEnabled) {
                response.Error = TStringBuilder() << "User " << request.User << " login denied: account is blocked";
            } else {
                response.Error = TStringBuilder() << "User " << request.User << " login denied: too many failed password attempts";
            }
        }
        return response;
    } else if (ShouldResetFailedAttemptCount(sid)) {
        ResetFailedLoginAttemptCount(&sid);
        response.Status = TCheckLockOutResponse::EStatus::RESET;
        return response;
    }
    response.Status = TCheckLockOutResponse::EStatus::UNLOCKED;
    return response;
}

bool TLoginProvider::NeedVerifyHash(const TLoginUserRequest& request, TPasswordCheckResult* checkResult, TString* passwordHash) {
    Y_ENSURE(checkResult);
    Y_ENSURE(passwordHash);

    if (FillUnavailableKey(checkResult)) {
        return false;
    }

    if (!request.ExternalAuth) {
        const auto* sid = GetUserSid(request.User);
        if (FillInvalidUser(sid, checkResult)) {
            return false;
        }

        *passwordHash = sid->PasswordHash;
        return Impl->NeedVerifyHash({.User = request.User, .Password = request.Password, .Hash = sid->PasswordHash}, checkResult);
    }

    return false;
}

bool TLoginProvider::VerifyHash(const TLoginUserRequest& request, const TString& passwordHash) {
    return TImpl::VerifyHash(request.Password, passwordHash);
}

void TLoginProvider::UpdateCache(const TLoginUserRequest& request, const TString& passwordHash, const bool isSuccessVerifying) {
    Impl->UpdateCache({.User = request.User, .Password = request.Password, .Hash = passwordHash}, isSuccessVerifying);
}

bool TLoginProvider::FillUnavailableKey(TPasswordCheckResult* checkResult) const {
    if (Keys.empty() || Keys.back().PrivateKey.empty()) {
        checkResult->FillUnavailableKey();
        return true;
    }
    return false;
}

TLoginProvider::TSidRecord* TLoginProvider::GetUserSid(const TString& user) {
    auto itUser = Sids.find(user);
    if (itUser == Sids.end() || itUser->second.Type != ESidType::USER) {
        return nullptr;
    }
    return &(itUser->second);
}

bool TLoginProvider::FillInvalidUser(const TSidRecord* sid, TPasswordCheckResult* checkResult) const {
    if (!sid) {
        checkResult->FillInvalidUser("Invalid user");
        return true;
    }
    return false;
}

TLoginProvider::TLoginUserResponse TLoginProvider::LoginUser(const TLoginUserRequest& request, const TPasswordCheckResult& checkResult) {
    TLoginUserResponse response;
    if (checkResult.Status == TLoginUserResponse::EStatus::UNAVAILABLE_KEY) {
        response.FillUnavailableKey();
        return response;
    }
    if (checkResult.Status == TLoginUserResponse::EStatus::INVALID_USER) {
        response.FillInvalidUser(checkResult.Error);
        return response;
    }

    if (FillUnavailableKey(&response)) {
        return response;
    }

    TSidRecord* sid = nullptr;
    if (!request.ExternalAuth) {
        sid = GetUserSid(request.User);
        if (FillInvalidUser(sid, &response)) {
            return response;
        }

        if (checkResult.Status == TLoginUserResponse::EStatus::INVALID_PASSWORD) {
            response.FillInvalidPassword();
            sid->LastFailedLogin = std::chrono::system_clock::now();
            sid->FailedLoginAttemptCount++;
            return response;
        }
    }
    Y_ENSURE(!checkResult.Error);

    const TKeyRecord& key = Keys.back();
    auto keyId = ToString(key.KeyId);
    const auto& publicKey = key.PublicKey;
    const auto& privateKey = key.PrivateKey;

    // encode jwt
    const auto now = std::chrono::system_clock::now();
    auto expires_at = now + MAX_TOKEN_EXPIRE_TIME;
    if (request.Options.ExpiresAfter != std::chrono::system_clock::duration::zero()) {
        expires_at = std::min(expires_at, now + request.Options.ExpiresAfter);
    }
    auto algorithm = jwt::algorithm::ps256(publicKey, privateKey);

    auto token = jwt::create()
            .set_key_id(keyId)
            .set_subject(request.User)
            .set_issued_at(now)
            .set_expires_at(expires_at);
    if (!Audience.empty()) {
        // jwt.h require audience claim to be a set
        token.set_audience(std::set<std::string>{Audience});
    }

    if (request.ExternalAuth) {
        token.set_payload_claim(EXTERNAL_AUTH_CLAIM_NAME, jwt::claim(request.ExternalAuth));
    } else {
        if (request.Options.WithUserGroups) {
            auto groups = GetGroupsMembership(request.User);
            token.set_payload_claim(GROUPS_CLAIM_NAME, jwt::claim(picojson::value(std::vector<picojson::value>(groups.begin(), groups.end()))));
        }
    }

    auto encoded_token = token.sign(algorithm);

    response.Token = TString(encoded_token);
    response.SanitizedToken = SanitizeJwtToken(response.Token);
    response.Status = TLoginUserResponse::EStatus::SUCCESS;

    if (sid) {
        sid->LastSuccessfulLogin = now;
        sid->FailedLoginAttemptCount = 0;
    }
    return response;
}

TLoginProvider::TLoginUserResponse TLoginProvider::LoginUser(const TLoginUserRequest& request) {
    TPasswordCheckResult checkResult;
    TString passwordHash;
    if (NeedVerifyHash(request, &checkResult, &passwordHash)) {
        const auto isSuccessVerifying = VerifyHash(request, passwordHash);
        UpdateCache(request, passwordHash, isSuccessVerifying);
        if (!isSuccessVerifying) {
            checkResult.FillInvalidPassword();
        }
    }
    return LoginUser(request, checkResult);
}

std::deque<TLoginProvider::TKeyRecord>::iterator TLoginProvider::FindKeyIterator(ui64 keyId) {
    if (!Keys.empty() && keyId >= Keys.front().KeyId && keyId <= Keys.back().KeyId) {
        auto it = std::next(Keys.begin(), keyId - Keys.front().KeyId);
        if (it->KeyId == keyId) {
            return it;
        }
    }
    return Keys.end();
}

const TLoginProvider::TKeyRecord* TLoginProvider::FindKey(ui64 keyId) {
    auto it = FindKeyIterator(keyId);
    if (it != Keys.end()) {
        return &(*it);
    }
    return nullptr;
}

TLoginProvider::TValidateTokenResponse TLoginProvider::ValidateToken(const TValidateTokenRequest& request) {
    TLoginProvider::TValidateTokenResponse response;
    try {
        jwt::decoded_jwt decoded_token = jwt::decode(request.Token);
        if (Audience) {
            // we check audience manually because we want an explicit error instead of wrong key id in case of databases mismatch
            auto audience = decoded_token.get_audience();
            if (audience.empty() || TString(*audience.begin()) != Audience) {
                response.WrongAudience = true;
                response.Error = "Wrong audience";
                return response;
            }
        }
        auto keyId = FromStringWithDefault<ui64>(decoded_token.get_key_id());
        const TKeyRecord* key = FindKey(keyId);
        if (key != nullptr) {
            static const size_t ISSUED_AT_LEEWAY_SEC = 2;
            auto verifier = jwt::verify()
                .allow_algorithm(jwt::algorithm::ps256(key->PublicKey))
                .issued_at_leeway(ISSUED_AT_LEEWAY_SEC);
            if (Audience) {
                // jwt.h require audience claim to be a set
                verifier.with_audience(std::set<std::string>{Audience});
            }

            verifier.verify(decoded_token);
            response.User = decoded_token.get_subject();
            response.ExpiresAt = decoded_token.get_expires_at();
            if (decoded_token.has_payload_claim(GROUPS_CLAIM_NAME)) {
                const jwt::claim& groups = decoded_token.get_payload_claim(GROUPS_CLAIM_NAME);
                if (groups.get_type() == jwt::claim::type::array) {
                    const picojson::array& array = groups.as_array();
                    std::vector<TString> groups;
                    groups.resize(array.size());
                    for (size_t i = 0; i < array.size(); ++i) {
                        groups[i] = array[i].get<std::string>();
                    }
                    response.Groups = groups;
                }
            }
            if (decoded_token.has_payload_claim(EXTERNAL_AUTH_CLAIM_NAME)) {
                const jwt::claim& externalAuthClaim = decoded_token.get_payload_claim(EXTERNAL_AUTH_CLAIM_NAME);
                if (externalAuthClaim.get_type() == jwt::claim::type::string) {
                    response.ExternalAuth = externalAuthClaim.as_string();
                }
            } else if (!Sids.empty()) {
                auto itUser = Sids.find(TString(decoded_token.get_subject()));
                if (itUser == Sids.end()) {
                    response.Error = "Token is valid, but subject wasn't found";
                }
            }
        } else {
            if (Keys.empty()) {
                response.Error = "Security state is empty";
                response.ErrorRetryable = true;
            } else if (keyId < Keys.front().KeyId) {
                response.Error = "The key of this token has expired";
            } else if (keyId > Keys.back().KeyId) {
                response.Error = "The key of this token is not available yet";
                response.ErrorRetryable = true;
            } else {
                response.Error = "Key not found";
            }
        }
    } catch (const jwt::signature_verification_exception& e) {
        response.Error = e.what(); // invalid token signature
    } catch (const jwt::token_verification_exception& e) {
        response.Error = e.what(); // invalid token
    } catch (const std::invalid_argument& e) {
        response.Error = "Token is not in correct format";
        response.TokenUnrecognized = true;
    } catch (const std::runtime_error& e) {
        response.Error = "Base64 decoding failed or invalid json";
        response.TokenUnrecognized = true;
    } catch (const std::exception& e) {
        response.Error = e.what();
    }
    return response;
}

TString TLoginProvider::GetTokenAudience(const TString& token) {
    try {
        jwt::decoded_jwt decoded_token = jwt::decode(token);
        auto audience = decoded_token.get_audience();
        if (!audience.empty()) {
            return TString(*audience.begin());
        }
    }
    catch (...) {
    }
    return {};
}

std::chrono::system_clock::time_point TLoginProvider::GetTokenExpiresAt(const TString& token) {
    try {
        jwt::decoded_jwt decoded_token = jwt::decode(token);
        return decoded_token.get_expires_at();
    }
    catch (...) {
    }
    return {};
}

bool TLoginProvider::IsItTimeToRotateKeys() const {
    return Keys.empty()
        || Keys.back().PrivateKey.empty()
        || KeysRotationTime + KEYS_ROTATION_PERIOD < std::chrono::system_clock::now();
}

void TLoginProvider::RotateKeys() {
    std::vector<ui64> keysExpired;
    std::vector<ui64> keysAdded;
    RotateKeys(keysExpired, keysAdded);
}

void TLoginProvider::RotateKeys(std::vector<ui64>& keysExpired, std::vector<ui64>& keysAdded) {
    TString publicKey;
    TString privateKey;
    Impl->GenerateKeyPair(publicKey, privateKey);
    ui64 newKeyId;
    if (Keys.empty()) {
        newKeyId = 1;
    } else {
        newKeyId = Keys.back().KeyId + 1;
    }
    auto now = std::chrono::system_clock::now();
    Keys.push_back({
        .KeyId = newKeyId,
        .PublicKey = publicKey,
        .PrivateKey = privateKey,
        .ExpiresAt = now + KEY_EXPIRE_TIME,
    });
    keysAdded.push_back(newKeyId);
    while (Keys.size() > MAX_SERVER_KEYS || (!Keys.empty() && Keys.front().ExpiresAt <= now)) {
        ui64 oldKeyId = Keys.front().KeyId;
        Keys.pop_front();
        keysExpired.push_back(oldKeyId);
    }
    KeysRotationTime = now;
}

void TLoginProvider::TImpl::GenerateKeyPair(TString& publicKey, TString& privateKey) const {
    static constexpr int bits = 2048;
    publicKey.clear();
    privateKey.clear();
    BIGNUM* bne = BN_new();
    int ret = BN_set_word(bne, RSA_F4);
    if (ret == 1) {
        RSA* r = RSA_new();
        ret = RSA_generate_key_ex(r, bits, bne, nullptr);
        if (ret == 1) {
            BIO* bioPublic = BIO_new(BIO_s_mem());
            ret = PEM_write_bio_RSA_PUBKEY(bioPublic, r);
            if (ret == 1) {
                BIO* bioPrivate = BIO_new(BIO_s_mem());
                ret = PEM_write_bio_RSAPrivateKey(bioPrivate, r, nullptr, nullptr, 0, nullptr, nullptr);
                size_t privateSize = BIO_pending(bioPrivate);
                size_t publicSize = BIO_pending(bioPublic);
                privateKey.resize(privateSize);
                publicKey.resize(publicSize);
                BIO_read(bioPrivate, &privateKey[0], privateSize);
                BIO_read(bioPublic, &publicKey[0], publicSize);
                BIO_free(bioPrivate);
            }
            BIO_free(bioPublic);
        }
        RSA_free(r);
    }
    BN_free(bne);
}

TString TLoginProvider::TImpl::GenerateHash(const TString& password) const {
    char salt[SALT_SIZE];
    char hash[HASH_SIZE];
    RAND_bytes(reinterpret_cast<unsigned char*>(salt), SALT_SIZE);
    ArgonHasher->Hash(
        reinterpret_cast<const ui8*>(password.data()),
        password.size(),
        reinterpret_cast<ui8*>(salt),
        SALT_SIZE,
        reinterpret_cast<ui8*>(hash),
        HASH_SIZE);
    NJson::TJsonValue json;
    json["type"] = "argon2id";
    json["salt"] = Base64Encode(TStringBuf(salt, SALT_SIZE));
    json["hash"] = Base64Encode(TStringBuf(hash, HASH_SIZE));
    return NJson::WriteJson(json, false);
}

bool TLoginProvider::TImpl::VerifyHash(const TString& password, const TString& passwordHash) {
    NJson::TJsonValue json;
    if (!NJson::ReadJsonTree(passwordHash, &json)) {
        return false;
    }
    TString type = json["type"].GetStringRobust();
    if (type != "argon2id") {
        return false;
    }
    TString salt = Base64Decode(json["salt"].GetStringRobust());
    TString hash = Base64Decode(json["hash"].GetStringRobust());
    return ArgonHasher->Verify(
        reinterpret_cast<const ui8*>(password.data()),
        password.size(),
        reinterpret_cast<const ui8*>(salt.data()),
        salt.size(),
        reinterpret_cast<const ui8*>(hash.data()),
        hash.size());
}

bool TLoginProvider::TImpl::NeedVerifyHash(const TLruCache::TKey& key, TPasswordCheckResult* checkResult) {
    Y_ENSURE(checkResult);

    if (!IsCacheUsed()) {
        ClearCache();
        return true;
    }

    if (SuccessPasswordsCache.Find(key) != SuccessPasswordsCache.End()) {
        checkResult->Status = TLoginUserResponse::EStatus::SUCCESS;
        return false;
    }

    if (WrongPasswordsCache.Find(key) != WrongPasswordsCache.End()) {
        checkResult->FillInvalidPassword();
        return false;
    }

    return true;
}

void TLoginProvider::TImpl::UpdateCache(const TLruCache::TKey& key, const bool isSuccessVerifying) {
    if (isSuccessVerifying) {
        SuccessPasswordsCache.Insert(key, true);
    } else {
        WrongPasswordsCache.Insert(key, false);
    }

}

bool TLoginProvider::TImpl::VerifyHashWithCache(const TLruCache::TKey& key) {
    if (!IsCacheUsed()) {
        ClearCache();
        return VerifyHash(key.Password, key.Hash);
    }

    if (SuccessPasswordsCache.Find(key) != SuccessPasswordsCache.End()) {
        return true;
    }

    if (WrongPasswordsCache.Find(key) != WrongPasswordsCache.End()) {
        return false;
    }

    const bool isSuccessVerifying = VerifyHash(key.Password, key.Hash);
    UpdateCache(key, isSuccessVerifying);
    return isSuccessVerifying;
}

void TLoginProvider::TImpl::UpdateCacheSettings(const TCacheSettings& cacheSettings) {
    SuccessPasswordsCache.Resize(cacheSettings.SuccessPasswordsCacheCapacity);
    WrongPasswordsCache.Resize(cacheSettings.WrongPasswordsCacheCapacity);
}

void TLoginProvider::TImpl::ClearCache() {
    if (SuccessPasswordsCache.Size() > 0) {
        SuccessPasswordsCache.Clear();
    }
    if (WrongPasswordsCache.Size() > 0) {
        WrongPasswordsCache.Clear();
    }
}

NLoginProto::TSecurityState TLoginProvider::GetSecurityState() const {
    NLoginProto::TSecurityState state;
    state.SetAudience(Audience);
    {
        auto& pbPublicKeys = *state.MutablePublicKeys();
        pbPublicKeys.Clear();
        for (const TKeyRecord& key : Keys) {
            NLoginProto::TPublicKey& publicKey = *pbPublicKeys.Add();
            publicKey.SetKeyId(key.KeyId);
            publicKey.SetKeyDataPEM(key.PublicKey);
            publicKey.SetExpiresAt(std::chrono::duration_cast<std::chrono::milliseconds>(key.ExpiresAt.time_since_epoch()).count());
            // no private key here
        }
    }
    {
        auto& pbSids = *state.MutableSids();
        pbSids.Clear();
        for (const auto& [sidName, sidInfo] : Sids) {
            NLoginProto::TSid& sid = *pbSids.Add();
            sid.SetType(sidInfo.Type);
            sid.SetName(sidInfo.Name);
            for (const auto& subSid : sidInfo.Members) {
                sid.AddMembers(subSid);
            }
            // no user hash here
        }
    }
    return state;
}

void TLoginProvider::UpdateSecurityState(const NLoginProto::TSecurityState& state) {
    Audience = state.GetAudience();
    {
        auto now = std::chrono::system_clock::now();
        while (Keys.size() > MAX_CLIENT_KEYS || (!Keys.empty() && Keys.front().ExpiresAt <= now)) {
            Keys.pop_front();
        }

        if (!Keys.empty() && state.PublicKeysSize() != 0) {
            auto keyId = state.GetPublicKeys(0).GetKeyId();
            auto itKey = FindKeyIterator(keyId);
            Keys.erase(itKey, Keys.end()); // erase tail which we are going to reinsert later
        }

        for (const auto& pbPublicKey : state.GetPublicKeys()) {
            Keys.push_back({
                .KeyId = pbPublicKey.GetKeyId(),
                .PublicKey = pbPublicKey.GetKeyDataPEM(),
                .ExpiresAt = std::chrono::system_clock::time_point(std::chrono::milliseconds(pbPublicKey.GetExpiresAt())),
            });
        }
    }
    {
        Sids.clear();
        ChildToParentIndex.clear();
        for (const auto& pbSid : state.GetSids()) {
            TSidRecord& sid = Sids[pbSid.GetName()];
            sid.Type = pbSid.GetType();
            sid.Name = pbSid.GetName();
            sid.PasswordHash = pbSid.GetHash();
            sid.IsEnabled = pbSid.GetIsEnabled();
            for (const auto& pbSubSid : pbSid.GetMembers()) {
                sid.Members.emplace(pbSubSid);
                ChildToParentIndex[pbSubSid].emplace(sid.Name);
            }
            sid.CreatedAt = std::chrono::system_clock::time_point(std::chrono::microseconds(pbSid.GetCreatedAt()));
            sid.FailedLoginAttemptCount = pbSid.GetFailedLoginAttemptCount();
            sid.LastFailedLogin = std::chrono::system_clock::time_point(std::chrono::microseconds(pbSid.GetLastFailedLogin()));
            sid.LastSuccessfulLogin = std::chrono::system_clock::time_point(std::chrono::microseconds(pbSid.GetLastSuccessfulLogin()));
        }
    }
}

TString TLoginProvider::SanitizeJwtToken(const TString& token) {
    const size_t signaturePos = token.find_last_of('.');
    if (signaturePos == TString::npos || signaturePos == token.size() - 1) {
        return {};
    }
    return TStringBuilder() << TStringBuf(token).SubString(0, signaturePos) << ".**"; // <token>.**
}

void TLoginProvider::UpdatePasswordCheckParameters(const TPasswordComplexity& passwordComplexity) {
    PasswordChecker.Update(passwordComplexity);
}

void TLoginProvider::UpdateAccountLockout(const TAccountLockout::TInitializer& accountLockoutInitializer) {
    AccountLockout.Update(accountLockoutInitializer);
}

void TLoginProvider::UpdateCacheSettings(const TCacheSettings& settings) {
    Impl->UpdateCacheSettings(settings);
}

}
