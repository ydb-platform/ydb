#pragma once
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <deque>
#include <util/generic/string.h>
#include <ydb/library/login/protos/login.pb.h>
#include <ydb/library/login/password_checker/password_checker.h>
#include <ydb/library/login/password_checker/hash_checker.h>
#include <ydb/library/login/account_lockout/account_lockout.h>
#include <ydb/library/login/cache/lru.h>

namespace NLogin {

using NLoginProto::ESidType;

class TLoginProvider {
public:
    static constexpr size_t MAX_SERVER_KEYS = 1000;
    static constexpr size_t MAX_CLIENT_KEYS = 100000;
    static constexpr auto KEYS_ROTATION_PERIOD = std::chrono::hours(6);
    static constexpr auto KEY_EXPIRE_TIME = std::chrono::hours(24);

    static constexpr size_t SALT_SIZE = THashChecker::SALT_SIZE;
    static constexpr size_t HASH_SIZE = THashChecker::HASH_SIZE;

    static constexpr const char* GROUPS_CLAIM_NAME = "https://ydb.tech/groups";
    static constexpr const char* EXTERNAL_AUTH_CLAIM_NAME = "external_authentication";
    static constexpr auto MAX_TOKEN_EXPIRE_TIME = std::chrono::hours(12);

    static constexpr size_t SUCCESS_PASSWORDS_CACHE_CAPACITY = 20;
    static constexpr size_t WRONG_PASSWORDS_CACHE_CAPACITY = 20;

    struct TBasicRequest {};

    struct TBasicResponse {
        TString Error;
        TString Warning;
        TString Notice;
    };

    struct TCheckLockOutRequest : TBasicRequest {
        TString User;
    };

    struct TCheckLockOutResponse : TBasicResponse {
        enum class EStatus {
            UNSPECIFIED,
            SUCCESS,
            UNLOCKED,
            INVALID_USER,
            RESET,
        };

        EStatus Status = EStatus::UNSPECIFIED;
    };

    struct TLoginUserRequest : TBasicRequest {
        struct TOptions {
            bool WithUserGroups = false;
            std::chrono::system_clock::duration ExpiresAfter = std::chrono::system_clock::duration::zero();
        };

        TString User;
        TString Password;
        TOptions Options;
        TString ExternalAuth;
    };

    struct TPasswordCheckResult : TBasicResponse {
    public:
        enum class EStatus {
            UNSPECIFIED,
            SUCCESS,
            INVALID_USER,
            INVALID_PASSWORD,
            UNAVAILABLE_KEY
        };

        EStatus Status = EStatus::UNSPECIFIED;

    public:
        void FillInvalidPassword() {
            Status = TLoginUserResponse::EStatus::INVALID_PASSWORD;
            Error = "Invalid password";
        }

        void FillUnavailableKey() {
            Status = TLoginUserResponse::EStatus::UNAVAILABLE_KEY;
            Error = "No key to generate token";
        }

        void FillInvalidUser(const TString& error) {
            Status = TLoginUserResponse::EStatus::INVALID_USER;
            Error = error;
        }
    };

    struct TLoginUserResponse : TPasswordCheckResult {
        TString Token;
        TString SanitizedToken; // Token for audit logs
    };

    struct TValidateTokenRequest : TBasicRequest {
        TString Token;
    };

    struct TValidateTokenResponse : TBasicResponse {
        bool TokenUnrecognized = false;
        bool ErrorRetryable = false;
        bool WrongAudience = false;
        TString User;
        std::optional<std::vector<TString>> Groups;
        std::chrono::system_clock::time_point ExpiresAt;
        TString ExternalAuth;
    };

    struct TCreateUserRequest : TBasicRequest {
        TString User;
        TString Password;
        bool IsHashedPassword = false;
        bool CanLogin = true;
    };

    struct TModifyUserRequest : TBasicRequest {
        TString User;
        std::optional<TString> Password;
        bool IsHashedPassword = false;
        std::optional<bool> CanLogin;
    };

    struct TRemoveUserResponse : TBasicResponse {
        std::vector<TString> TouchedGroups;
    };

    struct TCreateGroupRequest : TBasicRequest {
        struct TOptions {
            bool StrongCheckName = true;
        };

        TString Group;
        TOptions Options;
    };

    struct TAddGroupMembershipRequest : TBasicRequest {
        TString Group;
        TString Member;
    };

    struct TRemoveGroupMembershipRequest : TBasicRequest {
        TString Group;
        TString Member;
    };

    struct TRenameGroupRequest : TBasicRequest {
        struct TOptions {
            bool StrongCheckName = true;
        };

        TString Group;
        TString NewName;
        TOptions Options;
    };

    struct TRenameGroupResponse : TBasicResponse {
        std::vector<TString> TouchedGroups;
    };

    struct TRemoveGroupResponse : TBasicResponse {
        std::vector<TString> TouchedGroups;
    };

    struct TKeyRecord {
        ui64 KeyId;
        TString PublicKey;
        TString PrivateKey;
        std::chrono::system_clock::time_point ExpiresAt;
    };

    std::deque<TKeyRecord> Keys; // it's always ordered by KeyId
    std::chrono::time_point<std::chrono::system_clock> KeysRotationTime;

    struct TSidRecord {
        ESidType::SidType Type = ESidType::UNKNOWN;
        TString Name;
        TString PasswordHash;
        bool IsEnabled;
        std::unordered_set<TString> Members;
        std::chrono::system_clock::time_point CreatedAt;
        ui32 FailedLoginAttemptCount = 0;
        std::chrono::system_clock::time_point LastFailedLogin;
        std::chrono::system_clock::time_point LastSuccessfulLogin;
    };

    struct TCacheSettings {
        size_t SuccessPasswordsCacheCapacity = SUCCESS_PASSWORDS_CACHE_CAPACITY;
        size_t WrongPasswordsCacheCapacity = WRONG_PASSWORDS_CACHE_CAPACITY;
    };

    // our current audience (database name)
    TString Audience;

    // all users and groups
    std::unordered_map<TString, TSidRecord> Sids;

    // index for fast traversal
    std::unordered_map<TString, std::unordered_set<TString>> ChildToParentIndex;

    // servers duty to call this method periodically (and publish PublicKeys after that)
    const TKeyRecord* FindKey(ui64 keyId);
    void RotateKeys();
    void RotateKeys(std::vector<ui64>& keysExpired, std::vector<ui64>& keysAdded);
    bool IsItTimeToRotateKeys() const;
    NLoginProto::TSecurityState GetSecurityState() const;
    void UpdateSecurityState(const NLoginProto::TSecurityState& state);

    bool IsLockedOut(const TSidRecord& user) const;
    TCheckLockOutResponse CheckLockOutUser(const TCheckLockOutRequest& request);

    // Login
    TLoginUserResponse LoginUser(const TLoginUserRequest& request);
    // The next four methods are used (all together combined) when it's needed to separate hash verification which is quite cpu-intensive
    bool NeedVerifyHash(const TLoginUserRequest& request, TPasswordCheckResult* checkResult, TString* passwordHash);
    static bool VerifyHash(const TLoginUserRequest& request, const TString& passwordHash); // it's made static to be thread-safe
    void UpdateCache(const TLoginUserRequest& request, const TString& passwordHash, const bool isSuccessVerifying);
    TLoginProvider::TLoginUserResponse LoginUser(const TLoginUserRequest& request, const TPasswordCheckResult& checkResult);

    TValidateTokenResponse ValidateToken(const TValidateTokenRequest& request);

    TBasicResponse CreateUser(const TCreateUserRequest& request);
    TBasicResponse ModifyUser(const TModifyUserRequest& request);
    TRemoveUserResponse RemoveUser(const TString& user);
    bool CheckUserExists(const TString& user);

    TBasicResponse CreateGroup(const TCreateGroupRequest& request);
    TBasicResponse AddGroupMembership(const TAddGroupMembershipRequest& request);
    TBasicResponse RemoveGroupMembership(const TRemoveGroupMembershipRequest& request);
    TRenameGroupResponse RenameGroup(const TRenameGroupRequest& request);
    TRemoveGroupResponse RemoveGroup(const TString& group);
    bool CheckGroupExists(const TString& group);

    void UpdatePasswordCheckParameters(const TPasswordComplexity& passwordComplexity);
    void UpdateAccountLockout(const TAccountLockout::TInitializer& accountLockoutInitializer);
    void UpdateCacheSettings(const TCacheSettings& settings);

    TLoginProvider();
    TLoginProvider(const TAccountLockout::TInitializer& accountLockoutInitializer);
    TLoginProvider(const TPasswordComplexity& passwordComplexity,
        const TAccountLockout::TInitializer& accountLockoutInitializer,
        const std::function<bool()>& isCacheUsed,
        const TCacheSettings& cacheSettings);
    ~TLoginProvider();

    std::vector<TString> GetGroupsMembership(const TString& member) const;
    static TString GetTokenAudience(const TString& token);
    static std::chrono::system_clock::time_point GetTokenExpiresAt(const TString& token);
    static TString SanitizeJwtToken(const TString& token);

private:
    std::deque<TKeyRecord>::iterator FindKeyIterator(ui64 keyId);
    bool CheckSubjectExists(const TString& name, const ESidType::SidType& type);
    static bool StrongCheckAllowedName(const TString& name);
    static bool BasicCheckAllowedName(const TString& name);
    static bool CheckGroupNameAllowed(const bool strongCheckName, const TString& groupName);

    bool CheckLockoutByAttemptCount(const TSidRecord& sid) const;
    bool CheckLockout(const TSidRecord& sid) const;
    static void ResetFailedLoginAttemptCount(TSidRecord* sid);
    static void UnlockAccount(TSidRecord* sid);
    bool ShouldResetFailedAttemptCount(const TSidRecord& sid) const;
    bool ShouldUnlockAccount(const TSidRecord& sid) const;
    bool CheckPasswordOrHash(bool IsHashedPassword, const TString& user, const TString& password, TString& error) const;
    TSidRecord* GetUserSid(const TString& user);
    bool FillUnavailableKey(TPasswordCheckResult* checkResult) const;
    bool FillInvalidUser(const TSidRecord* sid, TPasswordCheckResult* checkResult) const;

private:
    struct TImpl;
    THolder<TImpl> Impl;

    TPasswordChecker PasswordChecker;
    THashChecker HashChecker;
    TAccountLockout AccountLockout;
};

}
