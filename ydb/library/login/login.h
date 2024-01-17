#pragma once
#include <optional>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <deque>
#include <util/generic/string.h>
#include <ydb/library/login/protos/login.pb.h>

namespace NLogin {

using NLoginProto::ESidType;

class TLoginProvider {
public:
    static constexpr size_t MAX_SERVER_KEYS = 1000;
    static constexpr size_t MAX_CLIENT_KEYS = 100000;
    static constexpr auto KEYS_ROTATION_PERIOD = std::chrono::hours(6);
    static constexpr auto KEY_EXPIRE_TIME = std::chrono::hours(24);

    static constexpr size_t SALT_SIZE = 16;
    static constexpr size_t HASH_SIZE = 32;

    static constexpr const char* GROUPS_CLAIM_NAME = "https://ydb.tech/groups";
    static constexpr const char* EXTERNAL_AUTH_CLAIM_NAME = "external_authentication";
    static constexpr auto MAX_TOKEN_EXPIRE_TIME = std::chrono::hours(12);

    struct TBasicRequest {};

    struct TBasicResponse {
        TString Error;
        TString Warning;
        TString Notice;
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

    struct TLoginUserResponse : TBasicResponse {
        TString Token;
    };

    struct TValidateTokenRequest : TBasicRequest {
        TString Token;
    };

    struct TValidateTokenResponse : TBasicResponse {
        bool TokenUnrecognized = false;
        bool ErrorRetryable = false;
        TString User;
        std::optional<std::vector<TString>> Groups;
        std::chrono::system_clock::time_point ExpiresAt;
        TString ExternalAuth;
    };

    struct TCreateUserRequest : TBasicRequest {
        TString User;
        TString Password;
    };

    struct TModifyUserRequest : TBasicRequest {
        TString User;
        TString Password;
    };

    struct TRemoveUserRequest : TBasicRequest {
        TString User;
        bool MissingOk;
    };

    struct TRemoveUserResponse : TBasicResponse {
        std::vector<TString> TouchedGroups;
    };

    struct TCreateGroupRequest : TBasicRequest {
        struct TOptions {
            bool CheckName = true;
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
            bool CheckName = true;
        };

        TString Group;
        TString NewName;
        TOptions Options;
    };

    struct TRenameGroupResponse : TBasicResponse {
        std::vector<TString> TouchedGroups;
    };

    struct TRemoveGroupRequest : TBasicRequest {
        TString Group;
        bool MissingOk;
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
        TString Hash;
        std::unordered_set<TString> Members;
    };

    // our current audience (database name)
    TString Audience;

    // all users and theirs hashs
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

    TLoginUserResponse LoginUser(const TLoginUserRequest& request);
    TValidateTokenResponse ValidateToken(const TValidateTokenRequest& request);

    TBasicResponse CreateUser(const TCreateUserRequest& request);
    TBasicResponse ModifyUser(const TModifyUserRequest& request);
    TRemoveUserResponse RemoveUser(const TRemoveUserRequest& request);
    bool CheckUserExists(const TString& name);

    TBasicResponse CreateGroup(const TCreateGroupRequest& request);
    TBasicResponse AddGroupMembership(const TAddGroupMembershipRequest& request);
    TBasicResponse RemoveGroupMembership(const TRemoveGroupMembershipRequest& request);
    TRenameGroupResponse RenameGroup(const TRenameGroupRequest& request);
    TRemoveGroupResponse RemoveGroup(const TRemoveGroupRequest& request);

    TLoginProvider();
    ~TLoginProvider();

    std::vector<TString> GetGroupsMembership(const TString& member);
    static TString GetTokenAudience(const TString& token);
    static std::chrono::system_clock::time_point GetTokenExpiresAt(const TString& token);

private:
    std::deque<TKeyRecord>::iterator FindKeyIterator(ui64 keyId);
    bool CheckSubjectExists(const TString& name, const ESidType::SidType& type);
    static bool CheckAllowedName(const TString& name);

    struct TImpl;
    THolder<TImpl> Impl;
};

}
