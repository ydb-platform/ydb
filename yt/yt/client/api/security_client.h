#pragma once

#include "client_common.h"

#include <yt/yt/client/security_client/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TAddMemberOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TRemoveMemberOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TCheckPermissionOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
    , public TPrerequisiteOptions
{
    std::optional<std::vector<std::string>> Columns;
    std::optional<bool> Vital;
};

struct TCheckPermissionResult
{
    TError ToError(
        const std::string& user,
        NYTree::EPermission permission,
        const std::optional<std::string>& columns = {}) const;

    NSecurityClient::ESecurityAction Action;
    NObjectClient::TObjectId ObjectId;
    std::optional<TString> ObjectName;
    NSecurityClient::TSubjectId SubjectId;
    std::optional<std::string> SubjectName;
};

struct TCheckPermissionResponse
    : public TCheckPermissionResult
{
    std::optional<std::vector<TCheckPermissionResult>> Columns;
};

struct TCheckPermissionByAclOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TPrerequisiteOptions
{
    bool IgnoreMissingSubjects = false;
};

struct TCheckPermissionByAclResult
{
    TError ToError(const std::string& user, NYTree::EPermission permission) const;

    NSecurityClient::ESecurityAction Action;
    NSecurityClient::TSubjectId SubjectId;
    std::optional<std::string> SubjectName;
    std::vector<std::string> MissingSubjects;
};

struct TSetUserPasswordOptions
    : public TTimeoutOptions
{
    bool PasswordIsTemporary = false;
};

struct TIssueTokenOptions
    : public TTimeoutOptions
{
    TString Description;
};

struct TIssueTemporaryTokenOptions
    : public TIssueTokenOptions
{
    TDuration ExpirationTimeout;
};

struct TIssueTokenResult
{
    std::string Token;
    //! Cypress node corresponding to issued token.
    //! Deleting this node will revoke the token.
    NCypressClient::TNodeId NodeId;
};

struct TRefreshTemporaryTokenOptions
    : public TTimeoutOptions
{ };

struct TRevokeTokenOptions
    : public TTimeoutOptions
{ };

struct TListUserTokensOptions
    : public TTimeoutOptions
{
    bool WithMetadata;
};

struct TListUserTokensResult
{
    // Tokens are SHA256-encoded.
    std::vector<TString> Tokens;
    THashMap<TString, NYson::TYsonString> Metadata;
};

struct TGetCurrentUserOptions
    : public TTimeoutOptions
{ };

struct TGetCurrentUserResult
    : public NYTree::TYsonStruct
{
    std::string User;

    REGISTER_YSON_STRUCT(TGetCurrentUserResult);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGetCurrentUserResult);

////////////////////////////////////////////////////////////////////////////////

struct ISecurityClient
{
    virtual ~ISecurityClient() = default;

    //! Return information about current user.
    virtual TFuture<TGetCurrentUserResultPtr> GetCurrentUser(
        const TGetCurrentUserOptions& options = {}) = 0;

    virtual TFuture<void> AddMember(
        const std::string& group,
        const std::string& member,
        const TAddMemberOptions& options = {}) = 0;

    virtual TFuture<void> RemoveMember(
        const std::string& group,
        const std::string& member,
        const TRemoveMemberOptions& options = {}) = 0;

    virtual TFuture<TCheckPermissionResponse> CheckPermission(
        const std::string& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = {}) = 0;

    virtual TFuture<TCheckPermissionByAclResult> CheckPermissionByAcl(
        const std::optional<std::string>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const TCheckPermissionByAclOptions& options = {}) = 0;

    // Methods below correspond to simple authentication scheme
    // and are intended to be used on clusters without third-party tokens (e.g. Yandex blackbox).
    virtual TFuture<void> SetUserPassword(
        const std::string& user,
        const TString& currentPasswordSha256,
        const TString& newPasswordSha256,
        const TSetUserPasswordOptions& options) = 0;

    virtual TFuture<TIssueTokenResult> IssueToken(
        const std::string& user,
        const TString& passwordSha256,
        const TIssueTokenOptions& options) = 0;

    virtual TFuture<void> RevokeToken(
        const std::string& user,
        const TString& passwordSha256,
        const TString& tokenSha256,
        const TRevokeTokenOptions& options) = 0;

    virtual TFuture<TListUserTokensResult> ListUserTokens(
        const std::string& user,
        const TString& passwordSha256,
        const TListUserTokensOptions& options) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
