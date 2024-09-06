#pragma once
#include "ticket_parser_log.h"
#include "ticket_parser_settings.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/security/certificate_check/cert_check.h>
#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/grpc/actor_client/grpc_service_cache.h>
#include <ydb/library/ncloud/impl/access_service.h>
#include <ydb/library/security/util.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/api/service_account_service.h>
#include <ydb/library/ycloud/api/user_account_service.h>
#include <ydb/library/ycloud/impl/access_service.h>
#include <ydb/library/ycloud/impl/service_account_service.h>
#include <ydb/library/ycloud/impl/user_account_service.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/generic/queue.h>
#include <util/stream/file.h>
#include <util/string/vector.h>

namespace NKikimr {

inline bool IsRetryableGrpcError(const NYdbGrpc::TGrpcStatus& status) {
    switch (status.GRpcStatusCode) {
    case grpc::StatusCode::UNAUTHENTICATED:
    case grpc::StatusCode::PERMISSION_DENIED:
    case grpc::StatusCode::INVALID_ARGUMENT:
    case grpc::StatusCode::NOT_FOUND:
        return false;
    }
    return true;
}


template <typename TDerived>
class TTicketParserImpl : public TActorBootstrapped<TDerived> {
private:
    using TThis = TTicketParserImpl;
    using TBase = TActorBootstrapped<TDerived>;

    struct TExternalAuthInfo {
        TString Type;
        TString Login;
    };

    struct TPermissionRecord {
        enum class TTypeCase {
            TYPE_NOT_SET,
            USER_ACCOUNT_TYPE,
            SERVICE_ACCOUNT_TYPE,
            ANONYMOUS_ACCOUNT_TYPE,
        };

        TString Subject;
        bool Required = false;
        TTypeCase SubjectType = TTypeCase::TYPE_NOT_SET;
        TEvTicketParser::TError Error;
        TStackVec<std::pair<TString, TString>> Attributes;

        bool IsPermissionReady() const {
            return !Subject.empty() || !Error.empty();
        }

        bool IsPermissionOk() const {
            return !Subject.empty();
        }

        bool IsRequired() const {
            return Required;
        }
    };

    template <typename TRequestType>
    struct TEvRequestWithKey : TRequestType {
        TString Key;

        TEvRequestWithKey(const TString& key)
            : Key(key)
        {}
    };

    using TEvAccessServiceAuthenticateRequest = TEvRequestWithKey<NCloud::TEvAccessService::TEvAuthenticateRequest>;
    using TEvAccessServiceAuthorizeRequest = TEvRequestWithKey<NCloud::TEvAccessService::TEvAuthorizeRequest>;
    using TEvAccessServiceBulkAuthorizeRequest = TEvRequestWithKey<NCloud::TEvAccessService::TEvBulkAuthorizeRequest>;
    using TEvAccessServiceGetUserAccountRequest = TEvRequestWithKey<NCloud::TEvUserAccountService::TEvGetUserAccountRequest>;
    using TEvAccessServiceGetServiceAccountRequest = TEvRequestWithKey<NCloud::TEvServiceAccountService::TEvGetServiceAccountRequest>;
    using TEvNebiusAccessServiceAuthorizeRequest = TEvRequestWithKey<NNebiusCloud::TEvAccessService::TEvAuthorizeRequest>;
    using TEvNebiusAccessServiceAuthenticateRequest = TEvRequestWithKey<NNebiusCloud::TEvAccessService::TEvAuthenticateRequest>;

    struct TTokenRefreshRecord {
        TString Key;
        TInstant RefreshTime;

        bool operator <(const TTokenRefreshRecord& o) const {
            return RefreshTime > o.RefreshTime;
        }
    };

protected:
    class TTokenRecordBase {
    private:
        TIntrusiveConstPtr<NACLib::TUserToken> Token;
    public:
        TTokenRecordBase(const TTokenRecordBase&) = delete;
        TTokenRecordBase& operator =(const TTokenRecordBase&) = delete;

        static constexpr const char* UnknownAuthType = "Unknown";
        static constexpr const char* UnsupportedAuthType = "Unsupported";
        static constexpr const char* BuiltinAuthType = "Builtin";
        static constexpr const char* LoginAuthType = "Login";
        static constexpr const char* AccessServiceAuthType = "AccessService";
        static constexpr const char* ApiKeyAuthType = "ApiKey";
        static constexpr const char* CertificateAuthType = "Certificate";

        TString Ticket;
        typename TDerived::ETokenType TokenType = TDerived::ETokenType::Unknown;
        NKikimr::TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature Signature;
        THashMap<TString, TPermissionRecord> Permissions;
        TString Subject; // login
        typename TPermissionRecord::TTypeCase SubjectType = TPermissionRecord::TTypeCase::TYPE_NOT_SET;
        TEvTicketParser::TError Error;
        TDeque<THolder<TEventHandle<TEvTicketParser::TEvAuthorizeTicket>>> AuthorizeRequests;
        ui64 ResponsesLeft = 0;
        TInstant InitTime;
        TInstant RefreshTime;
        TInstant ExpireTime;
        TInstant AccessTime;
        TDuration CurrentDelay = TDuration::Seconds(1);
        TString PeerName;
        TString Database;
        TStackVec<TString> AdditionalSIDs;
        bool RefreshRetryableErrorImmediately = false;
        TExternalAuthInfo ExternalAuthInfo;

        TTokenRecordBase(const TStringBuf ticket)
            : Ticket(ticket)
        {}

        void SetToken(const TIntrusivePtr<NACLib::TUserToken>& token) {
            // saving serialization info into the token instance.
            token->SaveSerializationInfo();
            Token = token;
        }

        const TIntrusiveConstPtr<NACLib::TUserToken> GetToken() const {
            return Token;
        }

        void UnsetToken() {
            Token = nullptr;
        }

        TString GetAttributeValue(const TString& permission, const TString& key) const {
            if (auto it = Permissions.find(permission); it != Permissions.end()) {
                for (const auto& pr : it->second.Attributes) {
                    if (pr.first == key) {
                        return pr.second;
                    }
                }
            }
            return TString();
        }

        template <typename T>
        void SetErrorRefreshTime(TTicketParserImpl<T>* ticketParser, TInstant now) {
            if (Error.Retryable) {
                SetRefreshTime(now, CurrentDelay);
                if (CurrentDelay < ticketParser->MaxErrorRefreshTime - ticketParser->MinErrorRefreshTime) {
                    static const double scaleFactor = 2.0;
                    CurrentDelay = Min(CurrentDelay * scaleFactor, ticketParser->MaxErrorRefreshTime - ticketParser->MinErrorRefreshTime);
                }
            } else {
                SetRefreshTime(now, ticketParser->RefreshTime - ticketParser->RefreshTime / 2);
            }
        }

        template <typename T>
        void SetOkRefreshTime(TTicketParserImpl<T>* ticketParser, TInstant now) {
            SetRefreshTime(now, ticketParser->RefreshTime - ticketParser->RefreshTime / 2);
        }

        void SetRefreshTime(TInstant now, TDuration delay) {
            const TDuration::TValue half = delay.GetValue() / 2;
            RefreshTime = now + TDuration::FromValue(half + RandomNumber<TDuration::TValue>(half));
        }

        TString GetSubject() const {
            return Subject;
        }

        TString GetAuthType() const {
            switch (TokenType) {
                case TDerived::ETokenType::Unknown:
                    return UnknownAuthType;
                case TDerived::ETokenType::Unsupported:
                    return UnsupportedAuthType;
                case TDerived::ETokenType::Builtin:
                    return BuiltinAuthType;
                case TDerived::ETokenType::Login:
                    return LoginAuthType;
                case TDerived::ETokenType::AccessService:
                    return AccessServiceAuthType;
                case TDerived::ETokenType::ApiKey:
                    return ApiKeyAuthType;
                case TDerived::ETokenType::Certificate:
                    return CertificateAuthType;
            }
        }

        bool NeedsRefresh() const {
            switch (TokenType) {
                case TDerived::ETokenType::Builtin:
                case TDerived::ETokenType::Certificate:
                    return false;
                case TDerived::ETokenType::Login:
                    return true;
                default:
                    return Signature.AccessKeyId.empty();
            }
        }

        bool IsTokenReady() const {
            return Token != nullptr;
        }

        bool IsExternalAuthEnabled() const {
            return !ExternalAuthInfo.Type.empty();
        }

        void EnableExternalAuth(const NLogin::TLoginProvider::TValidateTokenResponse& response) {
            ExternalAuthInfo.Login = response.User;
            ExternalAuthInfo.Type = response.ExternalAuth;
        }

        TString GetMaskedTicket() const {
            if (Signature.AccessKeyId) {
                return MaskTicket(Signature.AccessKeyId);
            }
            if (TokenType == TDerived::ETokenType::Certificate) {
                return GetCertificateFingerprint(Ticket);
            }
            return MaskTicket(Ticket);
        }
    };

protected:
    using IActorOps::Register;
    using IActorOps::Send;
    using IActorOps::Schedule;

    NKikimrProto::TAuthConfig Config;
    const TCertificateChecker CertificateChecker;
    TDuration ExpireTime = TDuration::Hours(24); // after what time ticket will expired and removed from cache

    template <typename TTokenRecord>
    TInstant GetExpireTime(const TTokenRecord& record, TInstant now) const {
        if ((record.TokenType == TDerived::ETokenType::AccessService || record.TokenType == TDerived::ETokenType::ApiKey) && record.Signature.AccessKeyId) {
            return GetAsSignatureExpireTime(now);
        }
        if (record.TokenType == TDerived::ETokenType::Login) {
            return record.ExpireTime;
        }
        return now + ExpireTime;
    }

    bool AccessServiceEnabled() const {
        return (AccessServiceValidatorV1 && AccessServiceValidatorV2) || NebiusAccessServiceValidator;
    }

    bool ApiKeyEnabled() const {
        return AccessServiceValidatorV1 && AccessServiceValidatorV2 && Config.GetUseAccessServiceApiKey();
    }

    bool IsAccessKeySignatureSupported() const {
        return AccessServiceValidatorV1 && AccessServiceValidatorV2; // Signature is supported by Yandex AccessService and is not supported by Nebius AccessService
    }

private:
    TString DomainName;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsReceived;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsSuccess;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsErrors;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsErrorsRetryable;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsErrorsPermanent;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsBuiltin;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsCertificate;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsLogin;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsAS;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsCacheHit;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsCacheMiss;
    ::NMonitoring::THistogramPtr CounterTicketsBuildTime;

    TDuration RefreshPeriod = TDuration::Seconds(1); // how often do we check for ticket freshness/expiration
    TDuration RefreshTime = TDuration::Hours(1); // within this time we will try to refresh valid ticket
    TDuration MinErrorRefreshTime = TDuration::Seconds(1); // between this and next time we will try to refresh retryable error
    TDuration MaxErrorRefreshTime = TDuration::Minutes(1);
    TDuration LifeTime = TDuration::Hours(1); // for how long ticket will remain in the cache after last access
    TDuration AsSignatureExpireTime = TDuration::Minutes(1);

    TActorId AccessServiceValidatorV1;
    TActorId AccessServiceValidatorV2;
    TActorId UserAccountService;
    TActorId ServiceAccountService;
    TActorId NebiusAccessServiceValidator;
    TString UserAccountDomain;
    TString AccessServiceDomain;
    TString ServiceDomain;

    TPriorityQueue<TTokenRefreshRecord> RefreshQueue;
    std::unordered_map<TString, NLogin::TLoginProvider> LoginProviders;
    bool UseLoginProvider = false;

    TDerived* GetDerived() {
        return static_cast<TDerived*>(this);
    }

    static TString GetKey(TEvTicketParser::TEvAuthorizeTicket* request) {
        TStringStream key;
        if (request->Signature.AccessKeyId) {
            const auto& sign = request->Signature;
            key << sign.AccessKeyId << "-" << sign.Signature << ":" << sign.StringToSign << ":"
                << sign.Service << ":" << sign.Region << ":" << sign.SignedAt.NanoSeconds();
        } else {
            key << request->Ticket;
        }
        key << ':';
        if (request->Database) {
            key << request->Database;
            key << ':';
        }
        for (const auto& entry : request->Entries) {
            for (auto it = entry.Attributes.begin(); it != entry.Attributes.end(); ++it) {
                if (it != entry.Attributes.begin()) {
                    key << '-';
                }
                key << it->second;
            }
            key << ':';
            for (auto it = entry.Permissions.begin(); it != entry.Permissions.end(); ++it) {
                if (it != entry.Permissions.begin()) {
                    key << '-';
                }
                key << it->Permission << "(" << it->Required << ")";
            }
        }
        return key.Str();
    }

    TInstant GetAsSignatureExpireTime(TInstant now) const {
        return now + AsSignatureExpireTime;
    }

    TInstant GetRefreshTime(TInstant now) const {
        return now + RefreshTime;
    }

    TDuration GetLifeTime() const {
        return LifeTime;
    }

    template <typename TRequest, typename TTokenRecord>
    static THolder<TRequest> CreateAccessServiceRequest(const TString& key, const TTokenRecord& record) {
        auto request = MakeHolder<TRequest>(key);

        if (record.Signature.AccessKeyId) {
            const auto& sign = record.Signature;
            auto& signature = *request->Request.mutable_signature();
            signature.set_access_key_id(sign.AccessKeyId);
            signature.set_string_to_sign(sign.StringToSign);
            signature.set_signature(sign.Signature);

            auto& v4params = *signature.mutable_v4_parameters();
            v4params.set_service(sign.Service);
            v4params.set_region(sign.Region);

            v4params.mutable_signed_at()->set_seconds(sign.SignedAt.Seconds());
            v4params.mutable_signed_at()->set_nanos(sign.SignedAt.NanoSeconds() % 1000000000ull);
        } else {
            if (record.TokenType == TDerived::ETokenType::ApiKey) {
                // we use the ApiKey only if this type is explicitly specified in the token
                request->Request.set_api_key(record.Ticket);
            } else {
                request->Request.set_iam_token(record.Ticket);
            }
        }

        return request;
    }

    template <typename TPathsContainerPtr>
    static void AddResourcePath(TPathsContainerPtr pathsContainer, const TString& id, const TString& type) {
        auto resourcePath = pathsContainer->add_resource_path();
        resourcePath->set_id(id);
        resourcePath->set_type(type);
    }

    template <typename TTokenRecord, typename TPathsContainerPtr>
    void AddResourcePaths(const TTokenRecord& record, const TString& permission, TPathsContainerPtr pathsContainer) const {
        if (const auto databaseId = record.GetAttributeValue(permission, "database_id"); databaseId) {
            AddResourcePath(pathsContainer, databaseId, "ydb.database");
        } else if (const auto serviceAccountId = record.GetAttributeValue(permission, "service_account_id"); serviceAccountId) {
            AddResourcePath(pathsContainer, serviceAccountId, "iam.serviceAccount");
        }

        if (const auto folderId = record.GetAttributeValue(permission, "folder_id"); folderId) {
            AddResourcePath(pathsContainer, folderId, "resource-manager.folder");
        }

        if (const auto cloudId = record.GetAttributeValue(permission, "cloud_id"); cloudId) {
            AddResourcePath(pathsContainer, cloudId, "resource-manager.cloud");
        }

        if (const TString gizmoId = record.GetAttributeValue(permission, "gizmo_id"); gizmoId) {
            AddResourcePath(pathsContainer, gizmoId, "iam.gizmo");
        }
    }

    static void AddNebiusResourcePath(nebius::iam::v1::AuthorizeCheck* pathsContainer, const TString& id) {
        pathsContainer->mutable_resource_path()->add_path()->set_id(id);
    }

    static void AddNebiusContainerId(nebius::iam::v1::AuthorizeCheck* pathsContainer, const TString& id) {
        pathsContainer->set_container_id(id);
    }

    template <typename TTokenRecord>
    void AddNebiusResourcePaths(const TTokenRecord& record, const TString& permission, nebius::iam::v1::AuthorizeCheck* pathsContainer) const {
        // Use attribute "database_id" as our resource id
        // IAM can link roles for resource
        if (const auto databaseId = record.GetAttributeValue(permission, "database_id"); databaseId) {
            AddNebiusResourcePath(pathsContainer, databaseId);
        }

        // Use attribute "folder_id" as container id that contains our database
        // IAM can link roles for containers hierarchy
        if (const auto folderId = record.GetAttributeValue(permission, "folder_id"); folderId) {
            AddNebiusContainerId(pathsContainer, folderId);
        }
    }

    template <typename TTokenRecord>
    void AccessServiceAuthorize(const TString& key, TTokenRecord& record) const {
        for (const auto& [permissionName, permissionRecord] : record.Permissions) {
            BLOG_TRACE("Ticket " << record.GetMaskedTicket() << " asking for AccessServiceAuthorization(" << permissionName << ")");

            auto request = CreateAccessServiceRequest<TEvAccessServiceAuthorizeRequest>(key, record);
            request->Request.set_permission(permissionName);
            AddResourcePaths(record, permissionName, &request->Request);
            record.ResponsesLeft++;
            Send(AccessServiceValidatorV1, request.Release());
        }
    }

    template <typename TTokenRecord>
    void AccessServiceBulkAuthorize(const TString& key, TTokenRecord& record) const {
        auto request = CreateAccessServiceRequest<TEvAccessServiceBulkAuthorizeRequest>(key, record);
        TStringBuilder requestForPermissions;
        for (const auto& [permissionName, permissionRecord] : record.Permissions) {
            auto action = request->Request.mutable_actions()->add_items();
            AddResourcePaths(record, permissionName, action);
            action->set_permission(permissionName);
            requestForPermissions << " " << permissionName;
        }
        request->Request.set_result_filter(yandex::cloud::priv::accessservice::v2::BulkAuthorizeRequest::ALL_FAILED);
        BLOG_TRACE("Ticket " << record.GetMaskedTicket() << " asking for AccessServiceBulkAuthorization(" << requestForPermissions << ")");
        record.ResponsesLeft++;
        Send(AccessServiceValidatorV2, request.Release());
    }

    template <typename TTokenRecord>
    void NebiusAccessServiceAuthorize(const TString& key, TTokenRecord& record) const {
        auto request = MakeHolder<TEvNebiusAccessServiceAuthorizeRequest>(key);
        TStringBuilder requestForPermissions;
        i64 i = 0;
        for (const auto& [permissionName, permissionRecord] : record.Permissions) {
            auto& check = (*request->Request.mutable_checks())[i];
            check.set_iam_token(record.Ticket);
            check.mutable_permission()->set_name(permissionName);
            AddNebiusResourcePaths(record, permissionName, &check);
            requestForPermissions << " " << permissionName;
            ++i;
        }
        BLOG_TRACE("Ticket " << record.GetMaskedTicket() << " asking for AccessServiceAuthorization(" << requestForPermissions << ")");
        record.ResponsesLeft++;
        Send(NebiusAccessServiceValidator, request.Release());
    }

    template <typename TTokenRecord>
    void RequestAccessServiceAuthorization(const TString& key, TTokenRecord& record) const {
        if (AppData()->FeatureFlags.GetEnableAccessServiceBulkAuthorization()) {
            AccessServiceBulkAuthorize(key, record);
        } else if (NebiusAccessServiceValidator) {
            NebiusAccessServiceAuthorize(key, record);
        } else {
            AccessServiceAuthorize(key, record);
        }
    }

    template <typename TTokenRecord>
    void AccessServiceAuthenticate(const TString& key, TTokenRecord& record) const {
        auto request = CreateAccessServiceRequest<TEvAccessServiceAuthenticateRequest>(key, record);
        Send(AccessServiceValidatorV1, request.Release());
    }

    template <typename TTokenRecord>
    void NebiusAccessServiceAuthenticate(const TString& key, TTokenRecord& record) const {
        auto request = MakeHolder<TEvNebiusAccessServiceAuthenticateRequest>(key);
        request->Request.set_iam_token(record.Ticket);
        Send(NebiusAccessServiceValidator, request.Release());
    }

    template <typename TTokenRecord>
    void RequestAccessServiceAuthentication(const TString& key, TTokenRecord& record) const {
        BLOG_TRACE("Ticket " << record.GetMaskedTicket() << " asking for AccessServiceAuthentication");
        record.ResponsesLeft++;

        if (NebiusAccessServiceValidator) {
            NebiusAccessServiceAuthenticate(key, record);
        } else {
            AccessServiceAuthenticate(key, record);
        }
    }

    template <typename TSubject> // Yandex IAM v1/v2
    bool GetSubjectId(const TSubject& subjectProto, TString& subjectId) {
        switch (subjectProto.type_case()) {
        case TSubject::TypeCase::kUserAccount:
            subjectId = subjectProto.user_account().id();
            return true;
        case TSubject::TypeCase::kServiceAccount:
            subjectId = subjectProto.service_account().id();
            return true;
        case TSubject::TypeCase::kAnonymousAccount:
            subjectId = "anonymous";
            return true;
        default:
            return false;
        }
    }

    bool GetSubjectId(const nebius::iam::v1::Account& accountProto, TString& subjectId) {
        using Account = nebius::iam::v1::Account;
        switch (accountProto.type_case()) {
        case Account::TypeCase::kUserAccount:
            subjectId = accountProto.user_account().id();
            return true;
        case Account::TypeCase::kServiceAccount:
            subjectId = accountProto.service_account().id();
            return true;
        default:
            return false;
        }
    }

    template <typename TProto>
    bool ApplySubjectName(const TProto& subjectProto, TString& subject, TString& error) {
        subject.clear();
        if (!GetSubjectId(subjectProto, subject)) {
            error = "Unknown subject type";
            return false;
        }

        if (subject.empty()) {
            error = "Empty subject id";
            return false;
        }

        subject += '@';
        subject += AccessServiceDomain;
        return true;
    }

    bool ApplySubjectName(const yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse& response, TString& subject, TString& error) {
        return ApplySubjectName(response.subject(), subject, error);
    }

    bool ApplySubjectName(const yandex::cloud::priv::accessservice::v2::BulkAuthorizeResponse& response, TString& subject, TString& error) {
        return ApplySubjectName(response.subject(), subject, error);
    }

    bool ApplySubjectName(const nebius::iam::v1::AuthenticateResponse& response, TString& subject, TString& error) {
        if (response.resultcode() != nebius::iam::v1::AuthenticateResponse::OK) {
            error = nebius::iam::v1::AuthenticateResponse::ResultCode_Name(response.resultcode());
            return false;
        }
        return ApplySubjectName(response.account(), subject, error);
    }

    template <typename TSubjectType>
    typename TPermissionRecord::TTypeCase ConvertSubjectType(const TSubjectType& type) {
        switch (type) {
        case TSubjectType::kUserAccount:
            return TPermissionRecord::TTypeCase::USER_ACCOUNT_TYPE;
        case TSubjectType::kServiceAccount:
            return TPermissionRecord::TTypeCase::SERVICE_ACCOUNT_TYPE;
        case TSubjectType::kAnonymousAccount:
            return TPermissionRecord::TTypeCase::ANONYMOUS_ACCOUNT_TYPE;
        default:
            return TPermissionRecord::TTypeCase::TYPE_NOT_SET;
        }
    }

    template <>
    typename TPermissionRecord::TTypeCase ConvertSubjectType<yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse>(const yandex::cloud::priv::servicecontrol::v1::AuthenticateResponse& response) {
        return ConvertSubjectType(response.subject().type_case());
    }

    template <>
    typename TPermissionRecord::TTypeCase ConvertSubjectType<nebius::iam::v1::Account::TypeCase>(const nebius::iam::v1::Account::TypeCase& type) {
        using Account = nebius::iam::v1::Account;
        switch (type) {
        case Account::kUserAccount:
            return TPermissionRecord::TTypeCase::USER_ACCOUNT_TYPE;
        case Account::kServiceAccount:
            return TPermissionRecord::TTypeCase::SERVICE_ACCOUNT_TYPE;
        default:
            return TPermissionRecord::TTypeCase::TYPE_NOT_SET;
        }
    }

    template <>
    typename TPermissionRecord::TTypeCase ConvertSubjectType<nebius::iam::v1::AuthenticateResponse>(const nebius::iam::v1::AuthenticateResponse& response) {
        return ConvertSubjectType(response.account().type_case());
    }

    template <typename TTokenRecord>
    bool CanInitBuiltinToken(const TString& key, TTokenRecord& record) {
        if (Config.GetUseBuiltinDomain() && (record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::Builtin)) {
            if(record.Ticket.EndsWith("@" BUILTIN_ACL_DOMAIN)) {
                record.TokenType = TDerived::ETokenType::Builtin;
                SetToken(key, record, new NACLib::TUserToken({
                    .OriginalUserToken = record.Ticket,
                    .UserSID = record.Ticket,
                    .AuthType = record.GetAuthType()
                }));
                CounterTicketsBuiltin->Inc();
                return true;
            }

            if (record.Ticket.EndsWith("@" BUILTIN_ERROR_DOMAIN)) {
                record.TokenType = TDerived::ETokenType::Builtin;
                SetError(key, record, { "Builtin error simulation" });
                CounterTicketsBuiltin->Inc();
                return true;
            }

            if (record.Ticket.EndsWith("@" BUILTIN_SYSTEM_DOMAIN)) {
                record.TokenType = TDerived::ETokenType::Builtin;
                SetError(key, record, { "System domain not available for user usage", false });
                CounterTicketsBuiltin->Inc();
                return true;
            }
        }
        return false;
    }

    template <typename TTokenRecord>
    bool CanInitTokenFromCertificate(const TString& key, TTokenRecord& record) {
        if (record.TokenType != TDerived::ETokenType::Certificate) {
            return false;
        }
        CounterTicketsCertificate->Inc();
        TCertificateChecker::TCertificateCheckResult certificateCheckResult = CertificateChecker.Check(record.Ticket);
        if (!certificateCheckResult.Error.empty()) {
            TEvTicketParser::TError error;
            error.Message = "Cannot create token from certificate. " + certificateCheckResult.Error.Message;
            error.Retryable = certificateCheckResult.Error.Retryable;
            SetError(key, record, error);
            return false;
        }
        NACLib::TUserToken::TUserTokenInitFields userTokenInitFields {
            .OriginalUserToken = record.Ticket,
            .UserSID = certificateCheckResult.UserSid,
            .AuthType = record.GetAuthType()
        };
        auto userToken = MakeIntrusive<NACLib::TUserToken>(std::move(userTokenInitFields));
        for (const auto& group : certificateCheckResult.Groups) {
            userToken->AddGroupSID(group);
        }
        SetToken(key, record, userToken);
        return true;
    }

    template <typename TTokenRecord>
    bool CanInitLoginToken(const TString& key, TTokenRecord& record) {
        if (UseLoginProvider && (record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::Login)) {
            TString database = Config.GetDomainLoginOnly() ? DomainName : record.Database;
            auto itLoginProvider = LoginProviders.find(database);
            if (itLoginProvider != LoginProviders.end()) {
                NLogin::TLoginProvider& loginProvider(itLoginProvider->second);
                auto response = loginProvider.ValidateToken({.Token = record.Ticket});
                if (response.Error) {
                    if (!response.TokenUnrecognized || record.TokenType != TDerived::ETokenType::Unknown) {
                        record.TokenType = TDerived::ETokenType::Login;
                        SetError(key, record, {.Message = response.Error, .Retryable = response.ErrorRetryable});
                        CounterTicketsLogin->Inc();
                        return true;
                    }
                } else {
                    record.TokenType = TDerived::ETokenType::Login;
                    record.ExpireTime = ToInstant(response.ExpiresAt);
                    CounterTicketsLogin->Inc();
                    if (response.ExternalAuth) {
                        record.EnableExternalAuth(response);
                        HandleExternalAuthentication(key, record, response);
                        return true;
                    }
                    TVector<NACLib::TSID> groups;
                    if (response.Groups.has_value()) {
                        const std::vector<TString>& tokenGroups = response.Groups.value();
                        groups.assign(tokenGroups.begin(), tokenGroups.end());
                    } else {
                        const std::vector<TString> providerGroups = loginProvider.GetGroupsMembership(response.User);
                        groups.assign(providerGroups.begin(), providerGroups.end());
                    }
                    SetToken(key, record, new NACLib::TUserToken({
                        .OriginalUserToken = record.Ticket,
                        .UserSID = response.User,
                        .GroupSIDs = groups,
                        .AuthType = record.GetAuthType()
                    }));
                    return true;
                }
            } else {
                if (record.TokenType == TDerived::ETokenType::Login) {
                    SetError(key, record, {.Message = "Login state is not available yet", .Retryable = false});
                    CounterTicketsLogin->Inc();
                    return true;
                }
            }
        }
        return false;
    }

    template <typename TTokenRecord>
    void HandleExternalAuthentication(const TString& key, TTokenRecord& record, const NLogin::TLoginProvider::TValidateTokenResponse& loginProviderResponse) {
        if (loginProviderResponse.ExternalAuth == Config.GetLdapAuthenticationDomain()) {
            SendRequestToLdap(key, record, loginProviderResponse.User);
        } else {
            SetError(key, record, {.Message = "Do not have suitable external auth provider"});
        }
    }

    template <typename TTokenRecord>
    void SendRequestToLdap(const TString& key, TTokenRecord& record, const TString& user) {
        if (Config.HasLdapAuthentication()) {
            Send(MakeLdapAuthProviderID(), new TEvLdapAuthProvider::TEvEnrichGroupsRequest(key, user));
        } else {
            SetError(key, record, {.Message = "LdapAuthProvider is not initialized", .Retryable = false});
        }
    }

    void Handle(TEvLdapAuthProvider::TEvEnrichGroupsResponse::TPtr& ev) {
        TEvLdapAuthProvider::TEvEnrichGroupsResponse* response = ev->Get();
        auto& userTokens = GetDerived()->GetUserTokens();
        auto it = userTokens.find(response->Key);
        if (it == userTokens.end()) {
            // Probably this is unnecessary. Record should be in storage
            BLOG_ERROR("Ticket " << MaskTicket(response->Key) << " has expired during build");
        } else {
            const auto& key = it->first;
            auto& record = it->second;
            if (response->Status == TEvLdapAuthProvider::EStatus::SUCCESS) {
                const TString domain {"@" + Config.GetLdapAuthenticationDomain()};
                TVector<NACLib::TSID> groups(response->Groups.cbegin(), response->Groups.cend());
                std::transform(groups.begin(), groups.end(), groups.begin(), [&domain](NACLib::TSID& group) {
                    return group.append(domain);
                });
                SetToken(key, record, new NACLib::TUserToken({
                    .OriginalUserToken = record.Ticket,
                    .UserSID = response->User + domain,
                    .GroupSIDs = groups,
                    .AuthType = record.GetAuthType()
                }));
            } else {
                SetError(key, record, response->Error);
            }
            Respond(record);
        }
    }

    void CrackTicket(const TString& ticketBody, TStringBuf& ticket, TStringBuf& ticketType) {
        ticket = ticketBody;
        ticketType = ticket.NextTok(' ');
        if (ticket.empty()) {
            ticket = ticketBody;
            ticketType.Clear();
        }
    }

    bool IsTicketCertificate(const TString& ticket) const {
        static const TStringBuf CertificateBeginMark = "-----BEGIN";
        static const TStringBuf CertificateEndMark = "-----END";
        size_t eolPos = ticket.find('\n');
        if (eolPos == TString::npos) {
            return false;
        }
        if (TString firstLine = ticket.substr(0, eolPos); !firstLine.Contains(CertificateBeginMark)) {
            return false;
        }
        if (ticket.rfind(CertificateEndMark) == TString::npos) {
            return false;
        }
        return true;
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
        TStringBuf ticket;
        TStringBuf ticketType;
        if (IsTicketCertificate(ev->Get()->Ticket)) {
            ticket = ev->Get()->Ticket;
            ticketType = TDerived::TTokenRecord::CertificateAuthType;
        } else {
            CrackTicket(ev->Get()->Ticket, ticket, ticketType);
        }

        TString key = GetKey(ev->Get());
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        CounterTicketsReceived->Inc();
        const auto& signature = ev->Get()->Signature;
        if (!IsAccessKeySignatureSupported() && (signature.AccessKeyId || signature.Signature)) {
            TEvTicketParser::TError error;
            error.Message = "Access key signature is not supported";
            error.Retryable = false;
            BLOG_ERROR("Ticket " << MaskTicket(signature.AccessKeyId) << ": " << error);
            Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, error), 0, cookie);
            return;
        }
        if (ticket.empty() && !signature.AccessKeyId) {
            TEvTicketParser::TError error;
            error.Message = "Ticket is empty";
            error.Retryable = false;
            BLOG_ERROR("Ticket " << MaskTicket(ticket) << ": " << error);
            Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, error), 0, cookie);
            return;
        }
        auto& userTokens = GetDerived()->GetUserTokens();
        auto it = userTokens.find(key);
        if (it != userTokens.end()) {
            auto& record = it->second;
            TInstant now = TlsActivationContext->Now();
            // we know about token
            if (record.IsTokenReady()) {
                // token already have built
                record.AccessTime = now;
                Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, record.GetToken()), 0, cookie);
            } else if (record.Error) {
                // token stores information about previous error
                record.AccessTime = now;
                Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, record.Error), 0, cookie);
            } else {
                // token building in progress
                record.AuthorizeRequests.emplace_back(ev.Release());
            }
            CounterTicketsCacheHit->Inc();
            return;
        } else {
            it = userTokens.emplace(key, ticket).first;
            CounterTicketsCacheMiss->Inc();
        }

        auto& record = it->second;
        record.CurrentDelay = MinErrorRefreshTime;
        record.RefreshRetryableErrorImmediately = true;
        record.PeerName = std::move(ev->Get()->PeerName);
        record.Database = std::move(ev->Get()->Database);
        record.Signature = ev->Get()->Signature;
        for (const auto& entry: ev->Get()->Entries) {
            for (const auto& permission : entry.Permissions) {
                auto& permissionRecord = record.Permissions[permission.Permission];
                permissionRecord.Attributes = entry.Attributes;
                permissionRecord.Required = permission.Required;
            }
        }

        SetTokenType(record, ticket, ticketType);

        InitTokenRecord(key, record);
        if (record.Error) {
            BLOG_ERROR("Ticket " << record.GetMaskedTicket() << ": " << record.Error);
            Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, record.Error), 0, cookie);
            return;
        }
        if (record.IsTokenReady()) {
            // offline check ready
            Send(sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, record.GetToken()), 0, cookie);
            return;
        }
        record.AuthorizeRequests.emplace_back(ev.Release());
    }

    static auto GetTokenType(TEvAccessServiceAuthenticateRequest* request) {
        return request->Request.has_api_key() ? TDerived::ETokenType::ApiKey : TDerived::ETokenType::AccessService;
    }

    static auto GetTokenType(TEvAccessServiceAuthorizeRequest* request) {
        return request->Request.has_api_key() ? TDerived::ETokenType::ApiKey : TDerived::ETokenType::AccessService;
    }

    static auto GetTokenType(TEvNebiusAccessServiceAuthenticateRequest*) {
        return TDerived::ETokenType::AccessService; // the only supported
    }

    static auto GetTokenType(TEvNebiusAccessServiceAuthorizeRequest*) {
        return TDerived::ETokenType::AccessService; // the only supported
    }

    template <typename TTokenRecord>
    bool ResolveAccountName(TTokenRecord& record, const TString& key) { // Returns true when resolve request is sent
        switch (record.SubjectType) {
        case TPermissionRecord::TTypeCase::USER_ACCOUNT_TYPE:
            if (UserAccountService) {
                BLOG_TRACE("Ticket " << record.GetMaskedTicket()
                            << " asking for UserAccount(" << record.Subject << ")");
                THolder<TEvAccessServiceGetUserAccountRequest> request = MakeHolder<TEvAccessServiceGetUserAccountRequest>(key);
                request->Token = record.Ticket;
                request->Request.set_user_account_id(TString(TStringBuf(record.Subject).NextTok('@')));
                Send(UserAccountService, request.Release());
                record.ResponsesLeft++;
                return true;
            }
            break;
        case TPermissionRecord::TTypeCase::SERVICE_ACCOUNT_TYPE:
            if (ServiceAccountService) {
                BLOG_TRACE("Ticket " << record.GetMaskedTicket()
                            << " asking for ServiceAccount(" << record.Subject << ")");
                THolder<TEvAccessServiceGetServiceAccountRequest> request = MakeHolder<TEvAccessServiceGetServiceAccountRequest>(key);
                request->Token = record.Ticket;
                request->Request.set_service_account_id(TString(TStringBuf(record.Subject).NextTok('@')));
                Send(ServiceAccountService, request.Release());
                record.ResponsesLeft++;
                return true;
            }
            break;
        default:
            break;
        }
        return false;
    }

    template <typename TEvRequest, typename TEvResponse>
    void HandleIamAuthenticateResponse(typename TEvResponse::TPtr& ev) {
        TEvResponse* response = ev->Get();
        TEvRequest* request = response->Request->template Get<TEvRequest>();
        auto& userTokens = GetDerived()->GetUserTokens();
        auto it = userTokens.find(request->Key);
        if (it == userTokens.end()) {
            // wtf? it should be there
            BLOG_ERROR("Ticket " << MaskTicket(request->Request.iam_token()) << " has expired during build");
        } else {
            const auto& key = it->first;
            auto& record = it->second;
            record.ResponsesLeft--;
            if (response->Status.Ok()) {
                record.TokenType = GetTokenType(request);
                TString errorMessage;
                if (ApplySubjectName(response->Response, record.Subject, errorMessage)) {
                    record.SubjectType = ConvertSubjectType(response->Response);
                    if (ResolveAccountName(record, key)) {
                        return;
                    }
                    SetToken(key, record, new NACLib::TUserToken({
                        .OriginalUserToken = record.Ticket,
                        .UserSID = record.Subject,
                        .AuthType = record.GetAuthType()
                    }));
                } else {
                    SetError(key, record, {errorMessage, false});
                }
            } else {
                if (record.ResponsesLeft == 0 && (record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::AccessService || record.TokenType == TDerived::ETokenType::ApiKey)) {
                    bool retryable = IsRetryableGrpcError(response->Status);
                    SetError(key, record, {response->Status.Msg, retryable});
                }
            }
            if (record.ResponsesLeft == 0) {
                Respond(record);
            }
        }
    }

    void Handle(NNebiusCloud::TEvAccessService::TEvAuthenticateResponse::TPtr& ev) {
        HandleIamAuthenticateResponse<TEvNebiusAccessServiceAuthenticateRequest, NNebiusCloud::TEvAccessService::TEvAuthenticateResponse>(ev);
    }

    void Handle(NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr& ev) {
        HandleIamAuthenticateResponse<TEvAccessServiceAuthenticateRequest, NCloud::TEvAccessService::TEvAuthenticateResponse>(ev);
    }

    void Handle(NCloud::TEvUserAccountService::TEvGetUserAccountResponse::TPtr& ev) {
        TEvAccessServiceGetUserAccountRequest* request = ev->Get()->Request->Get<TEvAccessServiceGetUserAccountRequest>();
        auto& userTokens = GetDerived()->GetUserTokens();
        auto it = userTokens.find(request->Key);
        if (it == userTokens.end()) {
            // wtf? it should be there
            BLOG_ERROR("Ticket has expired during build (TEvGetUserAccountResponse)");
        } else {
            const auto& key = it->first;
            auto& record = it->second;
            record.ResponsesLeft--;
            if (!ev->Get()->Status.Ok()) {
                SetError(key, record, {ev->Get()->Status.Msg});
            } else {
                GetDerived()->SetToken(key, record, ev);
            }
            if (record.ResponsesLeft == 0) {
                Respond(record);
            }
        }
    }

    void Handle(NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse::TPtr& ev) {
        TEvAccessServiceGetServiceAccountRequest* request = ev->Get()->Request->Get<TEvAccessServiceGetServiceAccountRequest>();
        auto& userTokens = GetDerived()->GetUserTokens();
        auto it = userTokens.find(request->Key);
        if (it == userTokens.end()) {
            // wtf? it should be there
            BLOG_ERROR("Ticket has expired during build (TEvGetServiceAccountResponse)");
        } else {
            const auto& key = it->first;
            auto& record = it->second;
            record.ResponsesLeft--;
            if (!ev->Get()->Status.Ok()) {
                SetError(key, record, {ev->Get()->Status.Msg});
            } else {
                SetToken(key, record, new NACLib::TUserToken(record.Ticket, ev->Get()->Response.name() + "@" + ServiceDomain, {}));
            }
            if (record.ResponsesLeft == 0) {
                Respond(record);
            }
        }
    }

    template <class TResourcePath>
    TString GetResourcePathIdForRequiredPermissions(const TResourcePath& resourcePath) {
        if (resourcePath.type() == "resource-manager.folder") {
            return " folder_id " + resourcePath.id();
        }
        if (resourcePath.type() == "resource-manager.cloud") {
            return " cloud_id " + resourcePath.id();
        }
        if (resourcePath.type() == "iam.serviceAccount") {
            return " service_account_id " + resourcePath.id();
        }
        return "";
    };

    template <typename TTokenRecord>
    void SetAccessServiceBulkAuthorizeError(const TString& key, TTokenRecord& record, const TString& errorMessage, bool isRetryableError) {
        for (auto& [permissionName, permissionRecord] : record.Permissions) {
            permissionRecord.Subject.clear();
            permissionRecord.Error = {.Message = errorMessage, .Retryable = isRetryableError};
            BLOG_TRACE("Ticket " << record.GetMaskedTicket()
                                << " permission " << permissionName
                                << " now has a " << (isRetryableError ? "retryable" : "permanent")  << " error \"" << errorMessage << "\""
                                << " retryable: " << isRetryableError);
        }
        SetError(key, record, {.Message = errorMessage, .Retryable = isRetryableError});
    }

    static TString ConcatenateErrorMessages(const std::vector<typename THashMap<TString, TPermissionRecord>::iterator>& requiredPermissions) {
        TStringBuilder errorMessage;
        auto it = requiredPermissions.cbegin();
        errorMessage << (*it)->second.Error.Message;
        ++it;
        for (; it != requiredPermissions.cend(); ++it) {
            errorMessage << ", " << (*it)->second.Error.Message;
        }
        return std::move(errorMessage);
    }

    void Handle(NNebiusCloud::TEvAccessService::TEvAuthorizeResponse::TPtr& ev) {
        NNebiusCloud::TEvAccessService::TEvAuthorizeResponse* response = ev->Get();
        TEvNebiusAccessServiceAuthorizeRequest* request = response->Request->Get<TEvNebiusAccessServiceAuthorizeRequest>();
        const TString& key(request->Key);
        auto& userTokens = GetDerived()->GetUserTokens();
        auto itToken = userTokens.find(key);
        if (itToken == userTokens.end()) {
            BLOG_ERROR("Ticket(key) "
                        << MaskTicket(key)
                        << " has expired during permission check");
        } else {
            auto& record = itToken->second;
            --record.ResponsesLeft;
            auto& examinedPermissions = record.Permissions;
            if (response->Status.Ok()) {
                if (response->Response.results_size() == 0) {
                    SetAccessServiceBulkAuthorizeError(key, record, TStringBuilder() << "Internal error: no results in authorize response", false);
                } else {
                    for (auto& [permissionName, permissionRecord] : examinedPermissions) {
                        permissionRecord.Error.clear();
                    }

                    size_t permissionDeniedCount = 0;
                    bool hasRequiredPermissionFailed = false;
                    std::vector<typename THashMap<TString, TPermissionRecord>::iterator> requiredPermissions;
                    THashSet<TString> processedPermissions;
                    bool processingError = false;
                    bool subjectIsResolved = false;
                    for (const auto& [resultKey, result] : response->Response.results()) {
                        const auto checkIt = request->Request.checks().find(resultKey);
                        if (checkIt == request->Request.checks().end()) {
                            SetAccessServiceBulkAuthorizeError(key, record, TStringBuilder() << "Internal error: unknown result key: " << resultKey, false);
                            BLOG_W("Internal error: unknown result key: " << resultKey << " for ticket " << record.GetMaskedTicket());
                            processingError = true;
                            break;
                        }
                        const auto& check = checkIt->second;

                        if (!subjectIsResolved && result.resultcode() == nebius::iam::v1::AuthorizeResult::OK) {
                            const auto& account = result.account();
                            TString errorMessage;
                            if (!ApplySubjectName(account, record.Subject, errorMessage)) {
                                SetAccessServiceBulkAuthorizeError(key, record, errorMessage, false);
                                processingError = true;
                                break;
                            }
                            record.SubjectType = ConvertSubjectType(account.type_case());
                            for (auto& [_, permissionRecord] : record.Permissions) {
                                if (permissionRecord.Error.empty()) {
                                    permissionRecord.Subject = record.Subject;
                                    permissionRecord.SubjectType = record.SubjectType;
                                }
                            }
                            subjectIsResolved = true;
                        }

                        const TString& permissionName = check.permission().name();
                        auto permissionIt = examinedPermissions.find(permissionName);
                        if (permissionIt != examinedPermissions.end()) {
                            processedPermissions.insert(permissionIt->first);
                            auto& permissionRecord = permissionIt->second;
                            if (result.resultcode() != nebius::iam::v1::AuthorizeResult::OK) {
                                permissionDeniedCount++;
                                permissionRecord.Subject.clear();
                                BLOG_TRACE("Ticket " << record.GetMaskedTicket() << " permission " << permissionName << " access denied for subject \"" << (record.Subject ? record.Subject : "<not resolved>") << "\"");
                                TStringBuilder errorMessage;
                                if (permissionRecord.IsRequired()) {
                                    hasRequiredPermissionFailed = true;
                                    errorMessage << permissionIt->first << " for";
                                    if (check.container_id()) {
                                        errorMessage << ' ' << check.container_id();
                                    }
                                    for (const auto& resourcePath : check.resource_path().path()) {
                                        errorMessage << ' ' << resourcePath.id();
                                    }
                                    errorMessage << " - ";
                                    requiredPermissions.push_back(permissionIt);
                                }
                                errorMessage << nebius::iam::v1::AuthorizeResult::ResultCode_Name(result.resultcode());
                                permissionRecord.Error = {.Message = errorMessage, .Retryable = false};
                            }
                        } else {
                            BLOG_W("Received response for unknown permission " << permissionName << " for ticket " << record.GetMaskedTicket());
                        }
                    }
                    if (!processingError) {
                        if (processedPermissions.size() != examinedPermissions.size()) {
                            auto printAbsentPermissions = [&]() -> TString {
                                TStringBuilder b;
                                for (const auto& [name, _] : examinedPermissions) {
                                    auto it = processedPermissions.find(name);
                                    if (it != processedPermissions.end()) {
                                        continue;
                                    }
                                    if (b) {
                                        b << ", ";
                                    }
                                    b << name;
                                }
                                return std::move(b);
                            };
                            BLOG_W("Received response with not all permissions. Absent permissions: " << printAbsentPermissions());
                            SetAccessServiceBulkAuthorizeError(key, record, TStringBuilder() << "Internal error: not all permissions in authorize response", false);
                        } else if (permissionDeniedCount < examinedPermissions.size() && !hasRequiredPermissionFailed) {
                            record.TokenType = TDerived::ETokenType::AccessService;
                            SetToken(key, record, new NACLib::TUserToken({
                                .OriginalUserToken = record.Ticket,
                                .UserSID = record.Subject,
                                .AuthType = record.GetAuthType()
                            }));
                        } else {
                            if (hasRequiredPermissionFailed) {
                                SetError(key, record, {.Message = ConcatenateErrorMessages(requiredPermissions), .Retryable = false});
                            } else {
                                SetError(key, record, {.Message = "Access Denied", .Retryable = false});
                            }
                        }
                    }
                }
            } else {
                SetAccessServiceBulkAuthorizeError(key, record, response->Status.Msg, IsRetryableGrpcError(response->Status));
            }
            if (record.ResponsesLeft == 0) {
                Respond(record);
            }
        }
    }

    void Handle(NCloud::TEvAccessService::TEvBulkAuthorizeResponse::TPtr& ev) {
        NCloud::TEvAccessService::TEvBulkAuthorizeResponse* response = ev->Get();
        TEvAccessServiceBulkAuthorizeRequest* request = response->Request->Get<TEvAccessServiceBulkAuthorizeRequest>();
        const TString& key(request->Key);
        auto& userTokens = GetDerived()->GetUserTokens();
        auto itToken = userTokens.find(key);
        if (itToken == userTokens.end()) {
            BLOG_ERROR("Ticket(key) "
                        << MaskTicket(key)
                        << " has expired during permission check");
        } else {
            auto& record = itToken->second;
            --record.ResponsesLeft;
            auto& examinedPermissions = record.Permissions;
            if (response->Status.Ok()) {
                if (response->Response.has_unauthenticated_error()) {
                    SetAccessServiceBulkAuthorizeError(key, record, response->Response.unauthenticated_error().message(), false);
                } else {
                    TString subjectNameErrorMessage;
                    if (ApplySubjectName(response->Response, record.Subject, subjectNameErrorMessage)) {
                        const auto& subject = response->Response.subject();
                        record.SubjectType = ConvertSubjectType(subject.type_case());
                        for (auto& [permissionName, permissionRecord] : examinedPermissions) {
                            permissionRecord.Subject = record.Subject;
                            permissionRecord.SubjectType = record.SubjectType;
                            permissionRecord.Error.clear();
                        }
                    }

                    size_t permissionDeniedCount = 0;
                    bool hasRequiredPermissionFailed = false;
                    std::vector<typename THashMap<TString, TPermissionRecord>::iterator> requiredPermissions;
                    TString permissionDeniedError;
                    const auto& results = response->Response.results();
                    for (const auto& result : results.items()) {
                        auto permissionDeniedIt = examinedPermissions.find(result.permission());
                        if (permissionDeniedIt != examinedPermissions.end()) {
                            permissionDeniedCount++;
                            auto& permissionDeniedRecord = permissionDeniedIt->second;
                            permissionDeniedRecord.Subject.clear();
                            BLOG_TRACE("Ticket " << record.GetMaskedTicket() << " permission " << result.permission() << " access denied for subject \"" << record.Subject << "\"");
                            TStringBuilder errorMessage;
                            if (permissionDeniedRecord.IsRequired()) {
                                hasRequiredPermissionFailed = true;
                                errorMessage << permissionDeniedIt->first << " for";
                                for (const auto& resourcePath : result.resource_path()) {
                                    errorMessage << GetResourcePathIdForRequiredPermissions(resourcePath);
                                }
                                errorMessage << " - ";
                                requiredPermissions.push_back(permissionDeniedIt);
                            }
                            permissionDeniedError = result.permission_denied_error().message();
                            errorMessage << permissionDeniedError;
                            permissionDeniedRecord.Error = {.Message = errorMessage, .Retryable = false};
                        } else {
                            BLOG_W("Received response for unknown permission " << result.permission() << " for ticket " << record.GetMaskedTicket());
                        }
                    }
                    if (permissionDeniedCount < examinedPermissions.size() && !hasRequiredPermissionFailed && subjectNameErrorMessage.empty()) {
                        record.TokenType = request->Request.has_api_key() ? TDerived::ETokenType::ApiKey : TDerived::ETokenType::AccessService;
                        if (ResolveAccountName(record, key)) {
                            return;
                        }
                        SetToken(request->Key, record, new NACLib::TUserToken(record.Ticket, record.Subject, {}));
                    } else {
                        if (hasRequiredPermissionFailed) {
                            SetError(key, record, {.Message = ConcatenateErrorMessages(requiredPermissions), .Retryable = false});
                        } else {
                            SetError(key, record, {.Message = permissionDeniedError ? permissionDeniedError : subjectNameErrorMessage, .Retryable = false});
                        }
                    }
                }
            } else {
                SetAccessServiceBulkAuthorizeError(key, record, response->Status.Msg, IsRetryableGrpcError(response->Status));
            }
            Respond(record);
        }
    }

    void Handle(NCloud::TEvAccessService::TEvAuthorizeResponse::TPtr& ev) {
        NCloud::TEvAccessService::TEvAuthorizeResponse* response = ev->Get();
        TEvAccessServiceAuthorizeRequest* request = response->Request->Get<TEvAccessServiceAuthorizeRequest>();
        const TString& key(request->Key);
        auto& userTokens = GetDerived()->GetUserTokens();
        auto itToken = userTokens.find(key);
        if (itToken == userTokens.end()) {
            BLOG_ERROR("Ticket(key) "
                        << MaskTicket(key)
                        << " has expired during permission check");
        } else {
            auto& record = itToken->second;
            TString permission = request->Request.permission();
            auto itPermission = record.Permissions.find(permission);
            if (itPermission != record.Permissions.end()) {
                if (response->Status.Ok()) {
                    TString errorMessage;
                    if (ApplySubjectName(response->Response.subject(), itPermission->second.Subject, errorMessage)) {
                        itPermission->second.SubjectType = ConvertSubjectType(response->Response.subject().type_case());
                        itPermission->second.Error.clear();
                        if (record.Subject.empty()) {
                            record.Subject = itPermission->second.Subject;
                            record.SubjectType = itPermission->second.SubjectType;
                        }
                        BLOG_TRACE("Ticket "
                                    << record.GetMaskedTicket()
                                    << " permission "
                                    << permission
                                    << " now has a valid subject \""
                                    << record.Subject
                                    << "\"");
                    }
                } else {
                    bool retryable = IsRetryableGrpcError(response->Status);
                    itPermission->second.Error = {response->Status.Msg, retryable};
                    if (itPermission->second.Subject.empty() || !retryable) {
                        itPermission->second.Subject.clear();
                        BLOG_TRACE("Ticket "
                                    << record.GetMaskedTicket()
                                    << " permission "
                                    << permission
                                    << " now has a permanent error \""
                                    << itPermission->second.Error
                                    << "\" "
                                    << " retryable:"
                                    << retryable);
                    } else if (retryable) {
                        BLOG_TRACE("Ticket "
                                    << record.GetMaskedTicket()
                                    << " permission "
                                    << permission
                                    << " now has a retryable error \""
                                    << response->Status.Msg
                                    << "\"");
                    }
                }
            } else {
                BLOG_W("Received response for unknown permission " << permission << " for ticket " << record.GetMaskedTicket());
            }
            if (--record.ResponsesLeft == 0) {
                ui32 permissionsOk = 0;
                ui32 retryableErrors = 0;
                bool requiredPermissionFailed = false;
                TEvTicketParser::TError error;
                for (const auto& [permission, rec] : record.Permissions) {
                    if (rec.IsPermissionOk()) {
                        ++permissionsOk;
                    } else if (rec.IsRequired()) {
                        TString id;
                        if (TString folderId = record.GetAttributeValue(permission, "folder_id")) {
                            id += "folder_id " + folderId;
                        } else if (TString cloudId = record.GetAttributeValue(permission, "cloud_id")) {
                            id += "cloud_id " + cloudId;
                        } else if (TString serviceAccountId = record.GetAttributeValue(permission, "service_account_id")) {
                            id += "service_account_id " + serviceAccountId;
                        }
                        error = rec.Error;
                        error.Message = permission + " for " + id + " - " + error.Message;
                        requiredPermissionFailed = true;
                        break;
                    } else {
                        if (rec.Error.Retryable) {
                            ++retryableErrors;
                            error = rec.Error;
                            break;
                        } else if (!error) {
                            error = rec.Error;
                        }
                    }
                }
                if (permissionsOk > 0 && retryableErrors == 0 && !requiredPermissionFailed) {
                    record.TokenType = request->Request.has_api_key() ? TDerived::ETokenType::ApiKey : TDerived::ETokenType::AccessService;
                    if (ResolveAccountName(record, key)) {
                        return;
                    }
                    SetToken(request->Key, record, new NACLib::TUserToken(record.Ticket, record.Subject, {}));
                } else if (record.ResponsesLeft == 0 && (record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::AccessService || record.TokenType == TDerived::ETokenType::ApiKey)) {
                    SetError(request->Key, record, error);
                }
            }
            if (record.ResponsesLeft == 0) {
                Respond(record);
            }
        }
    }

    void Handle(TEvTicketParser::TEvRefreshTicket::TPtr& ev) {
        const TString& ticket(ev->Get()->Ticket);
        auto& userTokens = GetDerived()->GetUserTokens();
        auto it = userTokens.find(ticket);
        if (it != userTokens.end()) {
            RefreshTicket(ticket, it->second);
        }
    }

    void Handle(TEvTicketParser::TEvUpdateLoginSecurityState::TPtr& ev) {
        auto& loginProvider = LoginProviders[ev->Get()->SecurityState.GetAudience()];
        loginProvider.UpdateSecurityState(ev->Get()->SecurityState);
        BLOG_D("Updated state for " << loginProvider.Audience << " keys " << GetLoginProviderKeys(loginProvider));
    }

    void Handle(TEvTicketParser::TEvDiscardTicket::TPtr& ev) {
        auto& userTokens = GetDerived()->GetUserTokens();
        userTokens.erase(ev->Get()->Ticket);
    }

    void HandleRefresh() {
        TInstant now = TlsActivationContext->Now();
        while (!RefreshQueue.empty() && RefreshQueue.top().RefreshTime <= now) {
            TString key = RefreshQueue.top().Key;
            RefreshQueue.pop();
            auto& userTokens = GetDerived()->GetUserTokens();
            auto it = userTokens.find(key);
            if (it == userTokens.end()) {
                continue;
            }
            auto& record = it->second;
            if ((record.ExpireTime > now) && (record.AccessTime + GetLifeTime() > now)) {
                BLOG_D("Refreshing ticket " << record.GetMaskedTicket());
                if (!RefreshTicket(key, record)) {
                    RefreshQueue.push({key, record.RefreshTime});
                }
            } else {
                BLOG_D("Expired ticket " << record.GetMaskedTicket());
                if (!record.AuthorizeRequests.empty()) {
                    record.Error = {"Timed out", true};
                    Respond(record);
                }
                userTokens.erase(it);
            }
        }
        Schedule(RefreshPeriod, new NActors::TEvents::TEvWakeup());
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        const auto& params = ev->Get()->Request.GetParams();
        TStringBuilder html;
        if (params.Has("token")) {
            TString token = params.Get("token");
            html << "<head>";
            html << "<style>";
            html << "table.ticket-parser-proplist > tbody > tr > td { padding: 1px 3px; } ";
            html << "table.ticket-parser-proplist > tbody > tr > td:first-child { font-weight: bold; text-align: right; } ";
            html << "</style>";
            for (const auto& [key, record] : GetDerived()->GetUserTokens()) {
                if (MD5::Calc(key) == token) {
                    html << "<div>";
                    html << "<table class='ticket-parser-proplist'>";
                    html << "<tr><td>Ticket</td><td>" << record.GetMaskedTicket() << "</td></tr>";
                    if (record.TokenType == TDerived::ETokenType::Login) {
                        TVector<TString> tokenData;
                        Split(record.Ticket, ".", tokenData);
                        if (tokenData.size() > 1) {
                            TString header;
                            TString payload;
                            try {
                                header = Base64DecodeUneven(tokenData[0]);
                            }
                            catch (const std::exception&) {
                                header = tokenData[0];
                            }
                            try {
                                payload = Base64DecodeUneven(tokenData[1]);
                            }
                            catch (const std::exception&) {
                                payload = tokenData[1];
                            }
                            html << "<tr><td>Header</td><td>" << header << "</td></tr>";
                            html << "<tr><td>Payload</td><td>" << payload << "</td></tr>";
                        }
                    }
                    TDerived::WriteTokenRecordInfo(html, record);
                    html << "</table>";
                    html << "</div>";
                    break;
                }
            }
            html << "</head>";
        } else {
            html << "<head>";
            html << "<script>$('.container').css('width', 'auto');</script>";
            html << "<style>";
            MakeHtmlTable(html);
            html << "</style>";
            html << "</head>";
            html << "<div style='margin-bottom: 10px; margin-left: 100px'>";
            html << "<table class='ticket-parser-proplist'>";
            html << "<tr><td>User Tokens</td><td>" << GetDerived()->GetUserTokens().size() << "</td></tr>";
            html << "<tr><td>Refresh Queue</td><td>" << RefreshQueue.size() << "</td></tr>";
            if (!RefreshQueue.empty()) {
                html << "<tr><td>Refresh Queue Time</td><td>" << RefreshQueue.top().RefreshTime << "</td></tr>";
            }
            WriteRefreshTimeValues(html);
            html << "<tr><td>Life Time</td><td>" << LifeTime << "</td></tr>";
            html << "<tr><td>Expire Time</td><td>" << ExpireTime << "</td></tr>";
            if (UseLoginProvider) {
                for (const auto& [databaseName, loginProvider] : LoginProviders) {
                    html << "<tr><td>LoginProvider Database</td><td>" << databaseName << " (" << loginProvider.Audience << ")</td></tr>";
                    html << "<tr><td>LoginProvider Keys</td><td>" << GetLoginProviderKeys(loginProvider) << "</td></tr>";
                    html << "<tr><td>LoginProvider Sids</td><td>" << loginProvider.Sids.size() << "</td></tr>";
                }
            }
            GetDerived()->WriteAuthorizeMethods(html);
            html << "</table>";
            html << "</div>";

            html << "<div>";
            html << "<table class='table simple-table1 table-hover table-condensed'>";
            html << "<thead><tr>";
            html << "<th>Ticket</th>";
            html << "<th>Database</th>";
            html << "<th>Subject</th>";
            html << "<th>Error</th>";
            html << "<th>Token</th>";
            html << "<th>Requests</th>";
            html << "<th>Responses Left</th>";
            html << "<th>Refresh</th>";
            html << "<th>Expire</th>";
            html << "<th>Access</th>";
            html << "<th>Peer</th>";
            html << "</tr></thead><tbody>";
            for (const auto& [key, record] : GetDerived()->GetUserTokens()) {
                WriteTokenRecordValues(html, key, record);
            }
            html << "</tbody></table>";
            html << "</div>";
        }
        Send(ev->Sender, new NMon::TEvHttpInfoRes(html));
    }

    static void MakeHtmlTable(TStringBuilder& html) {
        html << "table.ticket-parser-proplist > tbody > tr > td { padding: 1px 3px; } ";
        html << "table.ticket-parser-proplist > tbody > tr > td:first-child { font-weight: bold; text-align: right; } ";
        html << "table.simple-table1 th { margin: 0px 3px; text-align: center; } ";
        html << "table.simple-table1 > tbody > tr > td:nth-child(2) { text-align: right; }";
        html << "table.simple-table1 > tbody > tr > td:nth-child(7) { text-align: right; }";
        html << "table.simple-table1 > tbody > tr > td:nth-child(8) { text-align: right; }";
        html << "table.simple-table1 > tbody > tr > td:nth-child(9) { white-space: nowrap; }";
        html << "table.simple-table1 > tbody > tr > td:nth-child(10) { white-space: nowrap; }";
        html << "table.simple-table1 > tbody > tr > td:nth-child(11) { white-space: nowrap; }";
        html << "table.simple-table1 > tbody > tr > td:nth-child(12) { white-space: nowrap; }";
        html << "table.table-hover tbody tr:hover > td { background-color: #9dddf2; }";
    }

    void WriteRefreshTimeValues(TStringBuilder& html) {
        html << "<tr><td>Refresh Period</td><td>" << RefreshPeriod << "</td></tr>";
        html << "<tr><td>Refresh Time</td><td>" << RefreshTime << "</td></tr>";
        html << "<tr><td>Min Error Refresh Time</td><td>" << MinErrorRefreshTime << "</td></tr>";
        html << "<tr><td>Max Error Refresh Time</td><td>" << MaxErrorRefreshTime << "</td></tr>";
    }

protected:
    auto ParseTokenType(const TStringBuf tokenType) const {
        if (tokenType == "Login") {
            if (UseLoginProvider) {
                return TDerived::ETokenType::Login;
            } else {
                return TDerived::ETokenType::Unsupported;
            }
        }
        if (tokenType == "Bearer" || tokenType == "IAM") {
            if (AccessServiceEnabled()) {
                return TDerived::ETokenType::AccessService;
            } else {
                return TDerived::ETokenType::Unsupported;
            }
        } else if (tokenType == "ApiKey") {
            if (ApiKeyEnabled()) {
                return TDerived::ETokenType::ApiKey;
            } else {
                return TDerived::ETokenType::Unsupported;
            }
        }
        if (tokenType == "Certificate") {
            return TDerived::ETokenType::Certificate;
        }
        return TDerived::ETokenType::Unknown;
    }

    static TStringBuf GetTicketFromKey(const TStringBuf key) {
        return key.Before(':');
    }

    void EnrichUserTokenWithBuiltins(const TTokenRecordBase& record, TIntrusivePtr<NACLib::TUserToken>& token) {
        const TString& allAuthenticatedUsers = AppData()->AllAuthenticatedUsers;
        if (!allAuthenticatedUsers.empty()) {
            token->AddGroupSID(allAuthenticatedUsers);
        }
        for (const TString& sid : record.AdditionalSIDs) {
            token->AddGroupSID(sid);
        }
        if (!record.Permissions.empty()) {
            TString subject;
            TVector<TString> groups;
            for (const auto& [permission, rec] : record.Permissions) {
                if (rec.IsPermissionOk()) {
                    subject = rec.Subject;
                    AddPermissionSids(groups, record, permission);
                }
            }
            if (subject) {
                groups.emplace_back(subject);
            }
            if (record.Subject && record.Subject != subject) {
                groups.emplace_back(record.Subject);
            }

            for (const TString& group : groups) {
                token->AddGroupSID(group);
            }
        }
    }

    void AddPermissionSids(TVector<TString>& sids, const TTokenRecordBase& record, const TString& permission) const {
        sids.emplace_back(permission + '@' + AccessServiceDomain);
        if (const TString databaseId = record.GetAttributeValue(permission, "database_id"); databaseId) {
            sids.emplace_back(permission + '-' + databaseId + '@' + AccessServiceDomain);
        }
        if (const TString gizmoId = record.GetAttributeValue(permission, "gizmo_id"); gizmoId) {
            sids.emplace_back(permission + '-' + gizmoId + '@' + AccessServiceDomain);
        }
    }

    template <typename TTokenRecord>
    void InitTokenRecord(const TString& key, TTokenRecord& record, TInstant) {
        if (GetDerived()->CanInitAccessServiceToken(record)) {
            if (AccessServiceEnabled()) {
                if (record.Permissions) {
                    RequestAccessServiceAuthorization(key, record);
                } else {
                    RequestAccessServiceAuthentication(key, record);
                }
                CounterTicketsAS->Inc();
            }
        }

        if (record.TokenType == TDerived::ETokenType::Unknown && record.ResponsesLeft == 0) {
            record.Error.Message = "Could not find correct token validator";
            record.Error.Retryable = false;
        }
    }

    template <typename TTokenRecord>
    bool CanInitAccessServiceToken(const TTokenRecord& record) {
        switch(record.TokenType) {
            case TDerived::ETokenType::Unknown:
            case TDerived::ETokenType::AccessService:
                return true;
            case TDerived::ETokenType::ApiKey:
                return Config.GetUseAccessServiceApiKey();
            default:
                return false;
        }
    }

    template <typename TTokenRecord>
    void InitTokenRecord(const TString& key, TTokenRecord& record) {
        TInstant now = TlsActivationContext->Now();
        record.InitTime = now;
        record.AccessTime = now;
        record.ExpireTime = GetExpireTime(record, now);
        record.RefreshTime = GetRefreshTime(now);

        if (record.Error) {
            return;
        }

        if (CanInitBuiltinToken(key, record) ||
            CanInitLoginToken(key, record) ||
            CanInitTokenFromCertificate(key, record)) {
            return;
        }

        GetDerived()->InitTokenRecord(key, record, now);
    }

    template <typename TTokenRecord>
    void SetToken(const TString& key, TTokenRecord& record, TIntrusivePtr<NACLib::TUserToken> token) {
        TInstant now = TlsActivationContext->Now();
        record.Error.clear();
        EnrichUserTokenWithBuiltins(record, token);
        record.SetToken(token);
        if (!token->GetUserSID().empty()) {
            record.Subject = token->GetUserSID();
        }
        record.ExpireTime = GetExpireTime(record, now);
        if (record.NeedsRefresh()) {
            record.SetOkRefreshTime(this, now);
        } else {
            record.RefreshTime = record.ExpireTime;
        }
        record.RefreshRetryableErrorImmediately = true;
        CounterTicketsSuccess->Inc();
        CounterTicketsBuildTime->Collect((now - record.InitTime).MilliSeconds());
        BLOG_D("Ticket " << record.GetMaskedTicket() << " ("
                    << record.PeerName << ") has now valid token of " << record.Subject);
        RefreshQueue.push({.Key = key, .RefreshTime = record.RefreshTime});
    }

    template <typename TTokenRecord>
    void SetError(const TString& key, TTokenRecord& record, const TEvTicketParser::TError& error) {
        record.Error = error;
        TInstant now = TlsActivationContext->Now();
        if (record.Error.Retryable) {
            record.ExpireTime = GetExpireTime(record, now);
            record.SetErrorRefreshTime(this, now);
            CounterTicketsErrorsRetryable->Inc();
            BLOG_D("Ticket " << record.GetMaskedTicket() << " ("
                        << record.PeerName << ") has now retryable error message '" << error.Message << "'");
            if (record.RefreshRetryableErrorImmediately) {
                record.RefreshRetryableErrorImmediately = false;
                GetDerived()->CanRefreshTicket(key, record);
                Respond(record);
            }
        } else {
            record.UnsetToken();
            record.SetOkRefreshTime(this, now);
            CounterTicketsErrorsPermanent->Inc();
            BLOG_D("Ticket " << record.GetMaskedTicket() << " ("
                        << record.PeerName << ") has now permanent error message '" << error.Message << "'");
        }
        CounterTicketsErrors->Inc();
        RefreshQueue.push({.Key = key, .RefreshTime = record.RefreshTime});
    }

    void Respond(TTokenRecordBase& record) {
        if (record.IsTokenReady()) {
            for (const auto& request : record.AuthorizeRequests) {
                Send(request->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(record.Ticket, record.GetToken()), 0, request->Cookie);
            }
        } else {
            for (const auto& request : record.AuthorizeRequests) {
                Send(request->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(record.Ticket, record.Error), 0, request->Cookie);
            }
        }
        record.AuthorizeRequests.clear();
    }

    template <typename TTokenRecord>
    void SetTokenType(TTokenRecord& record, TStringBuf& ticket, const TStringBuf ticketType) {
        if (ticketType) {
            record.TokenType = GetDerived()->ParseTokenType(ticketType);
            switch (record.TokenType) {
                case TDerived::ETokenType::Unsupported:
                    record.Error.Message = "Token is not supported";
                    record.Error.Retryable = false;
                    break;
                case TDerived::ETokenType::Unknown:
                    record.Error.Message = "Unknown token";
                    record.Error.Retryable = false;
                    break;
                default:
                    break;
            }
        }
        if (record.Signature.AccessKeyId) {
            ticket = record.Signature.AccessKeyId;
            record.TokenType = TDerived::ETokenType::AccessService;
        }
    }

    static TString GetLoginProviderKeys(const NLogin::TLoginProvider& loginProvider) {
        TStringBuilder keys;
        for (const auto& [key, pubKey, privKey, expiresAt] : loginProvider.Keys) {
            if (!keys.empty()) {
                keys << ",";
            }
            keys << key;
        }
        return keys;
    }

    template <typename TTokenRecord>
    void SetToken(const TString& key, TTokenRecord& record, const NCloud::TEvUserAccountService::TEvGetUserAccountResponse::TPtr& ev) {
        SetToken(key, record, new NACLib::TUserToken({
                .OriginalUserToken = record.Ticket,
                .UserSID = ev->Get()->Response.yandex_passport_user_account().login() + "@" + UserAccountDomain,
                .AuthType = record.GetAuthType()
            }));
    }

    template <typename TTokenRecord>
    void ResetTokenRecord(TTokenRecord& record) {
        record.Subject.clear();
        record.InitTime = TlsActivationContext->Now();
    }

    template <typename TTokenRecord>
    bool CanRefreshAccessServiceTicket(const TTokenRecord& record) {
        if (!AccessServiceValidatorV1 && AccessServiceValidatorV2) {
            return false;
        }
        if (record.TokenType == TDerived::ETokenType::AccessService || record.TokenType == TDerived::ETokenType::ApiKey) {
            return (record.Error && record.Error.Retryable) || !record.Signature.AccessKeyId;
        }
        return record.TokenType == TDerived::ETokenType::Unknown;
    }

    template <typename TTokenRecord>
    bool CanRefreshLoginTicket(const TTokenRecord& record) {
        return record.TokenType == TDerived::ETokenType::Login && record.Error.empty();
    }

    template <typename TTokenRecord>
    bool RefreshTicketViaExternalAuthProvider(const TString& key, TTokenRecord& record) {
        const TExternalAuthInfo& externalAuthInfo = record.ExternalAuthInfo;
        if (externalAuthInfo.Type == Config.GetLdapAuthenticationDomain()) {
            if (Config.HasLdapAuthentication()) {
                Send(MakeLdapAuthProviderID(), new TEvLdapAuthProvider::TEvEnrichGroupsRequest(key, externalAuthInfo.Login));
                return true;
            }
            SetError(key, record, {.Message = "LdapAuthProvider is not initialized", .Retryable = false});
            return false;
        } else {
            SetError(key, record, {.Message = "Do not have suitable external auth provider"});
            return false;
        }
    }

    template <typename TTokenRecord>
    bool RefreshLoginTicket(const TString& key, TTokenRecord& record) {
        GetDerived()->ResetTokenRecord(record);
        const TString userSID = record.GetToken()->GetUserSID();
        if (record.IsExternalAuthEnabled()) {
            return RefreshTicketViaExternalAuthProvider(key, record);
        }
        const TString& database = Config.GetDomainLoginOnly() ? DomainName : record.Database;
        auto itLoginProvider = LoginProviders.find(database);
        if (itLoginProvider == LoginProviders.end()) {
            return false;
        }
        NLogin::TLoginProvider& loginProvider(itLoginProvider->second);
        if (loginProvider.CheckUserExists(userSID)) {
            const std::vector<TString> providerGroups = loginProvider.GetGroupsMembership(userSID);
            const TVector<NACLib::TSID> groups(providerGroups.begin(), providerGroups.end());
            SetToken(key, record, new NACLib::TUserToken({
                                    .OriginalUserToken = record.Ticket,
                                    .UserSID = userSID,
                                    .GroupSIDs = groups,
                                    .AuthType = record.GetAuthType()
                                }));
        } else {
            SetError(key, record, {.Message = "User not found", .Retryable = false});
        }
        return true;
    }

    template <typename TTokenRecord>
    bool CanRefreshTicket(const TString& key, TTokenRecord& record) {
        if (CanRefreshLoginTicket(record)) {
            return RefreshLoginTicket(key, record);
        }
        if (CanRefreshAccessServiceTicket(record)) {
            GetDerived()->ResetTokenRecord(record);
            if (record.Permissions) {
                RequestAccessServiceAuthorization(key, record);
            } else {
                RequestAccessServiceAuthentication(key, record);
            }
            return true;
        }
        return false;
    }

    template <typename TTokenRecord>
    bool RefreshTicket(const TString& key, TTokenRecord& record) {
        if (record.ResponsesLeft == 0 && record.AuthorizeRequests.empty()) {
            if (GetDerived()->CanRefreshTicket(key, record)) {
                return true;
            }
        }
        record.RefreshTime = GetRefreshTime(TlsActivationContext->Now());
        return false;
    }

    static TStringBuf HtmlBool(bool v) {
        return v ? "<span style='font-weight:bold'>&#x2611;</span>" : "<span style='font-weight:bold'>&#x2610;</span>";
    }

    template <typename TTokenRecord>
    static void WriteTokenRecordInfo(TStringBuilder& html, const TTokenRecord& record) {
        html << "<tr><td>Database</td><td>" << record.Database << "</td></tr>";
        html << "<tr><td>Subject</td><td>" << record.Subject << "</td></tr>";
        html << "<tr><td>Additional SIDs</td><td>" << JoinStrings(record.AdditionalSIDs.begin(), record.AdditionalSIDs.end(), ", ") << "</td></tr>";
        html << "<tr><td>Error</td><td>" << record.Error << "</td></tr>";
        html << "<tr><td>Requests Infly</td><td>" << record.AuthorizeRequests.size() << "</td></tr>";
        html << "<tr><td>Responses Left</td><td>" << record.ResponsesLeft << "</td></tr>";
        html << "<tr><td>Refresh Time</td><td>" << record.RefreshTime << "</td></tr>";
        html << "<tr><td>Expire Time</td><td>" << record.ExpireTime << "</td></tr>";
        html << "<tr><td>Access Time</td><td>" << record.AccessTime << "</td></tr>";
        html << "<tr><td>Peer Name</td><td>" << record.PeerName << "</td></tr>";
        if (record.IsTokenReady()) {
            html << "<tr><td>User SID</td><td>" << record.GetToken()->GetUserSID() << "</td></tr>";
            for (const TString& group : record.GetToken()->GetGroupSIDs()) {
                html << "<tr><td>Group SID</td><td>" << group << "</td></tr>";
            }
        }
        for (const auto& [permissionName, perm] : record.Permissions) {
            html << "<tr><td></td><td></td></tr>";
            html << "<tr><td>Permission \"" << permissionName << (perm.Required ? " (required)" : "") << "\"</td><td>" << (perm.Error ? "Error:" + perm.Error.ToString() : perm.Subject) << "</td></tr>";
            for (const auto& [attributeName, value] : perm.Attributes) {
                html << "<tr><td>Attribute \"" << attributeName << "\"</td><td>" << value << "</td></tr>";
            }
        }
    }

    void WriteAuthorizeMethods(TStringBuilder& html) {
        html << "<tr><td>Login</td><td>" << HtmlBool(UseLoginProvider) << "</td></tr>";
        html << "<tr><td>Access Service</td><td>" << HtmlBool((bool)AccessServiceValidatorV1 && (bool)AccessServiceValidatorV2) << "</td></tr>";
        html << "<tr><td>User Account Service</td><td>" << HtmlBool((bool)UserAccountService) << "</td></tr>";
        html << "<tr><td>Service Account Service</td><td>" << HtmlBool((bool)ServiceAccountService) << "</td></tr>";
        html << "<tr><td>Nebius Access Service</td><td>" << HtmlBool((bool)NebiusAccessServiceValidator) << "</td></tr>";
    }

    template <typename TTokenRecord>
    static void WriteTokenRecordValues(TStringBuilder& html, const TString& key, const TTokenRecord& record) {
        html << "<tr>";
        html << "<td>" << record.GetMaskedTicket() << "</td>";
        html << "<td>" << record.Database << "</td>";
        html << "<td>" << record.Subject << "</td>";
        html << "<td>" << record.Error << "</td>";
        html << "<td>" << "<a href='ticket_parser?token=" << MD5::Calc(key) << "'>" << HtmlBool(record.IsTokenReady()) << "</a>" << "</td>";
        html << "<td>" << record.AuthorizeRequests.size() << "</td>";
        html << "<td>" << record.ResponsesLeft << "</td>";
        html << "<td>" << record.RefreshTime << "</td>";
        html << "<td>" << record.ExpireTime << "</td>";
        html << "<td>" << record.AccessTime << "</td>";
        html << "<td>" << record.PeerName << "</td>";
        html << "</tr>";
    }

    void InitCounters(::NMonitoring::TDynamicCounterPtr counters) {
        CounterTicketsReceived = counters->GetCounter("TicketsReceived", true);
        CounterTicketsSuccess = counters->GetCounter("TicketsSuccess", true);
        CounterTicketsErrors = counters->GetCounter("TicketsErrors", true);
        CounterTicketsErrorsRetryable = counters->GetCounter("TicketsErrorsRetryable", true);
        CounterTicketsErrorsPermanent = counters->GetCounter("TicketsErrorsPermanent", true);
        CounterTicketsBuiltin = counters->GetCounter("TicketsBuiltin", true);
        CounterTicketsCertificate = counters->GetCounter("TicketsCertificate", true);
        CounterTicketsLogin = counters->GetCounter("TicketsLogin", true);
        CounterTicketsAS = counters->GetCounter("TicketsAS", true);
        CounterTicketsCacheHit = counters->GetCounter("TicketsCacheHit", true);
        CounterTicketsCacheMiss = counters->GetCounter("TicketsCacheMiss", true);
        CounterTicketsBuildTime = counters->GetHistogram("TicketsBuildTimeMs",
                                                         NMonitoring::ExplicitHistogram({0, 1, 5, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 60000}));
    }

    void FillAccessServiceSettings(NGrpcActorClient::TGrpcClientSettings& settings) {
        settings.Endpoint = Config.GetAccessServiceEndpoint();
        if (Config.GetUseAccessServiceTLS()) {
            settings.CertificateRootCA = TUnbufferedFileInput(Config.GetPathToRootCA()).ReadAll();
        }
        settings.GrpcKeepAliveTimeMs = Config.GetAccessServiceGrpcKeepAliveTimeMs();
        settings.GrpcKeepAliveTimeoutMs = Config.GetAccessServiceGrpcKeepAliveTimeoutMs();
    }

    void InitAuthProvider() {
        AccessServiceDomain = Config.GetAccessServiceDomain();
        UserAccountDomain = Config.GetUserAccountDomain();
        ServiceDomain = Config.GetServiceDomain();

        if (Config.GetUseAccessService()) {
            if (Config.GetAccessServiceType() == "Yandex_v2") {
                NCloud::TAccessServiceSettings settings;
                FillAccessServiceSettings(settings);

                AccessServiceValidatorV1 = Register(NCloud::CreateAccessServiceV1(settings), TMailboxType::HTSwap, AppData()->UserPoolId);
                if (Config.GetCacheAccessServiceAuthentication()) {
                    AccessServiceValidatorV1 = Register(NGrpcActorClient::CreateGrpcServiceCache<NCloud::TEvAccessService::TEvAuthenticateRequest, NCloud::TEvAccessService::TEvAuthenticateResponse>(
                                                            AccessServiceValidatorV1,
                                                            Config.GetGrpcCacheSize(),
                                                            TDuration::MilliSeconds(Config.GetGrpcSuccessLifeTime()),
                                                            TDuration::MilliSeconds(Config.GetGrpcErrorLifeTime())), TMailboxType::HTSwap, AppData()->UserPoolId);
                }
                if (Config.GetCacheAccessServiceAuthorization()) {
                    AccessServiceValidatorV1 = Register(NGrpcActorClient::CreateGrpcServiceCache<NCloud::TEvAccessService::TEvAuthorizeRequest, NCloud::TEvAccessService::TEvAuthorizeResponse>(
                                                            AccessServiceValidatorV1,
                                                            Config.GetGrpcCacheSize(),
                                                            TDuration::MilliSeconds(Config.GetGrpcSuccessLifeTime()),
                                                            TDuration::MilliSeconds(Config.GetGrpcErrorLifeTime())), TMailboxType::HTSwap, AppData()->UserPoolId);
                }

                AccessServiceValidatorV2 = Register(NCloud::CreateAccessServiceV2(settings), TMailboxType::HTSwap, AppData()->UserPoolId);
            } else if (Config.GetAccessServiceType() == "Nebius_v1") {
                NNebiusCloud::TAccessServiceSettings settings;
                FillAccessServiceSettings(settings);
                NebiusAccessServiceValidator = Register(NNebiusCloud::CreateAccessServiceV1(settings), TMailboxType::HTSwap, AppData()->UserPoolId);
            } else {
                Y_ABORT("Unknown AccessServiceType setting: \"%s\"", Config.GetAccessServiceType().c_str());
            }
        }

        if (Config.GetUseUserAccountService()) {
            NCloud::TUserAccountServiceSettings settings;
            settings.Endpoint = Config.GetUserAccountServiceEndpoint();
            if (Config.GetUseUserAccountServiceTLS()) {
                settings.CertificateRootCA = TUnbufferedFileInput(Config.GetPathToRootCA()).ReadAll();
            }
            UserAccountService = Register(CreateUserAccountService(settings), TMailboxType::HTSwap, AppData()->UserPoolId);
            if (Config.GetCacheUserAccountService()) {
                UserAccountService = Register(NGrpcActorClient::CreateGrpcServiceCache<NCloud::TEvUserAccountService::TEvGetUserAccountRequest, NCloud::TEvUserAccountService::TEvGetUserAccountResponse>(
                                                      UserAccountService,
                                                      Config.GetGrpcCacheSize(),
                                                      TDuration::MilliSeconds(Config.GetGrpcSuccessLifeTime()),
                                                      TDuration::MilliSeconds(Config.GetGrpcErrorLifeTime())), TMailboxType::HTSwap, AppData()->UserPoolId);
            }
        }

        if (Config.GetUseServiceAccountService()) {
            NCloud::TServiceAccountServiceSettings settings;
            settings.Endpoint = Config.GetServiceAccountServiceEndpoint();
            if (Config.GetUseServiceAccountServiceTLS()) {
                settings.CertificateRootCA = TUnbufferedFileInput(Config.GetPathToRootCA()).ReadAll();
            }
            ServiceAccountService = Register(NCloud::CreateServiceAccountService(settings), TMailboxType::HTSwap, AppData()->UserPoolId);
            if (Config.GetCacheServiceAccountService()) {
                ServiceAccountService = Register(NGrpcActorClient::CreateGrpcServiceCache<NCloud::TEvServiceAccountService::TEvGetServiceAccountRequest, NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse>(
                                                         ServiceAccountService,
                                                         Config.GetGrpcCacheSize(),
                                                         TDuration::MilliSeconds(Config.GetGrpcSuccessLifeTime()),
                                                         TDuration::MilliSeconds(Config.GetGrpcErrorLifeTime())), TMailboxType::HTSwap, AppData()->UserPoolId);
            }
        }

        if (Config.GetUseLoginProvider()) {
            UseLoginProvider = true;
        }
    }

    void InitTime() {
        RefreshPeriod = TDuration::Parse(Config.GetRefreshPeriod());
        RefreshTime = TDuration::Parse(Config.GetRefreshTime());
        MinErrorRefreshTime = TDuration::Parse(Config.GetMinErrorRefreshTime());
        MaxErrorRefreshTime = TDuration::Parse(Config.GetMaxErrorRefreshTime());
        LifeTime = TDuration::Parse(Config.GetLifeTime());
        ExpireTime = TDuration::Parse(Config.GetExpireTime());
        AsSignatureExpireTime = TDuration::Parse(Config.GetAsSignatureExpireTime());
    }

    void PassAway() override {
        if (AccessServiceValidatorV1) {
            Send(AccessServiceValidatorV1, new TEvents::TEvPoisonPill);
        }
        if (AccessServiceValidatorV2) {
            Send(AccessServiceValidatorV2, new TEvents::TEvPoisonPill);
        }
        if (UserAccountService) {
            Send(UserAccountService, new TEvents::TEvPoisonPill);
        }
        if (ServiceAccountService) {
            Send(ServiceAccountService, new TEvents::TEvPoisonPill);
        }
        if (NebiusAccessServiceValidator) {
            Send(NebiusAccessServiceValidator, new TEvents::TEvPoisonPill);
        }
        TBase::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::TICKET_PARSER_ACTOR; }

    void Bootstrap() {
        TIntrusivePtr<NMonitoring::TDynamicCounters> rootCounters = AppData()->Counters;
        TIntrusivePtr<NMonitoring::TDynamicCounters> authCounters = GetServiceCounters(rootCounters, "auth");
        NMonitoring::TDynamicCounterPtr counters = authCounters->GetSubgroup("subsystem", "TicketParser");
        GetDerived()->InitCounters(counters);

        GetDerived()->InitAuthProvider();
        if (AppData() && AppData()->DomainsInfo && AppData()->DomainsInfo->Domain) {
            DomainName = "/" + AppData()->DomainsInfo->GetDomain()->Name;
        }

        GetDerived()->InitTime();

        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "ticket_parser", "Ticket Parser", false, TActivationContext::ActorSystem(), this->SelfId());
        }

        Schedule(RefreshPeriod, new NActors::TEvents::TEvWakeup());
        TBase::Become(&TDerived::StateWork);
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTicketParser::TEvAuthorizeTicket, Handle);
            hFunc(TEvTicketParser::TEvRefreshTicket, Handle);
            hFunc(TEvTicketParser::TEvDiscardTicket, Handle);
            hFunc(TEvTicketParser::TEvUpdateLoginSecurityState, Handle);
            hFunc(TEvLdapAuthProvider::TEvEnrichGroupsResponse, Handle);
            hFunc(NCloud::TEvAccessService::TEvAuthenticateResponse, Handle);
            hFunc(NCloud::TEvAccessService::TEvAuthorizeResponse, Handle);
            hFunc(NCloud::TEvAccessService::TEvBulkAuthorizeResponse, Handle);
            hFunc(NCloud::TEvUserAccountService::TEvGetUserAccountResponse, Handle);
            hFunc(NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse, Handle);
            hFunc(NNebiusCloud::TEvAccessService::TEvAuthenticateResponse, Handle);
            hFunc(NNebiusCloud::TEvAccessService::TEvAuthorizeResponse, Handle);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleRefresh);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }

    static TString ReadFile(const TString& fileName) {
        TFileInput f(fileName);
        return f.ReadAll();
    }

    TTicketParserImpl(const TTicketParserSettings& settings)
        : Config(settings.AuthConfig)
        , CertificateChecker(settings.CertificateAuthValues)
    {}
};

}
