#pragma once
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/digest/md5/md5.h>
#include <ydb/library/ycloud/api/access_service.h>
#include <ydb/library/ycloud/api/user_account_service.h>
#include <ydb/library/ycloud/api/service_account_service.h>
#include <ydb/library/ycloud/impl/user_account_service.h>
#include <ydb/library/ycloud/impl/service_account_service.h>
#include <ydb/library/ycloud/impl/access_service.h>
#include <ydb/library/ycloud/impl/grpc_service_cache.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/mon/mon.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/library/security/util.h>
#include <util/string/vector.h>
#include <util/generic/queue.h>
#include "ticket_parser_log.h"

namespace NKikimr {

template <typename TDerived>
class TTicketParserImpl : public TActorBootstrapped<TDerived> {
    using TThis = TTicketParserImpl;
    using TBase = TActorBootstrapped<TDerived>;

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

    static bool IsRetryableGrpcError(const NGrpc::TGrpcStatus& status) {
        switch (status.GRpcStatusCode) {
        case grpc::StatusCode::UNAUTHENTICATED:
        case grpc::StatusCode::PERMISSION_DENIED:
        case grpc::StatusCode::INVALID_ARGUMENT:
        case grpc::StatusCode::NOT_FOUND:
            return false;
        }
        return true;
    }

    struct TPermissionRecord {
        TString Subject;
        bool Required = false;
        yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase SubjectType;
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
    using TEvAccessServiceGetUserAccountRequest = TEvRequestWithKey<NCloud::TEvUserAccountService::TEvGetUserAccountRequest>;
    using TEvAccessServiceGetServiceAccountRequest = TEvRequestWithKey<NCloud::TEvServiceAccountService::TEvGetServiceAccountRequest>;

    TString DomainName;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsReceived;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsSuccess;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsErrors;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsErrorsRetryable;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsErrorsPermanent;
    ::NMonitoring::TDynamicCounters::TCounterPtr CounterTicketsBuiltin;
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

    TActorId AccessServiceValidator;
    TActorId UserAccountService;
    TActorId ServiceAccountService;
    TString UserAccountDomain;
    TString AccessServiceDomain;
    TString ServiceDomain;

    struct TTokenRefreshRecord {
        TString Key;
        TInstant RefreshTime;

        bool operator <(const TTokenRefreshRecord& o) const {
            return RefreshTime > o.RefreshTime;
        }
    };

    TPriorityQueue<TTokenRefreshRecord> RefreshQueue;
    std::unordered_map<TString, NLogin::TLoginProvider> LoginProviders;
    bool UseLoginProvider = false;

    TInstant GetExpireTime(TInstant now) const {
        return now + ExpireTime;
    }

    TInstant GetRefreshTime(TInstant now) const {
        return now + RefreshTime;
    }

    TDuration GetLifeTime() const {
        return LifeTime;
    }

    template <typename TTokenRecord>
    void RequestAccessServiceAuthorization(const TString& key, TTokenRecord& record) const {
        for (const auto& [perm, permRecord] : record.Permissions) {
            const TString& permission(perm);
            BLOG_TRACE("Ticket " << MaskTicket(record.Ticket)
                        << " asking for AccessServiceAuthorization(" << permission << ")");
            THolder<TEvAccessServiceAuthorizeRequest> request = MakeHolder<TEvAccessServiceAuthorizeRequest>(key);
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
                request->Request.set_iam_token(record.Ticket);
            }
            request->Request.set_permission(permission);

            if (const auto databaseId = record.GetAttributeValue(permission, "database_id"); databaseId) {
                auto* resourcePath = request->Request.add_resource_path();
                resourcePath->set_id(databaseId);
                resourcePath->set_type("ydb.database");
            } else if (const auto serviceAccountId = record.GetAttributeValue(permission, "service_account_id"); serviceAccountId) {
                auto* resourcePath = request->Request.add_resource_path();
                resourcePath->set_id(serviceAccountId);
                resourcePath->set_type("iam.serviceAccount");
            }

            if (const auto folderId = record.GetAttributeValue(permission, "folder_id"); folderId) {
                auto* resourcePath = request->Request.add_resource_path();
                resourcePath->set_id(folderId);
                resourcePath->set_type("resource-manager.folder");
            }

            if (const auto cloudId = record.GetAttributeValue(permission, "cloud_id"); cloudId) {
                auto* resourcePath = request->Request.add_resource_path();
                resourcePath->set_id(cloudId);
                resourcePath->set_type("resource-manager.cloud");
            }

            record.ResponsesLeft++;
            Send(AccessServiceValidator, request.Release());
        }
    }

    template <typename TTokenRecord>
    void RequestAccessServiceAuthentication(const TString& key, TTokenRecord& record) const {
        BLOG_TRACE("Ticket " << MaskTicket(record.Ticket)
                    << " asking for AccessServiceAuthentication");
        THolder<TEvAccessServiceAuthenticateRequest> request = MakeHolder<TEvAccessServiceAuthenticateRequest>(key);
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
            request->Request.set_iam_token(record.Ticket);
        }
        record.ResponsesLeft++;
        Send(AccessServiceValidator, request.Release());
    }


    TString GetSubjectName(const yandex::cloud::priv::servicecontrol::v1::Subject& subject) {
        switch (subject.type_case()) {
        case yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::kUserAccount:
            return subject.user_account().id() + "@" + AccessServiceDomain;
        case yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::kServiceAccount:
            return subject.service_account().id() + "@" + AccessServiceDomain;
        case yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::kAnonymousAccount:
            return "anonymous" "@" + AccessServiceDomain;
        default:
            return "Unknown subject type";
        }
    }

    template <typename TTokenRecord>
    bool CanInitBuiltinToken(const TString& key, TTokenRecord& record) {
        if (record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::Builtin) {
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
                        TEvTicketParser::TError error;
                        error.Message = response.Error;
                        error.Retryable = response.ErrorRetryable;
                        SetError(key, record, error);
                        CounterTicketsLogin->Inc();
                        return true;
                    }
                } else {
                    record.TokenType = TDerived::ETokenType::Login;
                    TVector<NACLib::TSID> groups;
                    if (response.Groups.has_value()) {
                        const std::vector<TString>& tokenGroups = response.Groups.value();
                        groups.assign(tokenGroups.begin(), tokenGroups.end());
                    } else {
                        const std::vector<TString> providerGroups = loginProvider.GetGroupsMembership(response.User);
                        groups.assign(providerGroups.begin(), providerGroups.end());
                    }
                    record.ExpireTime = ToInstant(response.ExpiresAt);
                    SetToken(key, record, new NACLib::TUserToken({
                        .OriginalUserToken = record.Ticket,
                        .UserSID = response.User,
                        .GroupSIDs = groups,
                        .AuthType = record.GetAuthType()
                    }));
                    CounterTicketsLogin->Inc();
                    return true;
                }
            } else {
                if (record.TokenType == TDerived::ETokenType::Login) {
                    TEvTicketParser::TError error;
                    error.Message = "Login state is not available yet";
                    error.Retryable = false;
                    SetError(key, record, error);
                    CounterTicketsLogin->Inc();
                    return true;
                }
            }
        }
        return false;
    }

    void CrackTicket(const TString& ticketBody, TStringBuf& ticket, TStringBuf& ticketType) {
        ticket = ticketBody;
        ticketType = ticket.NextTok(' ');
        if (ticket.empty()) {
            ticket = ticketBody;
            ticketType.Clear();
        }
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
        TStringBuf ticket;
        TStringBuf ticketType;
        CrackTicket(ev->Get()->Ticket, ticket, ticketType);

        TString key = GetKey(ev->Get());
        TActorId sender = ev->Sender;
        ui64 cookie = ev->Cookie;

        CounterTicketsReceived->Inc();
        const auto& signature = ev->Get()->Signature;
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
            BLOG_ERROR("Ticket " << MaskTicket(ticket) << ": " << record.Error);
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

    void Handle(NCloud::TEvAccessService::TEvAuthenticateResponse::TPtr& ev) {
        NCloud::TEvAccessService::TEvAuthenticateResponse* response = ev->Get();
        TEvAccessServiceAuthenticateRequest* request = response->Request->Get<TEvAccessServiceAuthenticateRequest>();
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
                switch (response->Response.subject().type_case()) {
                case yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::kUserAccount:
                case yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::kServiceAccount:
                case yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::kAnonymousAccount:
                    record.Subject = GetSubjectName(response->Response.subject());
                    break;
                default:
                    record.Subject.clear();
                    SetError(key, record, {"Unknown subject type", false});
                    break;
                }
                record.TokenType = TDerived::ETokenType::AccessService;
                if (!record.Subject.empty()) {
                    switch (response->Response.subject().type_case()) {
                    case yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::kUserAccount:
                        if (UserAccountService) {
                            BLOG_TRACE("Ticket " << MaskTicket(record.Ticket)
                                        << " asking for UserAccount(" << record.Subject << ")");
                            THolder<TEvAccessServiceGetUserAccountRequest> request = MakeHolder<TEvAccessServiceGetUserAccountRequest>(key);
                            request->Token = record.Ticket;
                            request->Request.set_user_account_id(TString(TStringBuf(record.Subject).NextTok('@')));
                            Send(UserAccountService, request.Release());
                            record.ResponsesLeft++;
                            return;
                        }
                        break;
                    case yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::kServiceAccount:
                        if (ServiceAccountService) {
                            BLOG_TRACE("Ticket " << MaskTicket(record.Ticket)
                                        << " asking for ServiceAccount(" << record.Subject << ")");
                            THolder<TEvAccessServiceGetServiceAccountRequest> request = MakeHolder<TEvAccessServiceGetServiceAccountRequest>(key);
                            request->Token = record.Ticket;
                            request->Request.set_service_account_id(TString(TStringBuf(record.Subject).NextTok('@')));
                            Send(ServiceAccountService, request.Release());
                            record.ResponsesLeft++;
                            return;
                        }
                        break;
                    default:
                        break;
                    }
                    SetToken(key, record, new NACLib::TUserToken({
                        .OriginalUserToken = record.Ticket,
                        .UserSID = record.Subject,
                        .AuthType = record.GetAuthType()
                    }));
                }
            } else {
                if (record.ResponsesLeft == 0 && (record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::AccessService)) {
                    bool retryable = IsRetryableGrpcError(response->Status);
                    SetError(key, record, {response->Status.Msg, retryable});
                }
            }
            if (record.ResponsesLeft == 0) {
                Respond(record);
            }
        }
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
            TString subject;
            yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase subjectType = yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::TYPE_NOT_SET;
            auto itPermission = record.Permissions.find(permission);
            if (itPermission != record.Permissions.end()) {
                if (response->Status.Ok()) {
                    subject = GetSubjectName(response->Response.subject());
                    subjectType = response->Response.subject().type_case();
                    itPermission->second.Subject = subject;
                    itPermission->second.SubjectType = subjectType;
                    itPermission->second.Error.clear();
                    BLOG_TRACE("Ticket "
                                << MaskTicket(record.Ticket)
                                << " permission "
                                << permission
                                << " now has a valid subject \""
                                << subject
                                << "\"");
                } else {
                    bool retryable = IsRetryableGrpcError(response->Status);
                    itPermission->second.Error = {response->Status.Msg, retryable};
                    if (itPermission->second.Subject.empty() || !retryable) {
                        itPermission->second.Subject.clear();
                        BLOG_TRACE("Ticket "
                                    << MaskTicket(record.Ticket)
                                    << " permission "
                                    << permission
                                    << " now has a permanent error \""
                                    << itPermission->second.Error
                                    << "\" "
                                    << " retryable:"
                                    << retryable);
                    } else if (retryable) {
                        BLOG_TRACE("Ticket "
                                    << MaskTicket(record.Ticket)
                                    << " permission "
                                    << permission
                                    << " now has a retryable error \""
                                    << response->Status.Msg
                                    << "\"");
                    }
                }
            } else {
                BLOG_W("Received response for unknown permission " << permission << " for ticket " << MaskTicket(record.Ticket));
            }
            if (--record.ResponsesLeft == 0) {
                ui32 permissionsOk = 0;
                ui32 retryableErrors = 0;
                bool requiredPermissionFailed = false;
                TEvTicketParser::TError error;
                for (const auto& [permission, rec] : record.Permissions) {
                    if (rec.IsPermissionOk()) {
                        ++permissionsOk;
                        if (subject.empty()) {
                            subject = rec.Subject;
                            subjectType = rec.SubjectType;
                        }
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
                    record.TokenType = TDerived::ETokenType::AccessService;
                    switch (subjectType) {
                    case yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::kUserAccount:
                        if (UserAccountService) {
                            BLOG_TRACE("Ticket " << MaskTicket(record.Ticket)
                                        << " asking for UserAccount(" << subject << ")");
                            THolder<TEvAccessServiceGetUserAccountRequest> request = MakeHolder<TEvAccessServiceGetUserAccountRequest>(key);
                            request->Token = record.Ticket;
                            request->Request.set_user_account_id(TString(TStringBuf(subject).NextTok('@')));
                            Send(UserAccountService, request.Release());
                            record.ResponsesLeft++;
                            return;
                        }
                        break;
                    case yandex::cloud::priv::servicecontrol::v1::Subject::TypeCase::kServiceAccount:
                        if (ServiceAccountService) {
                            BLOG_TRACE("Ticket " << MaskTicket(record.Ticket)
                                        << " asking for ServiceAccount(" << subject << ")");
                            THolder<TEvAccessServiceGetServiceAccountRequest> request = MakeHolder<TEvAccessServiceGetServiceAccountRequest>(key);
                            request->Token = record.Ticket;
                            request->Request.set_service_account_id(TString(TStringBuf(subject).NextTok('@')));
                            Send(ServiceAccountService, request.Release());
                            record.ResponsesLeft++;
                            return;
                        }
                        break;
                    default:
                        break;
                    }
                    SetToken(request->Key, record, new NACLib::TUserToken(record.Ticket, subject, {}));
                } else if (record.ResponsesLeft == 0 && (record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::AccessService)) {
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
                BLOG_D("Refreshing ticket " << MaskTicket(record.Ticket));
                if (!RefreshTicket(key, record)) {
                    RefreshQueue.push({key, record.RefreshTime});
                }
            } else {
                BLOG_D("Expired ticket " << MaskTicket(record.Ticket));
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
                    html << "<tr><td>Ticket</td><td>" << MaskTicket(record.Ticket) << "</td></tr>";
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
            html << "<th>UID</th>";
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
                html << "<tr>";
                html << "<td>" << MaskTicket(record.Ticket) << "</td>";
                TDerived::WriteTokenRecordValues(html, key, record);
                html << "</tr>";
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
    using IActorOps::Register;
    using IActorOps::Send;
    using IActorOps::Schedule;

    NKikimrProto::TAuthConfig Config;
    TDuration ExpireTime = TDuration::Hours(24); // after what time ticket will expired and removed from cache

    auto ParseTokenType(const TStringBuf tokenType) const {
        if (tokenType == "Login") {
            if (UseLoginProvider) {
                return TDerived::ETokenType::Login;
            } else {
                return TDerived::ETokenType::Unsupported;
            }
        }
        if (tokenType == "Bearer" || tokenType == "IAM") {
            if (AccessServiceValidator) {
                return TDerived::ETokenType::AccessService;
            } else {
                return TDerived::ETokenType::Unsupported;
            }
        }
        return TDerived::ETokenType::Unknown;
    }

    class TTokenRecordBase {
    private:
        TIntrusiveConstPtr<NACLib::TUserToken> Token;
    public:
        TTokenRecordBase(const TTokenRecordBase&) = delete;
        TTokenRecordBase& operator =(const TTokenRecordBase&) = delete;

        TString Ticket;
        typename TDerived::ETokenType TokenType = TDerived::ETokenType::Unknown;
        NKikimr::TEvTicketParser::TEvAuthorizeTicket::TAccessKeySignature Signature;
        THashMap<TString, TPermissionRecord> Permissions;
        TString Subject; // login
        TEvTicketParser::TError Error;
        TDeque<THolder<TEventHandle<TEvTicketParser::TEvAuthorizeTicket>>> AuthorizeRequests;
        ui64 ResponsesLeft = 0;
        TInstant InitTime;
        TInstant RefreshTime;
        TInstant ExpireTime;
        TInstant AccessTime;
        TDuration CurrentMaxRefreshTime = TDuration::Seconds(1);
        TDuration CurrentMinRefreshTime = TDuration::Seconds(1);
        TString PeerName;
        TString Database;
        TStackVec<TString> AdditionalSIDs;

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
                if (CurrentMaxRefreshTime < ticketParser->MaxErrorRefreshTime) {
                    CurrentMaxRefreshTime += ticketParser->MinErrorRefreshTime;
                }
                CurrentMinRefreshTime = ticketParser->MinErrorRefreshTime;
            } else {
                CurrentMaxRefreshTime = ticketParser->RefreshTime;
                CurrentMinRefreshTime = CurrentMaxRefreshTime / 2;
            }
            SetRefreshTime(now);
        }

        template <typename T>
        void SetOkRefreshTime(TTicketParserImpl<T>* ticketParser, TInstant now) {
            CurrentMaxRefreshTime = ticketParser->RefreshTime;
            CurrentMinRefreshTime = CurrentMaxRefreshTime / 2;
            SetRefreshTime(now);
        }

        void SetRefreshTime(TInstant now) {
            if (CurrentMinRefreshTime < CurrentMaxRefreshTime) {
                TDuration currentDuration = CurrentMaxRefreshTime - CurrentMinRefreshTime;
                TDuration refreshDuration = CurrentMinRefreshTime + TDuration::MilliSeconds(RandomNumber<double>() * currentDuration.MilliSeconds());
                RefreshTime = now + refreshDuration;
            } else {
                RefreshTime = now + CurrentMinRefreshTime;
            }
        }

        TString GetSubject() const {
            return Subject;
        }

        TString GetAuthType() const {
            switch (TokenType) {
                case TDerived::ETokenType::Unknown:
                    return "Unknown";
                case TDerived::ETokenType::Unsupported:
                    return "Unsupported";
                case TDerived::ETokenType::Builtin:
                    return "Builtin";
                case TDerived::ETokenType::Login:
                    return "Login";
                case TDerived::ETokenType::AccessService:
                    return "AccessService";
            }
        }

        bool NeedsRefresh() const {
            switch (TokenType) {
                case TDerived::ETokenType::Builtin:
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
    };

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
        sids.emplace_back(permission + '-' + record.GetAttributeValue(permission, "database_id") + '@' + AccessServiceDomain);
    }

    template <typename TTokenRecord>
    void InitTokenRecord(const TString& key, TTokenRecord& record, TInstant) {
        if (GetDerived()->CanInitAccessServiceToken(record)) {
            if (AccessServiceValidator) {
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
        return record.TokenType == TDerived::ETokenType::Unknown || record.TokenType == TDerived::ETokenType::AccessService;
    }

    template <typename TTokenRecord>
    void InitTokenRecord(const TString& key, TTokenRecord& record) {
        TInstant now = TlsActivationContext->Now();
        record.InitTime = now;
        record.AccessTime = now;
        record.ExpireTime = GetExpireTime(now);
        record.RefreshTime = GetRefreshTime(now);

        if (record.Error) {
            return;
        }

        if (CanInitBuiltinToken(key, record) ||
            CanInitLoginToken(key, record)) {
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
        if (!record.ExpireTime) {
            record.ExpireTime = GetExpireTime(now);
        }
        if (record.NeedsRefresh()) {
            record.SetOkRefreshTime(this, now);
        } else {
            record.RefreshTime = record.ExpireTime;
        }
        CounterTicketsSuccess->Inc();
        CounterTicketsBuildTime->Collect((now - record.InitTime).MilliSeconds());
        BLOG_D("Ticket " << MaskTicket(record.Ticket) << " ("
                    << record.PeerName << ") has now valid token of " << record.Subject);
        RefreshQueue.push({.Key = key, .RefreshTime = record.RefreshTime});
    }

    template <typename TTokenRecord>
    void SetError(const TString& key, TTokenRecord& record, const TEvTicketParser::TError& error) {
        record.Error = error;
        TInstant now = TlsActivationContext->Now();
        if (record.Error.Retryable) {
            record.ExpireTime = GetExpireTime(now);
            record.SetErrorRefreshTime(this, now);
            CounterTicketsErrorsRetryable->Inc();
            BLOG_D("Ticket " << MaskTicket(record.Ticket) << " ("
                        << record.PeerName << ") has now retryable error message '" << error.Message << "'");
        } else {
            record.UnsetToken();
            record.SetOkRefreshTime(this, now);
            CounterTicketsErrorsPermanent->Inc();
            BLOG_D("Ticket " << MaskTicket(record.Ticket) << " ("
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
        if (!AccessServiceValidator) {
            return false;
        }
        if (record.TokenType == TDerived::ETokenType::AccessService) {
            return (record.Error && record.Error.Retryable) || !record.Signature.AccessKeyId;
        }
        return record.TokenType == TDerived::ETokenType::Unknown;
    }

    template <typename TTokenRecord>
    bool CanRefreshLoginTicket(const TTokenRecord& record) {
        return record.TokenType == TDerived::ETokenType::Login && record.Error.empty();
    }

    template <typename TTokenRecord>
    bool RefreshLoginTicket(const TString& key, TTokenRecord& record) {
        GetDerived()->ResetTokenRecord(record);
        const TString& database = Config.GetDomainLoginOnly() ? DomainName : record.Database;
        auto itLoginProvider = LoginProviders.find(database);
        if (itLoginProvider == LoginProviders.end()) {
            return false;
        }
        NLogin::TLoginProvider& loginProvider(itLoginProvider->second);
        const TString userSID = record.GetToken()->GetUserSID();
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
            TEvTicketParser::TError error;
            error.Message = "User not found";
            error.Retryable = false;
            SetError(key, record, error);
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
        html << "<tr><td>Access Service</td><td>" << HtmlBool((bool)AccessServiceValidator) << "</td></tr>";
        html << "<tr><td>User Account Service</td><td>" << HtmlBool((bool)UserAccountService) << "</td></tr>";
        html << "<tr><td>Service Account Service</td><td>" << HtmlBool((bool)ServiceAccountService) << "</td></tr>";
    }

    template <typename TTokenRecord>
    static void WriteTokenRecordValues(TStringBuilder& html, const TString& key, const TTokenRecord& record) {
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
    }

    void InitCounters(::NMonitoring::TDynamicCounterPtr counters) {
        CounterTicketsReceived = counters->GetCounter("TicketsReceived", true);
        CounterTicketsSuccess = counters->GetCounter("TicketsSuccess", true);
        CounterTicketsErrors = counters->GetCounter("TicketsErrors", true);
        CounterTicketsErrorsRetryable = counters->GetCounter("TicketsErrorsRetryable", true);
        CounterTicketsErrorsPermanent = counters->GetCounter("TicketsErrorsPermanent", true);
        CounterTicketsBuiltin = counters->GetCounter("TicketsBuiltin", true);
        CounterTicketsLogin = counters->GetCounter("TicketsLogin", true);
        CounterTicketsAS = counters->GetCounter("TicketsAS", true);
        CounterTicketsCacheHit = counters->GetCounter("TicketsCacheHit", true);
        CounterTicketsCacheMiss = counters->GetCounter("TicketsCacheMiss", true);
        CounterTicketsBuildTime = counters->GetHistogram("TicketsBuildTimeMs",
                                                         NMonitoring::ExplicitHistogram({0, 1, 5, 10, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 60000}));
    }

    void InitAuthProvider() {
        AccessServiceDomain = Config.GetAccessServiceDomain();
        UserAccountDomain = Config.GetUserAccountDomain();
        ServiceDomain = Config.GetServiceDomain();

        if (Config.GetUseAccessService()) {
            NCloud::TAccessServiceSettings settings;
            settings.Endpoint = Config.GetAccessServiceEndpoint();
            if (Config.GetUseAccessServiceTLS()) {
                settings.CertificateRootCA = TUnbufferedFileInput(Config.GetPathToRootCA()).ReadAll();
            }
            settings.GrpcKeepAliveTimeMs = Config.GetAccessServiceGrpcKeepAliveTimeMs();
            settings.GrpcKeepAliveTimeoutMs = Config.GetAccessServiceGrpcKeepAliveTimeoutMs();
            AccessServiceValidator = Register(NCloud::CreateAccessService(settings), TMailboxType::Simple, AppData()->IOPoolId);
            if (Config.GetCacheAccessServiceAuthentication()) {
                AccessServiceValidator = Register(NCloud::CreateGrpcServiceCache<NCloud::TEvAccessService::TEvAuthenticateRequest, NCloud::TEvAccessService::TEvAuthenticateResponse>(
                                                          AccessServiceValidator,
                                                          Config.GetGrpcCacheSize(),
                                                          TDuration::MilliSeconds(Config.GetGrpcSuccessLifeTime()),
                                                          TDuration::MilliSeconds(Config.GetGrpcErrorLifeTime())), TMailboxType::Simple, AppData()->UserPoolId);
            }
            if (Config.GetCacheAccessServiceAuthorization()) {
                AccessServiceValidator = Register(NCloud::CreateGrpcServiceCache<NCloud::TEvAccessService::TEvAuthorizeRequest, NCloud::TEvAccessService::TEvAuthorizeResponse>(
                                                          AccessServiceValidator,
                                                          Config.GetGrpcCacheSize(),
                                                          TDuration::MilliSeconds(Config.GetGrpcSuccessLifeTime()),
                                                          TDuration::MilliSeconds(Config.GetGrpcErrorLifeTime())), TMailboxType::Simple, AppData()->UserPoolId);
            }
        }

        if (Config.GetUseUserAccountService()) {
            NCloud::TUserAccountServiceSettings settings;
            settings.Endpoint = Config.GetUserAccountServiceEndpoint();
            if (Config.GetUseUserAccountServiceTLS()) {
                settings.CertificateRootCA = TUnbufferedFileInput(Config.GetPathToRootCA()).ReadAll();
            }
            UserAccountService = Register(CreateUserAccountService(settings), TMailboxType::Simple, AppData()->IOPoolId);
            if (Config.GetCacheUserAccountService()) {
                UserAccountService = Register(NCloud::CreateGrpcServiceCache<NCloud::TEvUserAccountService::TEvGetUserAccountRequest, NCloud::TEvUserAccountService::TEvGetUserAccountResponse>(
                                                      UserAccountService,
                                                      Config.GetGrpcCacheSize(),
                                                      TDuration::MilliSeconds(Config.GetGrpcSuccessLifeTime()),
                                                      TDuration::MilliSeconds(Config.GetGrpcErrorLifeTime())), TMailboxType::Simple, AppData()->UserPoolId);
            }
        }

        if (Config.GetUseServiceAccountService()) {
            NCloud::TServiceAccountServiceSettings settings;
            settings.Endpoint = Config.GetServiceAccountServiceEndpoint();
            if (Config.GetUseServiceAccountServiceTLS()) {
                settings.CertificateRootCA = TUnbufferedFileInput(Config.GetPathToRootCA()).ReadAll();
            }
            ServiceAccountService = Register(NCloud::CreateServiceAccountService(settings), TMailboxType::Simple, AppData()->IOPoolId);
            if (Config.GetCacheServiceAccountService()) {
                ServiceAccountService = Register(NCloud::CreateGrpcServiceCache<NCloud::TEvServiceAccountService::TEvGetServiceAccountRequest, NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse>(
                                                         ServiceAccountService,
                                                         Config.GetGrpcCacheSize(),
                                                         TDuration::MilliSeconds(Config.GetGrpcSuccessLifeTime()),
                                                         TDuration::MilliSeconds(Config.GetGrpcErrorLifeTime())), TMailboxType::Simple, AppData()->UserPoolId);
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
    }

    void PassAway() override {
        if (AccessServiceValidator) {
            Send(AccessServiceValidator, new TEvents::TEvPoisonPill);
        }
        if (UserAccountService) {
            Send(UserAccountService, new TEvents::TEvPoisonPill);
        }
        if (ServiceAccountService) {
            Send(ServiceAccountService, new TEvents::TEvPoisonPill);
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
        if (AppData() && AppData()->DomainsInfo && !AppData()->DomainsInfo->Domains.empty()) {
            DomainName = "/" + AppData()->DomainsInfo->Domains.begin()->second->Name;
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
            hFunc(NCloud::TEvAccessService::TEvAuthenticateResponse, Handle);
            hFunc(NCloud::TEvAccessService::TEvAuthorizeResponse, Handle);
            hFunc(NCloud::TEvUserAccountService::TEvGetUserAccountResponse, Handle);
            hFunc(NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse, Handle);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleRefresh);
            cFunc(TEvents::TSystem::PoisonPill, PassAway);
        }
    }

    TTicketParserImpl(const NKikimrProto::TAuthConfig& authConfig)
        : Config(authConfig) {}
};

}
