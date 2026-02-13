#include "kafka_sasl_auth_actor.h"

#include <ydb/core/audit/login_op.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/security/login_shared_func.h>
#include <ydb/core/security/sasl/plain_auth_actor.h>
#include <ydb/core/security/sasl/plain_ldap_auth_proxy_actor.h>
#include <ydb/library/login/sasl/plain.h>
#include <ydb/core/security/sasl/scram_auth_actor.h>
#include <ydb/library/login/sasl/scram.h>
#include <ydb/services/persqueue_v1/actors/persqueue_utils.h>


namespace NKafka {

using namespace NSasl;

constexpr char AuditLogKafkaLoginComponent[] = "kafka-login";

void AuditLogLogin(const NRawSocket::TNetworkConfig::TSocketAddressType& address, const TString& database,
    const TString& username, const Ydb::StatusIds_StatusCode status, const TString& reason,
    const TString& errorDetails, const TString& sanitizedToken, bool isAdmin = false)
{
    NAudit::LogLoginOperationResult(AuditLogKafkaLoginComponent, address->ToString(), database, username,
        status, reason, errorDetails, sanitizedToken, isAdmin);
}

const TDuration TKafkaSaslAuthActor::Timeout = TDuration::MilliSeconds(60000);

NActors::IActor* CreateKafkaSaslAuthActor(const TContext::TPtr context, const NRawSocket::TSocketDescriptor::TSocketAddressType address) {
    return new TKafkaSaslAuthActor(context, address);
}

void TKafkaSaslAuthActor::Bootstrap() {
    Become(&TKafkaSaslAuthActor::StateWork);
}

void TKafkaSaslAuthActor::HandleAuthRequest(TEvKafka::TEvAuthRequest::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& loginRequest = *ev->Get();
    CorrelationId = loginRequest.CorrelationId;
    const auto requestRowBytes = loginRequest.Request->AuthBytes.value_or(TKafkaRawBytes());
    AuthRequest = TString(requestRowBytes.data(), requestRowBytes.size());

    if (Context->AuthenticationStep != EAuthSteps::WAIT_AUTH) {
        SendResponseAndDie(EKafkaErrors::ILLEGAL_SASL_STATE,
                             "Request is not valid given the current SASL state.",
                             TStringBuilder() << "Current step: " << static_cast<int>(Context->AuthenticationStep),
                             ctx);
        return;
    }

    if (CurrentStateFunc() == &TThis::StateWork) {
        if (Context->SaslMechanism == "PLAIN") {
            StartPlainAuth(ctx);
        } else if (Context->SaslMechanism == "SCRAM-SHA-256") {
            StartScramAuth();
        } else {
            SendResponseAndDie(EKafkaErrors::UNSUPPORTED_SASL_MECHANISM,
                                "Does not support the requested SASL mechanism.",
                                TStringBuilder() << "Requested mechanism '" << Context->SaslMechanism << "'",
                                ctx);
            return;
        }

        SendDescribeRequest();
        Become(&TKafkaSaslAuthActor::StateResolveDatabase);
    } else if (CurrentStateFunc() == &TThis::StateSaslScramLogin) {
        SendScramLoginRequest(ctx);
        return;
    }
}

void TKafkaSaslAuthActor::StartPlainAuth(const NActors::TActorContext& ctx) {
    if (!TryParseAuthDataTo(ClientAuthData, ctx)) {
        return;
    }
}

void TKafkaSaslAuthActor::StartScramAuth() {
    DatabasePath = AppData()->TenantName;
}

void TKafkaSaslAuthActor::HandleFirstLoginResponse(NSasl::TEvSasl::TEvSaslScramFirstServerResponse::TPtr& ev) {
    auto& loginResponse = *ev->Get();
    AuthResponse = std::move(loginResponse.Msg);
    SendResponse();
}

void TKafkaSaslAuthActor::HandleLoginResult(const NYql::TIssue& issue, const std::string& reason, const std::string& token,
    const std::string& sanitizedToken, bool isAdmin, const NActors::TActorContext& ctx)
{
    Ydb::StatusIds_StatusCode status;
    TString errorMessage;
    TString errorDetails;
    switch (issue.GetCode()) {
    case NKikimrIssues::TIssuesIds::SUCCESS: {
        Ticket = "Login " + token;

        AuditLogLogin(Address, DatabasePath, ClientAuthData.UserName, Ydb::StatusIds::SUCCESS, /* reason */ "",
            /* errorDetails */ "", TString(sanitizedToken), isAdmin);
        SendTicketParserRequest();
        return;
    }
    case NKikimrIssues::TIssuesIds::ACCESS_DENIED: {
        status = Ydb::StatusIds::UNAUTHORIZED;
        errorMessage = issue.GetMessage();
        errorDetails = reason;
        break;
    }
    case NKikimrIssues::TIssuesIds::UNEXPECTED: {
        status = Ydb::StatusIds::INTERNAL_ERROR;
        errorMessage = issue.GetMessage();
        break;
    }
    case NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST: {
        status = Ydb::StatusIds::SCHEME_ERROR;
        errorMessage = "No database found";
        break;
    }
    case NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE: {
        status = Ydb::StatusIds::UNAVAILABLE;
        errorMessage = "SchemeShard is unreachable";
        break;
    }
    case NKikimrIssues::TIssuesIds::DEFAULT_ERROR: {
        status = Ydb::StatusIds::INTERNAL_ERROR;
        errorMessage = issue.GetMessage();
        break;
    }
    case NKikimrIssues::TIssuesIds::YDB_AUTH_UNAVAILABLE: {
        status = Ydb::StatusIds::UNAVAILABLE;
        errorMessage = issue.GetMessage();
        errorDetails = reason;
        break;
    }
    case NKikimrIssues::TIssuesIds::YDB_API_VALIDATION_ERROR: {
        status = Ydb::StatusIds::BAD_REQUEST;
        errorMessage = issue.GetMessage();
        errorDetails = reason;
        break;
    }
    default: {
        status = Ydb::StatusIds::GENERIC_ERROR;
        errorMessage = issue.GetMessage();
        break;
    }
    }

    if (ClientAuthData.UserName) {
        AuditLogLogin(Address, DatabasePath, ClientAuthData.UserName, status, errorMessage, errorDetails,
            /* sanitizedToken */ "");
    }

    if (errorMessage.empty()) {
        errorMessage = Ydb::StatusIds_StatusCode_Name(status);
    }

    SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", errorMessage, ctx);
}

void TKafkaSaslAuthActor::HandleLoginResult(TEvSasl::TEvSaslPlainLoginResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& loginResult = *ev->Get();
    HandleLoginResult(loginResult.Issue, "", loginResult.Token, loginResult.SanitizedToken, loginResult.IsAdmin, ctx);
}

void TKafkaSaslAuthActor::HandleLoginResult(TEvSasl::TEvSaslPlainLdapLoginResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& loginResult = *ev->Get();
    HandleLoginResult(loginResult.Issue, loginResult.Reason, loginResult.Token, loginResult.SanitizedToken, loginResult.IsAdmin, ctx);
}

void TKafkaSaslAuthActor::HandleLoginResult(TEvSasl::TEvSaslScramFinalServerResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    auto& loginResult = *ev->Get();
    AuthResponse = std::move(loginResult.Msg);
    ClientAuthData.UserName = std::move(loginResult.AuthcId);
    ScramAuthActor = TActorId();

    HandleLoginResult(loginResult.Issue, "", loginResult.Token, loginResult.SanitizedToken, loginResult.IsAdmin, ctx);
}

void TKafkaSaslAuthActor::Handle(NKikimr::TEvTicketParser::TEvAuthorizeTicketResult::TPtr& ev, const NActors::TActorContext& ctx) {
    if (ev->Get()->Error) {
        if (Context->SaslMechanism == "SCRAM-SHA-256") {
            AuthResponse = NLogin::NSasl::BuildErrorMsg(NLogin::NSasl::EScramServerError::OtherError);
        }

        SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", TString{ev->Get()->Error.Message}, ctx);
        return;
    }

    UserToken = ev->Get()->Token;
    SendResponseAndDie(EKafkaErrors::NONE_ERROR, "", "", ctx);
}

void TKafkaSaslAuthActor::HandleTimeout(const NActors::TActorContext& ctx) {
    const TString reason = "Login timeout";
    SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", reason, ctx);
}

void TKafkaSaslAuthActor::SendTicketParserRequest() {
    Send(NKikimr::MakeTicketParserID(), new NKikimr::TEvTicketParser::TEvAuthorizeTicket({
        .Ticket = Ticket,
        .Database = DatabasePath,
        .PeerName = TStringBuilder() << Address,
        .Entries = TicketParserEntries,
    }));

    Become(&TKafkaSaslAuthActor::StateTicketResolve);
}

void TKafkaSaslAuthActor::CleanupAndDie(const NActors::TActorContext& ctx) {
    if (ScramAuthActor) {
        Send(ScramAuthActor, new TEvents::TEvPoison());
    }

    Die(ctx);
}

void TKafkaSaslAuthActor::SendResponse() {
    auto responseToClient = std::make_shared<TSaslAuthenticateResponseData>();
    responseToClient->ErrorCode = EKafkaErrors::NONE_ERROR;
    responseToClient->AuthBytesStr = AuthResponse;
    responseToClient->AuthBytes = ToRawBytes(responseToClient->AuthBytesStr);
    responseToClient->ErrorMessage = "";

    KAFKA_LOG_D("Authentication first step finished."
                    << " FirstServerMessage='" << AuthResponse << "'");

    auto evResponse = std::make_unique<TEvKafka::TEvResponse>(CorrelationId, responseToClient, EKafkaErrors::NONE_ERROR);
    Send(Context->ConnectionId, evResponse.release());
}

void TKafkaSaslAuthActor::SendResponseAndDie(EKafkaErrors errorCode, const TString& errorMessage, const TString& details, const NActors::TActorContext& ctx) {
    auto isFailed = errorCode != EKafkaErrors::NONE_ERROR;

    auto responseToClient = std::make_shared<TSaslAuthenticateResponseData>();
    responseToClient->ErrorCode = errorCode;
    responseToClient->AuthBytesStr = AuthResponse;
    responseToClient->AuthBytes = ToRawBytes(responseToClient->AuthBytesStr);

    if (isFailed) {
        KAFKA_LOG_ERROR("Authentication failure. " << errorMessage << " " << details);
        responseToClient->ErrorMessage = TStringBuilder() << "Authentication failure. " << errorMessage;

        auto evResponse = std::make_shared<TEvKafka::TEvResponse>(CorrelationId, responseToClient, errorCode);
        auto authResult = new TEvKafka::TEvAuthResult(EAuthSteps::FAILED, evResponse, errorMessage);
        Send(Context->ConnectionId, authResult);
    } else {
        KAFKA_LOG_D("Authentication success. Database='" << DatabasePath << "', "
                                      << "FolderId='" << FolderId << "', "
                                      << "ServiceAccountId='" << ServiceAccountId << "', "
                                      << "DatabaseId='" << DatabaseId << "', "
                                      << "Coordinator='" << Coordinator << "', "
                                      << "ResourcePath='" << ResourcePath << "'");
        responseToClient->ErrorMessage = "";

        auto evResponse = std::make_shared<TEvKafka::TEvResponse>(CorrelationId, responseToClient, errorCode);
        auto authResult = new TEvKafka::TEvAuthResult(EAuthSteps::SUCCESS, evResponse, UserToken, DatabasePath, DatabaseId, FolderId, CloudId, ServiceAccountId, Coordinator,
                                                                ResourcePath, IsServerless, errorMessage, ResourseDatabasePath);
        Send(Context->ConnectionId, authResult);
    }

    Die(ctx);
}

bool TKafkaSaslAuthActor::TryParseAuthDataTo(TKafkaSaslAuthActor::TAuthData& authData, const NActors::TActorContext& ctx) {
    if (AuthRequest.empty()) {
        SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", "AuthBytes is empty.",  ctx);
        return false;
    }

    TVector<TString> tokens = StringSplitter(AuthRequest).Split('\0');
    if (tokens.size() != 3) {
        SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, TStringBuilder() << "Invalid SASL/PLAIN response: expected 3 tokens, got " << tokens.size(), "", ctx);
        return false;
    }

    // tokens[0] is authorizationIdFromClient. Ignored
    auto userAndDatabase = tokens[1];
    auto password = tokens[2];
    size_t atPos = userAndDatabase.rfind('@');
    if (atPos == TString::npos) {
        authData.UserName = "";
        DatabasePath = CanonizePath(userAndDatabase);
    } else {
        authData.UserName = userAndDatabase.substr(0, atPos);
        DatabasePath = CanonizePath(userAndDatabase.substr(atPos + 1));
    }

    authData.Password = password;
    return true;
}

void TKafkaSaslAuthActor::SendPlainLoginRequest(const NActors::TActorContext& ctx) {
    std::unique_ptr<IActor> authActor;
    if (IsUsernameFromLdapAuthDomain(ClientAuthData.UserName, AppData()->AuthConfig)) {
        const TString ldapUsername = PrepareLdapUsername(ClientAuthData.UserName, AppData()->AuthConfig);
        const TString authMsg = NLogin::NSasl::BuildSaslPlainAuthMsg(ldapUsername, ClientAuthData.Password);
        authActor = CreatePlainLdapAuthProxyActor(ctx.SelfID, DatabasePath, authMsg, Address->ToString());
    } else {
        const TString authMsg = NLogin::NSasl::BuildSaslPlainAuthMsg(ClientAuthData.UserName, ClientAuthData.Password);
        authActor = CreatePlainAuthActor(ctx.SelfID, DatabasePath, authMsg, Address->ToString());
    }

    Register(authActor.release());
    Become(&TKafkaSaslAuthActor::StateSaslPlainLogin, Timeout, new TEvents::TEvWakeup());
}

void TKafkaSaslAuthActor::SendScramLoginRequest(const NActors::TActorContext& ctx) {
    std::string authMsg = AuthRequest;
    if (!ScramAuthActor) {
        auto authActor = CreateScramAuthActor(ctx.SelfID, DatabasePath, NLoginProto::EHashType::ScramSha256, authMsg, Address->ToString());
        ScramAuthActor = Register(authActor.release());
        Become(&TKafkaSaslAuthActor::StateSaslScramLogin, Timeout, new TEvents::TEvWakeup());
    } else {
        auto event = std::make_unique<NKikimr::NSasl::TEvSasl::TEvSaslScramFinalClientRequest>();
        event->Msg = std::move(authMsg);
        Send(ScramAuthActor, event.release());
    }
}

void TKafkaSaslAuthActor::SendDescribeRequest() {
    auto schemeCacheRequest = std::make_unique<NKikimr::NSchemeCache::TSchemeCacheNavigate>();
    NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = NKikimr::SplitPath(DatabasePath);
    entry.Operation = NKikimr::NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.SyncVersion = false;
    schemeCacheRequest->ResultSet.emplace_back(entry);
    schemeCacheRequest->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
    Send(NKikimr::MakeSchemeCacheID(), MakeHolder<NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySet>(schemeCacheRequest.release()));
}

void TKafkaSaslAuthActor::GetPathByPathId(const TPathId& pathId) {
    auto schemeCacheRequest = std::make_unique<NKikimr::NSchemeCache::TSchemeCacheNavigate>();
    NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.TableId.PathId = pathId;
    entry.RequestType = NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    entry.Operation = NKikimr::NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.SyncVersion = false;
    entry.RedirectRequired = false;
    schemeCacheRequest->ResultSet.emplace_back(entry);
    schemeCacheRequest->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;
    Send(NKikimr::MakeSchemeCacheID(), MakeHolder<NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySet>(schemeCacheRequest.release()));
}

void TKafkaSaslAuthActor::HandleNavigate(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
    const NKikimr::NSchemeCache::TSchemeCacheNavigate* navigate = ev->Get()->Request.Get();
    if (navigate->ErrorCount) {
        switch(navigate->ResultSet.front().Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                return SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", TStringBuilder() << "Database with path '" << DatabasePath << "' doesn't exists", ctx);
            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                return SendResponseAndDie(EKafkaErrors::SASL_AUTHENTICATION_FAILED, "", TStringBuilder() << "Database with path '" << DatabasePath << "' access denied", ctx);
            default:
                return SendResponseAndDie(EKafkaErrors::BROKER_NOT_AVAILABLE, "", TStringBuilder() << "Internal error with navigate status " << navigate->ResultSet.front().Status, ctx);
        }
        return;
    }

    if (CurrentStateFunc() == &TThis::StateResolveDatabase) {
        Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);
        IsServerless = navigate->ResultSet.front().DomainInfo->IsServerless();

        for (const auto& attr : navigate->ResultSet.front().Attributes) {
            if (attr.first == "folder_id") {
                FolderId = attr.second;
            } else if (attr.first == "cloud_id") {
                CloudId = attr.second;
            } else if (attr.first == "database_id") {
                DatabaseId = attr.second;
            } else if (attr.first == "service_account_id") {
                ServiceAccountId = attr.second;
            } else if (attr.first == "serverless_rt_coordination_node_path") {
                Coordinator = attr.second;
            } else if (attr.first == "serverless_rt_topic_resource_ru") {
                ResourcePath = attr.second;
            } else if (attr.first == "kafka_api") {
                KafkaApiFlag = attr.second;
            }
        }

        if (IsServerless) {
            GetPathByPathId(navigate->ResultSet.front().DomainInfo->GetResourcesDomainKey());
            Become(&TKafkaSaslAuthActor::StateResolveSharedDatabase);
            return;
        } else {
            ResourseDatabasePath = NKikimr::JoinPath(navigate->ResultSet.front().Path);
        }
    } else if (CurrentStateFunc() == &TThis::StateResolveSharedDatabase) {
        ResourseDatabasePath = NKikimr::JoinPath(navigate->ResultSet.front().Path);
    }

    if (ResourseDatabasePath) {
        if (Context->SaslMechanism == "PLAIN") {
            if (ClientAuthData.UserName.empty()) {
                // ApiKey IAM authentification
                if (Context->Config.GetAuthViaApiKey()) {
                    Ticket = "ApiKey " + ClientAuthData.Password;
                } else {
                    Ticket = ClientAuthData.Password;
                }
                TicketParserEntries = NKikimr::NGRpcProxy::V1::GetTicketParserEntries(DatabaseId, FolderId);
                SendTicketParserRequest();
            } else {
                // Plain Login/Password authentication
                SendPlainLoginRequest(ctx);
            }
        } else if (Context->SaslMechanism == "SCRAM-SHA-256") {
            // Scram Login/Password authentication
            SendScramLoginRequest(ctx);
        }
    }
}

} // NKafka
