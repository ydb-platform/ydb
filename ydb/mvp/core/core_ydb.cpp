#include "core_ydb.h"
#include "core_ydb_impl.h"

#include <ydb/library/actors/http/http_cache.h>

NJson::TJsonWriterConfig GetDefaultJsonWriterConfig() {
    NJson::TJsonWriterConfig result;
    result.WriteNanAsString = true;
    return result;
}

NJson::TJsonReaderConfig NMVP::THandlerActorYdb::JsonReaderConfig;
NJson::TJsonWriterConfig NMVP::THandlerActorYdb::JsonWriterConfig = GetDefaultJsonWriterConfig();
TMap<std::pair<TStringBuf, TStringBuf>, TYdbUnitResources> DefaultUnitResources =
    {
        {
            {
                "compute",
                "slot"
            },
            {
                10.0,
                (ui64)50*1024*1024*1024,
                0
            }
        },
        {
            {
                "storage",
                "hdd"
            },
            {
                0,
                0,
                (ui64)500*1024*1024*1024
            }
        },
        {
            {
                "storage",
                "ssd"
            },
            {
                0,
                0,
                (ui64)100*1024*1024*1024
            }
        }
    };
TString TYdbLocation::UserToken;
TString TYdbLocation::CaCertificate;
TString TYdbLocation::SslCertificate;

NYdb::NScheme::TSchemeClient TYdbLocation::GetSchemeClient(const TRequest& request) const {
    NYdb::TCommonClientSettings clientSettings;
    TString authToken = request.GetAuthToken();
    if (authToken) {
        clientSettings.AuthToken(authToken);
    }
<<<<<<< HEAD
    if (TString database = TYdbLocation::GetDatabaseName(request)) {
=======
    TYdbLocation::GetDatabaseName(request);

    TString database = TYdbLocation::GetDatabaseName(request);
    if (database) {
>>>>>>> 8359e2fa0d (added GetAccessServiceTypeFromString method)
        clientSettings.Database(database);
    }
    return NYdb::NScheme::TSchemeClient(GetDriver(), clientSettings);
}

NYdb::NScheme::TSchemeClient TYdbLocation::GetSchemeClient(const NYdb::TCommonClientSettings& settings) const {
    NYdb::TCommonClientSettings clientSettings(settings);
    if (!UserToken.empty()) {
        clientSettings.AuthToken(UserToken);
    }
    return NYdb::NScheme::TSchemeClient(GetDriver(), clientSettings);
}

NYdb::TDriver& TYdbLocation::GetDriver() const {
    return Driver.GetRef([this](){ return new NYdb::TDriver(GetDriverConfig()); });
}

NYdb::TDriver TYdbLocation::GetDriver(TStringBuf endpoint, TStringBuf scheme) const {
    // TODO: caching
    return NYdb::TDriver(GetDriverConfig(endpoint, scheme));
}

std::unique_ptr<NYdb::NScheme::TSchemeClient> TYdbLocation::GetSchemeClientPtr(TStringBuf endpoint, const NYdb::TCommonClientSettings& settings) const {
    return GetSchemeClientPtr(endpoint, TStringBuf(), settings);
}

std::unique_ptr<NYdb::NTable::TTableClient> TYdbLocation::GetTableClientPtr(TStringBuf endpoint, const NYdb::NTable::TClientSettings& settings) const {
    return GetTableClientPtr(endpoint, TStringBuf(), settings);
}

std::unique_ptr<NYdb::NScheme::TSchemeClient> TYdbLocation::GetSchemeClientPtr(TStringBuf endpoint, TStringBuf scheme, const NYdb::TCommonClientSettings& settings) const {
    return std::make_unique<NYdb::NScheme::TSchemeClient>(GetDriver(endpoint, scheme), settings);
}

std::unique_ptr<NYdb::NDataStreams::V1::TDataStreamsClient> TYdbLocation::GetDataStreamsClientPtr(TStringBuf endpoint, TStringBuf scheme, const NYdb::TCommonClientSettings& settings) const {
    return std::make_unique<NYdb::NDataStreams::V1::TDataStreamsClient>(GetDriver(endpoint, scheme), settings);
}

std::unique_ptr<NYdb::NTable::TTableClient> TYdbLocation::GetTableClientPtr(TStringBuf endpoint, TStringBuf scheme, const NYdb::NTable::TClientSettings& settings) const {
    return std::make_unique<NYdb::NTable::TTableClient>(GetDriver(endpoint, scheme), settings);
}

std::unique_ptr<NYdb::NTopic::TTopicClient> TYdbLocation::GetTopicClientPtr(TStringBuf endpoint, TStringBuf scheme, const NYdb::NTopic::TTopicClientSettings& settings) const {
    return std::make_unique<NYdb::NTopic::TTopicClient>(GetDriver(endpoint, scheme), settings);
}

<<<<<<< HEAD
=======
NYdb::NTable::TTableClient TYdbLocation::GetTableClient(const TRequest& request, const TYdbLocation& location) const {
    auto defaultClientSettings = NYdb::NTable::TClientSettings().Database(location.RootDomain);
    NYdb::NTable::TClientSettings clientSettings(defaultClientSettings);

    TString authToken;
    switch (location.AccessServiceType) {
    case NMVP::EAccessServiceType::YandexV2:
        authToken = request.GetAuthToken();
        break;
    case NMVP::EAccessServiceType::NebiusV1:
        NMVP::TMvpTokenator* tokenator = MVPAppData()->Tokenator;
        if (tokenator && !location.ServiceTokenName.empty()) {
            authToken = tokenator->GetToken(location.ServiceTokenName);
        }
        break;
    }
    if (authToken) {
        clientSettings.AuthToken(authToken);
    }
    TString database = TYdbLocation::GetDatabaseName(request);
    if (database) {
        clientSettings.Database(database);
    }
    return GetTableClient(clientSettings);
}

>>>>>>> 8e0d57db1b (rewrite GetTableClient)
NYdb::NTable::TTableClient TYdbLocation::GetTableClient(const NYdb::NTable::TClientSettings& clientSettings) const {
    return NYdb::NTable::TTableClient(GetDriver(), clientSettings);
}

TString TYdbLocation::GetDatabaseName(const TRequest& request) const {
    TString database = request.Parameters["database"];
    if (database) {
        if (!database.StartsWith('/')) {
            database.insert(database.begin(), '/');
        }
        database.insert(0, RootDomain);
    }
    return database;
}

NYdb::NScripting::TScriptingClient TYdbLocation::GetScriptingClient(const TRequest& request) const {
    NYdb::NTable::TClientSettings clientSettings;
    TString authToken = request.GetAuthToken();
    if (authToken) {
        clientSettings.AuthToken(authToken);
    }
    TString database = GetDatabaseName(request);
    if (database) {
        clientSettings.Database(database);
    }
    return NYdb::NScripting::TScriptingClient(GetDriver(), clientSettings);
}

std::unique_ptr<NYdb::NScripting::TScriptingClient> TYdbLocation::GetScriptingClientPtr(TStringBuf endpoint, TStringBuf scheme, const NYdb::TCommonClientSettings& settings) const {
    return std::make_unique<NYdb::NScripting::TScriptingClient>(GetDriver(endpoint, scheme), settings);
}

NHttp::THttpOutgoingRequestPtr TYdbLocation::CreateHttpMonRequestGet(TStringBuf uri, const TRequest& request) const {
    NHttp::THttpOutgoingRequestPtr out;
    TString endpoint = GetEndpoint("http-mon", "http");
    if (!endpoint.empty()) {
        out = NHttp::THttpOutgoingRequest::CreateRequestGet(endpoint, uri);
        TString userToken = request.GetAuthToken();
        if (!userToken.empty()) {
            NHttp::THeadersBuilder httpHeaders;
            httpHeaders.Set("Authorization", "OAuth " + userToken);
            out->Set(httpHeaders);
        }
    }
    return out;
}

NHttp::THttpOutgoingRequestPtr TYdbLocation::CreateHttpMonRequestGet(TStringBuf uri) const {
    NHttp::THttpOutgoingRequestPtr out;
    TString endpoint = GetEndpoint("http-mon", "http");
    if (!endpoint.empty()) {
        out = NHttp::THttpOutgoingRequest::CreateRequestGet(endpoint, uri);
        if (!UserToken.empty()) {
            NHttp::THeadersBuilder httpHeaders;
            httpHeaders.Set("Authorization", UserToken);
            out->Set(httpHeaders);
        }
    }
    return out;
}

TString TYdbLocation::GetServerlessProxyUrl(const TString& database) const {
    if (!ServerlessDocumentProxyEndpoint.empty()) {
        return ServerlessDocumentProxyEndpoint + database;
    } else {
        return {};
    }
}

TParameters::TParameters(const NHttp::THttpIncomingRequestPtr& request)
    : Success(true)
    , UrlParameters(request->URL)
{
    TStringBuf contentTypeHeader = request->ContentType;
    TStringBuf contentType = contentTypeHeader.NextTok(';');
    if (contentType == "application/json") {
        Success = NJson::ReadJsonTree(request->Body, &NMVP::THandlerActorYdb::JsonReaderConfig, &PostData);
    }
}

TString TParameters::GetContentParameter(TStringBuf name) const {
    if (PostData.IsDefined()) {
        const NJson::TJsonValue* value;
        if (PostData.GetValuePointer(name, &value)) {
            return value->GetStringRobust();
        }
    }
    return TString();
}

TString TParameters::GetUrlParameter(TStringBuf name) const {
    return UrlParameters[name];
}

TString TParameters::operator [](TStringBuf name) const {
    TString value = GetUrlParameter(name);
    if (value.empty()) {
        value = GetContentParameter(name);
    }
    return value;
}

void TParameters::ParamsToProto(google::protobuf::Message& proto, TJsonSettings::TNameGenerator nameGenerator) const {
    using google::protobuf::Descriptor;
    using google::protobuf::Reflection;
    using google::protobuf::FieldDescriptor;
    using google::protobuf::EnumDescriptor;
    using google::protobuf::EnumValueDescriptor;
    const Descriptor& descriptor = *proto.GetDescriptor();
    const Reflection& reflection = *proto.GetReflection();
    for (int idx = 0; idx < descriptor.field_count(); ++idx) {
        const FieldDescriptor* field = descriptor.field(idx);
        TString name;
        if (nameGenerator) {
            name = nameGenerator(*field);
        } else {
            name = field->name();
        }
        TString value = UrlParameters[name];
        if (!value.empty()) {
            FieldDescriptor::CppType type = field->cpp_type();
            switch (type) {
            case FieldDescriptor::CPPTYPE_INT32:
                reflection.SetInt32(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_INT64:
                reflection.SetInt64(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_UINT32:
                reflection.SetUInt32(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_UINT64:
                reflection.SetUInt64(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_DOUBLE:
                reflection.SetDouble(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_FLOAT:
                reflection.SetFloat(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_BOOL:
                reflection.SetBool(&proto, field, FromString(value));
                break;
            case FieldDescriptor::CPPTYPE_ENUM: {
                const EnumDescriptor* enumDescriptor = field->enum_type();
                const EnumValueDescriptor* enumValueDescriptor = enumDescriptor->FindValueByName(value);
                int number = 0;
                if (enumValueDescriptor == nullptr && TryFromString(value, number)) {
                    enumValueDescriptor = enumDescriptor->FindValueByNumber(number);
                }
                if (enumValueDescriptor != nullptr) {
                    reflection.SetEnum(&proto, field, enumValueDescriptor);
                }
                break;
            }
            case FieldDescriptor::CPPTYPE_STRING:
                reflection.SetString(&proto, field, value);
                break;
            case FieldDescriptor::CPPTYPE_MESSAGE:
                break;
            }
        }
    }
}

TString TRequest::GetAuthToken() const {
    NHttp::THeaders headers(Request->Headers);
    return GetAuthToken(headers);
}

TString TRequest::GetAuthToken(const NHttp::THeaders& headers) const {
    NHttp::TCookies cookies(headers["Cookie"]);
    TStringBuf authorization = headers["Authorization"];
    if (!authorization.empty()) {
        TStringBuf scheme = authorization.NextTok(' ');
        if (scheme == "OAuth" || scheme == "Bearer") {
            return TString(authorization);
        }
    }
    TStringBuf subjectToken = headers["x-yacloud-subjecttoken"];
    if (!subjectToken.empty()) {
        return TString(subjectToken);
    }
    TStringBuf sessionId = cookies["Session_id"];
    if (!sessionId.empty()) {
        return BlackBoxTokenFromSessionId(sessionId);
    }
    return TString();
}

TString TRequest::GetAuthTokenForIAM() const {
    NHttp::THeaders headers(Request->Headers);
    return GetAuthTokenForIAM(headers);
}

TString TRequest::GetAuthTokenForIAM(const NHttp::THeaders& headers) const {
    TStringBuf authorization = headers["Authorization"];
    if (!authorization.empty()) {
        TStringBuf scheme = authorization.NextTok(' ');
        if (scheme == "Bearer") {
            return "Bearer " + TString(authorization);
        }
    }
    TStringBuf subjectToken = headers["x-yacloud-subjecttoken"];
    if (!subjectToken.empty()) {
        return "Bearer " + TString(subjectToken);
    }
    return TString();
}

void TRequest::SetHeader(NYdbGrpc::TCallMeta& meta, const TString& name, const TString& value) {
    for (auto& [exname, exvalue] : meta.Aux) {
        if (exname == name) {
            exvalue = value;
            return;
        }
    }
    meta.Aux.emplace_back(name, value);
}

void TRequest::ForwardHeaders(NYdbGrpc::TCallMeta& meta) const {
    NHttp::THeaders headers(Request->Headers);
    TString token = GetAuthToken(headers);
    if (!token.empty()) {
        SetHeader(meta, "authorization", "Bearer " + token);
        SetHeader(meta, NYdb::YDB_AUTH_TICKET_HEADER, token);
    }
    ForwardHeader(headers, meta, "x-request-id");
}

void TRequest::ForwardHeaders(NHttp::THttpOutgoingRequestPtr& request) const {
    NHttp::THeaders headers(Request->Headers);
    TString token = GetAuthToken(headers);
    if (!token.empty()) {
        request->Set("authorization", "Bearer " + token);
        request->Set(NYdb::YDB_AUTH_TICKET_HEADER, token);
    }
    ForwardHeader(headers, request, "x-request-id");
}

void TRequest::ForwardHeadersOnlyForIAM(NYdbGrpc::TCallMeta& meta) const {
    NHttp::THeaders headers(Request->Headers);
    TString token;
    TStringBuf srcAuthorization = headers["Authorization"];
    if (!srcAuthorization.empty()) {
        TStringBuf scheme = srcAuthorization.NextTok(' ');
        if (scheme == "Bearer") {
            token = srcAuthorization;
        }
    }
    TStringBuf subjectToken = headers["x-yacloud-subjecttoken"];
    if (!subjectToken.empty()) {
        token = subjectToken;
    }
    if (!token.empty()) {
        SetHeader(meta, "authorization", "Bearer " + token);
        SetHeader(meta, NYdb::YDB_AUTH_TICKET_HEADER, "Bearer " + token);
    }
    ForwardHeader(headers, meta, "x-request-id");
}

void TRequest::ForwardHeader(const NHttp::THeaders& header, NYdbGrpc::TCallMeta& meta, TStringBuf name) const {
    TStringBuf value(header[name]);
    if (!value.empty()) {
        SetHeader(meta, TString(name), TString(value));
    }
}

void TRequest::ForwardHeader(const NHttp::THeaders& header, NHttp::THttpOutgoingRequestPtr& request, TStringBuf name) const {
    TStringBuf value(header[name]);
    if (!value.empty()) {
        request->Set(name, value);
    }
}

void TRequest::ForwardHeadersOnlyForIAM(NHttp::THttpOutgoingRequestPtr& request) const {
    NHttp::THeaders headers(Request->Headers);
    TString token;
    TStringBuf srcAuthorization = headers["Authorization"];
    if (!srcAuthorization.empty()) {
        TStringBuf scheme = srcAuthorization.NextTok(' ');
        if (scheme == "Bearer") {
            token = srcAuthorization;
        }
    }
    ForwardHeader(headers, request, "x-yacloud-subjecttoken");
    if (!token.empty()) {
        request->Set("authorization", "Bearer " + token);
        request->Set(NYdb::YDB_AUTH_TICKET_HEADER, "Bearer " + token);
    }
    ForwardHeader(headers, request, "x-request-id");
}

TString GetAuthHeaderValue(const TString& tokenName) {
    NMVP::TMvpTokenator* tokenator = MVPAppData()->Tokenator;
    TString authHeaderValue;
    if (tokenator && !tokenName.empty()) {
        authHeaderValue = tokenator->GetToken(tokenName);
    }
    if (authHeaderValue.empty() && TYdbLocation::GetUserToken()) {
        authHeaderValue = TYdbLocation::GetUserToken();
    }
    return authHeaderValue;
}
