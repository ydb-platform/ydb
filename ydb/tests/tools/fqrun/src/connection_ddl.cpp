#include "connection_ddl.h"

#include <ydb/core/fq/libs/common/external_service.h>

#include <library/cpp/json/json_reader.h>
#include <util/stream/file.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

namespace NFqRun {
namespace {

using NYql::TAstNode;

bool IsCall(const TAstNode& node, TStringBuf name) {
    return node.IsList() && node.GetChildrenCount() && node.GetChild(0)->IsAtom() && node.GetChild(0)->GetContent() == name;
}

const TAstNode& Unquote(const TAstNode& node) {
    Y_ENSURE(IsCall(node, "quote") && node.GetChildrenCount() == 2, "Expected a literal DDL setting");
    return *node.GetChild(1);
}

TString Literal(const TAstNode& node) {
    const auto& value = Unquote(node);
    Y_ENSURE(value.IsAtom(), "DDL settings must be literals");
    return TString(value.GetContent());
}

using TProperties = THashMap<TString, TString>;

TString Required(const TProperties& properties, const TString& key) {
    auto it = properties.find(key);
    Y_ENSURE(it != properties.end() && !it->second.empty(), to_upper(key) << " is required");
    return it->second;
}

TConnectionDdl DecodeWrite(const TAstNode& write, const THashMap<TString, TString>& secrets) {
    Y_ENSURE(write.GetChildrenCount() == 6, "Unexpected connection DDL AST");
    const auto& key = *write.GetChild(3);
    Y_ENSURE(IsCall(key, "Key") && key.GetChildrenCount() == 3, "Expected external data source key");
    TProperties keys;
    for (ui32 i = 1; i < key.GetChildrenCount(); ++i) {
        const auto& entry = Unquote(*key.GetChild(i));
        Y_ENSURE(entry.IsList() && entry.GetChildrenCount() == 2, "Invalid DDL key");
        const auto& value = *entry.GetChild(1);
        Y_ENSURE(IsCall(value, "String") && value.GetChildrenCount() == 2, "DDL names must be literals");
        keys.emplace(Literal(*entry.GetChild(0)), Literal(*value.GetChild(1)));
    }
    Y_ENSURE(Required(keys, "typeId") == "EXTERNAL_DATA_SOURCE", "Expected external data source");

    TConnectionDdl result;
    result.Content.set_name(Required(keys, "objectId"));
    result.Content.mutable_acl()->set_visibility(FederatedQuery::Acl::PRIVATE);
    TProperties properties;
    const auto& options = Unquote(*write.GetChild(5));
    Y_ENSURE(options.IsList(), "Invalid DDL options");
    for (ui32 i = 0; i < options.GetChildrenCount(); ++i) {
        const auto& option = Unquote(*options.GetChild(i));
        Y_ENSURE(option.IsList() && option.GetChildrenCount() == 2, "Invalid DDL option");
        const auto name = Literal(*option.GetChild(0));
        if (name == "mode") {
            const auto mode = Literal(*option.GetChild(1));
            Y_ENSURE(mode == "createObject" || mode == "createObjectIfNotExists", "Only CREATE is supported");
            result.IfNotExists = mode == "createObjectIfNotExists";
        } else {
            Y_ENSURE(name == "features", "Unsupported DDL option");
            const auto& features = Unquote(*option.GetChild(1));
            Y_ENSURE(features.IsList(), "Invalid DDL properties");
            for (ui32 j = 0; j < features.GetChildrenCount(); ++j) {
                const auto& feature = Unquote(*features.GetChild(j));
                Y_ENSURE(feature.IsList() && feature.GetChildrenCount() == 2, "Invalid DDL property");
                properties.emplace(Literal(*feature.GetChild(0)), Literal(*feature.GetChild(1)));
            }
        }
    }
    for (const auto& [name, value] : properties) {
        Y_UNUSED(value);
        Y_ENSURE(name == "source_type" || name == "protocol" || name == "location" || name == "auth_method" || name == "token_secret_name" ||
                     name == "use_tls" || name == "ca_certificate" || name == "tvm_source_id" || name == "tvm_destination_id" ||
                     name == "tvm_secret_name" || name == "auth_provider" ||
                     name == "auth_parameters", "Unsupported ExternalService property: " << to_upper(name));
    }
    Y_ENSURE(Required(properties, "source_type") == "ExternalService", "SOURCE_TYPE must be ExternalService");
    auto& service = *result.Content.mutable_setting()->mutable_external_service();
    const auto protocol = Required(properties, "protocol");
    Y_ENSURE(protocol == "HTTP" || protocol == "GRPC", "PROTOCOL must be HTTP or GRPC");
    service.set_protocol(protocol == "HTTP" ? FederatedQuery::ExternalService::HTTP : FederatedQuery::ExternalService::GRPC);
    service.set_endpoint(Required(properties, "location"));
    if (auto it = properties.find("use_tls"); it != properties.end()) {
        const auto value = to_lower(it->second);
        Y_ENSURE(value == "true" || value == "false", "USE_TLS must be TRUE or FALSE");
        service.set_insecure(value == "false");
    }
    if (auto it = properties.find("ca_certificate"); it != properties.end()) {
        service.set_ca_certificate(it->second);
    }
    const auto auth = Required(properties, "auth_method");
    if (auth == "NONE") {
        service.mutable_auth()->mutable_none();
    } else if (auth == "CURRENT_IAM") {
        service.mutable_auth()->mutable_current_iam();
    } else if (auth == "WASM") {
        service.mutable_wasm_auth()->set_provider(Required(properties, "auth_provider"));
        service.mutable_wasm_auth()->set_parameters(Required(properties, "auth_parameters"));
    } else if (auth == "TVM") {
        auto& tvm = *service.mutable_tvm_auth();
        ui32 source = 0, destination = 0;
        Y_ENSURE(TryFromString(Required(properties, "tvm_source_id"), source) && source, "TVM_SOURCE_ID must be a nonzero uint32");
        Y_ENSURE(TryFromString(Required(properties, "tvm_destination_id"), destination) && destination,
            "TVM_DESTINATION_ID must be a nonzero uint32");
        tvm.set_source_id(source);
        tvm.set_destination_id(destination);
        auto it = secrets.find(Required(properties, "tvm_secret_name"));
        Y_ENSURE(it != secrets.end(), "TVM_SECRET_NAME is not present in --secrets-file");
        tvm.set_secret(it->second);
    } else {
        Y_ENSURE(auth == "TOKEN", "AUTH_METHOD must be NONE, CURRENT_IAM, TOKEN or TVM");
        const auto secret = Required(properties, "token_secret_name");
        auto it = secrets.find(secret);
        Y_ENSURE(it != secrets.end(), "TOKEN_SECRET_NAME is not present in --secrets-file");
        service.mutable_auth()->mutable_token()->set_token(it->second);
    }
    const auto issues = NFq::ValidateExternalService(service, false);
    Y_ENSURE(issues.Empty(), issues.ToString());
    return result;
}

void CollectWrites(const TAstNode& node, const THashMap<TString, TString>& secrets, std::vector<TConnectionDdl>& result) {
    if (IsCall(node, "Write") || IsCall(node, "Write!")) {
        result.push_back(DecodeWrite(node, secrets));
    } else if (node.IsList()) {
        for (ui32 i = 0; i < node.GetChildrenCount(); ++i) {
            CollectWrites(*node.GetChild(i), secrets, result);
        }
    }
}

}   // namespace

THashMap<TString, TString> LoadConnectionSecrets(const TString& path) {
    THashMap<TString, TString> result;
    if (path.empty()) {
        return result;
    }
    TFileInput input(path);
    NJson::TJsonValue json;
    Y_ENSURE(NJson::ReadJsonTree(&input, &json) && json.IsMap(), "--secrets-file must be a JSON object of strings");
    for (const auto& [name, value] : json.GetMap()) {
        Y_ENSURE(!name.empty() && value.IsString(), "--secrets-file must contain named string values");
        result.emplace(name, value.GetString());
    }
    return result;
}

std::vector<TConnectionDdl> ParseConnectionDdl(const TString& sql, const THashMap<TString, TString>& secrets) {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    google::protobuf::Arena arena;
    NYql::TIssues issues;
    auto* proto = NSQLTranslationV1::SqlAST(parsers, sql, "fqrun init query", issues, 10, false, &arena);
    Y_ENSURE(proto, "Invalid initialization SQL: " << issues.ToString());
    // Reject all non-CREATE statements before making any control-plane requests.
    // In particular, USE/PRAGMA/bindings must not silently change scope or credentials.
    const auto& query = static_cast<const NSQLv1Generated::TSQLv1ParserAST&>(*proto).GetRule_sql_query();
    Y_ENSURE(query.Alt_case() == NSQLv1Generated::TRule_sql_query::kAltSqlQuery1, "Expected CREATE EXTERNAL DATA SOURCE");
    const auto& list = query.GetAlt_sql_query1().GetRule_sql_stmt_list1();
    Y_ENSURE(list.HasBlock2(), "Initialization SQL must contain CREATE EXTERNAL DATA SOURCE");
    size_t count = 0;
    const auto validate = [&](const NSQLv1Generated::TRule_sql_stmt_core& core) {
        Y_ENSURE(core.Alt_case() == NSQLv1Generated::TRule_sql_stmt_core::kAltSqlStmtCore30,
            "--init-query supports only CREATE EXTERNAL DATA SOURCE");
        const auto& create = core.GetAlt_sql_stmt_core30().GetRule_create_external_data_source_stmt1();
        Y_ENSURE(!create.HasBlock2(), "CREATE OR REPLACE is unsupported");
        Y_ENSURE(!create.GetRule_object_ref7().HasBlock1(), "Connection names must not be cluster-qualified");
        ++count;
    };
    validate(list.GetBlock2().GetRule_sql_stmt1().GetRule_sql_stmt_core2());
    for (const auto& block : list.GetBlock2().GetBlock2()) {
        validate(block.GetRule_sql_stmt2().GetRule_sql_stmt_core2());
    }
    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;
    settings.SyntaxVersion = 1;
    settings.DefaultCluster = "fqrun";
    settings.ClusterMapping["fqrun"] = "kikimr";
    auto ast = NSQLTranslationV1::SqlToYql(lexers, parsers, sql, settings);
    Y_ENSURE(ast.IsOk(), "Invalid connection DDL: " << ast.Issues.ToString());
    std::vector<TConnectionDdl> result;
    CollectWrites(*ast.Root, secrets, result);
    Y_ENSURE(result.size() == count, "Unexpected number of connection DDL operations");
    return result;
}

}   // namespace NFqRun
