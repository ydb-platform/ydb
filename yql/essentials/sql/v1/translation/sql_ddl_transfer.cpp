#include "sql_ddl_transfer.h"

#include "object_processing.h"
#include "secret_settings.h"
#include "sql_expression.h"

#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/utils/yql_paths.h>

#include <util/string/join.h>

namespace NSQLTranslationV1 {

namespace {

TString GetLambdaText(TTranslation& ctx, TContext& context, const TRule_lambda_or_parameter& lambdaOrParameter) {
    static const TString StatementSeparator = ";\n";

    TVector<TString> statements;
    NYql::TIssues issues;
    if (!SplitQueryToStatements(context.Lexers, context.Parsers, context.Query, statements, issues, context.Settings)) {
        return {};
    }

    TStringBuilder result;
    for (const auto id : context.ForAllStatementsParts) {
        result << statements[id] << "\n";
    }

    switch (lambdaOrParameter.Alt_case()) {
        case NSQLv1Generated::TRule_lambda_or_parameter::kAltLambdaOrParameter1: {
            const auto& lambda = lambdaOrParameter.GetAlt_lambda_or_parameter1().GetRule_lambda1();

            auto& beginToken = lambda.GetRule_smart_parenthesis1().GetToken1();
            const NSQLv1Generated::TToken* endToken = nullptr;
            switch (lambda.GetBlock2().GetBlock2().GetAltCase()) {
                case TRule_lambda_TBlock2_TBlock2::AltCase::kAlt1:
                    endToken = &lambda.GetBlock2().GetBlock2().GetAlt1().GetToken3();
                    break;
                case TRule_lambda_TBlock2_TBlock2::AltCase::kAlt2:
                    endToken = &lambda.GetBlock2().GetBlock2().GetAlt2().GetToken3();
                    break;
                case TRule_lambda_TBlock2_TBlock2::AltCase::ALT_NOT_SET:
                    YQL_ENSURE(false, "Unreachable");
            }

            auto begin = GetQueryPosition(context.Query, beginToken);
            auto end = GetQueryPosition(context.Query, *endToken);
            if (begin == std::string::npos || end == std::string::npos) {
                return {};
            }

            result << "$__ydb_transfer_lambda = " << context.Query.substr(begin, end - begin + endToken->value().size()) << StatementSeparator;

            return result;
        }
        case NSQLv1Generated::TRule_lambda_or_parameter::kAltLambdaOrParameter2: {
            const auto& valueBlock = lambdaOrParameter.GetAlt_lambda_or_parameter2().GetRule_bind_parameter1().GetBlock2();
            const auto id = Id(valueBlock.GetAlt1().GetRule_an_id_or_type1(), ctx);
            result << "$__ydb_transfer_lambda = $" << id << StatementSeparator;
            return result;
        }
        case NSQLv1Generated::TRule_lambda_or_parameter::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }
}

} // anonymous namespace

bool TTransferTranslation::TransferSettingsEntry(std::map<TString, TNodePtr>& out,
                                                 const TRule_transfer_settings_entry& in, TSqlExpression& ctx, bool create)
{
    auto key = IdEx(in.GetRule_an_id1(), ctx);
    TNodePtr value = Unwrap(ctx.Build(in.GetRule_expr3()));

    if (!value) {
        ctx.Context().Error() << "Invalid transfer setting: " << key.Name;
        return false;
    }

    static const TSet<TString> ConfigSettings = [] {
        TSet<TString> settings = {
            "connection_string",
            "endpoint",
            "database",
            "token",
            "user",
            "password",
            "service_account_id",
            "initial_token",
            "resource_id",
            "ca_cert",
            "flush_interval",
            "batch_size_bytes",
            "directory",
            "metrics_level",
            "v_cpu_rate_limit"};

        for (const auto& names : REPLICATION_AND_TRANSFER_SECRETS_SETTINGS) {
            settings.insert(names.Name);
            settings.insert(names.Path);
        }

        return settings;
    }();

    static const TSet<TString> StateSettings = {
        "state",
        "failover_mode",
    };

    static const TSet<TString> CreateOnlySettings = {
        "consumer",
    };

    static const TSet<TString> MetricsLevelValues = {
        "default",
        "database",
        "object",
        "detailed",
    };

    const auto keyName = to_lower(key.Name);
    if (!ConfigSettings.count(keyName) && !StateSettings.contains(keyName) && !CreateOnlySettings.contains(keyName)) {
        ctx.Context().Error() << "Unknown transfer setting: " << key.Name;
        return false;
    }

    if (create && StateSettings.count(keyName)) {
        ctx.Context().Error() << key.Name << " is not supported in CREATE";
        return false;
    }

    if (!create && CreateOnlySettings.contains(keyName)) {
        ctx.Context().Error() << key.Name << " is not supported in ALTER";
        return false;
    }

    if (keyName == "metrics_level") {
        auto literalValue = value->GetLiteralValue();
        if (!literalValue) {
            ctx.Error() << " metrics_level value must be a string literal";
            return false;
        }

        if (!literalValue.empty() && literalValue[0] == '-') {
            ctx.Error() << "Invalid numeric value for metrics_value: negative numbers are not allowed";
            return false;
        }

        if (ui64 numericVal; TryFromString<ui64>(literalValue, numericVal)) {
            if (numericVal >= MetricsLevelValues.size()) {
                ctx.Error() << "Invalid numeric value for metrics_value " << numericVal << ", valid values: from 0 to "
                            << (MetricsLevelValues.size() - 1);
                return false;
            }
        } else {
            auto valueStr = to_lower(literalValue);
            if (!MetricsLevelValues.contains(valueStr)) {
                ctx.Error() << "Invalid metrics_level value: " << valueStr
                            << ". Allowed values: " << JoinSeq(", ", MetricsLevelValues);
                return false;
            }
        }
    }
    if (!out.emplace(keyName, value).second) {
        ctx.Context().Error() << "Duplicate transfer setting: " << key.Name;
    }

    return true;
}

bool TTransferTranslation::TransferSettings(std::map<TString, TNodePtr>& out,
                                            const TRule_transfer_settings& in, TSqlExpression& ctx, bool create,
                                            const TStringBuf& tablePathPrefix)
{
    if (!TransferSettingsEntry(out, in.GetRule_transfer_settings_entry1(), ctx, create)) {
        return false;
    }

    for (auto& block : in.GetBlock2()) {
        if (!TransferSettingsEntry(out, block.GetRule_transfer_settings_entry2(), ctx, create)) {
            return false;
        }
    }

    return VerifyAndAdjustSecretSettings(out, ctx.Context(), REPLICATION_AND_TRANSFER_SECRETS_SETTINGS, tablePathPrefix);
}

bool TTransferTranslation::ParseTransferLambda(
    TString& lambdaText,
    const TRule_lambda_or_parameter& lambdaOrParameter)
{
    TSqlExpression expr(*this);
    auto result = expr.Build(lambdaOrParameter);
    if (!result) {
        return false;
    }

    lambdaText = GetLambdaText(*this, Ctx_, lambdaOrParameter);
    if (lambdaText.empty()) {
        Ctx_.Error() << "Cannot parse lambda correctly";
    }

    return !lambdaText.empty();
}

TNodePtr TTransferTranslation::Build(const TRule_create_transfer_stmt& node) {
    // create_transfer_stmt: CREATE TRANSFER
    TObjectOperatorContext context(Ctx_.Scoped);
    if (node.GetRule_object_ref3().HasBlock1()) {
        const auto& cluster = node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1();
        if (!ClusterExpr(cluster, /*allowWildcard=*/false, context.ServiceId, context.Cluster)) {
            return {};
        }
    }

    const auto prefixPath = Ctx_.GetPrefixPath(context.ServiceId, context.Cluster);

    std::map<TString, TNodePtr> settings;
    TSqlExpression expr(*this);
    if (node.GetBlock10().HasRule_transfer_settings3() &&
        !TransferSettings(settings, node.GetBlock10().GetRule_transfer_settings3(), expr, /*create=*/true, prefixPath)) {
        return {};
    }

    const TString id = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
    const TString source = Id(node.GetRule_object_ref5().GetRule_id_or_at2(), *this).second;
    const TString target = Id(node.GetRule_object_ref7().GetRule_id_or_at2(), *this).second;
    TString transformLambda;
    if (!ParseTransferLambda(transformLambda, node.GetRule_lambda_or_parameter9())) {
        return {};
    }

    if (Ctx_.Scoped->ActivePragmas.contains(std::make_pair(TString(), TString("relativepathprefix")))) {
        return BuildCreateTransfer(Ctx_.Pos(), BuildTablePath(prefixPath, id),
                                   BuildTablePath(prefixPath, source), BuildTablePath(prefixPath, target),
                                   transformLambda, std::move(settings), context);
    }

    return BuildCreateTransfer(Ctx_.Pos(), BuildTablePath(prefixPath, id),
                               source, target, transformLambda, std::move(settings), context);
}

TNodePtr TTransferTranslation::Build(const TRule_alter_transfer_stmt& node) {
    // alter_transfer_stmt: ALTER TRANSFER
    TObjectOperatorContext context(Ctx_.Scoped);
    if (node.GetRule_object_ref3().HasBlock1()) {
        const auto& cluster = node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1();
        if (!ClusterExpr(cluster, /*allowWildcard=*/false, context.ServiceId, context.Cluster)) {
            return {};
        }
    }

    std::map<TString, TNodePtr> settings;
    std::optional<TString> transformLambda;
    TSqlExpression expr(*this);

    const auto prefixPath = Ctx_.GetPrefixPath(context.ServiceId, context.Cluster);

    auto transferAlterAction = [&](std::optional<TString>& transformLambda, const TRule_alter_transfer_action& in) {
        if (in.HasAlt_alter_transfer_action1()) {
            return TransferSettings(settings, in.GetAlt_alter_transfer_action1().GetRule_alter_transfer_set_setting1().GetRule_transfer_settings3(),
                                    expr, /*create=*/false, prefixPath);
        } else if (in.HasAlt_alter_transfer_action2()) {
            TString lb;
            if (!ParseTransferLambda(lb, in.GetAlt_alter_transfer_action2().GetRule_alter_transfer_set_using1().GetRule_lambda_or_parameter3())) {
                return false;
            }
            transformLambda = lb;
            return true;
        }

        return false;
    };

    if (!transferAlterAction(transformLambda, node.GetRule_alter_transfer_action4())) {
        return {};
    }
    for (auto& block : node.GetBlock5()) {
        if (!transferAlterAction(transformLambda, block.GetRule_alter_transfer_action2())) {
            return {};
        }
    }

    const TString id = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
    return BuildAlterTransfer(Ctx_.Pos(),
                              BuildTablePath(Ctx_.GetPrefixPath(context.ServiceId, context.Cluster), id),
                              std::move(transformLambda), std::move(settings), context);
}

TNodePtr TTransferTranslation::Build(const TRule_drop_transfer_stmt& node) {
    // drop_transfer_stmt: DROP TRANSFER
    TObjectOperatorContext context(Ctx_.Scoped);
    if (node.GetRule_object_ref3().HasBlock1()) {
        const auto& cluster = node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1();
        if (!ClusterExpr(cluster, /*allowWildcard=*/false, context.ServiceId, context.Cluster)) {
            return {};
        }
    }

    const TString id = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
    return BuildDropTransfer(Ctx_.Pos(),
                             BuildTablePath(Ctx_.GetPrefixPath(context.ServiceId, context.Cluster), id),
                             node.HasBlock4(), context);
}

} // namespace NSQLTranslationV1
