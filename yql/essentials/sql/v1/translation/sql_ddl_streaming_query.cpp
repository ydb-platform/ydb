#include "sql_ddl_streaming_query.h"

#include "antlr_token.h"
#include "object_processing.h"
#include "sql_expression.h"
#include "sql_query.h"

#include <util/string/ascii.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

bool TStreamingQueryTranslation::BuildStreamingQueryNestedSetting(TNodePtr value, TObjectFeatureNodePtr& settings) {
    const auto& items = value->GetTupleNode() ? value->GetTupleNode()->Elements() : TVector<TNodePtr>{value};
    if (items.empty()) {
        return true;
    }

    // Check structure: (key_1 = expr_1, key_2 = expr_2, …, key_N = expr_N)

    for (const auto& item : items) {
        const auto* call = item->GetCallNode();
        if (!call || call->GetOpName() != "==") {
            return true;
        }

        const auto& args = call->GetArgs();
        if (args.size() != 2) {
            return true;
        }

        const auto& nameArg = args[0];
        const auto* source = nameArg->GetSourceName();
        if (!nameArg->GetColumnName() || (source && !source->empty())) {
            return true;
        }
    }

    settings = new TObjectFeatureNode(value->GetPos());
    for (const auto& item : items) {
        const auto& args = item->GetCallNode()->GetArgs();
        if (!BuildStreamingQuerySettingValue(*args[0]->GetColumnName(), args[1], args[0]->GetPos(), *settings)) {
            return false;
        }
    }

    return true;
}

bool TStreamingQueryTranslation::BuildStreamingQuerySettingValue(TStringBuf name, TNodePtr value, TPosition pos, TObjectFeatureNode& features) {
    if (!value) {
        return false;
    }

    const auto id = to_lower(TString(name));
    if (id.StartsWith(TStreamingQuerySettings::RESERVED_FEATURE_PREFIX)) {
        Ctx_.Error(pos) << "Streaming query parameter name should not start with prefix '" << TStreamingQuerySettings::RESERVED_FEATURE_PREFIX << "': " << to_upper(id);
        return false;
    }

    const auto [feature, inserted] = features.AddFeature(id, pos);
    if (!inserted) {
        Ctx_.Error(pos) << "Found duplicated parameter: " << to_upper(id);
        return false;
    }

    if (TObjectFeatureNodePtr nestedFeatures; !BuildStreamingQueryNestedSetting(value, nestedFeatures)) {
        return false;
    } else if (nestedFeatures) {
        feature = nestedFeatures;
        return true;
    }

    if (const auto* name = value->GetColumnName(); name && (!value->GetSourceName() || value->GetSourceName()->empty())) {
        value = BuildLiteralRawString(value->GetPos(), *name);
    }

    feature = new TCallNodeImpl(value->GetPos(), "EvaluateExpr", {value});
    return true;
}

bool TStreamingQueryTranslation::ParseStreamingQuerySetting(const TRule_streaming_query_setting& node, TStreamingQuerySettings& settings) {
    // streaming_query_setting: an_id_or_type = expr

    YQL_ENSURE(settings.Features);

    const auto& name = Id(node.GetRule_an_id_or_type1(), *this);
    const auto pos = Ctx_.Pos();

    TColumnRefScope scope(Ctx_, EColumnRefState::AsStringLiteral);
    const auto value = Unwrap(TSqlExpression(*this).Build(node.GetRule_expr3()));

    return BuildStreamingQuerySettingValue(name, value, pos, *settings.Features);
}

bool TStreamingQueryTranslation::ParseStreamingQuerySettings(const TRule_streaming_query_settings& node, TStreamingQuerySettings& settings) {
    // streaming_query_settings: (
    //     streaming_query_setting
    //     (, streaming_query_setting)* ,?
    // )

    Ctx_.Token(node.GetToken1());

    if (!settings.Features) {
        settings.Features = new TObjectFeatureNode(Ctx_.Pos());
    }

    if (!ParseStreamingQuerySetting(node.GetRule_streaming_query_setting2(), settings)) {
        return false;
    }

    for (const auto& setting : node.GetBlock3()) {
        if (!ParseStreamingQuerySetting(setting.GetRule_streaming_query_setting2(), settings)) {
            return false;
        }
    }

    return true;
}

bool TStreamingQueryTranslation::ParseStreamingQueryDefinition(const TRule_streaming_query_definition& node, TStreamingQuerySettings& settings) {
    // streaming_query_definition: AS DO (BEGIN define_action_or_subquery_body END DO);

    const auto& inlineAction = node.GetRule_inline_action3();
    const auto& queryBegin = inlineAction.GetToken1();
    Ctx_.Token(queryBegin);

    if (!settings.Features) {
        settings.Features = new TObjectFeatureNode(Ctx_.Pos());
    }

    // Save query ast to perform type check and validation of allowed expressions

    const auto saveScoped = Ctx_.Scoped;
    Ctx_.Scoped = Ctx_.CreateScopedState(); // Reset scoped context to interrupt inheritance of global settings and named nodes
    Ctx_.AllScopes.push_back(Ctx_.Scoped);
    Ctx_.Scoped->Local = TScopedState::TLocal{};
    Ctx_.ScopeLevel++;
    TSqlQuery query(Ctx_, Ctx_.Settings.Mode, /* topLevel */ false, /* allowTopLevelPragmas */ true);
    TBlocks innerBlocks;

    TNodePtr clearWorldNode = new TAstListNodeImpl(Ctx_.Pos());
    clearWorldNode->Add("World");
    innerBlocks.push_back(clearWorldNode);

    const bool hasValidBody = DefineActionOrSubqueryBody(query, innerBlocks, inlineAction.GetRule_define_action_or_subquery_body2());
    auto queryNode = hasValidBody ? BuildQuery(Ctx_.Pos(), innerBlocks, /*topLevel=*/false, Ctx_.Scoped, Ctx_.SeqMode) : nullptr;
    if (!WarnUnusedNodes()) {
        return false;
    }
    Ctx_.ScopeLevel--;
    Ctx_.Scoped = saveScoped;

    if (!queryNode) {
        return false;
    }

    TNodePtr blockNode = new TAstListNodeImpl(Ctx_.Pos());
    blockNode->Add("block", blockNode->Q(queryNode));
    settings.Features->AddFeature(TStreamingQuerySettings::QUERY_AST_FEATURE, Ctx_.Pos()).first = blockNode;

    // Extract whole query text between BEGIN and END tokens

    const auto& queryEnd = inlineAction.GetToken3();
    Y_DEBUG_ABORT_UNLESS(IS_TOKEN(queryBegin.GetId(), BEGIN));
    Y_DEBUG_ABORT_UNLESS(IS_TOKEN(queryEnd.GetId(), END));

    auto beginPos = GetQueryPosition(Ctx_.Query, queryBegin);
    const auto endPos = GetQueryPosition(Ctx_.Query, queryEnd);
    if (beginPos == std::string::npos || endPos == std::string::npos) {
        Error() << "Failed to parse streaming query definition";
        return false;
    }

    beginPos += queryBegin.value().size();
    settings.Features->AddFeature(TStreamingQuerySettings::QUERY_TEXT_FEATURE, Ctx_.Pos()).first = BuildQuotedAtom(Ctx_.Pos(), Ctx_.Query.substr(beginPos, endPos - beginPos));

    return true;
}

bool TStreamingQueryTranslation::ParseAlterStreamingQueryAction(const TRule_alter_streaming_query_action& node, TStreamingQuerySettings& settings) {
    // alter_streaming_query_action:
    //     (SET streaming_query_settings)
    //   | (SET streaming_query_settings)? streaming_query_definition

    switch (node.GetAltCase()) {
        case TRule_alter_streaming_query_action::kAltAlterStreamingQueryAction1: {
            const auto& alterSettingsNode = node.GetAlt_alter_streaming_query_action1().GetRule_alter_streaming_query_set_settings1();
            Ctx_.Token(alterSettingsNode.GetToken1());

            if (!ParseStreamingQuerySettings(alterSettingsNode.GetRule_streaming_query_settings2(), settings)) {
                return false;
            }

            break;
        }
        case TRule_alter_streaming_query_action::kAltAlterStreamingQueryAction2: {
            const auto& action = node.GetAlt_alter_streaming_query_action2();

            if (action.HasBlock1()) {
                const auto& alterSettingsNode = action.GetBlock1().GetRule_alter_streaming_query_set_settings1();
                Ctx_.Token(alterSettingsNode.GetToken1());

                if (!ParseStreamingQuerySettings(alterSettingsNode.GetRule_streaming_query_settings2(), settings)) {
                    return false;
                }
            }

            if (!ParseStreamingQueryDefinition(action.GetRule_streaming_query_definition2(), settings)) {
                return false;
            }

            break;
        }
        case TRule_alter_streaming_query_action::ALT_NOT_SET:
            YQL_ENSURE(false, "Unreachable");
    }

    return true;
}

TNodePtr TStreamingQueryTranslation::Build(const TRule_create_streaming_query_stmt& node) {
    // create_streaming_query_stmt: CREATE (OR REPLACE)? STREAMING QUERY (IF NOT EXISTS)? object_ref
    //     (WITH streaming_query_settings)?
    //     streaming_query_definition;

    Ctx_.Token(node.GetToken1());

    // object_ref
    TObjectOperatorContext context(Ctx_.Scoped);
    const auto& objectPath = ParseObjectPath(node.GetRule_object_ref6(), context);
    if (!objectPath) {
        return {};
    }

    // OR REPLACE
    const bool replaceIfExists = node.HasBlock2();
    if (replaceIfExists) {
        Y_DEBUG_ABORT_UNLESS(
            IS_TOKEN(node.GetBlock2().GetToken1().GetId(), OR) &&
            IS_TOKEN(node.GetBlock2().GetToken2().GetId(), REPLACE));
    }

    // IF NOT EXISTS
    const bool existingOk = node.HasBlock5();
    if (existingOk) {
        Y_DEBUG_ABORT_UNLESS(
            IS_TOKEN(node.GetBlock5().GetToken1().GetId(), IF) &&
            IS_TOKEN(node.GetBlock5().GetToken2().GetId(), NOT) &&
            IS_TOKEN(node.GetBlock5().GetToken3().GetId(), EXISTS));
    }

    // WITH streaming_query_settings
    TStreamingQuerySettings settings;
    if (node.HasBlock7()) {
        const auto& features = node.GetBlock7().GetRule_create_streaming_query_features1();
        Ctx_.Token(features.GetToken1());

        if (!ParseStreamingQuerySettings(features.GetRule_streaming_query_settings2(), settings)) {
            return {};
        }
    }

    // streaming_query_definition
    if (!ParseStreamingQueryDefinition(node.GetRule_streaming_query_definition8(), settings)) {
        return {};
    }

    return BuildCreateStreamingQuery(Ctx_.Pos(), *objectPath, existingOk, replaceIfExists,
                                     std::move(settings.Features), context);
}

TNodePtr TStreamingQueryTranslation::Build(const TRule_alter_streaming_query_stmt& node) {
    // alter_streaming_query_stmt: ALTER STREAMING QUERY (IF EXISTS)? object_ref
    //     alter_streaming_query_action;

    // object_ref
    TObjectOperatorContext context(Ctx_.Scoped);
    const auto& objectPath = ParseObjectPath(node.GetRule_object_ref5(), context);
    if (!objectPath) {
        return {};
    }

    // IF EXISTS
    const bool missingOk = node.HasBlock4();
    if (missingOk) {
        Y_DEBUG_ABORT_UNLESS(
            IS_TOKEN(node.GetBlock4().GetToken1().GetId(), IF) &&
            IS_TOKEN(node.GetBlock4().GetToken2().GetId(), EXISTS));
    }

    // alter_streaming_query_action
    TStreamingQuerySettings settings;
    if (!ParseAlterStreamingQueryAction(node.GetRule_alter_streaming_query_action6(), settings)) {
        return {};
    }

    return BuildAlterStreamingQuery(Ctx_.Pos(), *objectPath, missingOk,
                                    std::move(settings.Features), context);
}

TNodePtr TStreamingQueryTranslation::Build(const TRule_drop_streaming_query_stmt& node) {
    // drop_streaming_query_stmt: DROP STREAMING QUERY (IF EXISTS)? object_ref;

    // object_ref
    TObjectOperatorContext context(Ctx_.Scoped);
    const auto& objectPath = ParseObjectPath(node.GetRule_object_ref5(), context);
    if (!objectPath) {
        return {};
    }

    // IF EXISTS
    const bool missingOk = node.HasBlock4();
    if (missingOk) {
        Y_DEBUG_ABORT_UNLESS(
            IS_TOKEN(node.GetBlock4().GetToken1().GetId(), IF) &&
            IS_TOKEN(node.GetBlock4().GetToken2().GetId(), EXISTS));
    }

    return BuildDropStreamingQuery(Ctx_.Pos(), *objectPath, missingOk, context);
}

} // namespace NSQLTranslationV1
