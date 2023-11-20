#include "yql_yt_table_desc.h"

#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/sql/sql.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/generic/scope.h>

namespace NYql {

namespace {

const TString RAW_VIEW_SQL = "select * from self_raw";

TExprNode::TPtr BuildProtoRemapper(const TMap<TString, TString>& protoFields, TExprContext& ctx) {
    auto rowArg = ctx.NewArgument(TPosition(), TStringBuf("row"));
    auto result = rowArg;
    for (auto& x : protoFields) {
        result = ctx.Builder(TPositionHandle())
            .Callable("ReplaceMember")
                .Add(0, result)
                .Atom(1, x.first)
                .Callable(2, "Apply")
                    .Callable(0, "Udf")
                        .Atom(0, "Protobuf.Parse")
                        .Callable(1, "Void")
                        .Seal()
                        .Callable(2, "Void")
                        .Seal()
                        .Atom(3, x.second)
                    .Seal()
                    .Callable(1, "Member")
                        .Add(0, result)
                        .Atom(1, x.first)
                    .Seal()
                .Seal()
            .Seal()
            .Build();
    }

    return ctx.NewLambda(TPosition(),
        ctx.NewArguments(TPosition(), { rowArg }), std::move(result));
}

TExprNode::TPtr BuildUdfRemapper(const THashMap<TString, TString>& metaAttrs, TExprContext& ctx) {
    return ctx.Builder(TPositionHandle())
        .Lambda()
            .Param(TStringBuf("row"))
            .Callable(TStringBuf("Apply"))
                .Callable(0, TStringBuf("Udf"))
                    .Atom(0, metaAttrs.at(YqlReadUdfAttribute))
                    .Do([&](TExprNodeBuilder& parent) -> TExprNodeBuilder& {
                        if (auto runConfigValue = metaAttrs.FindPtr(YqlReadUdfRunConfigAttribute)) {
                            parent.Callable(1, TStringBuf("String"))
                                    .Atom(0, *runConfigValue)
                                .Seal();
                        } else {
                            parent.Callable(1, TStringBuf("Void"))
                                .Seal();
                        }
                        return parent;
                    })
                    .Callable(2, TStringBuf("Void")) // User type
                    .Seal()
                    .Atom(3, metaAttrs.Value(YqlReadUdfTypeConfigAttribute, TString()))
                .Seal()
                .Arg(1, TStringBuf("row"))
            .Seal()
        .Seal()
        .Build();
}

TExprNode::TPtr BuildIgnoreTypeV3Remapper(const TStructExprType* rowType, TExprContext& ctx) {
    auto rowArg = ctx.NewArgument(TPosition(), TStringBuf("row"));
    auto result = rowArg;
    for (const TItemExprType* itemType : rowType->GetItems()) {
        auto untaggedType = itemType->GetItemType();
        while (untaggedType->GetKind() == ETypeAnnotationKind::Tagged) {
            untaggedType = untaggedType->Cast<TTaggedExprType>()->GetBaseType();
        }

        auto unwrappedType = untaggedType;
        if (unwrappedType->GetKind() == ETypeAnnotationKind::Optional) {
            unwrappedType = unwrappedType->Cast<TOptionalExprType>()->GetItemType();
        }

        if (unwrappedType->GetKind() != ETypeAnnotationKind::Data) {

            auto argumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{untaggedType});

            auto udfArgumentsType = ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
                argumentsType,
                ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{}),
                ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{})
            });

            auto member = ctx.Builder(TPositionHandle())
                .Callable("Member")
                    .Add(0, result)
                    .Atom(1, itemType->GetName())
                .Seal()
                .Build();

            for (auto type = itemType->GetItemType(); type->GetKind() == ETypeAnnotationKind::Tagged; type = type->Cast<TTaggedExprType>()->GetBaseType()) {
                member = ctx.Builder(TPositionHandle())
                    .Callable("Untag")
                        .Add(0, member)
                        .Atom(1, type->Cast<TTaggedExprType>()->GetTag())
                    .Seal()
                    .Build();
            }

            result = ctx.Builder(TPositionHandle())
                .Callable("ReplaceMember")
                    .Add(0, result)
                    .Atom(1, itemType->GetName())
                    .Callable(2, "Apply")
                        .Callable(0, "Udf")
                            .Atom(0, "Yson2.Serialize")
                            .Callable(1, "Void")
                            .Seal()
                            .Callable(2, "Void")
                            .Seal()
                        .Seal()
                        .Callable(1, "Apply")
                            .Callable(0, "Udf")
                                .Atom(0, "Yson2.From")
                                .Callable(1, "Void")
                                .Seal()
                                .Add(2, ExpandType(TPositionHandle(), *udfArgumentsType, ctx))
                            .Seal()
                            .Add(1, member)
                        .Seal()
                    .Seal()
                .Seal()
                .Build();
        }
    }

    if (result == rowArg) {
        // No items to remap
        return {};
    }
    return ctx.NewLambda(TPosition(),
        ctx.NewArguments(TPosition(), { rowArg }), std::move(result));
}

TExprNode::TPtr CompileViewSql(const TString& provider, const TString& cluster, const TString& sql, ui16 syntaxVersion,
    TExprContext& ctx, IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager,
    IRandomProvider& randomProvider, bool enableViewIsolation, IUdfResolver::TPtr udfResolver)
{
    NSQLTranslation::TTranslationSettings settings;
    settings.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;
    settings.DefaultCluster = cluster.empty() ? "view" : cluster;
    settings.ClusterMapping[settings.DefaultCluster] = cluster.empty() ? "data" : provider;
    settings.SyntaxVersion = syntaxVersion;
    settings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
    settings.FileAliasPrefix = "view_" + randomProvider.GenUuid4().AsGuidString() + "/";
    if (!enableViewIsolation) {
        settings.FileAliasPrefix.clear(); // disable FileAliasPrefix while preserving number of randomProvider calls
    }

    NYql::TAstParseResult sqlRes = NSQLTranslation::SqlToYql(sql, settings);
    ctx.IssueManager.RaiseIssues(sqlRes.Issues);
    if (!sqlRes.IsOk()) {
        return {};
    }

    TString oldAliasPrefix = moduleResolver->GetFileAliasPrefix();
    moduleResolver->SetFileAliasPrefix(TString{settings.FileAliasPrefix});
    Y_DEFER {
        moduleResolver->SetFileAliasPrefix(std::move(oldAliasPrefix));
    };
    TExprNode::TPtr exprRoot;
    if (!CompileExpr(*sqlRes.Root, exprRoot, ctx, moduleResolver, urlListerManager, false, Max<ui32>(), syntaxVersion)) {
        return {};
    }

    if (!enableViewIsolation) {
        return exprRoot;
    }

    constexpr TStringBuf OuterFuncs[] = {
        "SecureParam",
        "CurrentOperationId",
        "CurrentOperationSharedId",
        "CurrentAuthenticatedUser",
    };

    constexpr TStringBuf CodegenFuncs[] = {
        "FilePath",
        "FileContent",
        "FolderPath",
        "Files",
        "Configure!",
        "Udf",
        "ScriptUdf",
        "SqlCall",
    };

    TOptimizeExprSettings optSettings(nullptr);
    optSettings.VisitChanges = true;
    auto status = OptimizeExpr(exprRoot, exprRoot, [&](const TExprNode::TPtr& node, TExprContext& ctx) -> TExprNode::TPtr {
        for (const auto& name : OuterFuncs) {
            if (node->IsCallable(name)) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << name << " function can't be used in views"));
                return nullptr;
            }
        }

        if (node->IsCallable("FuncCode") && node->ChildrenSize() > 0) {
            if (!node->Head().IsCallable("String")) {
                ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "FuncCode should have constant function name in views"));
                return nullptr;
            }

            if (node->Head().Head().IsAtom()) {
                for (const auto& name : OuterFuncs) {
                    if (node->Head().Head().Content() == name) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << name << " function can't be used in views"));
                        return nullptr;
                    }
                }

                for (const auto& name : CodegenFuncs) {
                    if (node->Head().Head().Content() == name) {
                        ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), TStringBuilder() << name << " function can't be used inside generated code in views"));
                        return nullptr;
                    }
                }
            }
        }

        if (node->IsCallable("ScriptUdf") && node->ChildrenSize() > 0 && node->Head().Content().StartsWith("CustomPython")) {
            ctx.AddError(TIssue(ctx.GetPosition(node->Pos()), "CustomPython module can't be used in views"));
            return nullptr;
        }

        if (node->IsCallable({"Udf","SqlCall"}) && node->Head().IsAtom()) {
            auto origFunc = node->Head().Content();
            TStringBuf moduleName, funcName;
            if (!SplitUdfName(origFunc, moduleName, funcName)) {
                return node;
            }

            if (udfResolver->ContainsModule(TString(moduleName))) {
                return node;
            }

            return ctx.ChangeChild(*node, 0, 
                ctx.NewAtom(node->Head().Pos(), settings.FileAliasPrefix + origFunc));
        }

        return node;
    }, ctx, optSettings);

    if (status == IGraphTransformer::TStatus::Error) {
        return nullptr;
    };

    return exprRoot;
}

} // unnamed


bool TYtViewDescription::Fill(const TString& provider, const TString& cluster, const TString& sql, ui16 syntaxVersion, TExprContext& ctx,
    IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager, IRandomProvider& randomProvider, bool enableViewIsolation,
    IUdfResolver::TPtr udfResolver)
{
    Sql = sql;
    CompiledSql = CompileViewSql(provider, cluster, sql, syntaxVersion, ctx, moduleResolver, urlListerManager, randomProvider, enableViewIsolation, udfResolver);
    return bool(CompiledSql);
}

void TYtViewDescription::CleanupCompiledSQL()
{
    CompiledSql.Reset();
}

bool TYtTableDescriptionBase::Fill(const TString& provider, const TString& cluster, const TString& table,
    const TStructExprType* type, const TString& viewSql, ui16 syntaxVersion, const THashMap<TString, TString>& metaAttrs,
    TExprContext& ctx, IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager, IRandomProvider& randomProvider, bool enableViewIsolation,
    IUdfResolver::TPtr udfResolver)
{
    // (1) row type
    RawRowType = type;
    YQL_ENSURE(RawRowType && RawRowType->GetKind() == ETypeAnnotationKind::Struct);
    RowType = RawRowType;

    bool onlyRawView = false;
    if (TYtTableIntent::View == Intents) {
        for (auto& view : Views) {
            if (view.first == TStringBuf("raw")) {
                onlyRawView = true;
            } else {
                onlyRawView = false;
                break;
            }
        }
    }

    // (2) UDF remapper / proto fields
    if (!onlyRawView) {
        for (auto& x: metaAttrs) {
            if (x.first.StartsWith(YqlProtoFieldPrefixAttribute)) {
                auto fieldName = x.first.substr(YqlProtoFieldPrefixAttribute.size());
                if (fieldName.empty()) {
                    ctx.AddError(TIssue(TPosition(),
                        TStringBuilder() << "Proto field name should not be empty. Table: "
                        << cluster << '.' << table));
                    return false;
                }
                if (type->FindItem(fieldName)) { // ignore nonexisting fields
                    ProtoFields.insert({fieldName, x.second});
                } else {
                    ctx.AddWarning(YqlIssue(TPosition(), EYqlIssueCode::TIssuesIds_EIssueCode_YT_MISSING_PROTO_FIELD,
                        TStringBuilder() << "Proto field name for missing column " << fieldName << ". Table: "
                        << cluster << '.' << table));
                }
            }
        }

        const TString* udfName = metaAttrs.FindPtr(YqlReadUdfAttribute);
        if (udfName && !ProtoFields.empty()) {
            ctx.AddError(TIssue(TPosition(),
                TStringBuilder() << "UDF remapper and proto fields cannot be declared simultaneously. Table: "
                    << cluster << '.' << table));
            return false;
        }

        if (IgnoreTypeV3) {
            UdfApplyLambda = BuildIgnoreTypeV3Remapper(RawRowType->Cast<TStructExprType>(), ctx);
        }
        TExprNode::TPtr lambda;
        if (udfName) {
            lambda = BuildUdfRemapper(metaAttrs, ctx);
        } else if (!ProtoFields.empty()) {
            lambda = BuildProtoRemapper(ProtoFields, ctx);
        }
        if (lambda) {
            if (UdfApplyLambda) {
                UdfApplyLambda = ctx.Builder(TPositionHandle())
                    .Lambda()
                        .Param("row")
                        .Apply(lambda)
                            .With(0)
                                .Apply(UdfApplyLambda)
                                    .With(0, "row")
                                .Seal()
                            .Done()
                        .Seal()
                    .Seal()
                    .Build();
            } else {
                UdfApplyLambda = lambda;
            }
        }
        HasUdfApply = (bool)UdfApplyLambda;
    }

    // (3) views
    if (!FillViews(provider, cluster, table, metaAttrs, ctx, moduleResolver, urlListerManager, randomProvider, enableViewIsolation, udfResolver)) {
        return false;
    }

    if (viewSql) {
        if (!View) {
            if (!View.ConstructInPlace().Fill(provider, cluster, viewSql, syntaxVersion, ctx, moduleResolver, urlListerManager, randomProvider, enableViewIsolation, udfResolver)) {
                ctx.AddError(TIssue(TPosition(),
                    TStringBuilder() << "Can't load sql view, table: " << cluster << '.' << table));
                return false;
            }
        }
    }

    return true;
}

bool TYtTableDescriptionBase::FillViews(const TString& provider, const TString& cluster, const TString& table,
    const THashMap<TString, TString>& metaAttrs, TExprContext& ctx, IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager,
    IRandomProvider& randomProvider, bool allowViewIsolation, IUdfResolver::TPtr udfResolver)
{
    for (auto& view: Views) {
        TYtViewDescription& viewDesc = view.second;

        if (!viewDesc.CompiledSql) {
            TString viewSql;
            ui16 syntaxVersion = 1;
            if (view.first == "raw") {
                viewSql = RAW_VIEW_SQL;
            } else {
                auto sql = metaAttrs.FindPtr(YqlViewPrefixAttribute + view.first);
                if (!sql) {
                    ctx.AddError(TIssue(TPosition(),
                        TStringBuilder() << "View " << view.first
                        << " not found in table " << cluster << '.' << table
                        << " metadata").SetCode(TIssuesIds::YT_VIEW_NOT_FOUND, TSeverityIds::S_ERROR));
                    return false;
                }

                viewSql = *sql;
                auto sqlSyntaxVersion = metaAttrs.FindPtr("_yql_syntax_version_" + view.first);
                if (sqlSyntaxVersion) {
                    syntaxVersion = FromString<ui16>(*sqlSyntaxVersion);
                }
            }

            if (!viewDesc.Fill(provider, cluster, viewSql, syntaxVersion, ctx, moduleResolver, urlListerManager, randomProvider, allowViewIsolation, udfResolver)) {
                ctx.AddError(TIssue(TPosition(),
                    TStringBuilder() << "Can't load sql view " << viewSql.Quote()
                    << ", table: " << cluster << '.' << table
                    << ", view: " << view.first));
                return false;
            }
        }
    }
    return true;
}

void TYtTableDescriptionBase::CleanupCompiledSQL()
{
    UdfApplyLambda.Reset();
    for (auto& view : Views) {
        view.second.CleanupCompiledSQL();
    }
    if (View) {
        View->CleanupCompiledSQL();
    }
}

}
