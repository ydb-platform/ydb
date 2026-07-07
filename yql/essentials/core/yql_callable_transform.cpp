#include "yql_callable_transform.h"

#include <yql/essentials/core/langver/feature.gen.h>

namespace NYql {

IDataProvider* TCallableTransformerParsers::ParseCommit(const TExprNode& input, TExprContext& ctx, bool& isUniversal) {
    isUniversal = false;
    if (!EnsureMinArgsCount(input, 2, ctx)) {
        return nullptr;
    }

    if (!EnsureMaxArgsCount(input, 3, ctx)) {
        return nullptr;
    }

    if (input.Child(0)->GetTypeAnn() && input.Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        isUniversal = true;
        return nullptr;
    }

    if (!EnsureWorldType(*input.Child(0), ctx)) {
        return nullptr;
    }

    if (input.Child(1)->GetTypeAnn() && input.Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        isUniversal = true;
        return nullptr;
    }

    if (!EnsureDataSink(*input.Child(1), ctx)) {
        return nullptr;
    }

    if (input.ChildrenSize() == 3) {
        if (input.Child(2)->GetTypeAnn() && input.Child(2)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
            isUniversal = true;
            return nullptr;
        }

        if (!EnsureTuple(*input.Child(2), ctx)) {
            return nullptr;
        }

        for (auto& setting : input.Child(2)->Children()) {
            if (!EnsureTupleSize(*setting, 2, ctx)) {
                return nullptr;
            }

            auto nameNode = setting->Child(0);
            if (!EnsureAtom(*nameNode, ctx)) {
                return nullptr;
            }
        }
    }

    auto datasinkName = input.Child(1)->Child(0)->Content();
    auto datasink = Types_.DataSinkMap.FindPtr(datasinkName);
    if (!datasink) {
        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasink: " << datasinkName));
        return nullptr;
    }

    return (*datasink).Get();
}

IDataProvider* TCallableTransformerParsers::ParseRead(const TExprNode& input, TExprContext& ctx) {
    if (!EnsureMinArgsCount(input, 2, ctx)) {
        return nullptr;
    }

    if (!EnsureWorldType(*input.Child(0), ctx)) {
        return nullptr;
    }

    if (!EnsureDataSource(*input.Child(1), ctx)) {
        return nullptr;
    }

    auto datasourceName = input.Child(1)->Child(0)->Content();
    auto datasource = Types_.DataSourceMap.FindPtr(datasourceName);
    if (!datasource) {
        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasource: " << datasourceName));
        return nullptr;
    }

    return (*datasource).Get();
}

IDataProvider* TCallableTransformerParsers::ParseWrite(const TExprNode& input, TExprContext& ctx) {
    if (!EnsureMinArgsCount(input, 2, ctx)) {
        return nullptr;
    }

    if (!EnsureWorldType(*input.Child(0), ctx)) {
        return nullptr;
    }

    if (!EnsureDataSink(*input.Child(1), ctx)) {
        return nullptr;
    }

    auto datasinkName = input.Child(1)->Child(0)->Content();
    auto datasink = Types_.DataSinkMap.FindPtr(datasinkName);
    if (!datasink) {
        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasink: " << datasinkName));
        return nullptr;
    }

    return (*datasink).Get();
}

IDataProvider* TCallableTransformerParsers::ParseMaterialize(const TExprNode& input, TExprContext& ctx) {
    if (!IsAvailableLangVersion(NFeature::Materialize.MinLangVer, Types_.LangVer)) {
        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder()
            << "Materialize! is not available before version " << FormatLangVersion(NFeature::Materialize.MinLangVer).GetOrElse("<unknown>")));
        return nullptr;
    }

    if (!EnsureArgsCount(input, 4, ctx)) {
        return nullptr;
    }

    if (!EnsureWorldType(*input.Child(0), ctx)) {
        return nullptr;
    }

    if (!EnsureDataSink(*input.Child(1), ctx)) {
        return nullptr;
    }

    auto datasinkName = input.Child(1)->Child(0)->Content();
    auto datasink = Types_.DataSinkMap.FindPtr(datasinkName);
    if (!datasink) {
        ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasink: " << datasinkName));
        return nullptr;
    }

    if (!EnsureTuple(*input.Child(3), ctx)) {
        return nullptr;
    }

    return (*datasink).Get();
}

IDataProvider* TCallableTransformerParsers::ParseConfigure(const TExprNode& input, TExprContext& ctx, bool& isUniversal) {
    isUniversal= false;
    if (!EnsureMinArgsCount(input, 2, ctx)) {
        return nullptr;
    }

    if (input.Child(0)->GetTypeAnn() && input.Child(0)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        isUniversal = true;
        return nullptr;
    }

    if (!EnsureWorldType(*input.Child(0), ctx)) {
        return nullptr;
    }

    if (input.Child(1)->GetTypeAnn() && input.Child(1)->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Universal) {
        isUniversal = true;
        return nullptr;
    }

    if (!EnsureDataProvider(*input.Child(1), ctx)) {
        return nullptr;
    }

    if (input.Child(1)->IsCallable("DataSource")) {
        auto datasourceName = input.Child(1)->Child(0)->Content();
        auto datasource = Types_.DataSourceMap.FindPtr(datasourceName);
        if (!datasource) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasource: " << datasourceName));
            return nullptr;
        }

        return (*datasource).Get();
    }

    if (input.Child(1)->IsCallable("DataSink")) {
        auto datasinkName = input.Child(1)->Child(0)->Content();
        auto datasink = Types_.DataSinkMap.FindPtr(datasinkName);
        if (!datasink) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unsupported datasink: " << datasinkName));
            return nullptr;
        }

        return (*datasink).Get();
    }

    YQL_ENSURE(false, "Unexpected provider class");
}

} // namespace NYql
