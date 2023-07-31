#include "type_from_schema.h"

#include <library/cpp/yson/node/node_io.h>

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>

namespace {
    using namespace NYql;

#define REPORT(...) ctx.AddError(TIssue(TString(TStringBuilder() << __VA_ARGS__)))

    bool CheckStruct(const TStructExprType* got, const TStructExprType* expected, TExprContext& ctx) {
        auto status = true;

        if (expected) {
            for (const auto* gotNamedItem : got->GetItems()) {
                auto expectedIndex = expected->FindItem(gotNamedItem->GetName());
                if (expectedIndex) {
                    const auto* gotItem = gotNamedItem->GetItemType();
                    const auto* expectedItem = expected->GetItems()[*expectedIndex]->GetItemType();

                    auto arg = ctx.NewArgument(TPositionHandle(), "arg");
                    auto fieldConversionStatus = TrySilentConvertTo(arg, *gotItem, *expectedItem, ctx);
                    if (fieldConversionStatus.Level == IGraphTransformer::TStatus::Error) {
                        REPORT("Item " << TString{gotNamedItem->GetName()}.Quote() << " expected to be " <<
                               *expectedItem << ", but got " << *gotItem);
                        status = false;
                    }
                } else {
                    REPORT("Got unexpected item " << TString{gotNamedItem->GetName()}.Quote());
                    status = false;
                }
            }

            for (const auto* expectedNamedItem : expected->GetItems()) {
                if (expectedNamedItem->GetItemType()->GetKind() == ETypeAnnotationKind::Optional) {
                    continue;
                }
                if (!got->FindItem(expectedNamedItem->GetName())) {
                    REPORT("Expected item " << TString{expectedNamedItem->GetName()}.Quote());
                    status = false;
                }
            }
        }

        return status;
    }

    bool CheckVariantContent(const TStructExprType* got, const TStructExprType* expected, TExprContext& ctx) {
        auto status = true;

        if (expected) {
            for (const auto* gotNamedItem : got->GetItems()) {
                if (!expected->FindItem(gotNamedItem->GetName())) {
                    REPORT("Got unexpected alternative " << TString{gotNamedItem->GetName()}.Quote());
                    status = false;
                }
            }

            for (const auto* expectedNamedItem : expected->GetItems()) {
                if (!got->FindItem(expectedNamedItem->GetName())) {
                    REPORT("Expected alternative " << TString{expectedNamedItem->GetName()}.Quote());
                    status = false;
                }
            }
        }

        for (const auto* gotNamedItem : got->GetItems()) {
            const auto* gotItem = gotNamedItem->GetItemType();
            auto expectedIndex = expected ? expected->FindItem(gotNamedItem->GetName()) : Nothing();
            const auto* expectedItem = expected && expectedIndex ? expected->GetItems()[*expectedIndex]->GetItemType() : nullptr;

            TIssueScopeGuard issueScope(ctx.IssueManager, [&]() {
                return new TIssue(TPosition(), TStringBuilder() << "Alternative " << TString{gotNamedItem->GetName()}.Quote());
            });

            if (expectedItem && expectedItem->GetKind() != gotItem->GetKind()) {
                REPORT("Expected to be " << expectedItem->GetKind() << ", but got " << gotItem->GetKind());
                status = false;
            }

            if (gotItem->GetKind() != ETypeAnnotationKind::Struct) {
                REPORT("Expected to be Struct, but got " << gotItem->GetKind());
                status = false;
            }

            const auto* gotStruct = gotItem->Cast<TStructExprType>();
            const auto* expectedStruct = expectedItem ? expectedItem->Cast<TStructExprType>() : nullptr;

            if (!CheckStruct(gotStruct, expectedStruct, ctx)) {
                status = false;
            }
        }

        return status;
    }

    bool CheckVariantContent(const TTupleExprType* got, const TTupleExprType* expected, TExprContext& ctx) {
        if (expected && expected->GetSize() != got->GetSize()) {
            REPORT("Expected to have " << expected->GetSize() << " alternatives, but got " << got->GetSize());
            return false;
        }

        auto status = true;

        for (size_t i = 0; i < got->GetSize(); i++) {
            const auto* gotItem = got->GetItems()[i];
            const auto* expectedItem = expected ? expected->GetItems()[i] : nullptr;

            TIssueScopeGuard issueScope(ctx.IssueManager, [i]() {
                return new TIssue(TPosition(), TStringBuilder() << "Alternative #" << i);
            });

            if (expectedItem && expectedItem->GetKind() != gotItem->GetKind()) {
                REPORT("Expected " << expectedItem->GetKind() << ", but got " << gotItem->GetKind());
                status = false;
            }

            if (gotItem->GetKind() != ETypeAnnotationKind::Struct) {
                REPORT("Expected Struct, but got " << gotItem->GetKind());
                status = false;
            }

            const auto* gotStruct = gotItem->Cast<TStructExprType>();
            const auto* expectedStruct = expectedItem ? expectedItem->Cast<TStructExprType>() : nullptr;

            if (!CheckStruct(gotStruct, expectedStruct, ctx)) {
                status = false;
            }
        }

        return status;
    }

    bool CheckVariant(const TVariantExprType* got, const TVariantExprType* expected, TExprContext& ctx) {
        if (expected && expected->GetUnderlyingType()->GetKind() != got->GetUnderlyingType()->GetKind()) {
            REPORT("Expected Variant over " << expected->GetUnderlyingType()->GetKind() <<
                   ", but got Variant over " << got->GetUnderlyingType()->GetKind());
            return false;
        }

        switch (got->GetUnderlyingType()->GetKind()) {
            case ETypeAnnotationKind::Struct:
            {
                const auto* gotStruct = got->GetUnderlyingType()->Cast<TStructExprType>();
                const auto* expectedStruct = expected ? expected->GetUnderlyingType()->Cast<TStructExprType>() : nullptr;
                return CheckVariantContent(gotStruct, expectedStruct, ctx);
            }
            case ETypeAnnotationKind::Tuple:
            {
                const auto* gotTuple = got->GetUnderlyingType()->Cast<TTupleExprType>();
                const auto* expectedTuple = expected ? expected->GetUnderlyingType()->Cast<TTupleExprType>() : nullptr;
                return CheckVariantContent(gotTuple, expectedTuple, ctx);
            }
            default:
                Y_UNREACHABLE();
        }

        return false;
    }

    bool CheckSchema(const TTypeAnnotationNode* got, const TTypeAnnotationNode* expected, TExprContext& ctx, bool allowVariant) {
        if (expected && expected->GetKind() != got->GetKind()) {
            REPORT("Expected " << expected->GetKind() << ", but got " << got->GetKind());
            return false;
        }

        switch (got->GetKind()) {
            case ETypeAnnotationKind::Struct:
            {
                TIssueScopeGuard issueScope(ctx.IssueManager, []() { return new TIssue(TPosition(), "Toplevel struct"); });

                const auto* gotStruct = got->Cast<TStructExprType>();
                const auto* expectedStruct = expected ? expected->Cast<TStructExprType>() : nullptr;

                if (!gotStruct->Validate(TPositionHandle(), ctx)) {
                    return false;
                }

                return CheckStruct(gotStruct, expectedStruct, ctx);
            }
            case ETypeAnnotationKind::Variant:
                if (allowVariant) {
                    TIssueScopeGuard issueScope(ctx.IssueManager, []() { return new TIssue(TPosition(), "Toplevel variant"); });

                    const auto* gotVariant = got->Cast<TVariantExprType>();
                    const auto* expectedVariant = expected ? expected->Cast<TVariantExprType>() : nullptr;

                    if (!gotVariant->Validate(TPositionHandle(), ctx)) {
                        return false;
                    }

                    return CheckVariant(gotVariant, expectedVariant, ctx);
                }
                [[fallthrough]];
            default:
                if (allowVariant) {
                    REPORT("Expected Struct or Variant, but got " << got->GetKind());
                } else {
                    REPORT("Expected Struct, but got " << got->GetKind());
                }
                return false;
        }
    }
}

namespace NYql::NPureCalc {
    const TTypeAnnotationNode* MakeTypeFromSchema(const NYT::TNode& yson, TExprContext& ctx) {
        const auto* type = NCommon::ParseTypeFromYson(yson, ctx);

        if (!type) {
            ythrow TCompileError("", ctx.IssueManager.GetIssues().ToString())
                << "Incorrect schema: " << NYT::NodeToYsonString(yson, NYson::EYsonFormat::Text);
        }

        return type;
    }

    const TStructExprType* ExtendStructType(
        const TStructExprType* type, const THashMap<TString, NYT::TNode>& extraColumns, TExprContext& ctx)
    {
        if (extraColumns.empty()) {
            return type;
        }

        auto items = type->GetItems();
        for (const auto& pair : extraColumns) {
            items.push_back(ctx.MakeType<TItemExprType>(pair.first, MakeTypeFromSchema(pair.second, ctx)));
        }

        auto result = ctx.MakeType<TStructExprType>(items);

        if (!result->Validate(TPosition(), ctx)) {
            ythrow TCompileError("", ctx.IssueManager.GetIssues().ToString()) << "Incorrect extended struct type";
        }

        return result;
    }

    bool ValidateInputSchema(const TTypeAnnotationNode* type, TExprContext& ctx) {
        TIssueScopeGuard issueScope(ctx.IssueManager, []() { return new TIssue(TPosition(), "Input schema"); });
        return CheckSchema(type, nullptr, ctx, false);
    }

    bool ValidateOutputSchema(const TTypeAnnotationNode* type, TExprContext& ctx) {
        TIssueScopeGuard issueScope(ctx.IssueManager, []() { return new TIssue(TPosition(), "Output schema"); });
        return CheckSchema(type, nullptr, ctx, true);
    }

    bool ValidateOutputType(const TTypeAnnotationNode* type, const TTypeAnnotationNode* expected, TExprContext& ctx) {
        TIssueScopeGuard issueScope(ctx.IssueManager, []() { return new TIssue(TPosition(), "Program return type"); });
        return CheckSchema(type, expected, ctx, true);
    }
}
