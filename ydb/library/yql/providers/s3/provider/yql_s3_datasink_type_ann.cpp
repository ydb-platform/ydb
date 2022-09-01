#include "yql_s3_path.h"
#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TS3DataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TS3DataSinkTypeAnnotationTransformer(TS3State::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TS3DataSinkTypeAnnotationTransformer;
        AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
        AddHandler({TS3WriteObject::CallableName()}, Hndl(&TSelf::HandleWrite));
        AddHandler({TS3Target::CallableName()}, Hndl(&TSelf::HandleTarget));
        AddHandler({TS3SinkSettings::CallableName()}, Hndl(&TSelf::HandleSink));
        AddHandler({TS3SinkOutput::CallableName()}, Hndl(&TSelf::HandleOutput));
    }
private:
    TStatus HandleCommit(TExprBase input, TExprContext&) {
        const auto commit = input.Cast<TCoCommit>();
        input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleWrite(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 4U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TS3WriteObject::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSink(*input->Child(TS3WriteObject::idx_DataSink), S3ProviderName, ctx)) {
            return TStatus::Error;
        }

        if (!TS3Target::Match(input->Child(TS3WriteObject::idx_Target))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3WriteObject::idx_Target)->Pos()), "Expected S3 target."));
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TWorldExprType>());
        return TStatus::Ok;
    }

    TStatus HandleTarget(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 2U, 3U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TS3Target::idx_Path), ctx)) {
            return TStatus::Error;
        }

        const auto& path = input->Child(TS3Target::idx_Path)->Content();
        if (path.empty() || path.back() != '/') {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3Target::idx_Path)->Pos()), "Expected non empty path to directory ends with '/'."));
            return TStatus::Error;
        }

        TString normalized = NS3::NormalizePath(ToString(path));
        if (normalized == "/") {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3Target::idx_Path)->Pos()), "Unable to write to root directory"));
            return TStatus::Error;
        }

        if (normalized != path) {
            output = ctx.ChangeChild(*input, TS3Target::idx_Path, ctx.NewAtom(input->Child(TS3Target::idx_Path)->Pos(), normalized));
            return TStatus::Repeat;
        }

        if (!EnsureAtom(*input->Child(TS3Target::idx_Format), ctx) || !NCommon::ValidateFormat(input->Child(TS3Target::idx_Format)->Content(), ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TS3Target::idx_Settings && !EnsureTuple(*input->Child(TS3Target::idx_Settings), ctx)) {
            return TStatus::Error;
        }
/* TODO
        const auto compression = GetCompression(*input->Child(TS3Target::idx_Settings));
        if (!NCommon::ValidateCompression(compression, ctx)) {
            return TStatus::Error;
        }
*/
        input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleSink(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx)) {
            return TStatus::Error;
        }
        input->SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }

    TStatus HandleOutput(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 3U, 4U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureFlowType(*input->Child(TS3SinkOutput::idx_Input), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TS3SinkOutput::idx_Format), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureTupleOfAtoms(*input->Child(TS3SinkOutput::idx_KeyColumns), ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TS3SinkOutput::idx_Settings && !EnsureTuple(*input->Child(TS3SinkOutput::idx_Settings), ctx)) {
            return TStatus::Error;
        }

        if (const auto keysCount = input->Child(TS3SinkOutput::idx_KeyColumns)->ChildrenSize()) {
            const auto source = input->Child(TS3SinkOutput::idx_Input);
            const auto itemType = source->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
            if (!EnsureStructType(source->Pos(), *itemType, ctx)) {
                return TStatus::Error;
            }

            const auto structType = itemType->Cast<TStructExprType>();
            for (auto i = 0U; i < keysCount; ++i) {
                const auto key = input->Child(TS3SinkOutput::idx_KeyColumns)->Child(i);
                if (const auto keyType = structType->FindItemType(key->Content())) {
                    if (!EnsureDataType(key->Pos(), *keyType, ctx)) {
                        return TStatus::Error;
                    }
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(key->Pos()), "Missed key column."));
                    return TStatus::Error;
                }

                TTypeAnnotationNode::TListType itemTypes(keysCount + 1U, ctx.MakeType<TDataExprType>(EDataSlot::Utf8));
                itemTypes.front() = ctx.MakeType<TDataExprType>(EDataSlot::String);
                input->SetTypeAnn(ctx.MakeType<TFlowExprType>(ctx.MakeType<TTupleExprType>(itemTypes)));
            }
        } else
            input->SetTypeAnn(ctx.MakeType<TFlowExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String)));

        return TStatus::Ok;
    }

    const TS3State::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreateS3DataSinkTypeAnnotationTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3DataSinkTypeAnnotationTransformer>(state);
}

} // namespace NYql
