#include "yql_yt_provider_impl.h"
#include "yql_yt_table.h"
#include "yql_yt_table_desc.h"
#include "yql_yt_op_settings.h"
#include "yql_yt_helpers.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/string/cast.h>

namespace NYql {

using namespace NNodes;

class TYtIntentDeterminationTransformer : public TVisitorTransformerBase {
public:
    TYtIntentDeterminationTransformer(TYtState::TPtr state)
        : TVisitorTransformerBase(false)
        , State_(state)
    {
        // Handle initial callables after SQL parse
        AddHandler({TYtRead::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleRead));
        AddHandler({TYtWrite::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleWrite));

        // Handle callables for already parsed/optimized AST
        AddHandler({TYtReadTable::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleReadTable));
        AddHandler({TYtReadTableScheme::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleReadTableScheme));
        AddHandler({TYtDropTable::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleDropTable));
        AddHandler({TYtPublish::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandlePublish));
        AddHandler({TYtSort::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleOperation));
        AddHandler({TYtMap::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleOperation));
        AddHandler({TYtReduce::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleOperation));
        AddHandler({TYtMapReduce::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleOperation));
        AddHandler({TYtCopy::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleOperation));
        AddHandler({TYtMerge::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleOperation));
        AddHandler({TYtEquiJoin::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleOperation));
        AddHandler({TYtFill::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleOutOperation));
        AddHandler({TYtTouch::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleOutOperation));
        AddHandler({TYtWriteTable::CallableName()}, Hndl(&TYtIntentDeterminationTransformer::HandleWriteTable));
    }

    TStatus HandleRead(TExprBase input, TExprContext& ctx) {
        TYtRead read = input.Cast<TYtRead>();
        if (!EnsureArgsCount(read.Ref(), 5, ctx)) {
            return TStatus::Error;
        }

        auto cluster = TString{read.DataSource().Cluster().Value()};

        EYtSettingTypes acceptedSettings = EYtSettingType::View | EYtSettingType::Anonymous
            | EYtSettingType::InferScheme | EYtSettingType::ForceInferScheme
            | EYtSettingType::DoNotFailOnInvalidSchema | EYtSettingType::XLock
            | EYtSettingType::UserSchema | EYtSettingType::UserColumns | EYtSettingType::IgnoreTypeV3;
        for (auto path: read.Arg(2).Cast<TExprList>()) {
            if (auto table = path.Maybe<TYtPath>().Table()) {
                if (!TYtTableInfo::Validate(table.Cast().Ref(), acceptedSettings, ctx)) {
                    return TStatus::Error;
                }

                TYtTableInfo tableInfo(table.Cast(), false);
                if (!ProcessInputTableIntent(ctx.GetPosition(input.Pos()), cluster, tableInfo, ctx)) {
                    return TStatus::Error;
                }
            }
        }
        return TStatus::Ok;
    }

    TStatus HandleWrite(TExprBase input, TExprContext& ctx) {
        TYtWrite write = input.Cast<TYtWrite>();
        if (!EnsureArgsCount(write.Ref(), 5, ctx)) {
            return TStatus::Error;
        }

        if (!TYtTableInfo::Validate(write.Arg(2).Ref(), EYtSettingType::Mode | EYtSettingType::Initial | EYtSettingType::Anonymous, ctx)) {
            return TStatus::Error;
        }

        auto cluster = TString{write.DataSink().Cluster().Value()};
        TYtTableInfo tableInfo(write.Arg(2), false);

        TYtTableDescription& tableDesc = State_->TablesData->GetOrAddTable(
            cluster,
            tableInfo.Name,
            tableInfo.Epoch
        );

        if (NYql::HasSetting(tableInfo.Settings.Cast().Ref(), EYtSettingType::Anonymous)) {
            tableDesc.IsAnonymous = true;
            RegisterAnonymouseTable(cluster, tableInfo.Name);
        }

        if (auto mode = NYql::GetSetting(write.Arg(4).Ref(), EYtSettingType::Mode)) {
            try {
                switch (FromString<EYtWriteMode>(mode->Child(1)->Content())) {
                case EYtWriteMode::Drop:
                    tableDesc.Intents |= TYtTableIntent::Drop;
                    break;
                case EYtWriteMode::Append:
                    tableDesc.Intents |= TYtTableIntent::Append;
                    break;
                case EYtWriteMode::Renew:
                case EYtWriteMode::RenewKeepMeta:
                    tableDesc.Intents |= TYtTableIntent::Override;
                    break;
                case EYtWriteMode::Flush:
                    tableDesc.Intents |= TYtTableIntent::Flush;
                    break;
                default:
                    ctx.AddError(TIssue(ctx.GetPosition(mode->Child(1)->Pos()), TStringBuilder() << "Unsupported "
                        << TYtWrite::CallableName() << " mode: " << mode->Child(1)->Content()));
                    return TStatus::Error;

                }
            } catch (const yexception& e) {
                ctx.AddError(TIssue(ctx.GetPosition(mode->Child(1)->Pos()), TStringBuilder() << "Unsupported "
                    << TYtWrite::CallableName() << " mode: " << mode->Child(1)->Content() << ", " << e.what()));
                return TStatus::Error;
            }
        } else {
            tableDesc.Intents |= TYtTableIntent::Override;
        }

        if (!ValidateOutputTableIntent(ctx.GetPosition(input.Pos()), tableDesc.Intents, cluster, tableInfo.Name, ctx)) {
            return TStatus::Error;
        }

        return TStatus::Ok;
    }

    TStatus HandleReadTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        TYtReadTable read = TYtReadTable(input);

        auto cluster = TString{read.DataSource().Cluster().Value()};

        for (auto section: read.Input()) {
            for (auto path: section.Paths()) {
                if (auto table = path.Table().Maybe<TYtTable>()) {
                    TYtTableInfo tableInfo(table.Cast(), false);
                    if (!ProcessInputTableIntent(ctx.GetPosition(input->Pos()), cluster, tableInfo, ctx)) {
                        return TStatus::Error;
                    }
                }
            }
        }

        output = ResetTablesMeta(input, ctx, State_->Types->UseTableMetaFromGraph, State_->Types->EvaluationInProgress > 0);
        if (!output) {
            return TStatus::Error;
        }
        return TStatus::Ok;
    }

    TStatus HandleReadTableScheme(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        TYtReadTableScheme scheme = TYtReadTableScheme(input);

        auto cluster = TString{scheme.DataSource().Cluster().Value()};

        TYtTableInfo tableInfo(scheme.Table(), false);
        if (!ProcessInputTableIntent(ctx.GetPosition(input->Pos()), cluster, tableInfo, ctx)) {
            return TStatus::Error;
        }

        output = ResetTablesMeta(input, ctx, State_->Types->UseTableMetaFromGraph, State_->Types->EvaluationInProgress > 0);
        if (!output) {
            return TStatus::Error;
        }

        return TStatus::Ok;
    }

    TStatus HandleOperation(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        auto op = TYtTransientOpBase(input);
        auto cluster = TString{op.DataSink().Cluster().Value()};

        for (auto section: op.Input()) {
            for (auto path: section.Paths()) {
                if (auto table = path.Table().Maybe<TYtTable>()) {
                    TYtTableInfo tableInfo(table.Cast(), false);
                    if (!ProcessInputTableIntent(ctx.GetPosition(input->Pos()), cluster, tableInfo, ctx)) {
                        return TStatus::Error;
                    }
                }
            }
        }

        output = ResetTablesMeta(input, ctx, State_->Types->UseTableMetaFromGraph, State_->Types->EvaluationInProgress > 0);
        if (!output) {
            return TStatus::Error;
        }
        return TStatus::Ok;
    }

    TStatus HandleOutOperation(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        output = ResetTablesMeta(input, ctx, State_->Types->UseTableMetaFromGraph, State_->Types->EvaluationInProgress > 0);
        if (!output) {
            return TStatus::Error;
        }
        return TStatus::Ok;
    }

    TStatus HandleDropTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        auto drop = TYtDropTable(input);

        auto cluster = TString{drop.DataSink().Cluster().Value()};
        TYtTableInfo tableInfo(drop.Table(), false);

        TYtTableDescription& tableDesc = State_->TablesData->GetOrAddTable(
            cluster,
            tableInfo.Name,
            tableInfo.Epoch
        );
        if (NYql::HasSetting(tableInfo.Settings.Cast().Ref(), EYtSettingType::Anonymous)) {
            tableDesc.IsAnonymous = true;
            RegisterAnonymouseTable(cluster, tableInfo.Name);
        }
        tableDesc.Intents |= TYtTableIntent::Drop;

        UpdateDescriptorMeta(tableDesc, tableInfo);

        output = ResetTablesMeta(input, ctx, State_->Types->UseTableMetaFromGraph, State_->Types->EvaluationInProgress > 0);
        if (!output) {
            return TStatus::Error;
        }
        return TStatus::Ok;
    }

    TStatus HandlePublish(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        auto publish = TYtPublish(input);

        auto cluster = TString{publish.DataSink().Cluster().Value()};
        TYtTableInfo tableInfo(publish.Publish(), false);

        TYtTableDescription& tableDesc = State_->TablesData->GetOrAddTable(
            cluster,
            tableInfo.Name,
            tableInfo.Epoch
        );
        if (NYql::HasSetting(tableInfo.Settings.Cast().Ref(), EYtSettingType::Anonymous)) {
            tableDesc.IsAnonymous = true;
            RegisterAnonymouseTable(cluster, tableInfo.Name);
        }
        if (auto mode = NYql::GetSetting(publish.Settings().Ref(), EYtSettingType::Mode)) {
            try {
                switch (FromString<EYtWriteMode>(mode->Child(1)->Content())) {
                case EYtWriteMode::Append:
                    tableDesc.Intents |= TYtTableIntent::Append;
                    break;
                case EYtWriteMode::Renew:
                case EYtWriteMode::RenewKeepMeta:
                    tableDesc.Intents |= TYtTableIntent::Override;
                    break;
                case EYtWriteMode::Flush:
                    tableDesc.Intents |= TYtTableIntent::Flush;
                    break;
                default:
                    ctx.AddError(TIssue(ctx.GetPosition(mode->Child(1)->Pos()), TStringBuilder() << "Unsupported "
                        << TYtPublish::CallableName() << " mode: " << mode->Child(1)->Content()));
                    return TStatus::Error;

                }
            } catch (const yexception& e) {
                ctx.AddError(TIssue(ctx.GetPosition(mode->Child(1)->Pos()), TStringBuilder() << "Unsupported "
                    << TYtPublish::CallableName() << " mode: " << mode->Child(1)->Content() << ", " << e.what()));
                return TStatus::Error;
            }
        } else {
            tableDesc.Intents |= TYtTableIntent::Override;
        }

        if (!ValidateOutputTableIntent(ctx.GetPosition(input->Pos()), tableDesc.Intents, cluster, tableInfo.Name, ctx)) {
            return TStatus::Error;
        }

        UpdateDescriptorMeta(tableDesc, tableInfo);

        output = ResetTablesMeta(input, ctx, State_->Types->UseTableMetaFromGraph, State_->Types->EvaluationInProgress > 0);
        if (!output) {
            return TStatus::Error;
        }
        return TStatus::Ok;
    }

    TStatus HandleWriteTable(TExprBase input, TExprContext& ctx) {
        TYtWriteTable write = input.Cast<TYtWriteTable>();

        auto cluster = TString{write.DataSink().Cluster().Value()};
        TYtTableInfo tableInfo(write.Table(), false);

        TYtTableDescription& tableDesc = State_->TablesData->GetOrAddTable(
            cluster,
            tableInfo.Name,
            tableInfo.Epoch
        );

        if (NYql::HasSetting(tableInfo.Settings.Cast().Ref(), EYtSettingType::Anonymous)) {
            tableDesc.IsAnonymous = true;
            RegisterAnonymouseTable(cluster, tableInfo.Name);
        }

        if (auto mode = NYql::GetSetting(write.Settings().Ref(), EYtSettingType::Mode)) {
            try {
                switch (FromString<EYtWriteMode>(mode->Child(1)->Content())) {
                case EYtWriteMode::Drop:
                    tableDesc.Intents |= TYtTableIntent::Drop;
                    break;
                case EYtWriteMode::Append:
                    tableDesc.Intents |= TYtTableIntent::Append;
                    break;
                case EYtWriteMode::Renew:
                case EYtWriteMode::RenewKeepMeta:
                    tableDesc.Intents |= TYtTableIntent::Override;
                    break;
                case EYtWriteMode::Flush:
                    tableDesc.Intents |= TYtTableIntent::Flush;
                    break;
                default:
                    ctx.AddError(TIssue(ctx.GetPosition(mode->Child(1)->Pos()), TStringBuilder() << "Unsupported "
                        << TYtWrite::CallableName() << " mode: " << mode->Child(1)->Content()));
                    return TStatus::Error;

                }
            } catch (const yexception& e) {
                ctx.AddError(TIssue(ctx.GetPosition(mode->Child(1)->Pos()), TStringBuilder() << "Unsupported "
                    << TYtWrite::CallableName() << " mode: " << mode->Child(1)->Content() << ", " << e.what()));
                return TStatus::Error;
            }
        } else {
            tableDesc.Intents |= TYtTableIntent::Override;
        }

        if (!ValidateOutputTableIntent(ctx.GetPosition(input.Pos()), tableDesc.Intents, cluster, tableInfo.Name, ctx)) {
            return TStatus::Error;
        }

        UpdateDescriptorMeta(tableDesc, tableInfo);

        return TStatus::Ok;
    }

private:
    void UpdateDescriptorMeta(TYtTableDescription& tableDesc, const TYtTableInfo& tableInfo) const {
        if (!State_->Types->UseTableMetaFromGraph) {
            return;
        }

        if (tableInfo.Stat && !tableDesc.Stat) {
            if (tableDesc.Stat = tableInfo.Stat) {
                tableDesc.Stat->FromNode = {};
            }
        }

        if (tableInfo.Meta && !tableDesc.Meta) {
            tableDesc.Meta = tableInfo.Meta;
            tableDesc.Meta->FromNode = {};
            if (NYql::HasSetting(tableInfo.Settings.Ref(), EYtSettingType::WithQB)) {
                tableDesc.QB2RowSpec = tableInfo.RowSpec;
                tableDesc.QB2RowSpec->FromNode = {};
            } else {
                if (tableDesc.RowSpec = tableInfo.RowSpec) {
                    tableDesc.RowSpec->FromNode = {};
                }
            }
        }
    }

    bool ProcessInputTableIntent(TPosition pos, const TString& cluster, const TYtTableInfo& tableInfo, TExprContext& ctx) {
        if (!State_->Checkpoints.empty() && State_->Checkpoints.contains(std::make_pair(cluster, tableInfo.Name))) {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Reading from checkpoint " << tableInfo.Name.Quote() << " is not allowed"));
            return false;
        }

        TYtTableDescription& tableDesc = State_->TablesData->GetOrAddTable(
            cluster,
            tableInfo.Name,
            tableInfo.Epoch
        );

        if (NYql::HasSetting(tableInfo.Settings.Cast().Ref(), EYtSettingType::Anonymous)) {
            tableDesc.IsAnonymous = true;
            RegisterAnonymouseTable(cluster, tableInfo.Name);
        }

        TYtTableIntents intents = TYtTableIntent::Read;
        if (tableInfo.Settings) {
            auto view = NYql::GetSetting(tableInfo.Settings.Cast().Ref(), EYtSettingType::View);
            if (view) {
                if (tableInfo.Epoch.GetOrElse(0)) {
                    ctx.AddError(TIssue(pos, TStringBuilder()
                        << "Table " << tableInfo.Name.Quote() << " cannot have any view after replacing its content"));
                    return false;
                }
                tableDesc.Views.insert({TString{view->Child(1)->Content()}, {}});
                intents = TYtTableIntent::View; // Override Read intent
            }
            if (NYql::HasSetting(tableInfo.Settings.Cast().Ref(), EYtSettingType::XLock)) {
                intents |= TYtTableIntent::Override;
            }
        }
        tableDesc.Intents |= intents;
        if (tableInfo.Epoch.GetOrElse(0) == 0) {
            if (!NYql::HasSetting(tableInfo.Settings.Cast().Ref(), EYtSettingType::UserSchema)) {
                TExprNode::TPtr perForceInfer = NYql::GetSetting(tableInfo.Settings.Cast().Ref(),
                                                                 EYtSettingType::ForceInferScheme);
                TExprNode::TPtr perInfer = NYql::GetSetting(tableInfo.Settings.Cast().Ref(),
                                                            EYtSettingType::InferScheme);
                tableDesc.InferSchemaRows = State_->Configuration->InferSchema.Get().OrElse(State_->Configuration->ForceInferSchema.Get()).GetOrElse(0);
                if (perForceInfer) {
                    tableDesc.InferSchemaRows = (perForceInfer->ChildrenSize() == 2)
                        ? FromString<ui32>(perForceInfer->Tail().Content())
                        : 1;
                } else if (perInfer) {
                    tableDesc.InferSchemaRows = (perInfer->ChildrenSize() == 2)
                        ? FromString<ui32>(perInfer->Tail().Content())
                        : 1;
                }
                tableDesc.ForceInferSchema = perForceInfer
                    || State_->Configuration->ForceInferSchema.Get().GetOrElse(0) > 0;
            }
            tableDesc.IgnoreTypeV3 = State_->Configuration->IgnoreTypeV3.Get().GetOrElse(false)
                || NYql::HasSetting(tableInfo.Settings.Cast().Ref(), EYtSettingType::IgnoreTypeV3);
        }
        tableDesc.FailOnInvalidSchema = !NYql::HasSetting(tableInfo.Settings.Cast().Ref(), EYtSettingType::DoNotFailOnInvalidSchema);

        UpdateDescriptorMeta(tableDesc, tableInfo);

        return true;
    }

    bool ValidateOutputTableIntent(
        TPosition pos,
        TYtTableIntents intents,
        const TString& cluster,
        const TString& tableName,
        TExprContext& ctx)
    {
        if (
            !intents.HasFlags(TYtTableIntent::Flush) &&
            !State_->Checkpoints.empty() &&
            State_->Checkpoints.contains(std::make_pair(cluster, tableName)))
        {
            ctx.AddError(TIssue(pos, TStringBuilder() << "Writing to checkpoint " << tableName.Quote() << " is not allowed"));
            return false;
        }

        return true;
    }

    void RegisterAnonymouseTable(const TString& cluster, const TString& label) {
        auto& path = State_->AnonymousLabels[std::make_pair(cluster, label)];
        if (path.empty()) {
            path = "tmp/" + GetGuidAsString(State_->Types->RandomProvider->GenGuid());
            YQL_CLOG(INFO, ProviderYt) << "Anonymous label " << cluster << '.' << label << ": " << path;
        }
    }
private:
    TYtState::TPtr State_;
};

THolder<IGraphTransformer> CreateYtIntentDeterminationTransformer(TYtState::TPtr state) {
    return THolder(new TYtIntentDeterminationTransformer(state));
}

}
