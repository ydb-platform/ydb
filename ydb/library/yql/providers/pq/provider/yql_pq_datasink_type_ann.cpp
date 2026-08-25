#include "yql_pq_provider_impl.h"
#include "yql_pq_settings.h"

#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

bool EnsureStructTypeWithSingleStringMember(const TTypeAnnotationNode* input, TPositionHandle pos, TExprContext& ctx) {
    YQL_ENSURE(input);
    if (!EnsureStructType(pos, *input, ctx)) {
        return false;
    }

    const auto* itemSchema = input->Cast<TStructExprType>();
    if (itemSchema->GetSize() != 1) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Only struct with single string, yson or json field is accepted, but has struct with " << itemSchema->GetSize() << " members"));
        return false;
    }

    auto column = itemSchema->GetItems()[0];
    auto columnType = column->GetItemType();
    if (columnType->GetKind() != ETypeAnnotationKind::Data) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Column " << column->GetName() << " must have a data type, but has " << columnType->GetKind()));
        return false;
    }

    auto columnDataType = columnType->Cast<TDataExprType>();
    auto dataSlot = columnDataType->GetSlot();

    if (dataSlot != NUdf::EDataSlot::String &&
        dataSlot != NUdf::EDataSlot::Yson &&
        dataSlot != NUdf::EDataSlot::Json) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Column " << column->GetName() << " is not a string, yson or json, but " << NUdf::GetDataTypeInfo(dataSlot).Name));
        return false;
    }

    return true;
}

bool EnsureListOfStructTypeWithSingleStringMember(const TTypeAnnotationNode* input, TPositionHandle pos, TExprContext& ctx) {
    YQL_ENSURE(input);
    if (!EnsureListType(pos, *input, ctx)) {
        return false;
    }

    return EnsureStructTypeWithSingleStringMember(input->Cast<TListExprType>()->GetItemType(), pos, ctx);
}

class TPqDataSinkTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    explicit TPqDataSinkTypeAnnotationTransformer(TPqState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(std::move(state))
    {
        using TSelf = TPqDataSinkTypeAnnotationTransformer;
        AddHandler({TCoCommit::CallableName()}, Hndl(&TSelf::HandleCommit));
        AddHandler({TPqWriteTopic::CallableName()}, Hndl(&TSelf::HandleWriteTopic));
        AddHandler({NNodes::TPqClusterConfig::CallableName()}, Hndl(&TSelf::HandleClusterConfig));
        AddHandler({TDqPqTopicSink::CallableName()}, Hndl(&TSelf::HandleDqPqTopicSink));
        AddHandler({TPqInsert::CallableName()}, Hndl(&TSelf::HandleInsert));
    }

    TStatus HandleCommit(TExprBase input, TExprContext&) {
        const auto commit = input.Cast<TCoCommit>();
        input.Ptr()->SetTypeAnn(commit.World().Ref().GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleWriteTopic(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 6, ctx)) {
            return TStatus::Error;
        }

        const auto writeWorld = input->Child(TPqWriteTopic::idx_World);
        if (!EnsureWorldType(*writeWorld, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSink(*input->Child(TPqWriteTopic::idx_DataSink), PqProviderName, ctx)) {
            return TStatus::Error;
        }

        if (!TPqTopic::Match(input->Child(TPqWriteTopic::idx_Topic))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TPqWriteTopic::idx_Topic)->Pos()), "Expected PQ topic."));
            return TStatus::Error;
        }

        const auto writeInput = input->ChildPtr(TPqWriteTopic::idx_Input);
        if (const auto maybeTuple = TMaybeNode<TExprList>(writeInput)) {
            const auto tuple = maybeTuple.Cast();

            TVector<TExprBase> values;
            values.reserve(tuple.Size());
            for (const auto& value : tuple) {
                if (!EnsureStructTypeWithSingleStringMember(value.Ref().GetTypeAnn(), writeInput->Pos(), ctx)) {
                    return TStatus::Error;
                }

                values.emplace_back(value);
            }

            const auto list = Build<TCoAsList>(ctx, writeInput->Pos())
                .Add(std::move(values))
                .Done();

            input->ChildRef(TPqWriteTopic::idx_Input) = list.Ptr();
            return TStatus::Repeat;
        }

        if (!EnsureListOfStructTypeWithSingleStringMember(writeInput->GetTypeAnn(), writeInput->Pos(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TPqWriteTopic::idx_Mode), ctx)) {
            return TStatus::Error;
        }

        if (!ValidateWriteSetting(*input->Child(TPqWriteTopic::idx_Settings), ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(writeWorld->GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleClusterConfig(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(NNodes::TPqClusterConfig::idx_Endpoint), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(NNodes::TPqClusterConfig::idx_TvmId), ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleDqPqTopicSink(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 3, ctx)) {
            return TStatus::Error;
        }

        if (!TPqTopic::Match(input->Child(TDqPqTopicSink::idx_Topic))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TDqPqTopicSink::idx_Topic)->Pos()), "Expected PQ topic."));
            return TStatus::Error;
        }

        if (!EnsureValidSettings(*input->Child(TDqPqTopicSink::idx_Settings), {
            NDeliveryGuaranteeSetting::Name, EndpointSetting, UseSslSetting, AddBearerToTokenSetting
        }, [](TStringBuf, TExprNode& setting, TExprContext& ctx) {
            if (setting.ChildrenSize() != 2) {
                ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), "Expected single value for topic sink settings."));
                return false;
            }

            return EnsureAtom(*setting.Child(1), ctx);
        }, ctx)) {
            return TStatus::Error;
        }

        if (!TCoSecureParam::Match(input->Child(TDqPqTopicSink::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TDqPqTopicSink::idx_Token)->Pos()), "Expected secure parameter."));
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }

    TStatus HandleInsert(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TPqInsert::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSink(*input->Child(TPqInsert::idx_DataSink), PqProviderName, ctx)) {
            return TStatus::Error;
        }

        if (!TPqTopic::Match(input->Child(TPqInsert::idx_Topic))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TPqInsert::idx_Topic)->Pos()), "Expected PQ topic."));
            return TStatus::Error;
        }

        const auto insertInput = input->ChildPtr(TPqInsert::idx_Input);
        if (!EnsureListOfStructTypeWithSingleStringMember(insertInput->GetTypeAnn(), insertInput->Pos(), ctx)) {
            return TStatus::Error;
        }

        if (!ValidateWriteSetting(*input->Child(TPqInsert::idx_Settings), ctx)) {
            return TStatus::Error;
        }

        const auto* outputColumnType = insertInput->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>()->GetItems()[0];
        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(TVector{outputColumnType}))
        }));
        return TStatus::Ok;
    }

private:
    bool ValidateWriteSetting(TExprNode& settings, TExprContext& ctx) const {
        if (!State_->EnableSettingsValidation) {
            return true;
        }

        const auto validator = [state = State_](TStringBuf name, TExprNode& setting, TExprContext& ctx) {
            if (name == NDeliveryGuaranteeSetting::Name) {
                if (setting.ChildrenSize() != 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), TStringBuilder() << "Expected `" << NDeliveryGuaranteeSetting::PrettyName << "` = value"));
                    return false;
                }

                const auto& settingValue = setting.Child(1);
                if (!EnsureAtom(*settingValue, ctx)) {
                    return false;
                }

                if (!IsIn({NDeliveryGuaranteeSetting::ExactlyOnceValue, NDeliveryGuaranteeSetting::AtLeastOnceValue}, settingValue->Content())) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), TStringBuilder()
                        << "`" << NDeliveryGuaranteeSetting::PrettyName << "` must be '" << NDeliveryGuaranteeSetting::ExactlyOnceValue
                        << "' or '" << NDeliveryGuaranteeSetting::AtLeastOnceValue << "'"
                    ));
                    return false;
                }

                if (settingValue->Content() == NDeliveryGuaranteeSetting::ExactlyOnceValue) {
                    if (state->Configuration->EnableDeduplication.Get().GetOrElse(false)) {
                        ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), TStringBuilder()
                            << "`" << NDeliveryGuaranteeSetting::PrettyName << "` = '" << NDeliveryGuaranteeSetting::ExactlyOnceValue
                            << "' is not supported with enabled deduplication"
                        ));
                        return false;
                    }

                    if (!state->EnableExactlyOnceDeliveryGuaranty) {
                        ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), "Exactly once delivery guarantee is disabled. Please contact your system administrator to enable it."));
                        return false;
                    }
                }

                return true;
            }

            return false;
        };

        return EnsureValidSettings(settings, {NDeliveryGuaranteeSetting::Name}, validator, ctx);
    }

    TPqState::TPtr State_;
};

} // anonymous namespace

THolder<TVisitorTransformerBase> CreatePqDataSinkTypeAnnotationTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqDataSinkTypeAnnotationTransformer>(std::move(state));
}

} // namespace NYql
