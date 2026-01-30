#include "yql_pq_provider_impl.h"
#include "yql_pq_helpers.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/pushdown/collection.h>
#include <ydb/library/yql/providers/common/pushdown/settings.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/common/yql_names.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

struct TWatermarkPushdownSettings: public NPushdown::TSettings {
    TWatermarkPushdownSettings()
        : NPushdown::TSettings(NLog::EComponent::ProviderGeneric)
    {
        using EFlag = NPushdown::TSettings::EFeatureFlag;
        Enable(
            // Type features
            EFlag::DateTimeTypes |
            EFlag::DecimalType |
            EFlag::StringTypes |
            EFlag::TimestampCtor |
            EFlag::IntervalCtor |
            EFlag::ImplicitConversionToInt64 |
            EFlag::DoNotCheckCompareArgumentsTypes |

            // Expr features
            EFlag::ArithmeticalExpressions |
            EFlag::CastExpression |
            EFlag::DivisionExpressions |
            EFlag::JustPassthroughOperators |
            EFlag::UnaryOperators |
            EFlag::MinMax |
            EFlag::NonDeterministic
        );
    }
};

class TPqDataSourceTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    explicit TPqDataSourceTypeAnnotationTransformer(TPqState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TPqDataSourceTypeAnnotationTransformer;
        AddHandler({TCoConfigure::CallableName()}, Hndl(&TSelf::HandleConfigure));
        AddHandler({TPqReadTopic::CallableName()}, Hndl(&TSelf::HandleReadTopic));
        AddHandler({TPqTopic::CallableName()}, Hndl(&TSelf::HandleTopic));
        AddHandler({TDqPqTopicSource::CallableName()}, Hndl(&TSelf::HandleDqTopicSource));
        AddHandler({TCoSystemMetadata::CallableName()}, Hndl(&TSelf::HandleMetadata));
        AddHandler({TDqPqFederatedCluster::CallableName()}, Hndl(&TSelf::HandleFederatedCluster));
    }

    TStatus HandleFederatedCluster(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 3, 4, ctx)) {
            return TStatus::Error;
        }

        const auto name = input->Child(TDqPqFederatedCluster::idx_Name);
        const auto endpoint = input->Child(TDqPqFederatedCluster::idx_Endpoint);
        const auto database = input->Child(TDqPqFederatedCluster::idx_Database);

        if (!EnsureAtom(*name, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*endpoint, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*database, ctx)) {
            return TStatus::Error;
        }

        if (TDqPqFederatedCluster::idx_PartitionsCount < input->ChildrenSize()) {
            const auto partitionsCount = input->Child(TDqPqFederatedCluster::idx_PartitionsCount);
            if (!EnsureAtom(*partitionsCount, ctx)) {
                return TStatus::Error;
            }
            if (!TryFromString<ui32>(partitionsCount->Content())) {
                ctx.AddError(TIssue(ctx.GetPosition(partitionsCount->Pos()), TStringBuilder()
                    << "Expected integer, but got: " << partitionsCount->Content()));
                return TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleConfigure(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TCoConfigure::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TCoConfigure::idx_DataSource), PqProviderName, ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(input->Child(TCoConfigure::idx_World)->GetTypeAnn());
        return TStatus::Ok;
    }

    const TTypeAnnotationNode* GetReadTopicSchema(const TExprNode* topic, const TExprNode* columns, TExprContext& ctx, TColumnOrder& columnOrder) {
        const auto metadata = topic->Child(TPqTopic::idx_Metadata);
        TVector<const TItemExprType*> items;
        items.reserve((columns ? columns->ChildrenSize() : 0) + metadata->ChildrenSize());

        const auto* itemSchema = topic->GetTypeAnn()->Cast<TListExprType>()
            ->GetItemType()->Cast<TStructExprType>();

        std::unordered_set<TString> addedFields;
        if (columns) {
            columnOrder.Reserve(items.capacity());

            for (auto c : columns->Children()) {
                if (!EnsureAtom(*c, ctx)) {
                    return nullptr;
                }

                const auto columnName = c->Content();
                const auto index = itemSchema->FindItem(columnName);
                if (!index) {
                    ctx.AddError(TIssue(ctx.GetPosition(topic->Pos()), TStringBuilder()
                        << "Unable to find column: " << columnName));
                    return nullptr;
                }

                columnOrder.AddColumn(TString(columnName));
                items.push_back(itemSchema->GetItems()[*index]);
                addedFields.emplace(columnName);
            }

            for (auto c : itemSchema->GetItems()) {
                const TString columnName(c->GetName());
                if (addedFields.contains(columnName)) {
                    continue;
                }

                columnOrder.AddColumn(columnName);
                items.push_back(c);
            }
        } else {
            items = itemSchema->GetItems();
        }

        return ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(items));
    }

    TStatus HandleReadTopic(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 6, 9, ctx)) {
            return TStatus::Error;
        }

        const auto world = input->Child(TPqReadTopic::idx_World);
        const auto dataSource = input->Child(TPqReadTopic::idx_DataSource);
        const auto topic = input->Child(TPqReadTopic::idx_Topic);
        const auto columns = input->Child(TPqReadTopic::idx_Columns);
        const auto format = input->Child(TPqReadTopic::idx_Format);
        const auto compression = input->Child(TPqReadTopic::idx_Compression);
        const auto settings = input->Child(TPqReadTopic::idx_Settings);

        if (!EnsureWorldType(*world, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*dataSource, PqProviderName, ctx)) {
            return TStatus::Error;
        }

        if (!topic->IsCallable(TPqTopic::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(topic->Pos()), TStringBuilder()
                << "Expected PqTopic, but got: " << topic->Content()));
            return TStatus::Error;
        }

        TColumnOrder columnOrder;
        auto schema = GetReadTopicSchema(topic, columns, ctx, columnOrder);
        if (!schema) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*format, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*compression, ctx)) {
            return TStatus::Error;
        }

        if (!State_->IsRtmrMode() && !NCommon::ValidateFormatForInput(      // Rtmr has 3 field (key/subkey/value).
            format->Content(),
            schema->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>(),
            [](TStringBuf fieldName) {return FindPqMetaFieldDescriptorBySysColumn(TString(fieldName)).has_value(); },
            ctx)) {
            return TStatus::Error;
        }

        if (!NCommon::ValidateCompressionForInput(format->Content(), compression->Content(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureTuple(*settings, ctx)) {
            return TStatus::Error;
        }

        for (const auto& setting : settings->Children()) {
            if (!EnsureTupleMinSize(*setting, 1, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(setting->Head(), ctx)) {
                return TStatus::Error;
            }
        }

        if (TPqReadTopic::idx_Watermark < input->ChildrenSize()) {
            auto& watermark = input->ChildRef(TPqReadTopic::idx_Watermark);
            const auto status = ConvertToLambda(watermark, ctx, 1, 1);
            if (status != TStatus::Ok) {
                return status;
            }
            if (!UpdateLambdaAllArgumentsTypes(watermark, {schema->Cast<TListExprType>()->GetItemType()}, ctx)) {
                return TStatus::Error;
            }
            if (!watermark->GetTypeAnn()) {
                return TStatus::Repeat;
            }
            if (!EnsureSpecificDataType(*watermark, EDataSlot::Timestamp, ctx, true)) {
                return TStatus::Error;
            }

            const TCoLambda lambda(watermark);
            const auto lambdaArg = TExprBase(lambda.Args().Arg(0).Ptr());
            const auto lambdaBody = lambda.Body();
            if (!TestExprForPushdown(ctx, lambdaArg, lambdaBody, TWatermarkPushdownSettings())) {
                ctx.AddError(TIssue(ctx.GetPosition(watermark->Pos()), TStringBuilder()
                    << "Bad watermark expression"));
                return TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            world->GetTypeAnn(),
            schema
        }));

        if (!columns) {
            return TStatus::Ok;
        }

        return State_->Types->SetColumnOrder(*input, columnOrder, ctx);
    }

    TStatus HandleDqTopicSource(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 7, 8, ctx)) {
            return TStatus::Error;
        }

        const auto world = input->Child(TDqPqTopicSource::idx_World);
        const auto topic = input->Child(TDqPqTopicSource::idx_Topic);
        const auto settings = input->Child(TDqPqTopicSource::idx_Settings);
        const auto rowType = input->Child(TDqPqTopicSource::idx_RowType);

        if (!EnsureWorldType(*world, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureCallable(*topic, ctx)) {
            return TStatus::Error;
        }

        const auto cluster = topic->Child(TPqTopic::idx_Cluster);
        const auto path = topic->Child(TPqTopic::idx_Path);
        const auto metadata = topic->Child(TPqTopic::idx_Metadata);

        if (!EnsureAtom(*cluster, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*path, ctx)) {
            return TStatus::Error;
        }

        const TString topicCluster(cluster->Content());
        const TString topicPath(path->Content());
        const auto* meta = State_->FindTopicMeta(topicCluster, topicPath);
        if (!meta) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                << "Unknown topic `" << topicCluster << "`.`" << topicPath << "`"));
            return TStatus::Error;
        }

        if (TDqPqTopicSource::idx_Watermark < input->ChildrenSize()) {
            const auto watermark = input->Child(TDqPqTopicSource::idx_Watermark);
            if (!EnsureAtom(*watermark, ctx)) {
                return TStatus::Error;
            }
        }

        if (const auto maybeSkipJsonErrorsSetting = FindSetting(settings, SkipJsonErrors)) {
            const auto value = maybeSkipJsonErrorsSetting.Cast().Ptr();
            if (!EnsureAtom(*value, ctx)) {
                return TStatus::Error;
            }
            bool skipJsonErrorsSetting;
            if (!TryFromString<bool>(value->Content(), skipJsonErrorsSetting)) {
                ctx.AddError(TIssue(ctx.GetPosition(settings->Pos()), TStringBuilder()
                    << "Expected bool, but got: " << value->Content()));
                return TStatus::Error;
            }
        }

        if (const auto maybeSharedReadingSetting = FindSetting(settings, SharedReading)) {
            const auto value = maybeSharedReadingSetting.Cast().Ptr();
            if (!EnsureAtom(*value, ctx)) {
                return TStatus::Error;
            }
            bool sharedReadingSetting;
            if (!TryFromString<bool>(value->Content(), sharedReadingSetting)) {
                ctx.AddError(TIssue(ctx.GetPosition(settings->Pos()), TStringBuilder()
                    << "Expected bool, but got: " << value->Content()));
                return TStatus::Error;
            }
            if (sharedReadingSetting) {
                input->SetTypeAnn(ctx.MakeType<TStreamExprType>(rowType->GetTypeAnn()));
                return TStatus::Ok;
            }
        }

        if (metadata->ChildrenSize() == 0) {
            input->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String)));
            return TStatus::Ok;
        }

        TTypeAnnotationNode::TListType items;
        items.reserve(metadata->ChildrenSize() + 1);

        items.emplace_back(ctx.MakeType<TDataExprType>(EDataSlot::String));
        for (const auto& metadataField : metadata->Children()) {
            if (TCoNameValueTuple::idx_Value >= metadataField->ChildrenSize()) {
                ctx.AddError(TIssue(ctx.GetPosition(metadataField->Pos()), TStringBuilder()
                    << "index out of range"));
                return TStatus::Error;
            }
            const auto metadataSysColumn = metadataField->Child(TCoNameValueTuple::idx_Value);
            if (!EnsureAtom(*metadataSysColumn, ctx)) {
                return TStatus::Error;
            }
            const TString metadataSysColumnName(metadataSysColumn->Content());
            const auto descriptor = FindPqMetaFieldDescriptorBySysColumn(metadataSysColumnName);
            if (!descriptor) {
                ctx.AddError(TIssue(ctx.GetPosition(metadataField->Pos()), TStringBuilder()
                    << "Pq Meta Field Descriptor was not found"));
                return TStatus::Error;
            }
            items.emplace_back(ctx.MakeType<TDataExprType>(descriptor->Type));
        }

        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TTupleExprType>(items)));
        return TStatus::Ok;
    }

    TStatus HandleTopic(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (State_->IsRtmrMode()) {
            return HandleTopicInRtmrMode(input, ctx);
        }

        const auto metadata = input->Child(TPqTopic::idx_Metadata);
        const auto rowSpec = input->Child(TPqTopic::idx_RowSpec);

        TVector<const TItemExprType*> items;

        auto rowSchema = rowSpec->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        for (const auto& rowSchemaItem : rowSchema->GetItems()) {
            items.push_back(rowSchemaItem);
        }

        for (const auto& metadataField : metadata->Children()) {
            if (TCoNameValueTuple::idx_Value >= metadataField->ChildrenSize()) {
                ctx.AddError(TIssue(ctx.GetPosition(metadataField->Pos()), TStringBuilder()
                    << "index out of range"));
                return TStatus::Error;
            }
            const auto metadataSysColumn = metadataField->Child(TCoNameValueTuple::idx_Value);
            if (!EnsureAtom(*metadataSysColumn, ctx)) {
                return TStatus::Error;
            }
            const TString metadataSysColumnName(metadataSysColumn->Content());
            const auto descriptor = FindPqMetaFieldDescriptorBySysColumn(metadataSysColumnName);
            if (!descriptor) {
                ctx.AddError(TIssue(ctx.GetPosition(metadataField->Pos()), TStringBuilder()
                    << "Pq Meta Field Descriptor was not found"));
                return TStatus::Error;
            }
            items.emplace_back(ctx.MakeType<TItemExprType>(descriptor->SysColumn, ctx.MakeType<TDataExprType>(descriptor->Type)));
        }

        input->SetTypeAnn(ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(items)));
        return TStatus::Ok;
    }

    TStatus HandleMetadata(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        const auto key = input->ChildPtr(0);
        if (!EnsureCallable(*key, ctx)) {
            return TStatus::Error;
        }

        const auto metadataKey = TString(key->TailPtr()->Content());
        const auto descriptor = FindPqMetaFieldDescriptorByKey(metadataKey, State_->AllowTransparentSystemColumns);
        if (!descriptor) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                << "Metadata key " << metadataKey << " wasn't found"));
            return TStatus::Error;
        }

        const auto dependsOn = input->Child(1);
        if (!EnsureDependsOn(*dependsOn, ctx)) {
            return TStatus::Error;
        }

        const auto row = dependsOn->TailPtr();
        if (!EnsureStructType(*row, ctx)) {
            return TStatus::Error;
        }

        if (row->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct) {
            auto structType = row->GetTypeAnn()->Cast<TStructExprType>();
            auto pos = structType->FindItem(descriptor->SysColumn);
            if (!pos && descriptor->Key == "write_time") {
                // Allow user to specify column manually. It is required for analytics hopping now.
                // In future it will be removed (when custom event_time will be implemented)
                pos = structType->FindItem("_row_time");
            }

            if (!pos) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder()
                    << "Wrong place to use SystemMetadata"));
                return TStatus::Error;
            }

            bool isOptional = false;
            const TDataExprType* dataType = nullptr;
            if (!EnsureDataOrOptionalOfData(row->Pos(), structType->GetItems()[*pos]->GetItemType(), isOptional, dataType, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureSpecificDataType(row->Pos(), *dataType, descriptor->Type, ctx)) {
                return TStatus::Error;
            }

            output = ctx.Builder(input->Pos())
                .Callable("Member")
                    .Add(0, row)
                    .Atom(1, structType->GetItems()[*pos]->GetName(), TNodeFlags::Default)
                .Seal()
                .Build();

            return TStatus::Repeat;
        }

        input->SetTypeAnn(ctx.MakeType<TDataExprType>(descriptor->Type));
        return TStatus::Ok;
    }

private:
    TStatus HandleTopicInRtmrMode(const TExprNode::TPtr& input, TExprContext& ctx) {
        TVector<const TItemExprType*> items;
        auto stringType = ctx.MakeType<TDataExprType>(EDataSlot::String);
        items.push_back(ctx.MakeType<TItemExprType>("key", stringType));
        items.push_back(ctx.MakeType<TItemExprType>("subkey", stringType));
        items.push_back(ctx.MakeType<TItemExprType>("value", stringType));
        auto itemType = ctx.MakeType<TStructExprType>(items);

        input->SetTypeAnn(ctx.MakeType<TListExprType>(itemType));
        return TStatus::Ok;
    }

private:
    TPqState::TPtr State_;
};

} // anonymous namespace

THolder<TVisitorTransformerBase> CreatePqDataSourceTypeAnnotationTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqDataSourceTypeAnnotationTransformer>(state);
}

} // namespace NYql
