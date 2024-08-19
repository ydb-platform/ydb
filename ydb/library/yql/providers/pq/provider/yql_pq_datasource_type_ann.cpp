#include "yql_pq_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/pushdown/type_ann.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

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

    const TTypeAnnotationNode* GetReadTopicSchema(TPqTopic topic, TMaybeNode<TCoAtomList> columns, TExprContext& ctx, TColumnOrder& columnOrder) {
        TVector<const TItemExprType*> items;
        items.reserve((columns ? columns.Cast().Ref().ChildrenSize() : 0) + topic.Metadata().Size());

        const auto* itemSchema = topic.Ref().GetTypeAnn()->Cast<TListExprType>()
            ->GetItemType()->Cast<TStructExprType>();

        std::unordered_set<TString> addedFields;
        if (columns) {
            columnOrder.Reserve(items.capacity());

            for (auto c : columns.Cast().Ref().ChildrenList()) {
                if (!EnsureAtom(*c, ctx)) {
                    return nullptr;
                }
                auto index = itemSchema->FindItem(c->Content());
                if (!index) {
                    ctx.AddError(TIssue(ctx.GetPosition(topic.Pos()), TStringBuilder() << "Unable to find column: " << c->Content()));
                    return nullptr;
                }
                columnOrder.AddColumn(TString(c->Content()));
                items.push_back(itemSchema->GetItems()[*index]);
                addedFields.emplace(c->Content());
            }
        }

        for (auto c : itemSchema->GetItems()) {
            if (addedFields.contains(TString(c->GetName()))) {
                continue;
            }

            items.push_back(c);
        }

        return ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(items));
    }

    TStatus HandleReadTopic(TExprBase input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(input.Ref(), 6, 8, ctx)) {
            return TStatus::Error;
        }

        TPqReadTopic read = input.Cast<TPqReadTopic>();

        if (!EnsureWorldType(read.World().Ref(), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(read.DataSource().Ref(), PqProviderName, ctx)) {
            return TStatus::Error;
        }

        TPqTopic topic = read.Topic();
        if (!EnsureCallable(topic.Ref(), ctx)) {
            return TStatus::Error;
        }

        TColumnOrder columnOrder;
        auto schema = GetReadTopicSchema(topic, read.Columns().Maybe<TCoAtomList>(), ctx, columnOrder);
        if (!schema) {
            return TStatus::Error;
        }

        auto format = read.Format().Ref().Content();
        if (!State_->IsRtmrMode() && !NCommon::ValidateFormatForInput(      // Rtmr has 3 field (key/subkey/value).
            format,
            schema->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>(),
            [](TStringBuf fieldName) {return FindPqMetaFieldDescriptorBySysColumn(TString(fieldName)); },
            ctx)) {
            return TStatus::Error;
        }

        if (!NCommon::ValidateCompressionForInput(format, read.Compression().Ref().Content(), ctx)) {
            return TStatus::Error;
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            read.World().Ref().GetTypeAnn(),
            schema
        }));
        return State_->Types->SetColumnOrder(input.Ref(), columnOrder, ctx);
    }

    TStatus HandleDqTopicSource(TExprBase input, TExprContext& ctx) {
        if (!EnsureArgsCount(input.Ref(), 6, ctx)) {
            return TStatus::Error;
        }

        TDqPqTopicSource topicSource = input.Cast<TDqPqTopicSource>();
        TPqTopic topic = topicSource.Topic();

        if (!EnsureCallable(topic.Ref(), ctx)) {
            return TStatus::Error;
        }

        const auto cluster = TString(topic.Cluster().Value());
        const auto topicPath = TString(topic.Path().Value());
        const auto* meta = State_->FindTopicMeta(cluster, topicPath);
        if (!meta) {
            ctx.AddError(TIssue(ctx.GetPosition(input.Pos()), TStringBuilder() << "Unknown topic `" << cluster << "`.`" << topicPath << "`"));
            return TStatus::Error;
        }

        auto rowSchema = topic.RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        YQL_CLOG(DEBUG, ProviderGeneric) << "struct column order " << rowSchema->ToString();

        const TStatus filterAnnotationStatus = NYql::NPushdown::AnnotateFilterPredicate(input.Ptr(), TDqPqTopicSource::idx_FilterPredicate, rowSchema, ctx);
        if (filterAnnotationStatus != TStatus::Ok) {
            return filterAnnotationStatus;
        }

        if (topic.Metadata().Empty()) {
            input.Ptr()->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String)));
            return TStatus::Ok;
        }

        TTypeAnnotationNode::TListType tupleItems;
        tupleItems.reserve(topic.Metadata().Size() + 1);

        tupleItems.emplace_back(ctx.MakeType<TDataExprType>(EDataSlot::String));
        for (const auto metadataField : topic.Metadata()) {
            const auto metadataSysColumn = metadataField.Value().Maybe<TCoAtom>().Cast().StringValue();
            const auto* descriptor = FindPqMetaFieldDescriptorBySysColumn(metadataSysColumn);
            Y_ENSURE(descriptor);
            tupleItems.emplace_back(ctx.MakeType<TDataExprType>(descriptor->Type));
        }

        input.Ptr()->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TTupleExprType>(tupleItems)));
        return TStatus::Ok;
    }

    TStatus HandleTopic(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (State_->IsRtmrMode()) {
            return HandleTopicInRtmrMode(input, ctx);
        }

        TPqTopic topic(input);
        TVector<const TItemExprType*> outputItems;

        auto rowSchema = topic.RowSpec().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
        for (const auto& rowSchemaItem : rowSchema->GetItems()) {
            outputItems.push_back(rowSchemaItem);
        }

        for (auto nameValue : topic.Metadata()) {
            const auto metadataSysColumn = nameValue.Value().Maybe<TCoAtom>().Cast().StringValue();
            const auto* descriptor = FindPqMetaFieldDescriptorBySysColumn(metadataSysColumn);
            Y_ENSURE(descriptor);
            outputItems.emplace_back(ctx.MakeType<TItemExprType>(descriptor->SysColumn, ctx.MakeType<TDataExprType>(descriptor->Type)));
        }

        const auto* itemType = ctx.MakeType<TStructExprType>(outputItems);
        input->SetTypeAnn(ctx.MakeType<TListExprType>(itemType));
        return TStatus::Ok;
    }

    TStatus HandleMetadata(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        const auto key = input->ChildPtr(0);
        if (!EnsureCallable(*key, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto metadataKey = TString(key->TailPtr()->Content());
        const auto* descriptor = FindPqMetaFieldDescriptorByKey(metadataKey);
        if (!descriptor) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Metadata key " << metadataKey << " wasn't found"));
            return IGraphTransformer::TStatus::Error;
        }

        const auto dependsOn = input->Child(1);
        if (!EnsureDependsOn(*dependsOn, ctx)) {
            return IGraphTransformer::TStatus::Error;
        }

        const auto row = dependsOn->TailPtr();
        if (!EnsureStructType(*row, ctx)) {
            return IGraphTransformer::TStatus::Error;
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
                ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Wrong place to use SystemMetadata"));
                return IGraphTransformer::TStatus::Error;
            }

            bool isOptional = false;
            const TDataExprType* dataType = nullptr;
            if (!EnsureDataOrOptionalOfData(row->Pos(), structType->GetItems()[*pos]->GetItemType(), isOptional, dataType, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            if (!EnsureSpecificDataType(row->Pos(), *dataType, descriptor->Type, ctx)) {
                return IGraphTransformer::TStatus::Error;
            }

            output = ctx.Builder(input->Pos())
                .Callable("Member")
                    .Add(0, row)
                    .Atom(1, structType->GetItems()[*pos]->GetName(), TNodeFlags::Default)
                .Seal()
                .Build();

            return IGraphTransformer::TStatus::Repeat;
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

}

THolder<TVisitorTransformerBase> CreatePqDataSourceTypeAnnotationTransformer(TPqState::TPtr state) {
    return MakeHolder<TPqDataSourceTypeAnnotationTransformer>(state);
}

} // namespace NYql
