#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

bool ValidateS3PackedPaths(TPositionHandle pos, TStringBuf blob, bool isTextEncoded, TExprContext& ctx) {
    using namespace NYql::NS3Details;
    try {
        TPathList paths;
        UnpackPathsList(blob, isTextEncoded, paths);
        for (size_t i = 0; i < paths.size(); ++i) {
            if (std::get<0>(paths[i]).empty()) {
                ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Expected non-empty path (index " << i << ")"));
                return false;
            }
        }
    } catch (const std::exception& ex) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Failed to parse packed paths: " << ex.what()));
        return false;
    }
    return true;
}

bool ValidateS3Paths(const TExprNode& node, const TStructExprType*& extraColumnsType, TExprContext& ctx) {
    if (!EnsureTupleMinSize(node, 1, ctx)) {
        return false;
    }

    extraColumnsType = nullptr;
    for (auto& path : node.ChildrenList()) {
        if (!EnsureTupleSize(*path, 3, ctx)) {
            return false;
        }

        auto pathAndSizeList = path->Child(TS3Path::idx_Data);
        if (!TCoString::Match(pathAndSizeList)) {
            ctx.AddError(TIssue(ctx.GetPosition(pathAndSizeList->Pos()), "Expected String literal for Data"));
            return false;
        }

        const TExprNode* isTextEncoded = path->Child(TS3Path::idx_IsText);
        if (!TCoBool::Match(isTextEncoded)) {
            ctx.AddError(TIssue(ctx.GetPosition(isTextEncoded->Pos()), "Expected Bool literal for IsText"));
            return false;
        }

        if (!ValidateS3PackedPaths(pathAndSizeList->Pos(), pathAndSizeList->Head().Content(), FromString<bool>(isTextEncoded->Head().Content()), ctx)) {
            return false;
        }

        auto extraColumns = path->Child(TS3Path::idx_ExtraColumns);
        if (!EnsureStructType(*extraColumns, ctx) || !EnsurePersistable(*extraColumns, ctx)) {
            return false;
        }

        const TStructExprType* current = extraColumns->GetTypeAnn()->Cast<TStructExprType>();
        if (!extraColumnsType) {
            extraColumnsType = current;
        } else if (!IsSameAnnotation(*current, *extraColumnsType)) {
            ctx.AddError(TIssue(ctx.GetPosition(path->Pos()), TStringBuilder() << "Extra columns type mismatch: got "
                << *(const TTypeAnnotationNode*)current << ", expecting "
                << *(const TTypeAnnotationNode*)extraColumnsType));
            return false;
        }
    }
    return true;
}


class TS3DataSourceTypeAnnotationTransformer : public TVisitorTransformerBase {
public:
    TS3DataSourceTypeAnnotationTransformer(TS3State::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(state)
    {
        using TSelf = TS3DataSourceTypeAnnotationTransformer;
        AddHandler({TS3ReadObject::CallableName()}, Hndl(&TSelf::HandleRead));
        AddHandler({TS3Object::CallableName()}, Hndl(&TSelf::HandleObject));
        AddHandler({TS3SourceSettings::CallableName()}, Hndl(&TSelf::HandleS3SourceSettings));
        AddHandler({TS3ParseSettings::CallableName()}, Hndl(&TSelf::HandleS3ParseSettings));
        AddHandler({TCoConfigure::CallableName()}, Hndl(&TSelf::HandleConfig));
    }

    TStatus HandleS3SourceSettings(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 2U, ctx)) {
            return TStatus::Error;
        }

        const TStructExprType* extraColumnsType = nullptr;
        if (!ValidateS3Paths(*input->Child(TS3SourceSettings::idx_Paths), extraColumnsType, ctx)) {
            return TStatus::Error;
        }

        if (!TCoSecureParam::Match(input->Child(TS3SourceSettings::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3SourceSettings::idx_Token)->Pos()), TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
            return TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = ctx.MakeType<TDataExprType>(EDataSlot::String);
        if (extraColumnsType->GetSize()) {
            itemType = ctx.MakeType<TTupleExprType>(
                TTypeAnnotationNode::TListType{ itemType, ctx.MakeType<TDataExprType>(EDataSlot::Uint64) });
        }
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(itemType));
        return TStatus::Ok;
    }

    TStatus HandleS3ParseSettings(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 4U, 5U, ctx)) {
            return TStatus::Error;
        }

        const TStructExprType* extraColumnsType = nullptr;
        if (!ValidateS3Paths(*input->Child(TS3SourceSettings::idx_Paths), extraColumnsType, ctx)) {
            return TStatus::Error;
        }

        if (!TCoSecureParam::Match(input->Child(TS3ParseSettings::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3ParseSettings::idx_Token)->Pos()), TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TS3ParseSettings::idx_Format), ctx) ||
            !NCommon::ValidateFormat(input->Child(TS3ParseSettings::idx_Format)->Content(), ctx))
        {
            return TStatus::Error;
        }

        const auto& rowTypeNode = *input->Child(TS3ParseSettings::idx_RowType);
        if (!EnsureType(rowTypeNode, ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* rowType = rowTypeNode.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureStructType(rowTypeNode.Pos(), *rowType, ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TS3ParseSettings::idx_Settings && !EnsureTuple(*input->Child(TS3ParseSettings::idx_Settings), ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* itemType = ctx.MakeType<TResourceExprType>("ClickHouseClient.Block");
        if (extraColumnsType->GetSize()) {
            itemType = ctx.MakeType<TTupleExprType>(
                TTypeAnnotationNode::TListType{ itemType, ctx.MakeType<TDataExprType>(EDataSlot::Uint64) });
        }
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(itemType));
        return TStatus::Ok;
    }

    TStatus HandleRead(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 4U, 5U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TS3ReadObject::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TS3ReadObject::idx_DataSource), S3ProviderName, ctx)) {
            return TStatus::Error;
        }

        if (TS3ReadObject::Match(input->Child(TS3ReadObject::idx_Object))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3ReadObject::idx_Object)->Pos()), "Expected S3 object."));
            return TStatus::Error;
        }

        if (!EnsureType(*input->Child(TS3ReadObject::idx_RowType), ctx)) {
            return TStatus::Error;
        }

        const auto& rowTypeNode = *input->Child(TS3ReadObject::idx_RowType);
        if (!EnsureType(rowTypeNode, ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* rowType = rowTypeNode.GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        if (!EnsureStructType(rowTypeNode.Pos(), *rowType, ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(TS3ReadObject::idx_World)->GetTypeAnn(),
            ctx.MakeType<TListExprType>(rowType)
        }));

        if (input->ChildrenSize() > TS3ReadObject::idx_ColumnOrder) {
            const auto& order = *input->Child(TS3ReadObject::idx_ColumnOrder);
            if (!EnsureTupleOfAtoms(order, ctx)) {
                return TStatus::Error;
            }
            TVector<TString> columnOrder;
            THashSet<TStringBuf> uniqs;
            columnOrder.reserve(order.ChildrenSize());
            uniqs.reserve(order.ChildrenSize());

            for (auto& child : order.ChildrenList()) {
                TStringBuf col = child->Content();
                if (!uniqs.emplace(col).second) {
                    ctx.AddError(TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Duplicate column '" << col << "' in column order list"));
                    return TStatus::Error;
                }
                columnOrder.push_back(ToString(col));
            }
            return State_->Types->SetColumnOrder(*input, columnOrder, ctx);
        }

        return TStatus::Ok;
    }

    TStatus HandleConfig(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TCoConfigure::idx_World), ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TCoConfigure::idx_DataSource), S3ProviderName, ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(input->Child(TCoConfigure::idx_World)->GetTypeAnn());
        return TStatus::Ok;
    }

    TStatus HandleObject(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 2U, 3U, ctx)) {
            return TStatus::Error;
        }

        const TStructExprType* extraColumnsType = nullptr;
        if (!ValidateS3Paths(*input->Child(TS3Object::idx_Paths), extraColumnsType, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TS3Object::idx_Format), ctx) ||
            !NCommon::ValidateFormat(input->Child(TS3Object::idx_Format)->Content(), ctx))
        {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TS3Object::idx_Settings) {
            auto validator = [](TStringBuf name, const TExprNode& setting, TExprContext& ctx) {
                if ((name == "compression" || name == "projection" || name == "data.interval.unit") && setting.ChildrenSize() != 2) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()),
                        TStringBuilder() << "Expected single value setting for " << name << ", but got " << setting.ChildrenSize() - 1));
                    return false;
                }

                bool havePartitionedBy = false;
                if (name == "compression") {
                    auto& value = setting.Tail();
                    TStringBuf compression;
                    if (value.IsAtom()) {
                        compression = value.Content();
                    } else {
                        if (!EnsureStringOrUtf8Type(value, ctx)) {
                            return false;
                        }
                        if (!value.IsCallable({"String", "Utf8"})) {
                            ctx.AddError(TIssue(ctx.GetPosition(value.Pos()), "Expected literal string as compression value"));
                            return false;
                        }
                        compression = value.Head().Content();
                    }
                    return NCommon::ValidateCompression(compression, ctx);
                }
                if (name == "partitionedby") {
                    havePartitionedBy = true;
                    if (setting.ChildrenSize() < 2) {
                        ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), "Expected at least one column in partitioned_by setting"));
                        return false;
                    }

                    THashSet<TStringBuf> uniqs;
                    for (size_t i = 1; i < setting.ChildrenSize(); ++i) {
                        const auto& column = setting.Child(i);
                        if (!EnsureAtom(*column, ctx)) {
                            return false;
                        }
                        if (!uniqs.emplace(column->Content()).second) {
                            ctx.AddError(TIssue(ctx.GetPosition(column->Pos()),
                                TStringBuilder() << "Duplicate partitioned_by column '" << column->Content() << "'"));
                            return false;
                        }
                    }

                    return true;
                }

                if (name == "data.interval.unit") {
                    auto& value = setting.Tail();
                    TStringBuf unit;
                    if (value.IsAtom()) {
                        unit = value.Content();
                    } else {
                        if (!EnsureStringOrUtf8Type(value, ctx)) {
                            return false;
                        }
                        if (!value.IsCallable({"String", "Utf8"})) {
                            ctx.AddError(TIssue(ctx.GetPosition(value.Pos()), "Expected literal string as compression value"));
                            return false;
                        }
                        unit = value.Head().Content();
                    }
                    return NCommon::ValidateIntervalUnit(unit, ctx);
                }

                YQL_ENSURE(name == "projection");
                if (!EnsureAtom(setting.Tail(), ctx)) {
                    return false;
                }

                if (setting.Tail().Content().empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), "Expecting non-empty projection setting"));
                    return false;
                }

                if (!havePartitionedBy) {
                    ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), "Missing partitioned_by setting for projection"));
                    return false;
                }

                return true;
            };
            if (!EnsureValidSettings(*input->Child(TS3Object::idx_Settings),
                                     { "compression", "partitionedby", "projection", "data.interval.unit" }, validator, ctx))
            {
                return TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }
private:
    const TS3State::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreateS3DataSourceTypeAnnotationTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3DataSourceTypeAnnotationTransformer>(state);
}

} // namespace NYql
