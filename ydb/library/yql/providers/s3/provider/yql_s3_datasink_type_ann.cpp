#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/s3/object_listers/yql_s3_path.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

TExprNode::TListType GetPartitionKeys(const TExprNode::TPtr& partBy) {
    if (partBy) {
        auto children = partBy->ChildrenList();
        children.erase(children.cbegin());
        return children;
    }

    return {};
}
}

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
        AddHandler({TS3Insert::CallableName()}, Hndl(&TSelf::HandleInsert));
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

        auto source = input->Child(TS3WriteObject::idx_Input);
        if (!EnsureListType(*source, ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* sourceType = source->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!EnsureStructType(source->Pos(), *sourceType, ctx)) {
            return TStatus::Error;
        }

        auto target = input->Child(TS3WriteObject::idx_Target);
        if (!TS3Target::Match(target)) {
            ctx.AddError(TIssue(ctx.GetPosition(target->Pos()), "Expected S3 target."));
            return TStatus::Error;
        }

        TS3Target tgt(target);
        if (auto settings = tgt.Settings()) {
            if (auto userschema = GetSetting(settings.Cast().Ref(), "userschema")) {
                const TTypeAnnotationNode* targetType = userschema->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                if (!IsSameAnnotation(*targetType, *sourceType)) {
                    ctx.AddError(TIssue(ctx.GetPosition(source->Pos()),
                                        TStringBuilder() << "Type mismatch between schema type: " << *targetType
                                                         << " and actual data type: " << *sourceType << ", diff is: "
                                                         << GetTypeDiff(*targetType, *sourceType)));
                    return TStatus::Error;
                }
            }
        }

        input->SetTypeAnn(ctx.MakeType<TWorldExprType>());
        return TStatus::Ok;
    }

    TStatus HandleInsert(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 3U, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSink(*input->Child(TS3Insert::idx_DataSink), S3ProviderName, ctx)) {
            return TStatus::Error;
        }

        auto source = input->Child(TS3Insert::idx_Input);
        if (!EnsureListType(*source, ctx)) {
            return TStatus::Error;
        }

        const TTypeAnnotationNode* sourceType = source->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
        if (!EnsureStructType(source->Pos(), *sourceType, ctx)) {
            return TStatus::Error;
        }

        const auto structType = sourceType->Cast<TStructExprType>();
        auto target = input->Child(TS3Insert::idx_Target);
        if (!TS3Target::Match(target)) {
            ctx.AddError(TIssue(ctx.GetPosition(target->Pos()), "Expected S3 target."));
            return TStatus::Error;
        }

        TExprNode::TListType keys;
        TS3Target tgt(target);
        if (auto settings = tgt.Settings()) {
            if (auto userschema = GetSetting(settings.Cast().Ref(), "userschema")) {
                const TTypeAnnotationNode* targetType = userschema->Child(1)->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                if (!IsSameAnnotation(*targetType, *sourceType)) {
                    ctx.AddError(TIssue(ctx.GetPosition(source->Pos()),
                                        TStringBuilder() << "Type mismatch between schema type: " << *targetType
                                                         << " and actual data type: " << *sourceType << ", diff is: "
                                                         << GetTypeDiff(*targetType, *sourceType)));
                    return TStatus::Error;
                }
            }
            auto partBy = GetSetting(settings.Cast().Ref(), "partitionedby"sv);
            keys = GetPartitionKeys(partBy);
        }

        const auto format = tgt.Format();

        auto baseTargeType = AnnotateTargetBase(format, keys, structType, ctx);
        if (!baseTargeType) {
            return TStatus::Error;
        }

        auto t = ctx.MakeType<TTupleExprType>(
                    TTypeAnnotationNode::TListType{
                        ctx.MakeType<TListExprType>(
                            baseTargeType
                        )
                    });

        input->SetTypeAnn(t);
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
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3Target::idx_Path)->Pos()), "Expected non empty path to directory ending with '/'."));
            return TStatus::Error;
        }

        if (const auto& normalized = NS3::NormalizePath(ToString(path)); normalized == "/") {
            ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3Target::idx_Path)->Pos()), "Unable to write to root directory"));
            return TStatus::Error;
        } else if (normalized != path) {
            output = ctx.ChangeChild(*input, TS3Target::idx_Path, ctx.NewAtom(input->Child(TS3Target::idx_Path)->Pos(), normalized));
            return TStatus::Repeat;
        }

        const auto format = input->Child(TS3Target::idx_Format)->Content();
        if (!EnsureAtom(*input->Child(TS3Target::idx_Format), ctx) || !NCommon::ValidateFormatForOutput(format, ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TS3Target::idx_Settings) {
            if (!EnsureTuple(*input->Child(TS3Target::idx_Settings), ctx))
                return TStatus::Error;

            bool hasDateTimeFormat = false;
            bool hasDateTimeFormatName = false;
            bool hasTimestampFormat = false;
            bool hasTimestampFormatName = false;
            const auto validator = [&](TStringBuf name, TExprNode& setting, TExprContext& ctx) {
                if (name == "compression") {
                    const auto& value = setting.Tail();
                    if (!EnsureAtom(value, ctx)) {
                        return false;
                    }

                    return NCommon::ValidateCompressionForOutput(format, value.Content(), ctx);
                }

                if (name == "partitionedby") {
                    if (setting.ChildrenSize() < 2) {
                        ctx.AddError(TIssue(ctx.GetPosition(setting.Pos()), "Expected at least one column in partitioned_by setting"));
                        return false;
                    }

                    std::unordered_set<std::string_view> uniqs(setting.ChildrenSize());
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

                if (name == "userschema") {
                    return EnsureValidUserSchemaSetting(setting, ctx);
                }

                if (name == "data.datetime.formatname") {
                    hasDateTimeFormatName = true;
                    const auto& value = setting.Tail();
                    if (!EnsureAtom(value, ctx)) {
                        return false;
                    }

                    return NCommon::ValidateDateTimeFormatName(value.Content(), ctx);
                }

                if (name == "data.timestamp.formatname") {
                    hasTimestampFormatName = true;
                    const auto& value = setting.Tail();
                    if (!EnsureAtom(value, ctx)) {
                        return false;
                    }

                    return NCommon::ValidateTimestampFormatName(value.Content(), ctx);
                }

                if (name == "data.datetime.format") {
                    hasDateTimeFormat = true;
                    const auto& value = setting.Tail();
                    if (!EnsureAtom(value, ctx)) {
                        return false;
                    }

                    return true;
                }

                if (name == "data.timestamp.format") {
                    hasTimestampFormat = true;
                    const auto& value = setting.Tail();
                    if (!EnsureAtom(value, ctx)) {
                        return false;
                    }

                    return true;
                }

                if (name == "csvdelimiter") {
                    const auto& value = setting.Tail();
                    if (!EnsureAtom(value, ctx)) {
                        return false;
                    }

                    if (value.Content().Size() != 1) {
                        ctx.AddError(TIssue(ctx.GetPosition(value.Pos()), "csv_delimiter must be single character"));
                        return false;
                    }
                    return true;
                }

                if (name == "filepattern") {
                    // just skip, used in reading only
                    return true;
                }

                return true;
            };

            if (!EnsureValidSettings(*input->Child(TS3Target::idx_Settings), {"compression", "partitionedby", "mode", "userschema", "data.datetime.formatname", "data.datetime.format", "data.timestamp.formatname", "data.timestamp.format", "csvdelimiter", "filepattern"}, validator, ctx)) {
                return TStatus::Error;
            }

            if (hasDateTimeFormat && hasDateTimeFormatName) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3Target::idx_Settings)->Pos()), "Don't use data.datetime.format_name and data.datetime.format together"));
                return TStatus::Error;
            }

            if (hasTimestampFormat && hasTimestampFormatName) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Child(TS3Target::idx_Settings)->Pos()), "Don't use data.timestamp.format_name and data.timestamp.format together"));
                return TStatus::Error;
            }
        }

        input->SetTypeAnn(ctx.MakeType<TUnitExprType>());
        return TStatus::Ok;
    }

    TStatus HandleSink(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 4, ctx)) {
            return TStatus::Error;
        }
        input->SetTypeAnn(ctx.MakeType<TVoidExprType>());
        return TStatus::Ok;
    }

    TStatus HandleOutput(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 3U, 4U, ctx)) {
            return TStatus::Error;
        }

        const auto source = input->Child(TS3SinkOutput::idx_Input);
        if (!EnsureNewSeqType<false, false>(*source, ctx)) {
            return TStatus::Error;
        }

        if (ETypeAnnotationKind::Stream == source->GetTypeAnn()->GetKind()) {
            output = ctx.ChangeChild(*input, TS3SinkOutput::idx_Input, ctx.NewCallable(source->Pos(), "ToFlow", {input->ChildPtr(TS3SinkOutput::idx_Input)}));
            return TStatus::Repeat;
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

        const auto itemType = source->GetTypeAnn()->Cast<TFlowExprType>()->GetItemType();
        if (!EnsureStructType(source->Pos(), *itemType, ctx)) {
            return TStatus::Error;
        }

        const auto structType = itemType->Cast<TStructExprType>();
        const auto keys = input->Child(TS3SinkOutput::idx_KeyColumns)->ChildrenList();
        const TCoAtom format(input->Child(TS3SinkOutput::idx_Format));

        auto baseTargetType = AnnotateTargetBase(format, keys, structType, ctx);
        if (!baseTargetType) {
            return TStatus::Error;
        }

        input->SetTypeAnn(ctx.MakeType<TFlowExprType>(baseTargetType));
        return TStatus::Ok;
    }

private:
    const TTypeAnnotationNode* AnnotateTargetBase(TCoAtom format, const TExprNode::TListType& keys, const TStructExprType* structType, TExprContext& ctx) {
        const bool isSingleRowPerFileFormat = IsIn({TStringBuf("raw"), TStringBuf("json_list")}, format);

        auto keysCount = keys.size();
        if (keysCount) {
            if (isSingleRowPerFileFormat) {
                ctx.AddError(TIssue(ctx.GetPosition(format.Pos()), TStringBuilder() << "Partitioned isn't supported for " << (TStringBuf)format << " output format."));
                return nullptr;
            }

            for (auto i = 0U; i < keysCount; ++i) {
                const auto key = keys[i];
                if (const auto keyType = structType->FindItemType(key->Content())) {
                    if (!EnsureDataType(key->Pos(), *keyType, ctx)) {
                        return nullptr;
                    }
                } else {
                    ctx.AddError(TIssue(ctx.GetPosition(key->Pos()), "Missed key column."));
                    return nullptr;
                }
            }

            TTypeAnnotationNode::TListType itemTypes(keysCount + 1U, ctx.MakeType<TDataExprType>(EDataSlot::Utf8));
            itemTypes.front() = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::String));

            return ctx.MakeType<TTupleExprType>(itemTypes);
        }

        const TTypeAnnotationNode* listItemType = ctx.MakeType<TDataExprType>(EDataSlot::String);
        if (!isSingleRowPerFileFormat) {
            return ctx.MakeType<TOptionalExprType>(listItemType);
        }

        return listItemType;
    }

private:
    const TS3State::TPtr State_;
};

}

THolder<TVisitorTransformerBase> CreateS3DataSinkTypeAnnotationTransformer(TS3State::TPtr state) {
    return MakeHolder<TS3DataSinkTypeAnnotationTransformer>(state);
}

} // namespace NYql
