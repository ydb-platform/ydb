#include "yql_s3_dq_integration.h"
#include "yql_s3_mkql_compiler.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>
#include <ydb/library/yql/providers/s3/proto/sink.pb.h>
#include <ydb/library/yql/providers/s3/proto/source.pb.h>
#include <ydb/library/yql/providers/s3/range_helpers/file_tree_builder.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

TExprNode::TListType GetKeys(const TExprNode& settings) {
    for (auto i = 0U; i < settings.ChildrenSize(); ++i) {
        if (const auto& child = *settings.Child(i); child.Head().IsAtom("partitionedby")) {
            auto children = child.ChildrenList();
            children.erase(children.cbegin());
            return children;
        }
    }
    return {};
}

std::string_view GetCompression(const TExprNode& settings) {
    for (auto i = 0U; i < settings.ChildrenSize(); ++i) {
        if (settings.Child(i)->Head().IsAtom("compression")) {
            return settings.Child(i)->Tail().Content();
        }
    }

    return {};
}

bool GetMultipart(const TExprNode& settings) {
    for (auto i = 0U; i < settings.ChildrenSize(); ++i) {
        if (settings.Child(i)->Head().IsAtom("multipart")) {
            return FromString(settings.Child(i)->Tail().Content());
        }
    }

    return false;
}

using namespace NYql::NS3Details;

class TS3DqIntegration: public TDqIntegrationBase {
public:
    TS3DqIntegration(TS3State::TPtr state)
        : State_(state)
    {
    }

    ui64 Partition(const TDqSettings&, size_t maxPartitions, const TExprNode& node, TVector<TString>& partitions, TString*, TExprContext&, bool) override {
        std::vector<std::vector<TPath>> parts;
        if (const TMaybeNode<TDqSource> source = &node) {
            const auto settings = source.Cast().Settings().Cast<TS3SourceSettingsBase>();
            for (auto i = 0u; i < settings.Paths().Size(); ++i) {
                const auto& packed = settings.Paths().Item(i);
                TPathList paths;
                UnpackPathsList(
                    packed.Data().Literal().Value(),
                    FromString<bool>(packed.IsText().Literal().Value()),
                    paths);
                parts.reserve(parts.size() + paths.size());
                for (const auto& path : paths) {
                    parts.emplace_back(1U, path);
                }
            }
        }

        if (maxPartitions && parts.size() > maxPartitions) {
            if (const auto extraParts = parts.size() - maxPartitions; extraParts > maxPartitions) {
                const auto partsPerTask = (parts.size() - 1ULL) / maxPartitions + 1ULL;
                for (auto it = parts.begin(); parts.end() > it;) {
                    const auto to = it;
                    const auto up = to + std::min<std::size_t>(partsPerTask, std::distance(to, parts.end()));
                    for (auto jt = ++it; jt < up; ++jt)
                        std::move(jt->begin(), jt->end(), std::back_inserter(*to));
                    it = parts.erase(it, up);
                }
            } else {
                const auto dropEachPart = maxPartitions / extraParts;
                for (auto it = parts.begin(); parts.size() > maxPartitions;) {
                    const auto to = it + dropEachPart;
                    it = to - 1U;
                    std::move(to->begin(), to->end(), std::back_inserter(*it));
                    it = parts.erase(to);
                }
            }
        }

        partitions.reserve(parts.size());
        ui64 startIdx = 0;
        for (const auto& part : parts) {
            NS3::TRange range;
            range.SetStartPathIndex(startIdx);
            TFileTreeBuilder builder;
            std::for_each(part.cbegin(), part.cend(), [&builder, &startIdx](const TPath& f) {
                builder.AddPath(f.Path, f.Size, f.IsDirectory);
                ++startIdx;
            });
            builder.Save(&range);

            partitions.emplace_back();
            TStringOutput out(partitions.back());
            range.Save(&out);
        }

        return 0;
    }

    bool CanRead(const TExprNode& read, TExprContext&, bool) override {
        return TS3ReadObject::Match(&read);
    }

    TMaybe<ui64> EstimateReadSize(ui64 /*dataSizePerJob*/, ui32 /*maxTasksPerStage*/, const TVector<const TExprNode*>& read, TExprContext&) override {
        if (AllOf(read, [](const auto val) { return TS3ReadObject::Match(val); })) {
            return 0ul; // TODO: return real size
        }
        return Nothing();
    }

    TMaybe<TOptimizerStatistics> ReadStatistics(const TExprNode::TPtr& sourceWrap, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        double size = 0;
        double cols = 0;
        double rows = 0;
        if (const auto& maybeArrowSettings = TMaybeNode<TS3ArrowSettings>(sourceWrap->Child(0))) {
            const auto& arrowSettings = maybeArrowSettings.Cast();
            for (size_t i = 0; i < arrowSettings.Paths().Size(); ++i) {
                auto batch = arrowSettings.Paths().Item(i);
                TStringBuf packed = batch.Data().Literal().Value();
                bool isTextEncoded = FromString<bool>(batch.IsText().Literal().Value());
                TPathList paths;
                UnpackPathsList(packed, isTextEncoded, paths);

                for (const auto& path : paths) {
                    size += path.Size;
                }
            }

            if (arrowSettings.RowType().Maybe<TCoStructType>()) {
                cols = arrowSettings.RowType().Ptr()->ChildrenSize();
            }

            rows = size / 1024; // magic estimate
            return TOptimizerStatistics(rows, cols);
        } else {
            return Nothing();
        }
    }

    TExprNode::TPtr WrapRead(const TDqSettings&, const TExprNode::TPtr& read, TExprContext& ctx) override {
        if (const auto& maybeS3ReadObject = TMaybeNode<TS3ReadObject>(read)) {
            const auto& s3ReadObject = maybeS3ReadObject.Cast();

            const auto rowType = s3ReadObject.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()->GetItemType();
            const auto& clusterName = s3ReadObject.DataSource().Cluster().StringValue();

            const auto token = "cluster:default_" + clusterName;
            YQL_CLOG(INFO, ProviderS3) << "Wrap " << read->Content() << " with token: " << token;

            TExprNode::TListType settings(1U,
                ctx.Builder(s3ReadObject.Object().Pos())
                    .List()
                        .Atom(0, "format", TNodeFlags::Default)
                        .Add(1, s3ReadObject.Object().Format().Ptr())
                    .Seal().Build()
            );

            TExprNodeList extraColumnsExtents;
            for (size_t i = 0; i < s3ReadObject.Object().Paths().Size(); ++i) {
                auto batch = s3ReadObject.Object().Paths().Item(i);
                TStringBuf packed = batch.Data().Literal().Value();
                bool isTextEncoded = FromString<bool>(batch.IsText().Literal().Value());

                TPathList paths;
                UnpackPathsList(packed, isTextEncoded, paths);

                extraColumnsExtents.push_back(
                    ctx.Builder(batch.ExtraColumns().Pos())
                        .Callable("Replicate")
                            .Add(0, batch.ExtraColumns().Ptr())
                            .Callable(1, "Uint64")
                                .Atom(0, ToString(paths.size()), TNodeFlags::Default)
                            .Seal()
                        .Seal()
                        .Build()
                );
            }
            YQL_ENSURE(!extraColumnsExtents.empty());
            if (s3ReadObject.Object().Paths().Item(0).ExtraColumns().Ref().GetTypeAnn()->Cast<TStructExprType>()->GetSize()) {
                settings.push_back(
                    ctx.Builder(s3ReadObject.Object().Pos())
                        .List()
                            .Atom(0, "extraColumns")
                            .Add(1, ctx.NewCallable(s3ReadObject.Object().Pos(), "OrderedExtend", std::move(extraColumnsExtents)))
                        .Seal()
                        .Build()
                );
            }

            auto format = s3ReadObject.Object().Format().Ref().Content();
            if (const auto useCoro = State_->Configuration->SourceCoroActor.Get(); (!useCoro || *useCoro) && format != "raw" && format != "json_list") {
                bool supportedArrowTypes = false;
                if (State_->Types->UseBlocks) {
                    YQL_ENSURE(State_->Configuration->UseBlocksSource.Get().GetOrElse(true), "Scalar Source is not compatible with Blocks engine");
                }
                if (State_->Configuration->UseBlocksSource.Get().GetOrElse(State_->Types->UseBlocks) && State_->Types->ArrowResolver) {
                    TVector<const TTypeAnnotationNode*> allTypes;
                    for (const auto& x : rowType->Cast<TStructExprType>()->GetItems()) {
                        allTypes.push_back(x->GetItemType());
                    }

                    auto resolveStatus = State_->Types->ArrowResolver->AreTypesSupported(ctx.GetPosition(read->Pos()), allTypes, ctx);
                    YQL_ENSURE(resolveStatus != IArrowResolver::ERROR);
                    supportedArrowTypes = resolveStatus == IArrowResolver::OK;
                }
                return Build<TDqSourceWrap>(ctx, read->Pos())
                    .Input<TS3ParseSettingsBase>()
                        .CallableName((supportedArrowTypes && format == "parquet") ? TS3ArrowSettings::CallableName():
                                                                                     TS3ParseSettings::CallableName())
                        .Paths(s3ReadObject.Object().Paths())
                        .Token<TCoSecureParam>()
                            .Name().Build(token)
                        .Build()
                        .Format(s3ReadObject.Object().Format())
                        .RowType(ExpandType(s3ReadObject.Pos(), *rowType, ctx))
                        .Settings(s3ReadObject.Object().Settings())
                        .Build()
                    .RowType(ExpandType(s3ReadObject.Pos(), *rowType, ctx))
                    .DataSource(s3ReadObject.DataSource().Cast<TCoDataSource>())
                    .Settings(ctx.NewList(s3ReadObject.Object().Pos(), std::move(settings)))
                    .Done().Ptr();
            } else {
                if (const auto& objectSettings = s3ReadObject.Object().Settings()) {
                    settings.emplace_back(
                        ctx.Builder(objectSettings.Cast().Pos())
                            .List()
                                .Atom(0, "settings", TNodeFlags::Default)
                                .Add(1, objectSettings.Cast().Ptr())
                            .Seal().Build()
                    );
                }
                auto readSettings = s3ReadObject.Object().Settings().Cast().Ptr();

                int sizeLimitIndex = -1;
                int pathPatternIndex = -1;
                int pathPatternVariantIndex = -1;
                for (size_t childInd = 0; childInd < readSettings->ChildrenSize();
                     ++childInd) {
                    auto keyName = readSettings->Child(childInd)->Head().Content();
                    if (sizeLimitIndex == -1 && keyName == "readmaxbytes") {
                        sizeLimitIndex = childInd;
                    } else if (pathPatternIndex == -1 && keyName == "pathpattern") {
                        pathPatternIndex = childInd;
                    } else if (pathPatternVariantIndex == -1 && keyName == "pathpatternvariant") {
                        pathPatternVariantIndex = childInd;
                    }
                }

                auto emptyNode = Build<TCoVoid>(ctx, read->Pos()).Done().Ptr();
                return Build<TDqSourceWrap>(ctx, read->Pos())
                    .Input<TS3SourceSettings>()
                        .Paths(s3ReadObject.Object().Paths())
                        .Token<TCoSecureParam>()
                            .Name().Build(token)
                            .Build()
                        .SizeLimit(
                            sizeLimitIndex != -1 ? readSettings->Child(sizeLimitIndex)->TailPtr()
                                                 : emptyNode)
                        .PathPattern(
                            pathPatternIndex != -1
                                ? readSettings->Child(pathPatternIndex)->TailPtr()
                                : emptyNode)
                        .PathPatternVariant(
                            pathPatternVariantIndex != -1 ? readSettings->Child(pathPatternVariantIndex)->TailPtr()
                                               : emptyNode)
                        .Build()
                    .RowType(ExpandType(s3ReadObject.Pos(), *rowType, ctx))
                    .DataSource(s3ReadObject.DataSource().Cast<TCoDataSource>())
                    .Settings(ctx.NewList(s3ReadObject.Object().Pos(), std::move(settings)))
                    .Done().Ptr();
            }
        }
        return read;
    }

    void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sourceType) override {
        const TDqSource source(&node);
        if (const auto maySettings = source.Settings().Maybe<TS3SourceSettingsBase>()) {
            const auto settings = maySettings.Cast();
            const auto& cluster = source.DataSource().Cast<TS3DataSource>().Cluster().StringValue();
            const auto& connect = State_->Configuration->Clusters.at(cluster);

            NS3::TSource srcDesc;
            srcDesc.SetUrl(connect.Url);
            srcDesc.SetToken(settings.Token().Name().StringValue());

            const auto& paths = settings.Paths();
            YQL_ENSURE(paths.Size() > 0);
            const TStructExprType* extraColumnsType = paths.Item(0).ExtraColumns().Ref().GetTypeAnn()->Cast<TStructExprType>();

            if (const auto mayParseSettings = settings.Maybe<TS3ParseSettingsBase>()) {
                const auto parseSettings = mayParseSettings.Cast();
                srcDesc.SetFormat(parseSettings.Format().StringValue().c_str());
                srcDesc.SetArrow(bool(parseSettings.Maybe<TS3ArrowSettings>()));
                srcDesc.SetParallelRowGroupCount(State_->Configuration->ArrowParallelRowGroupCount.Get().GetOrElse(0));
                srcDesc.SetRowGroupReordering(State_->Configuration->ArrowRowGroupReordering.Get().GetOrElse(true));
                srcDesc.SetParallelDownloadCount(State_->Configuration->ParallelDownloadCount.Get().GetOrElse(0));

                const TStructExprType* fullRowType = parseSettings.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                // exclude extra columns to get actual row type we need to read from input
                auto rowTypeItems = fullRowType->GetItems();
                EraseIf(rowTypeItems, [extraColumnsType](const auto& item) { return extraColumnsType->FindItem(item->GetName()); });
                {
                    // TODO: pass context
                    TExprContext ctx;
                    srcDesc.SetRowType(NCommon::WriteTypeToYson(ctx.MakeType<TStructExprType>(rowTypeItems), NYT::NYson::EYsonFormat::Text));
                }

                if (const auto maySettings = parseSettings.Settings()) {
                    const auto& settings = maySettings.Cast();
                    for (auto i = 0U; i < settings.Ref().ChildrenSize(); ++i) {
                        srcDesc.MutableSettings()->insert(
                            {TString(settings.Ref().Child(i)->Head().Content()),
                             TString(
                                 settings.Ref().Child(i)->Tail().IsAtom()
                                     ? settings.Ref().Child(i)->Tail().Content()
                                     : settings.Ref().Child(i)->Tail().Head().Content())});
                    }
                }
            } else if (const auto maySourceSettings = source.Settings().Maybe<TS3SourceSettings>()){
                const auto sourceSettings = maySourceSettings.Cast();
                auto sizeLimit = sourceSettings.SizeLimit().Maybe<TCoAtom>();
                if (sizeLimit.IsValid()) {
                    srcDesc.MutableSettings()->insert(
                        {"sizeLimit", sizeLimit.Cast().StringValue()});
                }
                auto pathPattern = sourceSettings.PathPattern().Maybe<TCoAtom>();
                if (pathPattern.IsValid()) {
                    srcDesc.MutableSettings()->insert(
                        {"pathpattern", pathPattern.Cast().StringValue()});
                }
                auto pathPatternVariant =
                    sourceSettings.PathPatternVariant().Maybe<TCoAtom>();
                if (pathPatternVariant.IsValid()) {
                    srcDesc.MutableSettings()->insert(
                        {"pathpatternvariant", pathPatternVariant.Cast().StringValue()});
                }
            }

            if (extraColumnsType->GetSize()) {
                srcDesc.MutableSettings()->insert({"addPathIndex", "true"});
            }

            protoSettings.PackFrom(srcDesc);
            sourceType = "S3Source";
        }
    }

    TMaybe<bool> CanWrite(const TExprNode& write, TExprContext& ctx) override {
        Y_UNUSED(ctx);
        return TS3WriteObject::Match(&write);
    }

    TExprNode::TPtr WrapWrite(const TExprNode::TPtr& writeNode, TExprContext& ctx) override {
        TExprBase writeExpr(writeNode);
        const auto write = writeExpr.Cast<TS3WriteObject>();
        return Build<TS3Insert>(ctx, write.Pos())
            .DataSink(write.DataSink())
            .Target(write.Target())
            .Input(write.Input())
            .Done().Ptr();
    }

    void FillSinkSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sinkType) override {
        const TDqSink sink(&node);
        if (const auto maybeSettings = sink.Settings().Maybe<TS3SinkSettings>()) {
            const auto settings = maybeSettings.Cast();
            const auto& cluster = sink.DataSink().Cast<TS3DataSink>().Cluster().StringValue();
            const auto& connect = State_->Configuration->Clusters.at(cluster);

            NS3::TSink sinkDesc;
            sinkDesc.SetUrl(connect.Url);
            sinkDesc.SetToken(settings.Token().Name().StringValue());
            sinkDesc.SetPath(settings.Path().StringValue());
            sinkDesc.SetExtension(settings.Extension().StringValue());
            for (const auto& key : GetKeys(settings.Settings().Ref()))
                sinkDesc.MutableKeys()->Add(TString(key->Content()));

            if (const auto& memoryLimit = State_->Configuration->InFlightMemoryLimit.Get())
                sinkDesc.SetMemoryLimit(*memoryLimit);

            if (const auto& compression = GetCompression(settings.Settings().Ref()); !compression.empty())
                sinkDesc.SetCompression(TString(compression));

            sinkDesc.SetMultipart(GetMultipart(settings.Settings().Ref()));
            sinkDesc.SetAtomicUploadCommit(State_->Configuration->AtomicUploadCommit.Get().GetOrElse(false));

            protoSettings.PackFrom(sinkDesc);
            sinkType = "S3Sink";
        }
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        RegisterDqS3MkqlCompilers(compiler, State_);
    }
private:
    const TS3State::TPtr State_;
};

}

THolder<IDqIntegration> CreateS3DqIntegration(TS3State::TPtr state) {
    return MakeHolder<TS3DqIntegration>(state);
}

}
