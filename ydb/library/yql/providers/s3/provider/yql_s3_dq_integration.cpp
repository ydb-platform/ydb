#include "yql_s3_dq_integration.h"
#include "yql_s3_mkql_compiler.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_predicate_pushdown.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_predicate_pushdown.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_read_actor.h>
#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>
#include <ydb/library/yql/providers/s3/proto/range.pb.h>
#include <ydb/library/yql/providers/s3/proto/sink.pb.h>
#include <ydb/library/yql/providers/s3/proto/source.pb.h>
#include <ydb/library/yql/providers/s3/range_helpers/file_tree_builder.h>
#include <ydb/library/yql/providers/s3/range_helpers/path_list_reader.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/plan/plan_utils.h>

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/yson/node/node_io.h>

namespace NYql {

using namespace NNodes;

namespace {

TString GetLastName(const TString& fullName) {
    auto n = fullName.find_last_of('/');
    return (n == fullName.npos) ? fullName : fullName.substr(n + 1);
}

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

std::optional<ui64> TryExtractLimitHint(const TS3SourceSettingsBase& settings) {
    auto limitHint = settings.RowsLimitHint();
    if (limitHint.Ref().Content().empty()) {
        return std::nullopt;
    }

    return FromString<ui64>(limitHint.Ref().Content());
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
        std::optional<ui64> mbLimitHint;
        bool hasDirectories = false;
        if (const TMaybeNode<TDqSource> source = &node) {
            const auto settings = source.Cast().Settings().Cast<TS3SourceSettingsBase>();
            mbLimitHint = TryExtractLimitHint(settings);

            for (auto i = 0u; i < settings.Paths().Size(); ++i) {
                const auto& packed = settings.Paths().Item(i);
                TPathList paths;
                UnpackPathsList(
                    packed.Data().Literal().Value(),
                    FromString<bool>(packed.IsText().Literal().Value()),
                    paths);
                parts.reserve(parts.size() + paths.size());
                for (const auto& path : paths) {
                    if (path.IsDirectory) {
                        hasDirectories = true;
                    }
                    parts.emplace_back(1U, path);
                }
            }
        }

        constexpr ui64 maxTaskRatio = 20;
        if (!maxPartitions || (mbLimitHint && maxPartitions > *mbLimitHint / maxTaskRatio)) {
            maxPartitions = std::max(*mbLimitHint / maxTaskRatio, ui64{1});
            YQL_CLOG(TRACE, ProviderS3) << "limited max partitions to " << maxPartitions;
        }

        auto useRuntimeListing = State_->Configuration->UseRuntimeListing.Get().GetOrElse(false);

        YQL_CLOG(DEBUG, ProviderS3) << " useRuntimeListing=" << useRuntimeListing;
        if (useRuntimeListing) {
            size_t partitionCount = hasDirectories ? maxPartitions : Min(parts.size(), maxPartitions);
            partitions.reserve(partitionCount);
            for (size_t i = 0; i < partitionCount; ++i) {
                NS3::TRange range;
                TFileTreeBuilder builder;
                builder.Save(&range);

                partitions.emplace_back();
                TStringOutput out(partitions.back());
                range.Save(&out);
            }
            YQL_CLOG(DEBUG, ProviderS3) << " hasDirectories=" << hasDirectories << ", partitionCount=" << partitionCount << ", maxPartitions=" << maxPartitions;
            return 0;
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

        YQL_CLOG(DEBUG, ProviderS3) << " hasDirectories=" << hasDirectories << ", partitionCount=" << partitions.size() << ", maxPartitions=" << maxPartitions;;
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
        int cols = 0;
        double rows = 0;
        if (const auto& maybeParseSettings = TMaybeNode<TS3ParseSettings>(sourceWrap->Child(0))) {
            const auto& parseSettings = maybeParseSettings.Cast();
            for (size_t i = 0; i < parseSettings.Paths().Size(); ++i) {
                auto batch = parseSettings.Paths().Item(i);
                TStringBuf packed = batch.Data().Literal().Value();
                bool isTextEncoded = FromString<bool>(batch.IsText().Literal().Value());
                TPathList paths;
                UnpackPathsList(packed, isTextEncoded, paths);

                for (const auto& path : paths) {
                    size += path.Size;
                }
            }

            TVector<TString>* primaryKey = nullptr;
            if (auto constraints = GetSetting(parseSettings.Settings().Ref(), "constraints"sv)) {
                auto node = NYT::NodeFromYsonString(constraints->Child(1)->Content());
                auto* primaryKeyNode = node.AsMap().FindPtr("primary_key");
                if (primaryKeyNode) {
                    TVector<TString> parsed;
                    for (auto col : primaryKeyNode->AsList()) {
                        parsed.push_back(col.AsString());
                    }
                    State_->PrimaryKeys.emplace_back(std::move(parsed));
                    primaryKey = &State_->PrimaryKeys.back();
                }
            }

            if (parseSettings.RowType().Maybe<TCoStructType>()) {
                cols = parseSettings.RowType().Ptr()->ChildrenSize();
            }

            rows = size / 1024; // magic estimate
            return primaryKey 
                ? TOptimizerStatistics(BaseTable, rows, cols, size, size, TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(new TOptimizerStatistics::TKeyColumns(*primaryKey)))
                : TOptimizerStatistics(BaseTable, rows, cols, size, size);
        } else {
            return Nothing();
        }
    }

    TExprNode::TPtr WrapRead(const TDqSettings&, const TExprNode::TPtr& read, TExprContext& ctx) override {
        if (const auto& maybeS3ReadObject = TMaybeNode<TS3ReadObject>(read)) {
            const auto& s3ReadObject = maybeS3ReadObject.Cast();
            YQL_ENSURE(s3ReadObject.Ref().GetTypeAnn(), "No type annotation for node " << s3ReadObject.Ref().Content());

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
                return Build<TDqSourceWrap>(ctx, read->Pos())
                    .Input<TS3ParseSettings>()
                        .Paths(s3ReadObject.Object().Paths())
                        .Token<TCoSecureParam>()
                            .Name().Build(token)
                        .Build()
                        .RowsLimitHint(ctx.NewAtom(read->Pos(), ""))
                        .Path(s3ReadObject.Path())
                        .Format(s3ReadObject.Object().Format())
                        .RowType(ExpandType(s3ReadObject.Pos(), *rowType, ctx))
                        .FilterPredicate(s3ReadObject.FilterPredicate())
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
                        .RowsLimitHint(ctx.NewAtom(read->Pos(), ""))
                        .Path(s3ReadObject.Path())
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

    void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sourceType, size_t maxPartitions) override {
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

            if (auto hintStr = settings.RowsLimitHint().Ref().Content(); !hintStr.empty()) {
                srcDesc.SetRowsLimitHint(FromString<i64>(hintStr));
            }

            if (const auto mayParseSettings = settings.Maybe<TS3ParseSettings>()) {
                const auto parseSettings = mayParseSettings.Cast();
                srcDesc.SetFormat(parseSettings.Format().StringValue().c_str());
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
 
                if (auto predicate = parseSettings.FilterPredicate(); !IsEmptyFilterPredicate(predicate)) {
                    TStringBuilder err;
                    if (!SerializeFilterPredicate(predicate, srcDesc.mutable_predicate(), err)) {
                        ythrow yexception() << "Failed to serialize filter predicate for source: " << err;
                    }
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

            srcDesc.SetAsyncDecoding(State_->Configuration->AsyncDecoding.Get().GetOrElse(false));
            srcDesc.SetAsyncDecompressing(State_->Configuration->AsyncDecompressing.Get().GetOrElse(false));

#if defined(_linux_) || defined(_darwin_)

            auto useRuntimeListing = State_->Configuration->UseRuntimeListing.Get().GetOrElse(false);
            srcDesc.SetUseRuntimeListing(useRuntimeListing);

            auto fileQueueBatchSizeLimit = State_->Configuration->FileQueueBatchSizeLimit.Get().GetOrElse(1000000);
            srcDesc.MutableSettings()->insert({"fileQueueBatchSizeLimit", ToString(fileQueueBatchSizeLimit)});

            auto fileQueueBatchObjectCountLimit = State_->Configuration->FileQueueBatchObjectCountLimit.Get().GetOrElse(1000);
            srcDesc.MutableSettings()->insert({"fileQueueBatchObjectCountLimit", ToString(fileQueueBatchObjectCountLimit)});

            YQL_CLOG(DEBUG, ProviderS3) << " useRuntimeListing=" << useRuntimeListing;

            if (useRuntimeListing) {
                TPathList paths;
                for (auto i = 0u; i < settings.Paths().Size(); ++i) {
                    const auto& packed = settings.Paths().Item(i);
                    TPathList pathsChunk;
                    UnpackPathsList(
                        packed.Data().Literal().Value(),
                        FromString<bool>(packed.IsText().Literal().Value()),
                        paths);
                    paths.insert(paths.end(),
                        std::make_move_iterator(pathsChunk.begin()),
                        std::make_move_iterator(pathsChunk.end()));
                }

                NS3::TRange range;
                range.SetStartPathIndex(0);
                TFileTreeBuilder builder;
                std::for_each(paths.cbegin(), paths.cend(), [&builder](const TPath& f) {
                    builder.AddPath(f.Path, f.Size, f.IsDirectory);
                });
                builder.Save(&range);

                TVector<TString> serialized(1);
                TStringOutput out(serialized.front());
                range.Save(&out);

                paths.clear();
                ReadPathsList(srcDesc, {}, serialized, paths);

                const NDq::TS3ReadActorFactoryConfig& readActorConfig = State_->Configuration->S3ReadActorFactoryConfig;
                ui64 fileSizeLimit = readActorConfig.FileSizeLimit;
                if (srcDesc.HasFormat()) {
                    if (auto it = readActorConfig.FormatSizeLimits.find(srcDesc.GetFormat()); it != readActorConfig.FormatSizeLimits.end()) {
                        fileSizeLimit = it->second;
                    }
                }
                if (srcDesc.HasFormat() && srcDesc.HasRowType()) {
                    if (srcDesc.GetFormat() == "parquet") {
                        fileSizeLimit = readActorConfig.BlockFileSizeLimit;
                    }
                }

                TString pathPattern = "*";
                auto pathPatternVariant = NS3Lister::ES3PatternVariant::FilePattern;
                auto hasDirectories = std::find_if(paths.begin(), paths.end(), [](const TPath& a) {
                                        return a.IsDirectory;
                                    }) != paths.end();

                if (hasDirectories) {
                    auto pathPatternValue = srcDesc.GetSettings().find("pathpattern");
                    if (pathPatternValue == srcDesc.GetSettings().cend()) {
                        ythrow yexception() << "'pathpattern' must be configured for directory listing";
                    }
                    pathPattern = pathPatternValue->second;

                    auto pathPatternVariantValue = srcDesc.GetSettings().find("pathpatternvariant");
                    if (pathPatternVariantValue == srcDesc.GetSettings().cend()) {
                        ythrow yexception()
                            << "'pathpatternvariant' must be configured for directory listing";
                    }
                    if (!TryFromString(pathPatternVariantValue->second, pathPatternVariant)) {
                        ythrow yexception()
                            << "Unknown 'pathpatternvariant': " << pathPatternVariantValue->second;
                    }
                }
                auto consumersCount = hasDirectories ? maxPartitions : paths.size();

                auto fileQueuePrefetchSize = State_->Configuration->FileQueuePrefetchSize.Get()
                    .GetOrElse(consumersCount * srcDesc.GetParallelDownloadCount() * 3);

                YQL_CLOG(DEBUG, ProviderS3) << " hasDirectories=" << hasDirectories << ", consumersCount=" << consumersCount;

                ui64 readLimit = std::numeric_limits<ui64>::max();
                if (const auto sizeLimitIter = srcDesc.MutableSettings()->find("sizeLimit"); sizeLimitIter != srcDesc.MutableSettings()->cend()) {
                    readLimit = FromString<ui64>(sizeLimitIter->second);
                }

                auto fileQueueActor = NActors::TActivationContext::ActorSystem()->Register(
                    NDq::CreateS3FileQueueActor(
                        0ul,
                        std::move(paths),
                        fileQueuePrefetchSize,
                        fileSizeLimit,
                        readLimit,
                        useRuntimeListing,
                        consumersCount,
                        fileQueueBatchSizeLimit,
                        fileQueueBatchObjectCountLimit,
                        State_->Gateway,
                        State_->GatewayRetryPolicy,
                        connect.Url,
                        TS3Credentials(State_->CredentialsFactory, State_->Configuration->Tokens.at(cluster)),
                        pathPattern,
                        pathPatternVariant,
                        NS3Lister::ES3PatternType::Wildcard,
                        State_->Configuration->AllowLocalFiles
                    ),
                    NActors::TMailboxType::HTSwap,
                    State_->ExecutorPoolId
                );

                NActorsProto::TActorId protoId;
                ActorIdToProto(fileQueueActor, &protoId);
                TString stringId;
                google::protobuf::TextFormat::PrintToString(protoId, &stringId);

                srcDesc.MutableSettings()->insert({"fileQueueActor", stringId});
            }
#endif
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
            sinkDesc.SetAtomicUploadCommit(State_->Configuration->AllowAtomicUploadCommit && State_->Configuration->AtomicUploadCommit.Get().GetOrElse(false));

            protoSettings.PackFrom(sinkDesc);
            sinkType = "S3Sink";
        }
    }

    bool FillSourcePlanProperties(const NNodes::TExprBase& node, TMap<TString, NJson::TJsonValue>& properties) override {
        if (!node.Maybe<TDqSource>()) {
            return false;
        }

        auto source = node.Cast<TDqSource>();
        if (auto maybeSettingsBase = source.Settings().Maybe<TS3SourceSettingsBase>()) {
            const auto settingsBase = maybeSettingsBase.Cast();
            auto path = settingsBase.Path().StringValue();
            properties["Path"] = path;
            if (auto limit = settingsBase.RowsLimitHint().StringValue()) {
                properties["RowsLimitHint"] = limit;
            }

            auto s3DataSource = source.DataSource().Cast<TS3DataSource>();
            auto cluster = GetLastName(s3DataSource.Cluster().StringValue());
            TString name;
            if (s3DataSource.Name()) {
                name = GetLastName(s3DataSource.Name().Cast().StringValue());
            }
            if (!name) {
                name = cluster;
                if (path) {
                    name = name + '.' + path;
                }
            }

            properties["ExternalDataSource"] = cluster;
            properties["Name"] = name;


            if (source.Settings().Maybe<TS3SourceSettings>()) {
                properties["Format"] = "raw";
                return true;
            }

            if (auto maybeSettings = source.Settings().Maybe<TS3ParseSettings>()) {
                const TS3ParseSettings settings = maybeSettings.Cast();
                properties["Format"] = settings.Format().StringValue();
                if (const auto& compression = GetCompression(settings.Settings().Ref()); !compression.empty()) {
                    properties["Compression"] = TString{compression};
                }
                if (auto predicate = settings.FilterPredicate(); !IsEmptyFilterPredicate(predicate)) {
                    properties["Filter"] = NPlanUtils::PrettyExprStr(predicate);
                }
                const TStructExprType* fullRowType = settings.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
                auto rowTypeItems = fullRowType->GetItems();
                auto& columns = properties["ReadColumns"];
                for (auto& item : rowTypeItems) {
                    columns.AppendValue(item->GetName());
                }
                return true;
            }
        }
        return false;
    }

    bool FillSinkPlanProperties(const NNodes::TExprBase& node, TMap<TString, NJson::TJsonValue>& properties) override {
        if (!node.Maybe<TDqSink>()) {
            return false;
        }

        auto sink = node.Cast<TDqSink>();
        if (auto maybeS3SinkSettings = sink.Settings().Maybe<TS3SinkSettings>()) {
            auto s3SinkSettings = maybeS3SinkSettings.Cast();
            properties["Extension"] = s3SinkSettings.Extension().StringValue();
            if (auto settingsList = s3SinkSettings.Settings().Maybe<TExprList>()) {
                for (const TExprNode::TPtr& s : s3SinkSettings.Settings().Raw()->Children()) {
                    if (s->ChildrenSize() >= 2 && s->Child(0)->Content() == "compression"sv) {
                        auto val = s->Child(1)->Content();
                        if (val) {
                            properties["Compression"] = TString(val);
                        }
                    }
                }
            }
            return true;
        }
        return false;
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
