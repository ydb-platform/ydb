#include "yql_yt_native.h"

#include "yql_yt_lambda_builder.h"
#include "yql_yt_qb2.h"
#include "yql_yt_session.h"
#include "yql_yt_spec.h"
#include "yql_yt_transform.h"
#include "yql_yt_native_folders.h"

#include <ydb/library/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <ydb/library/yql/providers/yt/lib/config_clusters/config_clusters.h>
#include <ydb/library/yql/providers/yt/lib/log/yt_logger.h>

#include <ydb/library/yql/providers/yt/lib/mkql_helpers/mkql_helpers.h>
#include <ydb/library/yql/providers/yt/lib/res_pull/res_or_pull.h>
#include <ydb/library/yql/providers/yt/lib/res_pull/table_limiter.h>
#include <ydb/library/yql/providers/yt/lib/yson_helpers/yson_helpers.h>
#include <ydb/library/yql/providers/yt/lib/infer_schema/infer_schema.h>
#include <ydb/library/yql/providers/yt/lib/infer_schema/infer_schema_rpc.h>
#include <ydb/library/yql/providers/yt/lib/schema/schema.h>
#include <ydb/library/yql/providers/yt/lib/skiff/yql_skiff_schema.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/job/yql_job_base.h>
#include <ydb/library/yql/providers/yt/job/yql_job_calc.h>
#include <ydb/library/yql/providers/yt/job/yql_job_registry.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_helpers.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_mkql_compiler.h>

#include <ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_type_flags.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/result/expr_nodes/yql_res_expr_nodes.h>
#include <ydb/library/yql/providers/stat/expr_nodes/yql_stat_expr_nodes.h>
#include <ydb/library/yql/providers/stat/uploader/yql_stat_uploader.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/core/yql_type_helpers.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/containers/sorted_vector/sorted_vector.h>
#include <library/cpp/streams/brotli/brotli.h>

#include <util/folder/tempdir.h>
#include <util/generic/ptr.h>
#include <util/generic/yexception.h>
#include <util/generic/xrange.h>
#include <util/generic/size_literals.h>
#include <util/generic/scope.h>
#include <util/stream/null.h>
#include <util/stream/str.h>
#include <util/stream/input.h>
#include <util/stream/file.h>
#include <util/system/execpath.h>
#include <util/system/guard.h>
#include <util/system/shellcommand.h>
#include <util/ysaveload.h>

#include <algorithm>
#include <iterator>
#include <variant>
#include <exception>

namespace NYql {

using namespace NYT;
using namespace NCommon;
using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NNodes;
using namespace NThreading;

namespace NNative {

namespace {
    THashMap<TString, std::pair<NYT::TNode, TString>> TestTables;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

void DumpLocalTable(const TString& tableContent, const TString& path) {
    if (!path) {
        return;
    }

    TFileOutput out(path);
    out.Write(tableContent.Data(), tableContent.Size());
    out.Flush();
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

template<typename TOptions>
struct TMetaPerServerRequest {
    TVector<size_t> TableIndicies;
    typename TExecContext<TOptions>::TPtr ExecContext;
};

///////////////////////////////////////////////////////////////////////////////////////////////////////

namespace {

TString ToColumn(const NYT::TSortColumn& item) {
    return TStringBuilder() << item.Name() << '(' << (item.SortOrder() == NYT::ESortOrder::SO_ASCENDING) << ')';
}

TString ToColumn(const std::pair<TString, int>& item) {
    return TStringBuilder() << item.first << '(' << bool(item.second) << ')';
}

template <typename T>
TString ToColumnList(const TVector<T>& list) {
    TStringBuilder builder;
    builder << '[';
    for (auto& col: list) {
        builder << ToColumn(col) << ',';
    }
    if (!list.empty()) {
        builder.pop_back();
    }
    return (builder << ']');
}

const NYT::TJobBinaryConfig GetJobBinary(const NYT::TRawMapOperationSpec& spec) {
    return spec.MapperSpec_.GetJobBinary();
}

const NYT::TJobBinaryConfig GetJobBinary(const NYT::TRawMapReduceOperationSpec& spec) {
    return spec.MapperSpec_.GetJobBinary();
}

const NYT::TJobBinaryConfig GetJobBinary(const NYT::TRawReduceOperationSpec& spec) {
    return spec.ReducerSpec_.GetJobBinary();
}

const TVector<std::tuple<TLocalFilePath, TAddLocalFileOptions>> GetLocalFiles(const NYT::TRawMapOperationSpec& spec) {
    return spec.MapperSpec_.GetLocalFiles();
}

const TVector<std::tuple<TLocalFilePath, TAddLocalFileOptions>> GetLocalFiles(const NYT::TRawMapReduceOperationSpec& spec) {
    return spec.MapperSpec_.GetLocalFiles();
}

const TVector<std::tuple<TLocalFilePath, TAddLocalFileOptions>> GetLocalFiles(const NYT::TRawReduceOperationSpec& spec) {
    return spec.ReducerSpec_.GetLocalFiles();
}

const TVector<TRichYPath> GetRemoteFiles(const NYT::TRawMapOperationSpec& spec) {
    return spec.MapperSpec_.Files_;
}

const TVector<TRichYPath> GetRemoteFiles(const NYT::TRawMapReduceOperationSpec& spec) {
    return spec.MapperSpec_.Files_;
}

const TVector<TRichYPath> GetRemoteFiles(const NYT::TRawReduceOperationSpec& spec) {
    return spec.ReducerSpec_.Files_;
}

template <typename TType>
inline TType OptionFromString(const TString& value) {
    if constexpr (std::is_same_v<TString, TType>) {
        return value;
    } else if constexpr (std::is_same_v<NYT::TNode, TType>) {
        return NYT::NodeFromYsonString(value);
    } else {
        return FromString<TType>(value);
    }
}

template <typename TType>
inline TType OptionFromNode(const NYT::TNode& value) {
    if constexpr (std::is_same_v<NYT::TNode, TType>) {
        return value;
    } else if constexpr (std::is_integral_v<TType>) {
        return static_cast<TType>(value.AsInt64());
    } else {
        return FromString<TType>(value.AsString());
    }
}

} // unnamed

///////////////////////////////////////////////////////////////////////////////////////////////////////

class TYtNativeGateway : public IYtGateway {
public:
    TYtNativeGateway(const TYtNativeServices& services)
        : Services_(services)
        , Clusters_(MakeIntrusive<TConfigClusters>(*Services_.Config))
        , MkqlCompiler_(MakeIntrusive<NCommon::TMkqlCommonCallableCompiler>())
        , UrlMapper_(*Services_.Config)
    {
        RegisterYtMkqlCompilers(*MkqlCompiler_);
        SetYtLoggerGlobalBackend(
            Services_.Config->HasYtLogLevel() ? Services_.Config->GetYtLogLevel() : -1,
            Services_.Config->GetYtDebugLogSize(),
            Services_.Config->GetYtDebugLogFile(),
            Services_.Config->GetYtDebugLogAlwaysWrite()
        );
    }

    ~TYtNativeGateway() {
    }

    void OpenSession(TOpenSessionOptions&& options) final {
        TString sessionId = options.SessionId();
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        TSession::TPtr session = MakeIntrusive<TSession>(std::move(options), Services_.Config->GetGatewayThreads());
        with_lock(Mutex_) {
            if (!Sessions_.insert({sessionId, session}).second) {
                YQL_LOG_CTX_THROW yexception() << "Session already exists: " << sessionId;
            }
        }
    }

    TFuture<void> CloseSession(TCloseSessionOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        with_lock(Mutex_) {
            auto it = Sessions_.find(options.SessionId());
            if (it != Sessions_.end()) {
                auto session = it->second;
                Sessions_.erase(it);
                try {
                    session->Close();
                } catch (...) {
                    YQL_CLOG(ERROR, ProviderYt) << CurrentExceptionMessage();
                    return MakeErrorFuture<void>(std::current_exception());
                }
            }
        }

        return MakeFuture();
    }

    TFuture<void> CleanupSession(TCleanupSessionOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        if (auto session = GetSession(options.SessionId(), false)) {
            if (session->OperationSemaphore) {
                session->OperationSemaphore->Cancel();
                session->OperationSemaphore.Drop();
            }
            auto logCtx = NYql::NLog::CurrentLogContextPath();
            return session->Queue_->Async([session, logCtx] {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                try {
                    session->TxCache_.AbortAll();
                } catch (...) {
                    YQL_CLOG(ERROR, ProviderYt) << CurrentExceptionMessage();
                    return MakeErrorFuture<void>(std::current_exception());
                }

                return MakeFuture();
            });
        }

        return MakeFuture();
    }

    TFuture<TFinalizeResult> Finalize(TFinalizeOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        try {
            TSession::TPtr session = GetSession(options.SessionId());
            auto logCtx = NYql::NLog::CurrentLogContextPath();
            return session->Queue_->Async([session, logCtx, abort=options.Abort(), detachSnapshotTxs=options.DetachSnapshotTxs()] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                return ExecFinalize(session, abort, detachSnapshotTxs);
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TFinalizeResult>());
        }
    }

    TFuture<TCanonizePathsResult> CanonizePaths(TCanonizePathsOptions&& options) final {
        if (Services_.Config->GetLocalChainTest()) {
            TCanonizePathsResult res;
            std::transform(
                options.Paths().begin(), options.Paths().end(),
                std::back_inserter(res.Data),
                [] (const TCanonizeReq& req) {
                    return CanonizedPath(req.Path());
                });
            res.SetSuccess();
            return MakeFuture(res);
        }

        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        try {
            TSession::TPtr session = GetSession(options.SessionId());

            TVector<TCanonizeReq> paths(std::move(options.Paths()));
            if (YQL_CLOG_ACTIVE(INFO, ProviderYt)) {
                for (size_t i: xrange(Min<size_t>(paths.size(), 10))) {
                    YQL_CLOG(INFO, ProviderYt) << paths[i].Cluster() << '.' << paths[i].Path();
                }
                if (paths.size() > 10) {
                    YQL_CLOG(INFO, ProviderYt) << "...total tables=" << paths.size();
                }
            }

            THashMap<TString, TMetaPerServerRequest<TCanonizePathsOptions>> reqPerServer;
            for (size_t i: xrange(paths.size())) {
                TCanonizeReq& path = paths[i];
                auto cluster = path.Cluster();
                auto ytServer = Clusters_->GetServer(cluster);
                auto& r = reqPerServer[ytServer];
                if (r.TableIndicies.empty()) {
                    r.ExecContext = MakeExecCtx(TCanonizePathsOptions(options), session, cluster, nullptr, nullptr);
                }

                // Use absolute path to prevent adding YT_PREFFIX
                if (!path.Path().StartsWith("//") && !path.Path().StartsWith("<")) {
                    path.Path(TString("//").append(path.Path()));
                }

                r.TableIndicies.push_back(i);
            }

            auto logCtx = NYql::NLog::CurrentLogContextPath();
            return session->Queue_->Async([session, paths = std::move(paths), reqPerServer = std::move(reqPerServer), logCtx] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                return ExecCanonizePaths(paths, reqPerServer);
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TCanonizePathsResult>());
        }
    }

    TFuture<TTableInfoResult> GetTableInfo(TGetTableInfoOptions&& options) final {
        if (Services_.Config->GetLocalChainTest()) {
            TTableInfoResult result;
            for (const auto& t : options.Tables()) {
                const auto it = TestTables.find(t.Table());
                result.Data.emplace_back();
                auto& table = result.Data.back();
                table.Meta = MakeIntrusive<TYtTableMetaInfo>();
                if (table.Meta->DoesExist = TestTables.cend() != it) {
                    table.Meta->Attrs.emplace(YqlRowSpecAttribute, NYT::NodeToYsonString(it->second.first));
                    table.Stat = MakeIntrusive<TYtTableStatInfo>();
                    table.Stat->Id = "stub";
                    // Prevent empty table optimizations
                    table.Stat->RecordsCount = 1;
                    table.Stat->DataSize = 1;
                    table.Stat->ChunkCount = 1;
                }

                table.WriteLock = HasModifyIntents(t.Intents());
            }
            result.SetSuccess();
            return MakeFuture<TTableInfoResult>(std::move(result));
        }
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        try {
            TSession::TPtr session = GetSession(options.SessionId());
            session->EnsureInitializedSemaphore(options.Config());

            TString tmpFolder = GetTablesTmpFolder(*options.Config());

            YQL_CLOG(INFO, ProviderYt) << "ReadOnly=" << options.ReadOnly() << ", Epoch=" << options.Epoch();
            TVector<TTableReq> tables(std::move(options.Tables()));
            if (YQL_CLOG_ACTIVE(INFO, ProviderYt)) {
                for (size_t i: xrange(Min<size_t>(tables.size(), 10))) {
                    YQL_CLOG(INFO, ProviderYt) << tables[i].Cluster() << '.' << tables[i].Table()
                        << ", LockOnly=" << tables[i].LockOnly() << ", Intents=" << tables[i].Intents();
                }
                if (tables.size() > 10) {
                    YQL_CLOG(INFO, ProviderYt) << "...total tables=" << tables.size();
                }
            }

            THashMap<TString, TMetaPerServerRequest<TGetTableInfoOptions>> reqPerServer;
            for (size_t i: xrange(tables.size())) {
                TTableReq& table = tables[i];
                auto cluster = table.Cluster();
                auto ytServer = Clusters_->GetServer(cluster);
                auto& r = reqPerServer[ytServer];
                if (r.TableIndicies.empty()) {
                    r.ExecContext = MakeExecCtx(TGetTableInfoOptions(options), session, cluster, nullptr, nullptr);
                }
                table.Table(NYql::TransformPath(tmpFolder, table.Table(), table.Anonymous(), session->UserName_));
                r.TableIndicies.push_back(i);
            }

            auto logCtx = NYql::NLog::CurrentLogContextPath();
            bool readOnly = options.ReadOnly();
            ui32 epoch = options.Epoch();
            return session->Queue_->Async([session, tables = std::move(tables), reqPerServer = std::move(reqPerServer), readOnly, epoch, logCtx] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                return ExecGetTableInfo(tables, reqPerServer, readOnly, epoch);
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TTableInfoResult>());
        }
    }

    TFuture<TTableRangeResult> GetTableRange(TTableRangeOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        YQL_CLOG(INFO, ProviderYt) << "Server=" << options.Cluster() << ", Prefix=" << options.Prefix()
            << ", Suffix=" << options.Suffix() << ", Filter=" << (options.Filter() ? "(present)" : "null");

        auto pos = options.Pos();

        try {
            TSession::TPtr session = GetSession(options.SessionId());
            session->EnsureInitializedSemaphore(options.Config());

            TString tmpFolder = GetTablesTmpFolder(*options.Config());
            TString tmpTablePath = NYql::TransformPath(tmpFolder,
                TStringBuilder() << "tmp/" << GetGuidAsString(session->RandomProvider_->GenGuid()), true, session->UserName_);

            auto cluster = options.Cluster();
            auto filter = options.Filter();
            auto exprCtx = options.ExprCtx();

            TExpressionResorceUsage extraUsage;
            TString lambda;
            if (filter) {
                YQL_ENSURE(exprCtx);
                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                    Services_.FunctionRegistry->SupportsSizedAllocators());
                alloc.SetLimit(options.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                NKikimr::NMiniKQL::TTypeEnvironment typeEnv(alloc);
                TProgramBuilder pgmBuilder(typeEnv, *Services_.FunctionRegistry);

                auto returnType = pgmBuilder.NewListType(pgmBuilder.NewTupleType({
                    pgmBuilder.NewDataType(NUdf::TDataType<char*>::Id),
                    pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id)
                }));
                TCallableBuilder inputNodeBuilder(typeEnv, MrRangeInputListInternal, returnType);
                auto inputNode = TRuntimeNode(inputNodeBuilder.Build(), false);
                auto pgm = pgmBuilder.Filter(inputNode, [&](TRuntimeNode item) {
                    TMkqlBuildContext ctx(*MkqlCompiler_, pgmBuilder, *exprCtx, filter->UniqueId(), {{&filter->Head().Head(), pgmBuilder.Nth(item, 0)}});
                    return pgmBuilder.Coalesce(MkqlBuildExpr(filter->Tail(), ctx), pgmBuilder.NewDataLiteral(false));
                });
                lambda = SerializeRuntimeNode(pgm, typeEnv);
                extraUsage = ScanExtraResourceUsage(filter->Tail(), *options.Config());
            }

            auto execCtx = MakeExecCtx(std::move(options), session, cluster, filter, exprCtx);
            if (lambda) {
                return session->Queue_->Async([execCtx, tmpTablePath, lambda, extraUsage] () {
                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                    execCtx->MakeUserFiles();
                    return ExecGetTableRange(execCtx, tmpTablePath, lambda, extraUsage);
                });
            }

            return session->Queue_->Async([execCtx] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                return ExecGetTableRange(execCtx);
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TTableRangeResult>(pos));
        }
    }

    TFuture<TFolderResult> GetFolder(TFolderOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        YQL_CLOG(INFO, ProviderYt) << "Server=" << options.Cluster() << ", Prefix=" << options.Prefix();

        try {
            TSession::TPtr session = GetSession(options.SessionId());

            auto batchOptions = TBatchFolderOptions(options.SessionId())
                .Cluster(options.Cluster())
                .Pos(options.Pos())
                .Config(options.Config())
                .Folders({{options.Prefix(), options.Attributes()}});
            auto execCtx = MakeExecCtx(std::move(batchOptions), session, options.Cluster(), nullptr, nullptr);

            if (auto filePtr = MaybeGetFilePtrFromCache(execCtx->GetOrCreateEntry(), execCtx->Options_.Folders().front())) {
                TFolderResult res;
                res.SetSuccess();
                res.ItemsOrFileLink = *filePtr;
                return MakeFuture(res);
            }

            auto getFolderFuture = session->Queue_->Async([execCtx] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                return ExecGetFolder(execCtx);
            });
            auto resolvedLinksFuture = getFolderFuture.Apply([options, this, session] (const TFuture<TBatchFolderResult>& f) {
                TVector<IYtGateway::TResolveOptions::TItemWithReqAttrs> resolveItems;
                auto res = f.GetValue();

                if (!res.Success()) {
                    YQL_CLOG(INFO, ProviderYt) << "Skipping resolve for unsuccessful batch folder list call";
                    return res;
                }
                if (res.Items.empty()) {
                    YQL_CLOG(INFO, ProviderYt) << "Skipping resolve for empty batch folder result";
                    return res;
                }

                for (auto&& item : res.Items) {
                    YQL_CLOG(DEBUG, ProviderYt) << "Adding to batch get command: " << item.Path;
                    IYtGateway::TResolveOptions::TItemWithReqAttrs resolveItem {
                        .Item = item,
                        .AttrKeys = options.Attributes()
                    };
                    resolveItems.push_back(std::move(resolveItem));
                }

                auto resolveOptions = TResolveOptions(options.SessionId())
                    .Cluster(options.Cluster())
                    .Pos(options.Pos())
                    .Config(options.Config())
                    .Items(resolveItems);
                auto execCtx = MakeExecCtx(std::move(resolveOptions), session, options.Cluster(), nullptr, nullptr);
                return ExecResolveLinks(execCtx);
            });

            return resolvedLinksFuture.Apply([execCtx] (const TFuture<TBatchFolderResult>& f) {
                const ui32 countLimit = execCtx->Options_.Config()->FolderInlineItemsLimit.Get().GetOrElse(100);
                const ui64 sizeLimit = execCtx->Options_.Config()->FolderInlineDataLimit.Get().GetOrElse(100_KB);

                auto resolveRes = f.GetValue();

                TFolderResult res;
                res.AddIssues(resolveRes.Issues());

                if (!resolveRes.Success()) {
                    res.SetStatus(resolveRes.Status());
                    return res;
                }
                res.SetSuccess();

                YQL_CLOG(INFO, ProviderYt) << "Batch get command got: " << resolveRes.Items.size() << " items";

                TVector<TFolderResult::TFolderItem> items;
                for (auto& batchItem : resolveRes.Items) {
                    TFolderResult::TFolderItem item {
                        .Path = std::move(batchItem.Path),
                        .Type = std::move(batchItem.Type),
                        .Attributes = NYT::NodeToYsonString(batchItem.Attributes)
                    };
                    items.emplace_back(std::move(item));
                }
                if (items.size() > countLimit) {
                    res.ItemsOrFileLink = SaveItemsToTempFile(execCtx, items);
                    return res;
                }
                ui64 total_size = std::accumulate(items.begin(), items.end(), 0, [] (ui64 size, const TFolderResult::TFolderItem& i) {
                    return size + i.Type.length() + i.Path.length() + i.Attributes.length();
                });
                if (total_size > sizeLimit) {
                    res.ItemsOrFileLink = SaveItemsToTempFile(execCtx, items);
                    return res;
                }
                res.ItemsOrFileLink = std::move(items);
                return res;
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TFolderResult>(options.Pos()));
        }
    }

    TFuture<TBatchFolderResult> GetFolders(TBatchFolderOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        YQL_CLOG(INFO, ProviderYt) << "Server=" << options.Cluster();

        auto pos = options.Pos();
        try {
            TSession::TPtr session = GetSession(options.SessionId());

            auto cluster = options.Cluster();
            auto execCtx = MakeExecCtx(std::move(options), session, cluster, nullptr, nullptr);

            return session->Queue_->Async([execCtx] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                return ExecGetFolder(execCtx);
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TBatchFolderResult>(pos));
        }
    }


    TFuture<TBatchFolderResult> ResolveLinks(TResolveOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        YQL_CLOG(INFO, ProviderYt) << "Server=" << options.Cluster();

        auto pos = options.Pos();
        try {
            TSession::TPtr session = GetSession(options.SessionId());

            auto cluster = options.Cluster();
            auto execCtx = MakeExecCtx(std::move(options), session, cluster, nullptr, nullptr);

            return session->Queue_->Async([execCtx] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                return ExecResolveLinks(execCtx);
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TBatchFolderResult>(pos));
        }
    }

    TFuture<TResOrPullResult> ResOrPull(const TExprNode::TPtr& node, TExprContext& ctx, TResOrPullOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        try {
            YQL_CLOG(INFO, ProviderYt) << node->Content();
            TSession::TPtr session = GetSession(options.SessionId());
            session->EnsureInitializedSemaphore(options.Config());
            if (auto pull = TMaybeNode<TPull>(node)) {
                return DoPull(session, pull.Cast(), ctx, std::move(options));
            } else if (auto result = TMaybeNode<TResult>(node)) {
                return DoResult(session, result.Cast(), ctx, std::move(options));
            } else {
                YQL_LOG_CTX_THROW yexception() << "Don't know how to execute " << node->Content();
            }
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TResOrPullResult>());
        }
    }

    TFuture<TRunResult> Run(const TExprNode::TPtr& node, TExprContext& ctx, TRunOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        auto nodePos = ctx.GetPosition(node->Pos());
        try {
            YQL_CLOG(INFO, ProviderYt) << node->Content();
            TSession::TPtr session = GetSession(options.SessionId());
            session->EnsureInitializedSemaphore(options.Config());

            TYtOpBase opBase(node);

            auto cluster = TString{opBase.DataSink().Cluster().Value()};

            auto execCtx = MakeExecCtx(std::move(options), session, cluster, opBase.Raw(), &ctx);

            if (auto transientOp = opBase.Maybe<TYtTransientOpBase>()) {
                THashSet<TString> extraSysColumns;
                if (NYql::HasSetting(transientOp.Settings().Ref(), EYtSettingType::KeySwitch)
                    && !transientOp.Maybe<TYtMapReduce>().Mapper().Maybe<TCoLambda>().IsValid()) {
                    extraSysColumns.insert("keyswitch");
                }
                execCtx->SetInput(transientOp.Cast().Input(), opBase.Maybe<TYtWithUserJobsOpBase>().IsValid(), extraSysColumns);
            }
            if (auto outputOp = opBase.Maybe<TYtOutputOpBase>()) {
                execCtx->SetOutput(outputOp.Cast().Output());
            }

            TFuture<void> future;
            if (auto op = opBase.Maybe<TYtSort>()) {
                future = DoSort(op.Cast(), execCtx);
            } else if (auto op = opBase.Maybe<TYtCopy>()) {
                future = DoCopy(op.Cast(), execCtx);
            } else if (auto op = opBase.Maybe<TYtMerge>()) {
                future = DoMerge(op.Cast(), execCtx);
            } else if (auto op = opBase.Maybe<TYtMap>()) {
                future = DoMap(op.Cast(), execCtx, ctx);
            } else if (auto op = opBase.Maybe<TYtReduce>()) {
                future = DoReduce(op.Cast(), execCtx, ctx);
            } else if (auto op = opBase.Maybe<TYtMapReduce>()) {
                future = DoMapReduce(op.Cast(), execCtx, ctx);
            } else if (auto op = opBase.Maybe<TYtFill>()) {
                future = DoFill(op.Cast(), execCtx, ctx);
            } else if (auto op = opBase.Maybe<TYtTouch>()) {
                future = DoTouch(op.Cast(), execCtx);
            } else if (auto op = opBase.Maybe<TYtDropTable>()) {
                future = DoDrop(op.Cast(), execCtx);
            } else if (auto op = opBase.Maybe<TYtStatOut>()) {
                future = DoStatOut(op.Cast(), execCtx);
            } else if (auto op = opBase.Maybe<TYtDqProcessWrite>()) {
                future = DoTouch(op.Cast(), execCtx); // Do touch just for creating temporary tables.
            } else {
                ythrow yexception() << "Don't know how to execute " << node->Content();
            }

            if (Services_.Config->GetLocalChainTest()) {
                return future.Apply([execCtx](const TFuture<void>&) {
                    TRunResult result;
                    result.OutTableStats.reserve(execCtx->OutTables_.size());
                    for (const auto& table : execCtx->OutTables_) {
                        result.OutTableStats.emplace_back(table.Name, MakeIntrusive<TYtTableStatInfo>());
                        result.OutTableStats.back().second->Id = "stub";
                    }
                    result.SetSuccess();
                    return MakeFuture<TRunResult>(std::move(result));
                });
            }

            return future.Apply([execCtx, pos = nodePos](const TFuture<void>& f) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                try {
                    f.GetValue(); // rethrow error if any
                    execCtx->StoreQueryCache();
                    execCtx->SetNodeExecProgress("Fetching attributes of output tables");
                    return MakeRunResult(execCtx->OutTables_, execCtx->GetEntry());
                } catch (...) {
                    return ResultFromCurrentException<TRunResult>(pos);
                }
            });

        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TRunResult>(nodePos));
        }
    }

    TFuture<TCalcResult> Calc(const TExprNode::TListType& nodes, TExprContext& ctx, TCalcOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        try {
            TSession::TPtr session = GetSession(options.SessionId());
            session->EnsureInitializedSemaphore(options.Config());
            auto cluster = options.Cluster();

            if (YQL_CLOG_ACTIVE(INFO, ProviderYt)) {
                for (size_t i: xrange(Min<size_t>(nodes.size(), 10))) {
                    YQL_CLOG(INFO, ProviderYt) << "Cluster=" << cluster << ": " << nodes[i]->Content();
                }
                if (nodes.size() > 10) {
                    YQL_CLOG(INFO, ProviderYt) << "...total nodes to calc=" << nodes.size();
                }
            }

            TExpressionResorceUsage extraUsage;
            TString lambda;
            {
                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                    Services_.FunctionRegistry->SupportsSizedAllocators());
                alloc.SetLimit(options.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                TNativeYtLambdaBuilder builder(alloc, Services_, *session);
                TVector<TRuntimeNode> tupleNodes;
                for (auto& node: nodes) {
                    tupleNodes.push_back(builder.BuildLambda(*MkqlCompiler_, node, ctx));
                    auto nodeUsage = ScanExtraResourceUsage(*node, *options.Config());
                    extraUsage.Cpu = Max<double>(extraUsage.Cpu, nodeUsage.Cpu);
                    extraUsage.Memory = Max<ui64>(extraUsage.Memory, nodeUsage.Memory);
                }
                lambda = SerializeRuntimeNode(builder.MakeTuple(tupleNodes), builder.GetTypeEnvironment());
            }

            TString tmpFolder = GetTablesTmpFolder(*options.Config());
            TString tmpTablePath = NYql::TransformPath(tmpFolder,
                TStringBuilder() << "tmp/" << GetGuidAsString(session->RandomProvider_->GenGuid()), true, session->UserName_);

            auto execCtx = MakeExecCtx(std::move(options), session, cluster, nullptr, nullptr);

            return session->Queue_->Async([execCtx, lambda, extraUsage, tmpTablePath] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                execCtx->MakeUserFiles();
                return ExecCalc(lambda, extraUsage, tmpTablePath, execCtx, {}, TNodeResultFactory());
            })
            .Apply([execCtx](const TFuture<NYT::TNode>& f) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                try {
                    const NYT::TNode& node = f.GetValue();
                    YQL_ENSURE(node.IsList());
                    TCalcResult res;
                    for (auto& n: node.AsList()) {
                        res.Data.push_back(n);
                    }
                    res.SetSuccess();
                    return res;
                } catch (...) {
                    return ResultFromCurrentException<TCalcResult>();
                }
            });

        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TCalcResult>());
        }
    }

    TFuture<TPublishResult> Publish(const TExprNode::TPtr& node, TExprContext& ctx, TPublishOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        auto nodePos = ctx.GetPosition(node->Pos());
        try {
            auto publish = TYtPublish(node);
            EYtWriteMode mode = EYtWriteMode::Renew;
            if (const auto modeSetting = NYql::GetSetting(publish.Settings().Ref(), EYtSettingType::Mode)) {
                mode = FromString<EYtWriteMode>(modeSetting->Child(1)->Content());
            }
            const bool initial = NYql::HasSetting(publish.Settings().Ref(), EYtSettingType::Initial);

            std::unordered_map<EYtSettingType, TString> strOpts;
            for (const auto& setting : publish.Settings().Ref().Children()) {
                if (setting->ChildrenSize() == 2) {
                    strOpts.emplace(FromString<EYtSettingType>(setting->Head().Content()), setting->Tail().Content());
                } else if (setting->ChildrenSize() == 1) {
                    strOpts.emplace(FromString<EYtSettingType>(setting->Head().Content()), TString());;
                }
            }

            YQL_CLOG(INFO, ProviderYt) << "Mode: " << mode << ", IsInitial: " << initial;

            TSession::TPtr session = GetSession(options.SessionId());
            session->EnsureInitializedSemaphore(options.Config());

            auto cluster = publish.DataSink().Cluster().StringValue();

            TVector<TString> src;
            ui64 chunksCount = 0;
            ui64 dataSize = 0;
            std::unordered_set<TString> columnGroups;
            for (auto out: publish.Input()) {
                auto outTable = GetOutTable(out).Cast<TYtOutTable>();
                src.emplace_back(outTable.Name().Value());
                if (auto columnGroupSetting = NYql::GetSetting(outTable.Settings().Ref(), EYtSettingType::ColumnGroups)) {
                    columnGroups.emplace(columnGroupSetting->Tail().Content());
                } else {
                    columnGroups.emplace();
                }
                auto stat = TYtTableStatInfo(outTable.Stat());
                chunksCount += stat.ChunkCount;
                dataSize += stat.DataSize;
                if (src.size() <= 10) {
                    YQL_CLOG(INFO, ProviderYt) << "Input: " << cluster << '.' << src.back();
                }
            }
            if (src.size() > 10) {
                YQL_CLOG(INFO, ProviderYt) << "...total input tables=" << src.size();
            }
            TString srcColumnGroups = columnGroups.size() == 1 ? *columnGroups.cbegin() : TString();

            bool combineChunks = false;
            if (auto minChunkSize = options.Config()->MinPublishedAvgChunkSize.Get()) {
                combineChunks = *minChunkSize == 0
                    || (chunksCount > 1 && dataSize > chunksCount && (dataSize / chunksCount) < minChunkSize->GetValue());
            }

            const auto dst = publish.Publish().Name().StringValue();
            YQL_CLOG(INFO, ProviderYt) << "Output: " << cluster << '.' << dst;
            if (combineChunks) {
                YQL_CLOG(INFO, ProviderYt) << "Use chunks combining";
            }
            if (Services_.Config->GetLocalChainTest()) {
                if (!src.empty()) {
                    const auto& path = NYql::TransformPath(GetTablesTmpFolder(*options.Config()), src.front(), true, session->UserName_);
                    const auto it = TestTables.find(path);
                    YQL_ENSURE(TestTables.cend() != it);
                    YQL_ENSURE(TestTables.emplace(dst, it->second).second);
                }

                TPublishResult result;
                result.SetSuccess();
                return MakeFuture<TPublishResult>(std::move(result));
            }


            bool isAnonymous = NYql::HasSetting(publish.Publish().Settings().Ref(), EYtSettingType::Anonymous);
            const ui32 dstEpoch = TEpochInfo::Parse(publish.Publish().Epoch().Ref()).GetOrElse(0);
            auto execCtx = MakeExecCtx(std::move(options), session, cluster, node.Get(), &ctx);

            return session->Queue_->Async([execCtx, src = std::move(src), dst, dstEpoch, isAnonymous, mode, initial, srcColumnGroups, combineChunks, strOpts = std::move(strOpts)] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                return ExecPublish(execCtx, src, dst, dstEpoch, isAnonymous, mode, initial, srcColumnGroups, combineChunks, strOpts);
            })
            .Apply([nodePos] (const TFuture<void>& f) {
                try {
                    f.GetValue();
                    TPublishResult res;
                    res.SetSuccess();
                    return res;
                } catch (...) {
                    return ResultFromCurrentException<TPublishResult>(nodePos);
                }
            });

        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TPublishResult>(nodePos));
        }
    }

    TFuture<TCommitResult> Commit(TCommitOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        try {
            TSession::TPtr session = GetSession(options.SessionId());
            auto cluster = options.Cluster();

            auto execCtx = MakeExecCtx(std::move(options), session, cluster, nullptr, nullptr);
            return session->Queue_->Async([execCtx] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                try {
                    execCtx->Session_->TxCache_.Commit(execCtx->YtServer_);

                    TCommitResult res;
                    res.SetSuccess();
                    return res;
                } catch (...) {
                    return ResultFromCurrentException<TCommitResult>();
                }
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TCommitResult>());
        }
    }

    TFuture<TDropTrackablesResult> DropTrackables(TDropTrackablesOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        try {
            TSession::TPtr session = GetSession(options.SessionId());

            if (YQL_CLOG_ACTIVE(INFO, ProviderYt)) {
                for (size_t i: xrange(Min<size_t>(options.Pathes().size(), 10))) {
                    const auto& path = options.Pathes()[i].Path;
                    const auto& cluster = options.Pathes()[i].Cluster;
                    YQL_CLOG(INFO, ProviderYt) << "Dropping temporary table '" << path << "' on cluster '" << cluster << "'";
                }
                if (options.Pathes().size() > 10) {
                    YQL_CLOG(INFO, ProviderYt) << "...total dropping tables=" << options.Pathes().size();
                }
            }

            THashMap<TString, TVector<TString>> pathsByCluster;
            for (const auto& i : options.Pathes()) {
                pathsByCluster[i.Cluster].push_back(i.Path);
            }

            TVector<TFuture<void>> futures;
            for (const auto& i : pathsByCluster) {
                auto cluster = i.first;
                auto paths = i.second;


                auto execCtx = MakeExecCtx(TDropTrackablesOptions(options), session, cluster, nullptr, nullptr);

                futures.push_back(session->Queue_->Async([execCtx, paths] () {
                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                    return ExecDropTrackables(paths, execCtx);
                }));
            }

            return WaitExceptionOrAll(futures).Apply([] (const TFuture<void>& f) {
                try {
                    f.GetValue(); // rethrow error if any

                    TDropTrackablesResult res;
                    res.SetSuccess();
                    return res;
                } catch (...) {
                    return ResultFromCurrentException<TDropTrackablesResult>();
                }
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TDropTrackablesResult>());
        }
    }

    TFuture<TPathStatResult> PathStat(TPathStatOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        try {
            TSession::TPtr session = GetSession(options.SessionId());
            auto logCtx = NYql::NLog::CurrentLogContextPath();
            auto cluster = options.Cluster();

            if (YQL_CLOG_ACTIVE(INFO, ProviderYt)) {
                for (size_t i: xrange(Min<size_t>(options.Paths().size(), 10))) {
                    YQL_CLOG(INFO, ProviderYt) << "Cluster: " << cluster << ", table: " << NYT::NodeToYsonString(NYT::PathToNode(options.Paths()[i].Path()));
                }
                if (options.Paths().size() > 10) {
                    YQL_CLOG(INFO, ProviderYt) << "...total tables=" << options.Paths().size();
                }
            }

            auto execCtx = MakeExecCtx(std::move(options), session, cluster, nullptr, nullptr);

            return session->Queue_->Async([execCtx, logCtx] () {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                bool onlyCached = false;
                return ExecPathStat(execCtx, onlyCached);
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TPathStatResult>());
        }
    }

    TPathStatResult TryPathStat(TPathStatOptions&& options) final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);

        TSession::TPtr session = GetSession(options.SessionId());
        auto logCtx = NYql::NLog::CurrentLogContextPath();
        auto cluster = options.Cluster();

        auto execCtx = MakeExecCtx(std::move(options), session, cluster, nullptr, nullptr);

        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
        bool onlyCached = true;
        return ExecPathStat(execCtx, onlyCached);
    }

    bool TryParseYtUrl(const TString& url, TString* cluster, TString* path) const final {
        TString server;
        if (!UrlMapper_.MapYtUrl(url, &server, path)) {
            return false;
        }

        if (cluster) {
            *cluster = Clusters_->GetNameByYtName(server);
        }
        return true;
    }

    TString GetDefaultClusterName() const final {
        return Clusters_->GetDefaultClusterName();
    }

    TString GetClusterServer(const TString& cluster) const final {
        return Clusters_->TryGetServer(cluster);
    }

    NYT::TRichYPath GetRealTable(const TString& sessionId, const TString& cluster, const TString& table, ui32 epoch, const TString& tmpFolder) const final {
        auto richYPath = NYT::TRichYPath(table);
        if (TSession::TPtr session = GetSession(sessionId, true)) {
            if (auto ytServer = Clusters_->TryGetServer(cluster)) {
                auto entry = session->TxCache_.GetEntry(ytServer);
                if (auto p = entry->Snapshots.FindPtr(std::make_pair(table, epoch))) {
                    richYPath.Path(std::get<0>(*p)).TransactionId(std::get<1>(*p)).OriginalPath(NYT::AddPathPrefix(table, NYT::TConfig::Get()->Prefix));
                } else {
                    auto realTableName = NYql::TransformPath(tmpFolder, table, true, session->UserName_);
                    realTableName = NYT::AddPathPrefix(realTableName, NYT::TConfig::Get()->Prefix);
                    richYPath = NYT::TRichYPath(realTableName);
                    richYPath.TransactionId(session->TxCache_.GetEntry(ytServer)->Tx->GetId());
                }
            }
        }
        YQL_CLOG(DEBUG, ProviderYt) << "Real table path: " << NYT::NodeToYsonString(NYT::PathToNode(richYPath), NYT::NYson::EYsonFormat::Text);
        return richYPath;
    }

    NYT::TRichYPath GetWriteTable(const TString& sessionId, const TString& cluster, const TString& table, const TString& tmpFolder) const final {
        if (TSession::TPtr session = GetSession(sessionId, true)) {
            if (auto ytServer = Clusters_->TryGetServer(cluster)) {
                auto entry = session->TxCache_.GetEntry(ytServer);
                auto realTableName = NYql::TransformPath(tmpFolder, table, true, session->UserName_);
                realTableName = NYT::AddPathPrefix(realTableName, NYT::TConfig::Get()->Prefix);
                auto richYPath = NYT::TRichYPath(realTableName);
                richYPath.TransactionId(entry->Tx->GetId());
                YQL_CLOG(DEBUG, ProviderYt) << "Write table path: " << NYT::NodeToYsonString(NYT::PathToNode(richYPath), NYT::NYson::EYsonFormat::Text);
                return richYPath;
            }
        }
        YQL_CLOG(DEBUG, ProviderYt) << "(Alternative) Write table path: " << NYT::NodeToYsonString(NYT::PathToNode(NYT::TRichYPath(table)), NYT::NYson::EYsonFormat::Text);
        return NYT::TRichYPath(table);
    }

    TFuture<TRunResult> Prepare(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) const final {
        YQL_LOG_CTX_SCOPE(TStringBuf("Gateway"), __FUNCTION__);
        auto nodePos = ctx.GetPosition(node->Pos());
        try {
            YQL_CLOG(INFO, ProviderYt) << node->Content();
            const auto session = GetSession(options.SessionId());
            session->EnsureInitializedSemaphore(options.Config());

            TYtOutputOpBase opBase(node);

            const auto cluster = TString{opBase.DataSink().Cluster().Value()};
            const auto execCtx = MakeExecCtx(std::move(options), session, cluster, opBase.Raw(), &ctx);
            execCtx->SetOutput(opBase.Output());

            auto future = DoPrepare(opBase, execCtx);

            return future.Apply([execCtx, pos = nodePos](const TFuture<bool>& f) {
                try {
                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                    if (f.GetValue()) {
                        return MakeRunResult(execCtx->OutTables_, execCtx->GetEntry());
                    } else {
                        TRunResult res;
                        res.SetSuccess();
                        std::transform(execCtx->OutTables_.cbegin(), execCtx->OutTables_.cend(), std::back_inserter(res.OutTableStats),
                            [](const TOutputInfo& info) -> std::pair<TString, TYtTableStatInfo::TPtr> { return { info.Name, nullptr }; });
                        return res;
                    }
                } catch (...) {
                    return ResultFromCurrentException<TRunResult>(pos);
                }
            });
        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TRunResult>(nodePos));
        }
    }

    NThreading::TFuture<TRunResult> GetTableStat(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) final {
        if (TSession::TPtr session = GetSession(options.SessionId(), false)) {
            const TYtOutputOpBase opBase(node);
            if (const auto cluster = TString{opBase.DataSink().Cluster().Value()}; auto ytServer = Clusters_->TryGetServer(cluster)) {
                auto entry = session->TxCache_.GetEntry(ytServer);
                auto execCtx = MakeExecCtx(std::move(options), session, cluster, opBase.Raw(), &ctx);
                execCtx->SetOutput(opBase.Output());
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "TODO: Support multi out.");
                const auto tableName = execCtx->OutTables_.front().Name;
                const auto tmpFolder = GetTablesTmpFolder(*execCtx->Options_.Config());
                const auto realTableName = NYT::AddPathPrefix(NYql::TransformPath(tmpFolder, execCtx->OutTables_.front().Name, true, session->UserName_), NYT::TConfig::Get()->Prefix);
                auto batchGet = entry->Tx->CreateBatchRequest();
                auto f = batchGet->Get(realTableName + "/@", TGetOptions()
                    .AttributeFilter(TAttributeFilter()
                        .AddAttribute(TString("id"))
                        .AddAttribute(TString("dynamic"))
                        .AddAttribute(TString("row_count"))
                        .AddAttribute(TString("chunk_row_count"))
                        .AddAttribute(TString("uncompressed_data_size"))
                        .AddAttribute(TString("data_weight"))
                        .AddAttribute(TString("chunk_count"))
                        .AddAttribute(TString("modification_time"))
                        .AddAttribute(TString("sorted_by"))
                        .AddAttribute(TString("revision"))
                        .AddAttribute(TString("content_revision"))
                    )
                ).Apply([tableName, execCtx = std::move(execCtx)](const TFuture<NYT::TNode>& f) {
                    execCtx->StoreQueryCache();
                    auto attrs = f.GetValue();
                    auto statInfo = MakeIntrusive<TYtTableStatInfo>();
                    statInfo->Id = tableName;
                    statInfo->RecordsCount = GetTableRowCount(attrs);
                    statInfo->DataSize = GetDataWeight(attrs).GetOrElse(0);
                    statInfo->ChunkCount = attrs["chunk_count"].AsInt64();
                    TString strModifyTime = attrs["modification_time"].AsString();
                    statInfo->ModifyTime = TInstant::ParseIso8601(strModifyTime).Seconds();
                    statInfo->TableRevision = attrs["revision"].IntCast<ui64>();
                    statInfo->Revision = GetContentRevision(attrs);
                    TRunResult result;
                    result.OutTableStats.emplace_back(statInfo->Id, statInfo);
                    result.SetSuccess();
                    return result;
                });

                batchGet->ExecuteBatch();

                return f;
            }
        }

        return MakeFuture(TRunResult());
    }

    TFullResultTableResult PrepareFullResultTable(TFullResultTableOptions&& options) override {
        try {
            TString cluster = options.Cluster();
            auto outTable = options.OutTable();
            TSession::TPtr session = GetSession(options.SessionId(), true);

            auto execCtx = MakeExecCtx(std::move(options), session, cluster, nullptr, nullptr);
            execCtx->SetSingleOutput(outTable);

            const auto& out = execCtx->OutTables_.front();
            NYT::TNode attrs = NYT::TNode::CreateMap();
            PrepareAttributes(attrs, out, execCtx, cluster, true);

            TFullResultTableResult res;
            if (auto entry = execCtx->TryGetEntry()) {
                res.RootTransactionId = GetGuidAsString(entry->Tx->GetId());
                if (entry->CacheTxId) {
                    res.ExternalTransactionId = GetGuidAsString(entry->CacheTxId);
                }
            }

            const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(cluster).GetOrElse(NTCF_LEGACY);
            res.Server = execCtx->YtServer_;
            res.Path = NYT::AddPathPrefix(out.Path, NYT::TConfig::Get()->Prefix);
            res.RefName = out.Path;
            res.CodecSpec = execCtx->GetOutSpec(false, nativeTypeCompat);
            res.TableAttrs = NYT::NodeToYsonString(attrs);

            res.SetSuccess();
            return res;
        } catch (...) {
            return ResultFromCurrentException<TFullResultTableResult>();
        }
    }

    void SetStatUploader(IStatUploader::TPtr statUploader) final {
        YQL_ENSURE(!StatUploader_, "StatUploader already set");
        StatUploader_ = statUploader;
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        Y_UNUSED(compiler);
    }

    TGetTablePartitionsResult GetTablePartitions(TGetTablePartitionsOptions&& options) override {
        try {
            TSession::TPtr session = GetSession(options.SessionId());
            const TString tmpFolder = GetTablesTmpFolder(*options.Config());

            auto execCtx = MakeExecCtx(std::move(options), session, options.Cluster(), nullptr, nullptr);
            auto entry = execCtx->GetOrCreateEntry();

            TVector<NYT::TRichYPath> paths;
            for (const auto& pathInfo: execCtx->Options_.Paths()) {
                const auto tablePath = TransformPath(tmpFolder, pathInfo->Table->Name, pathInfo->Table->IsTemp, session->UserName_);
                NYT::TRichYPath richYtPath{NYT::AddPathPrefix(tablePath, NYT::TConfig::Get()->Prefix)};
                if (auto p = entry->Snapshots.FindPtr(std::make_pair(pathInfo->Table->Name, pathInfo->Table->Epoch.GetOrElse(0)))) {
                    richYtPath.Path(std::get<0>(*p)).TransactionId(std::get<1>(*p)).OriginalPath(NYT::AddPathPrefix(pathInfo->Table->Name, NYT::TConfig::Get()->Prefix));
                }
                pathInfo->FillRichYPath(richYtPath);  // n.b. throws exception, if there is no RowSpec (we assume it is always there)
                paths.push_back(std::move(richYtPath));
            }

            auto apiOptions = NYT::TGetTablePartitionsOptions()
                .PartitionMode(NYT::ETablePartitionMode::Unordered)
                .DataWeightPerPartition(execCtx->Options_.DataSizePerJob())
                .MaxPartitionCount(execCtx->Options_.MaxPartitions())
                .AdjustDataWeightPerPartition(execCtx->Options_.AdjustDataWeightPerPartition());
            auto res = TGetTablePartitionsResult();
            res.Partitions = entry->Tx->GetTablePartitions(paths, apiOptions);
            res.SetSuccess();
            return res;
        } catch (...) {
            return ResultFromCurrentException<TGetTablePartitionsResult>({}, true);
        }
    }

    void AddCluster(const TYtClusterConfig& cluster) override {
        Clusters_->AddCluster(cluster, false);
    }

private:
    class TNodeResultBuilder {
    public:
        void WriteValue(const NUdf::TUnboxedValue& value, TType* type) {
            if (type->IsTuple()) {
                auto tupleType = AS_TYPE(NMiniKQL::TTupleType, type);
                for (ui32 i: xrange(tupleType->GetElementsCount())) {
                    Node_.Add(NCommon::ValueToNode(value.GetElement(i), tupleType->GetElementType(i)));
                }
            } else if (type->IsList()) {
                auto itemType = AS_TYPE(NMiniKQL::TListType, type)->GetItemType();
                const auto it = value.GetListIterator();
                for (NUdf::TUnboxedValue item; it.Next(item);) {
                    Node_.Add(NCommon::ValueToNode(item, itemType));
                }
            } else {
                Node_.Add(NCommon::ValueToNode(value, type));
            }
        }

        bool WriteNext(const NYT::TNode& item) {
            Node_.Add(item);
            return true;
        }

        NYT::TNode Make() {
            if (Node_.IsUndefined()) {
                return NYT::TNode::CreateList();
            }
            return std::move(Node_);
        }
    private:
        NYT::TNode Node_;
    };

    struct TNodeResultFactory {
        using TResult = NYT::TNode;

        bool UseResultYson() const {
            return false;
        }

        THolder<TNodeResultBuilder> Create(const TCodecContext& codecCtx, const NKikimr::NMiniKQL::THolderFactory& holderFactory) const {
            Y_UNUSED(codecCtx);
            Y_UNUSED(holderFactory);

            return Create();
        }

        THolder<TNodeResultBuilder> Create() const {
            return MakeHolder<TNodeResultBuilder>();
        }
    };

    class TYsonExprResultFactory {
    public:
        using TResult = std::pair<TString, bool>;

        TYsonExprResultFactory(TMaybe<ui64> rowLimit, TMaybe<ui64> byteLimit, const TVector<TString>& columns, bool hasListResult)
            : RowLimit_(rowLimit)
            , ByteLimit_(byteLimit)
            , Columns_(columns)
            , HasListResult_(hasListResult)
        {
        }

        bool UseResultYson() const {
            return true;
        }

        THolder<TYsonExecuteResOrPull> Create(TCodecContext& codecCtx, const NKikimr::NMiniKQL::THolderFactory& holderFactory) const {
            Y_UNUSED(codecCtx);
            Y_UNUSED(holderFactory);

            return Create();
        }

        THolder<TYsonExecuteResOrPull> Create() const {
            THolder<TYsonExecuteResOrPull> res;

            res = MakeHolder<TYsonExecuteResOrPull>(RowLimit_, ByteLimit_, Columns_);
            if (HasListResult_) {
                res->SetListResult();
            }
            return res;
        }

    private:
        const TMaybe<ui64> RowLimit_;
        const TMaybe<ui64> ByteLimit_;
        const TVector<TString> Columns_;
        const bool HasListResult_;
    };

    class TSkiffExprResultFactory {
    public:
        using TResult = std::pair<TString, bool>;

        TSkiffExprResultFactory(TMaybe<ui64> rowLimit, TMaybe<ui64> byteLimit, bool hasListResult, const NYT::TNode& attrs, const TString& optLLVM)
            : RowLimit_(rowLimit)
            , ByteLimit_(byteLimit)
            , HasListResult_(hasListResult)
            , Attrs_(attrs)
            , OptLLVM_(optLLVM)
        {
        }

        bool UseResultYson() const {
            return true;
        }

        THolder<TSkiffExecuteResOrPull> Create(TCodecContext& codecCtx, const NKikimr::NMiniKQL::THolderFactory& holderFactory) const {
            THolder<TSkiffExecuteResOrPull> res;

            res = MakeHolder<TSkiffExecuteResOrPull>(RowLimit_, ByteLimit_, codecCtx, holderFactory, Attrs_, OptLLVM_);
            if (HasListResult_) {
                res->SetListResult();
            }

            return res;
        }

        THolder<TSkiffExecuteResOrPull> Create() const {
            YQL_ENSURE(false, "Unexpected skiff result builder creation");
        }
    private:
        const TMaybe<ui64> RowLimit_;
        const TMaybe<ui64> ByteLimit_;
        const bool HasListResult_;
        const NYT::TNode Attrs_;
        const TString OptLLVM_;
    };

    static TFinalizeResult ExecFinalize(const TSession::TPtr& session, bool abort, bool detachSnapshotTxs) {
        try {
            TFinalizeResult res;
            if (detachSnapshotTxs) {
                YQL_CLOG(INFO, ProviderYt) << "Detaching all snapshot transactions";
                session->TxCache_.DetachSnapshotTxs();
            }
            if (abort) {
                YQL_CLOG(INFO, ProviderYt) << "Aborting all transactions for hidden query";
                session->TxCache_.AbortAll();
            } else {
                session->TxCache_.Finalize();
            }
            res.SetSuccess();
            return res;
        } catch (...) {
            return ResultFromCurrentException<TFinalizeResult>();
        }
    }

    static TCanonizePathsResult ExecCanonizePaths(const TVector<TCanonizeReq>& paths,
        const THashMap<TString, TMetaPerServerRequest<TCanonizePathsOptions>>& reqPerServer)
    {
        try {
            TCanonizePathsResult res;
            res.SetSuccess();
            res.Data.resize(paths.size());

            for (auto& grp: reqPerServer) {
                auto entry = grp.second.ExecContext->GetOrCreateEntry();
                auto batch = entry->Tx->CreateBatchRequest();
                TVector<TFuture<void>> batchRes(Reserve(grp.second.TableIndicies.size()));

                for (auto idx: grp.second.TableIndicies) {
                    const TCanonizeReq& canonReq = paths[idx];
                    batchRes.push_back(batch->CanonizeYPath(canonReq.Path()).Apply([idx, &res] (const TFuture<TRichYPath>& f) {
                        auto& normalizedPath = f.GetValue();
                        TString path = normalizedPath.Path_;

                        // Convert back from absolute path to relative
                        // All futhrer YT operations will use the path with YT_PREFIX
                        if (path.StartsWith("//")) {
                            path = path.substr(2);
                        }
                        res.Data[idx].Path = path;
                        if (normalizedPath.Columns_) {
                            res.Data[idx].Columns.ConstructInPlace(normalizedPath.Columns_->Parts_);
                        }
                        res.Data[idx].Ranges = normalizedPath.GetRanges();
                        res.Data[idx].AdditionalAttributes = SerializeRichYPathAttrs(normalizedPath);
                    }));

                }
                batch->ExecuteBatch();
                for (size_t i: xrange(batchRes.size())) {
                    try {
                        batchRes[i].GetValue();
                    }
                    catch (...) {
                        FillResultFromCurrentException(res, paths.at(grp.second.TableIndicies.at(i)).Pos());
                    }
                }
            }

            return res;
        } catch (...) {
            return ResultFromCurrentException<TCanonizePathsResult>();
        }
    }

    static TVector<std::pair<size_t, TString>> BatchLockTables(const NYT::ITransactionPtr& tx, const TVector<TTableReq>& tables,
        const TVector<size_t>& tablesToLock, TMaybe<ELockMode> lockMode = {})
    {
        auto batchLock = tx->CreateBatchRequest();
        TVector<TFuture<std::pair<size_t, TString>>> batchLockRes;
        batchLockRes.reserve(tablesToLock.size());

        for (auto idx: tablesToLock) {
            const TTableReq& tableReq = tables[idx];

            auto tablePath = tableReq.Table();
            ELockMode mode = lockMode.GetOrElse(HasExclusiveModifyIntents(tableReq.Intents()) ? LM_EXCLUSIVE : LM_SHARED);

            batchLockRes.push_back(batchLock->Lock(tablePath, mode).Apply([idx](const TFuture<ILockPtr>& res) {
                try {
                    auto lock = res.GetValue();
                    TString lockId = TStringBuilder() << '#' << GetGuidAsString(lock->GetId());
                    return std::make_pair(idx, lockId);
                } catch (const TErrorResponse& e) {
                    // Yt returns NoSuchTransaction as inner issue for ResolveError
                    if (!e.IsResolveError() || e.IsNoSuchTransaction()) {
                        throw;
                    }
                    return std::make_pair(idx, TString());
                }
            }));
        }

        batchLock->ExecuteBatch();

        auto batchGet = tx->CreateBatchRequest();
        TVector<TFuture<std::pair<size_t, TString>>> batchGetRes;
        batchGetRes.reserve(tablesToLock.size());

        for (auto& f: batchLockRes) {
            auto lockRes = f.GetValue();
            size_t idx = lockRes.first;
            TString lockId = lockRes.second;
            if (lockId) {
                batchGetRes.push_back(batchGet->Get(lockId + "/@node_id").Apply([idx](const TFuture<NYT::TNode>& res) {
                    TString id = TStringBuilder()  << '#' << res.GetValue().AsString();
                    return std::make_pair(idx, id);
                }));
            } else {
                batchGetRes.push_back(MakeFuture(lockRes));
            }
        }
        batchGet->ExecuteBatch();

        TVector<std::pair<size_t, TString>> res;
        res.reserve(tablesToLock.size());

        std::transform(batchGetRes.begin(), batchGetRes.end(), std::back_inserter(res),
            [] (const TFuture<std::pair<size_t, TString>>& f) { return f.GetValue(); });

        return res;
    }

    // Returns tables, which require additional snapshot lock
    static TVector<size_t> ProcessTablesToXLock(
        const TExecContext<TGetTableInfoOptions>::TPtr& execCtx,
        const TTransactionCache::TEntry::TPtr& entry,
        const NYT::ITransactionPtr& lockTx,
        const TVector<TTableReq>& tables,
        const TVector<size_t>& tablesToXLock,
        ui32 epoch,
        TTableInfoResult& res)
    {
        NSorted::TSimpleMap<size_t, TString> existingIdxs;

        auto lockIds = BatchLockTables(lockTx, tables, tablesToXLock);

        if (0 == epoch) {
            auto batchGet = lockTx->CreateBatchRequest();
            TVector<TFuture<void>> batchGetRes;
            with_lock(entry->Lock_) {
                for (auto& lockRes: lockIds) {
                    const TTableReq& tableReq = tables[lockRes.first];
                    auto tablePath = tableReq.Table();
                    if (auto p = entry->Snapshots.FindPtr(std::make_pair(tablePath, epoch))) {
                        const ui64 revision = std::get<2>(*p);
                        if (lockRes.second) {
                            batchGetRes.push_back(batchGet->Get(lockRes.second + "/@revision").Apply([revision, tablePath](const TFuture<NYT::TNode>& f) {
                                const NYT::TNode& attr = f.GetValue();
                                if (attr.IntCast<ui64>() != revision) {
                                    YQL_LOG_CTX_THROW TErrorException(TIssuesIds::YT_CONCURRENT_TABLE_MODIF)
                                        << "Table " << tablePath.Quote()
                                        << " was modified before taking exclusive lock for it."
                                        << " Aborting query to prevent data lost";
                                }
                            }));
                        } else {
                            YQL_LOG_CTX_THROW TErrorException(TIssuesIds::YT_CONCURRENT_TABLE_MODIF)
                                << "Table " << tablePath.Quote()
                                << " was dropped before taking exclusive lock for it."
                                << " Aborting query to prevent data lost";

                        }
                    }
                }
            }
            if (batchGetRes) {
                batchGet->ExecuteBatch();
                WaitExceptionOrAll(batchGetRes).GetValue();
            }
        }

        auto batchGetSort = lockTx->CreateBatchRequest();
        TVector<TFuture<std::pair<size_t, bool>>> batchGetSortRes;
        TVector<TString> ensureParents;
        TVector<TString> ensureParentsTmp;
        auto batchLock = lockTx->CreateBatchRequest();
        TVector<TFuture<void>> batchLockRes;

        for (auto& lockRes: lockIds) {
            size_t idx = lockRes.first;
            TString id = lockRes.second;
            const TTableReq& tableReq = tables[idx];
            auto tablePath = tableReq.Table();
            TYtTableMetaInfo::TPtr metaRes;
            if (!tableReq.LockOnly()) {
                metaRes = res.Data[idx].Meta = MakeIntrusive<TYtTableMetaInfo>();
            }
            const bool loadMeta = !tableReq.LockOnly();
            const bool exclusive = HasExclusiveModifyIntents(tableReq.Intents());
            if (id) {
                if (metaRes) {
                    metaRes->DoesExist = true;
                }
                YQL_CLOG(INFO, ProviderYt) << "Lock " << tablePath.Quote() << " with "
                    << (exclusive ? LM_EXCLUSIVE : LM_SHARED)
                    << " mode (" << id << ')';

                if (loadMeta) {
                    existingIdxs.emplace_back(idx, id);
                }
                if (!exclusive) {
                    batchGetSortRes.push_back(batchGetSort->Get(id + "/@", TGetOptions().AttributeFilter(TAttributeFilter().AddAttribute("sorted_by")))
                        .Apply([idx](const TFuture<NYT::TNode>& f) {
                            const NYT::TNode& attrs = f.GetValue();
                            return std::make_pair(idx, attrs.HasKey("sorted_by") && !attrs["sorted_by"].AsList().empty());
                        })
                    );
                }
            } else {
                if (metaRes) {
                    metaRes->DoesExist = false;
                }
                tablePath = NYT::AddPathPrefix(tablePath, NYT::TConfig::Get()->Prefix);
                TString folder;
                TString tableName = tablePath;
                auto slash = tableName.rfind('/');
                if (TString::npos != slash) {
                    folder = tableName.substr(0, slash);
                    tableName = tableName.substr(slash + 1);
                    if (folder == "/") {
                        folder = "#" + lockTx->Get("//@id").AsString();
                    } else {
                        (tableReq.Anonymous() ? ensureParentsTmp : ensureParents).push_back(tablePath);
                    }
                }
                YQL_CLOG(INFO, ProviderYt) << "Lock " << tableName.Quote() << " child of "
                    << folder.Quote() << " with " << LM_SHARED << " mode";
                batchLockRes.push_back(batchLock->Lock(folder, LM_SHARED,
                    TLockOptions().ChildKey(tableName)). Apply([] (const TFuture<ILockPtr>& f) { f.GetValue(); }));
            }
        }

        TVector<size_t> tablesToUpgradeLock;
        if (batchGetSortRes) {
            batchGetSort->ExecuteBatch();
            for (auto& f: batchGetSortRes) {
                auto& sortRes = f.GetValue();
                if (sortRes.second) {
                    tablesToUpgradeLock.push_back(sortRes.first);
                }
            }
            if (tablesToUpgradeLock) {
                auto upgradeLockRes = BatchLockTables(lockTx, tables, tablesToUpgradeLock, LM_EXCLUSIVE);
                for (auto& upgradedRes: upgradeLockRes) {
                    size_t idx = upgradedRes.first;
                    TString id = upgradedRes.second;
                    const TTableReq& tableReq = tables[idx];
                    YQL_CLOG(INFO, ProviderYt) << "Upgrade " << tableReq.Table().Quote() << " lock to " << LM_EXCLUSIVE << " mode (" << id << ')';
                    if (!tableReq.LockOnly()) {
                        existingIdxs[idx] = id; // Override existing record
                    }
                }
            }
        }

        if (ensureParentsTmp) {
            CreateParents(ensureParentsTmp, entry->CacheTx);
        }
        if (ensureParents) {
            CreateParents(ensureParents, entry->GetRoot());
        }

        if (batchLockRes) {
            batchLock->ExecuteBatch();
            WaitExceptionOrAll(batchLockRes).GetValue();
        }

        if (existingIdxs) {
            FillMetadataResult(execCtx, entry, lockTx, existingIdxs, tables, res);
        }

        return tablesToUpgradeLock;
    }

    static TTableInfoResult ExecGetTableInfo(const TVector<TTableReq>& tables,
        const THashMap<TString, TMetaPerServerRequest<TGetTableInfoOptions>>& reqPerServer, bool readOnly, ui32 epoch)
    {
        try {
            TTableInfoResult res;
            res.Data.resize(tables.size());

            for (auto& grp: reqPerServer) {
                auto entry = grp.second.ExecContext->GetOrCreateEntry();

                NSorted::TSimpleMap<size_t, TString> existingIdxs;

                TVector<size_t> checkpointsToXLock;
                TVector<size_t> tablesToXLock;
                for (auto idx: grp.second.TableIndicies) {
                    const TTableReq& tableReq = tables[idx];
                    if (HasModifyIntents(tableReq.Intents())) {
                        if (tableReq.Intents().HasFlags(TYtTableIntent::Flush)) {
                            checkpointsToXLock.push_back(idx);
                        } else {
                            tablesToXLock.push_back(idx);
                        }
                        res.Data[idx].WriteLock = true;
                    }
                }

                TVector<size_t> tablesToSLock;
                bool makeUniqSLock = false;
                if (!readOnly) {
                    if (tablesToXLock || checkpointsToXLock) {
                        entry->CreateDefaultTmpFolder();
                    }
                    if (tablesToXLock) {
                        tablesToSLock = ProcessTablesToXLock(grp.second.ExecContext, entry, entry->Tx, tables, tablesToXLock, epoch, res);
                        makeUniqSLock = !tablesToSLock.empty();
                    }

                    // each checkpoint has unique transaction
                    for (auto idx: checkpointsToXLock) {
                        auto lockTx = entry->GetOrCreateCheckpointTx(tables[idx].Table());
                        ProcessTablesToXLock(grp.second.ExecContext, entry, lockTx, tables, {idx}, epoch, res);
                    }
                }

                for (auto idx: grp.second.TableIndicies) {
                    const TTableReq& tableReq = tables[idx];
                    if (!tableReq.LockOnly() && (readOnly || HasReadIntents(tableReq.Intents()))) {
                        auto metaRes = res.Data[idx].Meta;
                        if (!metaRes || metaRes->DoesExist) {
                            tablesToSLock.push_back(idx);
                        }
                    }
                }

                if (tablesToSLock) {
                    if (makeUniqSLock) {
                        std::sort(tablesToSLock.begin(), tablesToSLock.end());
                        tablesToSLock.erase(std::unique(tablesToSLock.begin(), tablesToSLock.end()), tablesToSLock.end());
                    }

                    auto snapshotTx = entry->GetSnapshotTx(epoch != 0);
                    auto snapshotTxId = snapshotTx->GetId();
                    auto snapshotTxIdStr = GetGuidAsString(snapshotTxId);

                    auto lockIds = BatchLockTables(snapshotTx, tables, tablesToSLock, LM_SNAPSHOT);

                    TVector<std::tuple<TString, TString, size_t>> locks;

                    for (auto& lockRes: lockIds) {
                        size_t idx = lockRes.first;
                        TString id = lockRes.second;

                        const TTableReq& tableReq = tables[idx];

                        bool loadMeta = false;
                        auto metaRes = res.Data[idx].Meta;
                        if (!metaRes) {
                            metaRes = res.Data[idx].Meta = MakeIntrusive<TYtTableMetaInfo>();
                            loadMeta = true;
                        }

                        auto tablePath = tableReq.Table();
                        if (id) {
                            if (loadMeta) {
                                metaRes->DoesExist = true;
                                existingIdxs.emplace_back(idx, id);
                            }
                            locks.emplace_back(tablePath, id, idx);
                            YQL_CLOG(INFO, ProviderYt) << "Snapshot " << tablePath.Quote() << " -> " << id << ", tx=" << snapshotTxIdStr;
                        } else {
                            YQL_ENSURE(loadMeta);
                            metaRes->DoesExist = false;
                        }
                    }

                    if (existingIdxs) {
                        FillMetadataResult(grp.second.ExecContext, entry, snapshotTx, existingIdxs, tables, res);
                    }

                    if (locks) {
                        with_lock(entry->Lock_) {
                            for (auto& l: locks) {
                                entry->Snapshots[std::make_pair(std::get<0>(l), epoch)] = std::make_tuple(std::get<1>(l), snapshotTxId, res.Data[std::get<2>(l)].Stat->TableRevision);
                            }
                        }
                    }
                }
            }

            res.SetSuccess();
            return res;
        } catch (...) {
            return ResultFromCurrentException<TTableInfoResult>();
        }
    }

    static TFuture<TTableRangeResult> ExecGetTableRange(const TExecContext<TTableRangeOptions>::TPtr& execCtx,
        const TString& tmpTablePath = {}, TString filterLambda = {}, const TExpressionResorceUsage& extraUsage = {})
    {
        auto pos = execCtx->Options_.Pos();
        try {
            auto entry = execCtx->GetOrCreateEntry();

            TString prefix = execCtx->Options_.Prefix();
            TString suffix = execCtx->Options_.Suffix();

            auto cacheKey = std::make_tuple(prefix, suffix, filterLambda);
            with_lock(entry->Lock_) {
                if (auto p = entry->RangeCache.FindPtr(cacheKey)) {
                    YQL_CLOG(INFO, ProviderYt) << "Found range in cache for key ('" << prefix << "','" << suffix << "',<filter with size " << filterLambda.Size() << ">) - number of items " << p->size();
                    return MakeFuture(MakeTableRangeResult(*p));
                }
            }

            if (!prefix.empty() && !entry->Tx->Exists(prefix)) {
                YQL_CLOG(INFO, ProviderYt) << "Storing empty range to cache with key ('" << std::get<0>(cacheKey) << "','" << std::get<1>(cacheKey) << "',<filter with size " << std::get<2>(cacheKey).size() << ">)";
                with_lock(entry->Lock_) {
                    entry->RangeCache.emplace(std::move(cacheKey), std::vector<NYT::TRichYPath>{});
                }

                TTableRangeResult rangeRes;
                rangeRes.SetSuccess();
                return MakeFuture(rangeRes);
            }

            std::vector<TString> names;
            std::vector<std::exception_ptr> errors;

            bool foundInPartialCache = false;
            with_lock(entry->Lock_) {
                if (auto p = entry->PartialRangeCache.FindPtr(prefix)) {
                    std::tie(names, errors) = *p;
                    foundInPartialCache = true;
                }
            }

            if (!foundInPartialCache) {
                auto typeAttrFilter = TAttributeFilter().AddAttribute("type").AddAttribute("_yql_type").AddAttribute("broken");
                auto nodeList = entry->Tx->List(prefix,
                    TListOptions().AttributeFilter(typeAttrFilter));
                TVector<
                    std::pair<
                        TString, //name
                        std::variant<TString, std::exception_ptr> //type or exception
                    >
                > items(nodeList.size());
                {
                    auto batchGet = entry->Tx->CreateBatchRequest();
                    TVector<TFuture<void>> batchRes;
                    for (size_t i: xrange(nodeList.size())) {
                        const auto& node = nodeList[i];
                        items[i].first = node.AsString();
                        items[i].second = GetTypeFromNode(node, true);
                        if (std::get<TString>(items[i].second) == "link") {
                            if (!node.GetAttributes().HasKey("broken") || !node.GetAttributes()["broken"].AsBool()) {
                                batchRes.push_back(batchGet->Get(prefix + "/" + node.AsString() + "/@", TGetOptions().AttributeFilter(typeAttrFilter))
                                   .Apply([i, &items](const TFuture<NYT::TNode> &f) {
                                       try {
                                           items[i].second = GetTypeFromAttributes(f.GetValue(), true);
                                       } catch (...) {
                                           items[i].second = std::current_exception();
                                       }
                                   }));
                            }
                        }
                    }
                    batchGet->ExecuteBatch();
                    WaitExceptionOrAll(batchRes).GetValue();
                }

                names.reserve(items.size());
                errors.reserve(items.size());
                for (const auto& item: items) {
                    if (const auto* type = std::get_if<TString>(&item.second)) {
                        if (
                                (suffix.empty() && ("table" == *type || "view" == *type)) ||
                                (!suffix.empty() && "map_node" == *type)
                        ) {
                            names.push_back(item.first);
                            errors.emplace_back();
                        }
                    } else {
                        auto exptr = std::get<std::exception_ptr>(item.second);
                        if (filterLambda) {
                            // Delayed error processing
                            names.push_back(item.first);
                            errors.push_back(std::move(exptr));
                        } else {
                            std::rethrow_exception(exptr);
                        }
                    }
                }
                YQL_ENSURE(names.size() == errors.size());
                YQL_CLOG(INFO, ProviderYt) << "Got " << names.size() << " items in folder '" << prefix << "'. Storing to partial cache";
                with_lock(entry->Lock_) {
                    entry->PartialRangeCache.emplace(prefix, std::make_pair(names, errors));
                }
            } else {
                YQL_CLOG(INFO, ProviderYt) << "Found range in partial cache for '" << prefix << "' - number of items " << names.size();
            }

            if (filterLambda && !names.empty()) {
                YQL_CLOG(DEBUG, ProviderYt) << "Executing range filter";
                {
                    TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                        execCtx->FunctionRegistry_->SupportsSizedAllocators());
                    alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                    TNativeYtLambdaBuilder builder(alloc, execCtx->FunctionRegistry_, *execCtx->Session_);
                    TProgramBuilder pgmBuilder(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_);

                    TRuntimeNode root = DeserializeRuntimeNode(filterLambda, builder.GetTypeEnvironment());

                    root = builder.TransformAndOptimizeProgram(root, [&](TInternName name)->TCallableVisitFunc {
                        if (name == MrRangeInputListInternal) {
                            return [&](NMiniKQL::TCallable& callable, const TTypeEnvironment& env)->TRuntimeNode {
                                Y_UNUSED(callable);
                                Y_UNUSED(env);
                                TVector<TRuntimeNode> inputs;
                                for (size_t i = 0; i < names.size(); ++i) {
                                    inputs.push_back(pgmBuilder.NewTuple({
                                        pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(names[i]),
                                        pgmBuilder.NewDataLiteral(ui64(i))
                                    }));
                                }
                                auto inputNode = pgmBuilder.AsList(inputs);
                                return inputNode;
                            };
                        }
                        return TCallableVisitFunc();
                    });
                    filterLambda = SerializeRuntimeNode(root, builder.GetTypeEnvironment());
                }

                auto logCtx = execCtx->LogCtx_;
                return ExecCalc(filterLambda, extraUsage, tmpTablePath, execCtx, entry, TNodeResultFactory())
                    .Apply([logCtx, prefix, suffix, entry, pos, errors = std::move(errors), cacheKey = std::move(cacheKey)](const TFuture<NYT::TNode>& f) mutable {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
                        std::vector<TString> names;
                        try {
                            const NYT::TNode& node = f.GetValue();
                            YQL_ENSURE(node.IsList());
                            for (auto& n: node.AsList()) {
                                YQL_ENSURE(n.IsList());
                                if (auto err = errors.at(n.AsList().at(1).AsUint64())) {
                                    std::rethrow_exception(err);
                                }
                                names.push_back(n.AsList().at(0).AsString());
                            }
                            return MakeTableRangeResult(std::move(names), std::move(cacheKey), prefix, suffix, entry);
                        } catch (...) {
                            return ResultFromCurrentException<TTableRangeResult>(pos);
                        }
                    });
            }
            return MakeFuture(MakeTableRangeResult(std::move(names), std::move(cacheKey), prefix, suffix, entry));

        } catch (...) {
            return MakeFuture(ResultFromCurrentException<TTableRangeResult>(pos));
        }
    }

    static TTableRangeResult MakeTableRangeResult(const std::vector<NYT::TRichYPath>& paths) {
        TTableRangeResult rangeRes;
        rangeRes.SetSuccess();

        for (auto& normalizedPath: paths) {
            TCanonizedPath canonPath;
            canonPath.Path = normalizedPath.Path_;
            if (normalizedPath.Columns_) {
                canonPath.Columns.ConstructInPlace(normalizedPath.Columns_->Parts_);
            }
            canonPath.Ranges = normalizedPath.GetRanges();
            rangeRes.Tables.push_back(std::move(canonPath));
        }

        SortBy(rangeRes.Tables, [] (const TCanonizedPath& path) { return path.Path; });

        return rangeRes;
    }

    static TTableRangeResult MakeTableRangeResult(std::vector<TString>&& names, std::tuple<TString, TString, TString>&& cacheKey,
        TString prefix, TString suffix, const TTransactionCache::TEntry::TPtr& entry)
    {
        TTableRangeResult rangeRes;
        rangeRes.SetSuccess();
        std::vector<NYT::TRichYPath> cached;
        if (prefix) {
            prefix.append('/');
        }
        if (suffix) {
            if (!names.empty()) {
                auto batchCanonize = entry->Tx->CreateBatchRequest();
                auto batchExists = entry->Tx->CreateBatchRequest();
                TVector<TFuture<void>> batchCanonizeRes;
                TVector<TFuture<void>> batchExistsRes;
                for (TString& name: names) {
                    name.prepend(prefix).append('/').append(suffix);
                    batchCanonizeRes.push_back(batchCanonize->CanonizeYPath(name)
                        .Apply([&batchExists, &batchExistsRes, &rangeRes, &cached] (const TFuture<TRichYPath>& f) {
                            TCanonizedPath canonPath;
                            auto normalizedPath = f.GetValue();
                            if (normalizedPath.Path_.StartsWith(TConfig::Get()->Prefix)) {
                                normalizedPath.Path_ = normalizedPath.Path_.substr(TConfig::Get()->Prefix.size());
                            }
                            canonPath.Path = normalizedPath.Path_;
                            if (normalizedPath.Columns_) {
                                canonPath.Columns.ConstructInPlace(normalizedPath.Columns_->Parts_);
                            }
                            canonPath.Ranges = normalizedPath.GetRanges();
                            batchExistsRes.push_back(batchExists->Exists(canonPath.Path)
                                .Apply([canonPath = std::move(canonPath), normalizedPath = std::move(normalizedPath), &rangeRes, &cached] (const NThreading::TFuture<bool>& f) {
                                    if (f.GetValue()) {
                                        rangeRes.Tables.push_back(std::move(canonPath));
                                        cached.push_back(std::move(normalizedPath));
                                    }
                                }));
                        }));
                }
                batchCanonize->ExecuteBatch();
                WaitExceptionOrAll(batchCanonizeRes).GetValue();

                batchExists->ExecuteBatch();
                WaitExceptionOrAll(batchExistsRes).GetValue();
            }
        }
        else {
            if (prefix.StartsWith(TConfig::Get()->Prefix)) {
                prefix = prefix.substr(TConfig::Get()->Prefix.size());
            }
            for (auto& name: names) {
                auto fullName = prefix + name;
                rangeRes.Tables.push_back(TCanonizedPath{fullName, Nothing(), {}, Nothing()});
                cached.push_back(NYT::TRichYPath(fullName));
            }
        }

        YQL_CLOG(INFO, ProviderYt) << "Storing " << cached.size() << " items to range cache with key ('" << std::get<0>(cacheKey) << "','" << std::get<1>(cacheKey) << "',<filter with size " << std::get<2>(cacheKey).size() << ">)";
        with_lock(entry->Lock_) {
            entry->RangeCache.emplace(std::move(cacheKey), std::move(cached));
        }

        SortBy(rangeRes.Tables, [] (const TCanonizedPath& path) { return path.Path; });

        return rangeRes;
    }

    static TFuture<void> ExecPublish(
        const TExecContext<TPublishOptions>::TPtr& execCtx,
        const TVector<TString>& src,
        const TString& dst,
        const ui32 dstEpoch,
        const bool isAnonymous,
        EYtWriteMode mode,
        const bool initial,
        const TString& srcColumnGroups,
        const bool combineChunks,
        const std::unordered_map<EYtSettingType, TString>& strOpts)
    {
        TString tmpFolder = GetTablesTmpFolder(*execCtx->Options_.Config());
        auto cluster = execCtx->Cluster_;
        auto entry = execCtx->GetEntry();

        TVector<TString> srcPaths;
        for (auto& p: src) {
            srcPaths.push_back(NYql::TransformPath(tmpFolder, p, true, execCtx->Session_->UserName_));
        }

        auto dstPath = NYql::TransformPath(tmpFolder, dst, isAnonymous, execCtx->Session_->UserName_);
        if (execCtx->Hidden) {
            const auto origDstPath = dstPath;
            dstPath = NYql::TransformPath(
                tmpFolder,
                TStringBuilder() << "tmp/" << GetGuidAsString(CreateDefaultRandomProvider()->GenGuid()),
                true,
                execCtx->Session_->UserName_);
            if (entry->Tx->Exists(origDstPath) && EYtWriteMode::Flush != mode) {
                entry->Tx->Copy(
                    origDstPath,
                    dstPath,
                    TCopyOptions().Force(true));
                entry->DeleteAtFinalizeInternal(dstPath);
            }
            if (EYtWriteMode::Flush == mode) {
                mode = EYtWriteMode::Renew;
            }
            YQL_CLOG(INFO, ProviderYt) << "Hidden query publish destination: " << dstPath;
        }

        auto publishTx = EYtWriteMode::Flush == mode ? entry->GetCheckpointTx(dstPath) : entry->Tx;

        if (isAnonymous) {
            entry->CreateDefaultTmpFolder();
        }
        CreateParents(TVector<TString>{dstPath}, entry->GetRoot());

        const bool exists = entry->Tx->Exists(dstPath);
        if ((EYtWriteMode::Append == mode || EYtWriteMode::RenewKeepMeta == mode) && !exists) {
            mode = EYtWriteMode::Renew;
        }
        if (isAnonymous) {
            entry->DeleteAtFinalize(dstPath);
        }

        TYqlRowSpecInfo::TPtr rowSpec = execCtx->Options_.DestinationRowSpec();

        bool appendToSorted = false;
        if (EYtWriteMode::Append == mode && !strOpts.contains(EYtSettingType::MonotonicKeys)) {
            NYT::TNode attrs = entry->Tx->Get(dstPath + "/@", TGetOptions()
                .AttributeFilter(TAttributeFilter()
                    .AddAttribute(TString("sorted_by"))
                )
            );
            appendToSorted = attrs.HasKey("sorted_by") && !attrs["sorted_by"].AsList().empty();
        }

        auto yqlAttrs = NYT::TNode::CreateMap();

        auto storageAttrs = NYT::TNode::CreateMap();
        if (appendToSorted || EYtWriteMode::RenewKeepMeta == mode) {
            yqlAttrs = GetUserAttributes(entry->Tx, dstPath, false);
            storageAttrs = entry->Tx->Get(dstPath + "/@", TGetOptions()
                .AttributeFilter(TAttributeFilter()
                    .AddAttribute("compression_codec")
                    .AddAttribute("erasure_codec")
                    .AddAttribute("replication_factor")
                    .AddAttribute("media")
                    .AddAttribute("primary_medium")
                )
            );
        }

        bool forceMerge = combineChunks;

        NYT::MergeNodes(yqlAttrs, GetUserAttributes(entry->Tx, srcPaths.back(), true));
        NYT::MergeNodes(yqlAttrs, YqlOpOptionsToAttrs(execCtx->Session_->OperationOptions_));
        if (EYtWriteMode::RenewKeepMeta == mode) {
            auto dstAttrs = entry->Tx->Get(dstPath + "/@", TGetOptions()
                .AttributeFilter(TAttributeFilter()
                    .AddAttribute("annotation")
                    .AddAttribute("expiration_time")
                    .AddAttribute("expiration_timeout")
                    .AddAttribute("tablet_cell_bundle")
                    .AddAttribute("enable_dynamic_store_read")
                )
            );
            if (dstAttrs.AsMap().contains("tablet_cell_bundle") && dstAttrs["tablet_cell_bundle"] != "default") {
                forceMerge = true;
            }
            dstAttrs.AsMap().erase("tablet_cell_bundle");
            if (dstAttrs.AsMap().contains("enable_dynamic_store_read")) {
                forceMerge = true;
            }
            dstAttrs.AsMap().erase("enable_dynamic_store_read");
            NYT::MergeNodes(yqlAttrs, dstAttrs);
        }
        NYT::TNode& rowSpecNode = yqlAttrs[YqlRowSpecAttribute];
        const auto nativeYtTypeCompatibility = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(cluster).GetOrElse(NTCF_LEGACY);
        const bool rowSpecCompactForm = execCtx->Options_.Config()->UseYqlRowSpecCompactForm.Get().GetOrElse(DEFAULT_ROW_SPEC_COMPACT_FORM);
        rowSpec->FillAttrNode(rowSpecNode, nativeYtTypeCompatibility, rowSpecCompactForm);

        const auto multiSet = execCtx->Options_.Config()->_UseMultisetAttributes.Get().GetOrElse(DEFAULT_USE_MULTISET_ATTRS);

        auto commitCheckpoint = [entry, dstPath, mode] (const TFuture<void>& f) {
            f.GetValue();
            if (EYtWriteMode::Flush == mode) {
                entry->CommitCheckpointTx(dstPath);
            }
        };

        if (EYtWriteMode::Renew == mode || EYtWriteMode::RenewKeepMeta == mode) {
            const auto expirationIt = strOpts.find(EYtSettingType::Expiration);
            bool isTimestamp = false, isDuration = false;
            TInstant stamp;
            TDuration duration;
            if (expirationIt != strOpts.cend()) {
                isDuration = TDuration::TryParse(expirationIt->second, duration);
                if (!isDuration) {
                    isTimestamp = TInstant::TryParseIso8601(expirationIt->second, stamp);
                }
            }
            const TMaybe<TInstant> deadline =
                execCtx->Options_.Config()->ExpirationDeadline.Get(cluster);
            const TMaybe<TDuration> interval =
                execCtx->Options_.Config()->ExpirationInterval.Get(cluster);
            if (deadline || isTimestamp) {
                yqlAttrs["expiration_time"] = isTimestamp ? stamp.ToStringUpToSeconds()
                                                          : deadline->ToStringUpToSeconds();
            }
            if (interval || isDuration) {
                yqlAttrs["expiration_timeout"] = isDuration ? duration.MilliSeconds()
                                                         : (*interval).MilliSeconds();
            }
            if (execCtx->Options_.Config()->NightlyCompress.Get(cluster).GetOrElse(false)) {
                yqlAttrs["force_nightly_compress"] = true;
            }
        }

        const auto userAttrsIt = strOpts.find(EYtSettingType::UserAttrs);
        if (userAttrsIt != strOpts.cend()) {
            const NYT::TNode mapNode = NYT::NodeFromYsonString(userAttrsIt->second);
            const auto& map = mapNode.AsMap();
            for (auto it = map.cbegin(); it != map.cend(); ++it) {
                yqlAttrs[it->first] = it->second;
            }
        }

        bool forceTransform = false;

#define DEFINE_OPT(name, attr, transform)                                                               \
        auto dst##name = isAnonymous                                                                    \
            ? execCtx->Options_.Config()->Temporary##name.Get(cluster)                                  \
            : execCtx->Options_.Config()->Published##name.Get(cluster);                                 \
        if (EYtWriteMode::RenewKeepMeta == mode && storageAttrs.HasKey(attr)                            \
            && execCtx->Options_.Config()->Temporary##name.Get(cluster)) {                              \
            dst##name = OptionFromNode<decltype(dst##name)::value_type>(storageAttrs[attr]);            \
        }                                                                                               \
        if (const auto it = strOpts.find(EYtSettingType::name); it != strOpts.cend()) {                 \
            dst##name = OptionFromString<decltype(dst##name)::value_type>(it->second);                  \
        }                                                                                               \
        if (dst##name && dst##name != execCtx->Options_.Config()->Temporary##name.Get(cluster)) {       \
            forceMerge = true;                                                                          \
            forceTransform = forceTransform || transform;                                               \
            YQL_CLOG(INFO, ProviderYt) << "Option " #name " forces merge";                              \
        }

        DEFINE_OPT(CompressionCodec, "compression_codec", true);
        DEFINE_OPT(ErasureCodec, "erasure_codec", true);
        DEFINE_OPT(ReplicationFactor, "replication_factor", false);
        DEFINE_OPT(Media, "media", true);
        DEFINE_OPT(PrimaryMedium, "primary_medium", true);

#undef DEFINE_OPT

        NYT::TNode columnGroupsSpec;
        if (const auto it = strOpts.find(EYtSettingType::ColumnGroups); it != strOpts.cend() && execCtx->Options_.Config()->OptimizeFor.Get(cluster).GetOrElse(NYT::OF_LOOKUP_ATTR) != NYT::OF_LOOKUP_ATTR) {
            columnGroupsSpec = NYT::NodeFromYsonString(it->second);
            if (it->second != srcColumnGroups) {
                forceMerge = forceTransform = true;
            }
        }

        TFuture<void> res;
        if (EYtWriteMode::Flush == mode || EYtWriteMode::Append == mode || srcPaths.size() > 1 || forceMerge) {
            TFuture<bool> cacheCheck = MakeFuture<bool>(false);
            if (EYtWriteMode::Flush != mode && isAnonymous) {
                execCtx->SetCacheItem({dstPath}, {NYT::TNode::CreateMap()}, tmpFolder);
                cacheCheck = execCtx->LookupQueryCacheAsync();
            }
            res = cacheCheck.Apply([mode, srcPaths, execCtx, rowSpec, forceTransform,
                                    appendToSorted, initial, entry, dstPath, dstEpoch, yqlAttrs, combineChunks,
                                    dstCompressionCodec, dstErasureCodec, dstReplicationFactor, dstMedia, dstPrimaryMedium,
                                    nativeYtTypeCompatibility, publishTx, cluster,
                                    commitCheckpoint, columnGroupsSpec = std::move(columnGroupsSpec)] (const auto& f) mutable
            {
                if (f.GetValue()) {
                    execCtx->QueryCacheItem.Destroy();
                    return MakeFuture();
                }
                // Use explicit columns for source tables to cut aux columns
                TVector<TString> columns;
                for (auto item: rowSpec->GetType()->GetItems()) {
                    columns.emplace_back(item->GetName());
                }
                for (auto item: rowSpec->GetAuxColumns()) {
                    columns.emplace_back(item.first);
                }

                TMergeOperationSpec mergeSpec;
                if (appendToSorted) {
                    if (initial) {
                        auto p = entry->Snapshots.FindPtr(std::make_pair(dstPath, dstEpoch));
                        YQL_ENSURE(p, "Table " << dstPath << " has no snapshot");
                        mergeSpec.AddInput(TRichYPath(std::get<0>(*p)).TransactionId(std::get<1>(*p)).OriginalPath(NYT::AddPathPrefix(dstPath, NYT::TConfig::Get()->Prefix)).Columns(columns));
                    } else {
                        mergeSpec.AddInput(TRichYPath(dstPath).Columns(columns));
                    }
                }
                for (auto& s: srcPaths) {
                    auto path = TRichYPath(s).Columns(columns);
                    if (EYtWriteMode::Flush == mode) {
                        path.TransactionId(entry->Tx->GetId());
                    }
                    mergeSpec.AddInput(path);
                }

                auto ytDst = TRichYPath(dstPath);
                if (EYtWriteMode::Append == mode && !appendToSorted) {
                    ytDst.Append(true);
                } else {
                    NYT::TNode fullSpecYson;
                    rowSpec->FillCodecNode(fullSpecYson);
                    const auto schema = RowSpecToYTSchema(fullSpecYson, nativeYtTypeCompatibility, columnGroupsSpec);
                    ytDst.Schema(schema);

                    if (EYtWriteMode::Append != mode && EYtWriteMode::RenewKeepMeta != mode) {
                        yqlAttrs["schema"] = schema.ToNode();
                        if (dstCompressionCodec) {
                            yqlAttrs["compression_codec"] = *dstCompressionCodec;
                        }
                        if (dstErasureCodec) {
                            yqlAttrs["erasure_codec"] = ToString(*dstErasureCodec);
                        }
                        if (dstReplicationFactor) {
                            yqlAttrs["replication_factor"] = static_cast<i64>(*dstReplicationFactor);
                        }
                        if (dstMedia) {
                            yqlAttrs["media"] = *dstMedia;
                        }
                        if (dstPrimaryMedium) {
                            yqlAttrs["primary_medium"] = *dstPrimaryMedium;
                        }
                        if (auto optimizeFor = execCtx->Options_.Config()->OptimizeFor.Get(cluster)) {
                            if (schema.Columns().size()) {
                                yqlAttrs["optimize_for"] = ToString(*optimizeFor);
                            }
                        }

                        YQL_CLOG(INFO, ProviderYt) << "Creating " << dstPath << " with attrs: " << NYT::NodeToYsonString(yqlAttrs);
                        publishTx->Create(dstPath, NT_TABLE, TCreateOptions().Force(true).Attributes(yqlAttrs));

                        yqlAttrs.Clear();
                    }
                }
                mergeSpec.Output(ytDst);
                mergeSpec.ForceTransform(forceTransform);

                if (rowSpec->IsSorted()) {
                    mergeSpec.Mode(MM_SORTED);
                    mergeSpec.MergeBy(ToYTSortColumns(rowSpec->GetForeignSort()));
                } else {
                    mergeSpec.Mode(MM_ORDERED);
                }

                NYT::TNode spec = execCtx->Session_->CreateSpecWithDesc();
                EYtOpProps flags = EYtOpProp::PublishedAutoMerge;
                if (combineChunks) {
                    flags |= EYtOpProp::PublishedChunkCombine;
                }

                FillSpec(spec, *execCtx, entry, 0., Nothing(), flags);

                if (combineChunks) {
                    mergeSpec.CombineChunks(true);
                }

                return execCtx->RunOperation([publishTx, mergeSpec = std::move(mergeSpec), spec = std::move(spec)]() {
                    return publishTx->Merge(mergeSpec, TOperationOptions().StartOperationMode(TOperationOptions::EStartOperationMode::AsyncPrepare).CreateOutputTables(false).Spec(spec));
                })
                .Apply([execCtx](const auto& f){
                    f.GetValue();
                    execCtx->StoreQueryCache();
                });
            });
        }
        else {
            publishTx->Copy(srcPaths.front(), dstPath, TCopyOptions().Force(true));
            res = MakeFuture();
        }

        std::function<void(const TFuture<void>&)> setAttrs = [logCtx = execCtx->LogCtx_, entry, publishTx, dstPath, mode, yqlAttrs, multiSet] (const TFuture<void>& f) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);
            f.GetValue();
            if (yqlAttrs.IsUndefined()) {
                return;
            }
            YQL_CLOG(INFO, ProviderYt) << "Setting attrs for " << dstPath << ": " << NYT::NodeToYsonString(yqlAttrs);
            if (multiSet) {
                try {
                    publishTx->MultisetAttributes(dstPath + "/@", yqlAttrs.AsMap(), NYT::TMultisetAttributesOptions());
                }
                catch (const TErrorResponse& e) {
                    if (EYtWriteMode::Append != mode || !e.IsConcurrentTransactionLockConflict()) {
                        throw;
                    }
                }
            } else {
                auto batch = publishTx->CreateBatchRequest();

                TVector<TFuture<void>> batchRes;

                for (auto& attr: yqlAttrs.AsMap()) {
                    batchRes.push_back(batch->Set(TStringBuilder() << dstPath << "/@" << attr.first, attr.second));
                }

                batch->ExecuteBatch();
                ForEach(batchRes.begin(), batchRes.end(), [mode] (const TFuture<void>& f) {
                    try {
                        f.GetValue();
                    }
                    catch (const TErrorResponse& e) {
                        if (EYtWriteMode::Append != mode || !e.IsConcurrentTransactionLockConflict()) {
                            throw;
                        }
                    }
                });
            }
        };
        return res.Apply(setAttrs).Apply(commitCheckpoint);
    }

    static TFuture<void> ExecDropTrackables(const TVector<TString>& paths,
        const TExecContext<TDropTrackablesOptions>::TPtr& execCtx)
    {
        if (paths.empty()) {
            return MakeFuture();
        }

        const auto tmpFolder = GetTablesTmpFolder(*execCtx->Options_.Config());
        TVector<TString> toRemove;
        for (const auto& p : paths) {
            toRemove.push_back(NYql::TransformPath(tmpFolder, p, true, execCtx->Session_->UserName_));
        }

        if (execCtx->Config_->GetLocalChainTest()) {
            for (const auto& path : toRemove) {
                YQL_ENSURE(TestTables.erase(path));
            }
            return MakeFuture();
        }

        const auto entry = execCtx->GetEntry();

        toRemove = entry->CancelDeleteAtFinalize(toRemove);
        if (toRemove.empty()) {
            return MakeFuture();
        }

        auto batch = entry->Tx->CreateBatchRequest();
        TVector<TFuture<void>> batchResults;
        for (const auto& p : toRemove) {
            batchResults.push_back(batch->Remove(p, TRemoveOptions().Force(true)));
        }
        batch->ExecuteBatch();
        return WaitExceptionOrAll(batchResults);
    }

    static void FillMetadataResult(
        const TExecContext<TGetTableInfoOptions>::TPtr& execCtx,
        const TTransactionCache::TEntry::TPtr& entry,
        const ITransactionPtr& tx,
        const NSorted::TSimpleMap<size_t, TString>& idxs,
        const TVector<TTableReq>& tables,
        TTableInfoResult& result)
    {
        TVector<NYT::TNode> attributes(tables.size());
        {
            auto batchGet = tx->CreateBatchRequest();
            TVector<TFuture<void>> batchRes(Reserve(idxs.size()));
            for (auto& idx: idxs) {
                batchRes.push_back(batchGet->Get(idx.second + "/@").Apply([&attributes, idx] (const TFuture<NYT::TNode>& res) {
                    attributes[idx.first] = res.GetValue();
                }));
            }
            batchGet->ExecuteBatch();
            WaitExceptionOrAll(batchRes).GetValue();
        }
        {
            auto batchGet = tx->CreateBatchRequest();
            TVector<TFuture<void>> batchRes;
            auto getOpts = TGetOptions()
                .AttributeFilter(TAttributeFilter()
                    .AddAttribute("type")
                    .AddAttribute(TString{QB2Premapper})
                    .AddAttribute(TString{YqlRowSpecAttribute})
                );
            for (auto& idx: idxs) {
                batchRes.push_back(batchGet->Get(tables[idx.first].Table() + "&/@", getOpts).Apply([idx, &attributes](const TFuture<NYT::TNode>& f) {
                    try {
                        NYT::TNode attrs = f.GetValue();
                        if (GetTypeFromAttributes(attrs, false) == "link") {
                            // override some attributes by the link ones
                            if (attrs.HasKey(QB2Premapper)) {
                                attributes[idx.first][QB2Premapper] = attrs[QB2Premapper];
                            }
                            if (attrs.HasKey(YqlRowSpecAttribute)) {
                                attributes[idx.first][YqlRowSpecAttribute] = attrs[YqlRowSpecAttribute];
                            }
                        }
                    } catch (const TErrorResponse& e) {
                        // Yt returns NoSuchTransaction as inner issue for ResolveError
                        if (!e.IsResolveError() || e.IsNoSuchTransaction()) {
                            throw;
                        }
                        // Just ignore. Original table path may be deleted at this time
                    }
                }));
            }
            batchGet->ExecuteBatch();
            WaitExceptionOrAll(batchRes).GetValue();
        }

        auto batchGet = tx->CreateBatchRequest();
        TVector<TFuture<void>> batchRes;

        TVector<std::pair<size_t, TString>> idxsToInferFromContent;

        for (auto& idx: idxs) {
            try {
                NYT::TNode& attrs = attributes[idx.first];

                TYtTableMetaInfo::TPtr metaInfo = result.Data[idx.first].Meta;
                TYtTableStatInfo::TPtr statInfo = MakeIntrusive<TYtTableStatInfo>();
                result.Data[idx.first].Stat = statInfo;

                auto type = GetTypeFromAttributes(attrs, false);
                ui16 viewSyntaxVersion = 1;
                if (type == "document") {
                    if (attrs.HasKey(YqlTypeAttribute)) {
                        auto typeAttr = attrs[YqlTypeAttribute];
                        type = typeAttr.AsString();
                        auto verAttr = typeAttr.Attributes()["syntax_version"];
                        viewSyntaxVersion = verAttr.IsUndefined() ? 1 : verAttr.AsInt64();
                    }
                }

                if (type != "table" && type != "replicated_table" && type != YqlTypeView) {
                    YQL_LOG_CTX_THROW TErrorException(TIssuesIds::YT_ENTRY_NOT_TABLE_OR_VIEW) << "Input " << tables[idx.first].Table() << " is not a table or a view, got: " << type;
                }

                statInfo->Id = attrs["id"].AsString();
                statInfo->TableRevision = attrs["revision"].IntCast<ui64>();
                statInfo->Revision = GetContentRevision(attrs);

                if (type == YqlTypeView) {
                    batchRes.push_back(batchGet->Get(idx.second).Apply([metaInfo, viewSyntaxVersion](const TFuture<NYT::TNode>& f) {
                        metaInfo->SqlView = f.GetValue().AsString();
                        metaInfo->SqlViewSyntaxVersion = viewSyntaxVersion;
                        metaInfo->CanWrite = false;
                    }));
                    continue;
                }

                bool isDynamic = attrs.AsMap().contains("dynamic") && NYT::GetBool(attrs["dynamic"]);
                auto rowCount = attrs[isDynamic ? "chunk_row_count" : "row_count"].AsInt64();
                statInfo->RecordsCount = rowCount;
                statInfo->DataSize = GetDataWeight(attrs).GetOrElse(0);
                statInfo->ChunkCount = attrs["chunk_count"].AsInt64();
                TString strModifyTime = attrs["modification_time"].AsString();
                statInfo->ModifyTime = TInstant::ParseIso8601(strModifyTime).Seconds();
                metaInfo->IsDynamic = isDynamic;
                if (statInfo->IsEmpty()) {
                    YQL_CLOG(INFO, ProviderYt) << "Empty table : " << tables[idx.first].Table() << ", modify time: " << strModifyTime << ", revision: " << statInfo->Revision;
                }

                bool schemaValid = ValidateTableSchema(
                    tables[idx.first].Table(), attrs,
                    tables[idx.first].IgnoreYamrDsv(), tables[idx.first].IgnoreWeakSchema()
                );

                metaInfo->YqlCompatibleScheme = schemaValid;

                TransferTableAttributes(attrs, [metaInfo] (const TString& name, const TString& value) {
                    metaInfo->Attrs[name] = value;
                });

                if (attrs.AsMap().contains("erasure_codec") && attrs["erasure_codec"].AsString() != "none") {
                    metaInfo->Attrs["erasure_codec"] = attrs["erasure_codec"].AsString();
                }
                if (attrs.AsMap().contains("optimize_for") && attrs["optimize_for"].AsString() != "scan") {
                    metaInfo->Attrs["optimize_for"] = attrs["optimize_for"].AsString();
                }
                if (attrs.AsMap().contains("schema_mode") && attrs["schema_mode"].AsString() == "weak") {
                    metaInfo->Attrs["schema_mode"] = attrs["schema_mode"].AsString();
                }
                if (attrs.AsMap().contains(SecurityTagsName)) {
                    TVector<TString> securityTags;
                    for (const auto& tag : attrs[SecurityTagsName].AsList()) {
                        securityTags.push_back(tag.AsString());
                    }
                    if (!securityTags.empty()) {
                        metaInfo->Attrs[SecurityTagsName] = JoinSeq(';', securityTags);
                    }
                }

                NYT::TNode schemaAttrs;
                if (tables[idx.first].ForceInferSchema() && tables[idx.first].InferSchemaRows() > 0) {
                    metaInfo->Attrs.erase(YqlRowSpecAttribute);
                    if (isDynamic) {
                        schemaAttrs = GetSchemaFromAttributes(attrs, false, tables[idx.first].IgnoreWeakSchema());
                    } else if (!statInfo->IsEmpty()) {
                        idxsToInferFromContent.push_back(idx);
                    }
                } else {
                    if (attrs.HasKey(QB2Premapper)) {
                        metaInfo->Attrs[QB2Premapper] = NYT::NodeToYsonString(attrs[QB2Premapper], NYT::NYson::EYsonFormat::Text);
                        metaInfo->Attrs[TString{YqlRowSpecAttribute}.append("_qb2")] = NYT::NodeToYsonString(
                            QB2PremapperToRowSpec(attrs[QB2Premapper], attrs[SCHEMA_ATTR_NAME]), NYT::NYson::EYsonFormat::Text);
                    }

                    if (schemaValid) {
                        schemaAttrs = GetSchemaFromAttributes(attrs, false, tables[idx.first].IgnoreWeakSchema());
                    } else if (!attrs.HasKey(YqlRowSpecAttribute) && !isDynamic && tables[idx.first].InferSchemaRows() > 0 && !statInfo->IsEmpty()) {
                        idxsToInferFromContent.push_back(idx);
                    }
                }

                if (!schemaAttrs.IsUndefined()) {
                    for (auto& item: schemaAttrs.AsMap()) {
                        metaInfo->Attrs[item.first] = NYT::NodeToYsonString(item.second, NYT::NYson::EYsonFormat::Text);
                    }
                }
            } catch (const TErrorException& e) {
                throw;
            } catch (...) {
                throw yexception() << "Error loading '" << tables[idx.first].Table() << "' table metadata: " << CurrentExceptionMessage();
            }
        }
        if (batchRes) {
            batchGet->ExecuteBatch();
            WaitExceptionOrAll(batchRes).GetValue();
        }

        if (idxsToInferFromContent) {
            TString tmpFolder = GetTablesTmpFolder(*execCtx->Options_.Config());
            TString tmpTablePath = NYql::TransformPath(tmpFolder,
                TStringBuilder() << "tmp/" << GetGuidAsString(execCtx->Session_->RandomProvider_->GenGuid()), true, execCtx->Session_->UserName_);

            auto inferResult = ExecInferSchema(tmpTablePath, execCtx, entry, tx, idxsToInferFromContent, tables);
            for (size_t i : xrange(idxsToInferFromContent.size())) {
                size_t idx = idxsToInferFromContent[i].first;
                NYT::TNode& attrs = attributes[idx];
                TYtTableMetaInfo::TPtr metaInfo = result.Data[idx].Meta;

                if (auto inferSchema = inferResult[i]) {
                    NYT::TNode schemaAttrs;
                    if (tables[idx].ForceInferSchema()) {
                        schemaAttrs = GetSchemaFromAttributes(attrs, true, tables[idx].IgnoreWeakSchema());
                    }
                    schemaAttrs[INFER_SCHEMA_ATTR_NAME] = *inferSchema;
                    for (auto& item: schemaAttrs.AsMap()) {
                        metaInfo->Attrs[item.first] = NYT::NodeToYsonString(item.second, NYT::NYson::EYsonFormat::Text);
                    }
                    metaInfo->InferredScheme = true;
                }
            }
        }
    }

    using TMaybeSchema = TMaybe<NYT::TNode>;
    static TVector<TMaybeSchema> ExecInferSchema(const TString& tmpTable,
        const TExecContext<TGetTableInfoOptions>::TPtr& execCtx,
        const TTransactionCache::TEntry::TPtr& entry,
        const ITransactionPtr& tx,
        const TVector<std::pair<size_t, TString>>& idxs,
        const TVector<TTableReq>& tables)
    {
        size_t jobThreshold = execCtx->Options_.Config()->InferSchemaTableCountThreshold.Get().GetOrElse(Max<ui32>());

        TVector<TMaybeSchema> result;
        if (idxs.size() <= jobThreshold) {
            result.reserve(idxs.size());
            auto mode = execCtx->Options_.Config()->InferSchemaMode.Get().GetOrElse(EInferSchemaMode::Sequential);
            if (EInferSchemaMode::Sequential == mode) {
                for (auto& idx : idxs) {
                    YQL_ENSURE(tables[idx.first].InferSchemaRows() > 0);
                    result.push_back(InferSchemaFromTableContents(tx, idx.second, tables[idx.first].Table(), tables[idx.first].InferSchemaRows()));
                }
                return result;
            }
            if (EInferSchemaMode::RPC == mode) {
#ifdef __linux__
                std::vector<TTableInferSchemaRequest> requests;
                requests.reserve(idxs.size());
                for (auto& idx : idxs) {
                    YQL_ENSURE(tables[idx.first].InferSchemaRows() > 0);
                    requests.push_back({idx.second, tables[idx.first].Table(), tables[idx.first].InferSchemaRows()});
                }
                return InferSchemaFromTablesContents(execCtx->YtServer_, execCtx->GetAuth(), tx->GetId(), requests, execCtx->Session_->Queue_);
#else
                ythrow yexception() << "Unimplemented RPC reader on non-linux platforms";
#endif
            }
            result.resize(idxs.size());
            std::vector<NThreading::TFuture<void>> futures;
            futures.reserve(idxs.size());
            size_t i = 0;
            for (auto& idx : idxs) {
                YQL_ENSURE(tables[idx.first].InferSchemaRows() > 0);
                futures.push_back(execCtx->Session_->Queue_->Async([i, idx, &result, &tables, &tx](){
                        YQL_CLOG(INFO, ProviderYt) << "Infering schema for table '" << tables[idx.first].Table() << "'";
                        result[i] = InferSchemaFromTableContents(tx, idx.second, tables[idx.first].Table(), tables[idx.first].InferSchemaRows());
                    }));
                ++i;
            }
            (NThreading::WaitAll(futures)).Wait();
            return result;
        }

        YQL_ENSURE(!idxs.empty());

        TRawMapOperationSpec mapOpSpec;
        mapOpSpec.Format(TFormat::YsonBinary());
        auto job = MakeIntrusive<TYqlInferSchemaJob>();

        {
            TUserJobSpec userJobSpec;
            FillUserJobSpec(userJobSpec, execCtx, {}, 0, 0, false);
            mapOpSpec.MapperSpec(userJobSpec);
        }

        TVector<TString> inputTables;
        for (auto& idx : idxs) {
            YQL_ENSURE(tables[idx.first].InferSchemaRows() > 0);
            inputTables.push_back(tables[idx.first].Table());
            auto path = NYT::TRichYPath(idx.second)
                .AddRange(NYT::TReadRange::FromRowIndices(0, tables[idx.first].InferSchemaRows()));
            mapOpSpec.AddInput(path);
        }
        mapOpSpec.AddOutput(tmpTable);
        job->SetTableNames(inputTables);

        FillOperationSpec(mapOpSpec, execCtx);

        NYT::TNode spec = execCtx->Session_->CreateSpecWithDesc(execCtx->CodeSnippets_);
        FillSpec(spec, *execCtx, entry, 0., Nothing(), EYtOpProp::WithMapper);
        spec["job_count"] = 1;

        TOperationOptions opOpts;
        FillOperationOptions(opOpts, execCtx, entry);
        opOpts.StartOperationMode(TOperationOptions::EStartOperationMode::AsyncPrepare).Spec(spec);

        auto tmpTx = tx->StartTransaction();
        PrepareTempDestination(tmpTable, execCtx, entry, tmpTx);

        execCtx->RunOperation<false>([tmpTx, job, mapOpSpec = std::move(mapOpSpec), opOpts = std::move(opOpts)](){
            return tmpTx->RawMap(mapOpSpec, job, opOpts);
        }).GetValueSync();

        result.resize(idxs.size());
        auto reader = tmpTx->CreateTableReader<NYT::TNode>(tmpTable);
        for (; reader->IsValid(); reader->Next()) {
            auto& row = reader->GetRow();
            size_t tableIdx = row["index"].AsUint64();
            YQL_ENSURE(tableIdx < idxs.size());

            auto schema = NYT::NodeFromYsonString(row["schema"].AsString());
            if (schema.IsString()) {
                YQL_LOG_CTX_THROW yexception() << schema.AsString();
            }
            result[tableIdx] = schema;
        }
        reader.Drop();
        tmpTx->Abort();
        return result;
    }

    static std::pair<TString, NYT::TNode> ParseYTType(const TExprNode& node,
        TExprContext& ctx,
        const TExecContext<TResOrPullOptions>::TPtr& execCtx,
        const TMaybe<NYql::TColumnOrder>& columns = Nothing())
    {
        const auto sequenceItemType = GetSequenceItemType(node.Pos(), node.GetTypeAnn(), false, ctx);

        auto rowSpecInfo = MakeIntrusive<TYqlRowSpecInfo>();
        rowSpecInfo->SetType(sequenceItemType->Cast<TStructExprType>(), execCtx->Options_.Config()->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES) ? NTCF_ALL : NTCF_NONE);
        if (columns) {
            rowSpecInfo->SetColumnOrder(columns);
        }

        NYT::TNode tableSpec = NYT::TNode::CreateMap();
        rowSpecInfo->FillCodecNode(tableSpec[YqlRowSpecAttribute]);

        auto resultYTType = NodeToYsonString(RowSpecToYTSchema(tableSpec[YqlRowSpecAttribute], execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY)).ToNode());
        auto resultRowSpec = NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, NYT::TNode::CreateList().Add(tableSpec));
        return {resultYTType, resultRowSpec};
    }

    TFuture<TResOrPullResult> DoPull(const TSession::TPtr& session, NNodes::TPull pull, TExprContext& ctx, TResOrPullOptions&& options) {
        if (options.FillSettings().Discard) {
            TResOrPullResult res;
            res.SetSuccess();
            return MakeFuture(res);
        }
        TVector<TString> columns(NCommon::GetResOrPullColumnHints(pull.Ref()));
        if (columns.empty()) {
            columns = NCommon::GetStructFields(pull.Input().Ref().GetTypeAnn());
        }

        bool ref = NCommon::HasResOrPullOption(pull.Ref(), "ref");
        bool autoRef = NCommon::HasResOrPullOption(pull.Ref(), "autoref");

        auto cluster = TString{GetClusterName(pull.Input())};
        auto execCtx = MakeExecCtx(std::move(options), session, cluster, pull.Raw(), &ctx);

        if (auto read = pull.Input().Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
            execCtx->SetInput(read.Cast().Input(), false, {});
        } else {
            execCtx->SetInput(pull.Input(), false, {});
        }

        TRecordsRange range;
        if (!ref) {
            if (auto read = pull.Input().Maybe<TCoRight>().Input().Maybe<TYtReadTable>()) {
                YQL_ENSURE(read.Cast().Input().Size() == 1);
                range.Fill(read.Cast().Input().Item(0).Settings().Ref());
            }
        }

        TString type;
        NYT::TNode rowSpec;
        if (execCtx->Options_.FillSettings().Format == IDataProvider::EResultFormat::Skiff) {
            auto ytType =  ParseYTType(pull.Input().Ref(), ctx, execCtx, columns);

            type = ytType.first;
            rowSpec = ytType.second;
        } else if (NCommon::HasResOrPullOption(pull.Ref(), "type")) {
            TStringStream typeYson;
            ::NYson::TYsonWriter typeWriter(&typeYson);
            NCommon::WriteResOrPullType(typeWriter, pull.Input().Ref().GetTypeAnn(), columns);
            type = typeYson.Str();
        }

        auto pos = ctx.GetPosition(pull.Pos());

        return session->Queue_->Async([rowSpec, type, ref, range, autoRef, execCtx, columns, pos] () {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            execCtx->MakeUserFiles();
            try {
                TResOrPullResult res;
                TStringStream out;

                auto fillSettings = execCtx->Options_.FillSettings();
                fillSettings.Format = IDataProvider::EResultFormat::Yson;

                ::NYson::TYsonWriter writer(&out, NCommon::GetYsonFormat(fillSettings), ::NYson::EYsonType::Node, false);
                writer.OnBeginMap();

                if (type) {
                    writer.OnKeyedItem("Type");
                    writer.OnRaw(type);
                }

                bool truncated = false;
                if (!ref) {
                    truncated = ExecPull(execCtx, writer, range, rowSpec, columns);
                }

                if (ref || (truncated && autoRef)) {
                    writer.OnKeyedItem("Ref");
                    writer.OnBeginList();
                    TVector<TString> keepTables;
                    for (auto& table: execCtx->InputTables_) {
                        writer.OnListItem();
                        if (table.Temp) {
                            keepTables.push_back(table.Name);
                        }
                        NYql::WriteTableReference(writer, YtProviderName, execCtx->Cluster_, table.Name, table.Temp, columns);
                    }
                    writer.OnEndList();
                    if (!keepTables.empty()) {
                        auto entry = execCtx->GetEntry();
                        // TODO: check anonymous tables
                        entry->CancelDeleteAtFinalize(keepTables);
                    }
                }

                if (truncated) {
                    writer.OnKeyedItem("Truncated");
                    writer.OnBooleanScalar(true);
                }

                writer.OnEndMap();
                res.Data = out.Str();
                res.SetSuccess();

                return res;
            } catch (...) {
                return ResultFromCurrentException<TResOrPullResult>(pos);
            }
        });
    }

    static bool ExecPull(const TExecContext<TResOrPullOptions>::TPtr& execCtx,
        ::NYson::TYsonWriter& writer,
        const TRecordsRange& range,
        const NYT::TNode& rowSpec,
        const TVector<TString>& columns)
    {
        TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
            execCtx->FunctionRegistry_->SupportsSizedAllocators());
        alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
        TMemoryUsageInfo memInfo("Pull");
        TTypeEnvironment env(alloc);
        THolderFactory holderFactory(alloc.Ref(), memInfo, execCtx->FunctionRegistry_);
        NCommon::TCodecContext codecCtx(env, *execCtx->FunctionRegistry_, &holderFactory);

        bool useSkiff = execCtx->Options_.Config()->UseSkiff.Get(execCtx->Cluster_).GetOrElse(DEFAULT_USE_SKIFF);

        const bool testRun = execCtx->Config_->GetLocalChainTest();

        TVector<TString> tables;
        for (const TInputInfo& table: execCtx->InputTables_) {
            auto tablePath = table.Path;
            tables.push_back(table.Temp ? TString() : table.Name);
        }

        TMkqlIOSpecs specs;
        if (useSkiff) {
            specs.SetUseSkiff(execCtx->Options_.OptLLVM(), testRun ? TMkqlIOSpecs::ESystemField(0) : TMkqlIOSpecs::ESystemField::RangeIndex | TMkqlIOSpecs::ESystemField::RowIndex);
        }
        const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);
        specs.Init(codecCtx, execCtx->GetInputSpec(!useSkiff, nativeTypeCompat, false), tables, columns);

        auto run = [&] (IExecuteResOrPull& pullData)  {
            TMkqlIOCache specsCache(specs, holderFactory);

            if (testRun) {
                YQL_ENSURE(execCtx->InputTables_.size() == 1U, "Support single input only.");
                const auto itI = TestTables.find(execCtx->InputTables_.front().Path.Path_);
                YQL_ENSURE(TestTables.cend() != itI);

                TMkqlInput input(MakeStringInput(std::move(itI->second.second), false));
                TMkqlReaderImpl reader(input, 0, 4 << 10, 0);
                reader.SetSpecs(specs, holderFactory);
                for (reader.Next(); reader.IsValid(); reader.Next()) {
                    if (!pullData.WriteNext(specsCache, reader.GetRow(), 0)) {
                        return true;
                    }
                }
            } else  if (auto limiter = TTableLimiter(range)) {
                auto entry = execCtx->GetEntry();
                bool stop = false;
                for (size_t i = 0; i < execCtx->InputTables_.size(); ++i) {
                    TString srcTableName = execCtx->InputTables_[i].Name;
                    NYT::TRichYPath srcTable = execCtx->InputTables_[i].Path;
                    bool isDynamic = execCtx->InputTables_[i].Dynamic;
                    ui64 recordsCount = execCtx->InputTables_[i].Records;
                    if (!isDynamic) {
                        if (!limiter.NextTable(recordsCount)) {
                            continue;
                        }
                    } else {
                        limiter.NextDynamicTable();
                    }

                    if (isDynamic) {
                        YQL_ENSURE(srcTable.GetRanges().Empty());
                        stop = NYql::SelectRows(entry->Client, srcTableName, i, specsCache, pullData, limiter);
                    } else {
                        auto readTx = entry->Tx;
                        if (srcTable.TransactionId_) {
                            readTx = entry->GetSnapshotTx(*srcTable.TransactionId_);
                            srcTable.TransactionId_.Clear();
                        }
                        if (execCtx->YamrInput) {
                            stop = NYql::IterateYamredRows(readTx, srcTable, i, specsCache, pullData, limiter, execCtx->Sampling);
                        } else {
                            stop = NYql::IterateYsonRows(readTx, srcTable, i, specsCache, pullData, limiter, execCtx->Sampling);
                        }
                    }
                    if (stop || limiter.Exceed()) {
                        break;
                    }
                }
            }
            return false;
        };

        switch (execCtx->Options_.FillSettings().Format) {
            case IDataProvider::EResultFormat::Yson: {
                TYsonExecuteResOrPull pullData(execCtx->Options_.FillSettings().RowsLimitPerWrite,
                    execCtx->Options_.FillSettings().AllResultsBytesLimit, MakeMaybe(columns));

                if (run(pullData)) {
                    return true;
                }
                specs.Clear();

                writer.OnKeyedItem("Data");
                writer.OnBeginList();
                writer.OnRaw(pullData.Finish(), ::NYson::EYsonType::ListFragment);
                writer.OnEndList();
                return pullData.IsTruncated();
            }
            case IDataProvider::EResultFormat::Skiff: {
                THashMap<TString, ui32> structColumns;
                for (size_t index = 0; index < columns.size(); index++) {
                    structColumns.emplace(columns[index], index);
                }

                auto skiffNode = SingleTableSpecToInputSkiff(rowSpec[YqlIOSpecTables][0], structColumns, false, false, false);

                writer.OnKeyedItem("SkiffType");
                writer.OnRaw(NodeToYsonString(skiffNode), ::NYson::EYsonType::Node);

                TSkiffExecuteResOrPull pullData(execCtx->Options_.FillSettings().RowsLimitPerWrite,
                    execCtx->Options_.FillSettings().AllResultsBytesLimit,
                    codecCtx,
                    holderFactory,
                    rowSpec,
                    execCtx->Options_.OptLLVM(),
                    columns);

                if (run(pullData)) {
                    return true;
                }
                specs.Clear();

                writer.OnKeyedItem("Data");
                writer.OnStringScalar(pullData.Finish());

                return pullData.IsTruncated();
            }
            default:
                YQL_LOG_CTX_THROW yexception() << "Invalid result type: " << execCtx->Options_.FillSettings().Format;
            }
    }

    TFuture<TResOrPullResult> DoResult(const TSession::TPtr& session, NNodes::TResult result, TExprContext& ctx, TResOrPullOptions&& options) {
        TVector<TString> columns(NCommon::GetResOrPullColumnHints(result.Ref()));
        if (columns.empty()) {
            columns = NCommon::GetStructFields(result.Input().Ref().GetTypeAnn());
        }

        TString lambda;
        bool hasListResult = false;
        {
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                Services_.FunctionRegistry->SupportsSizedAllocators());
            alloc.SetLimit(options.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            TNativeYtLambdaBuilder builder(alloc, Services_, *session);
            auto rootNode = builder.BuildLambda(*MkqlCompiler_, result.Input().Ptr(), ctx);
            hasListResult = rootNode.GetStaticType()->IsList();
            lambda = SerializeRuntimeNode(rootNode, builder.GetTypeEnvironment());
        }

        auto extraUsage = ScanExtraResourceUsage(result.Input().Ref(), *options.Config());

        TString cluster = options.UsedCluster();
        if (cluster.empty()) {
            cluster = options.Config()->DefaultCluster.Get().GetOrElse(TString());
        }
        if (cluster.empty()) {
            cluster = Clusters_->GetDefaultClusterName();
        }
        TString tmpFolder = GetTablesTmpFolder(*options.Config());
        TString tmpTablePath = NYql::TransformPath(tmpFolder,
            TStringBuilder() << "tmp/" << GetGuidAsString(session->RandomProvider_->GenGuid()), true, session->UserName_);
        bool discard = options.FillSettings().Discard;
        auto execCtx = MakeExecCtx(std::move(options), session, cluster, result.Input().Raw(), &ctx);
        auto pos = ctx.GetPosition(result.Pos());

        TString type, skiffType;
        NYT::TNode rowSpec;
        if (execCtx->Options_.FillSettings().Format == IDataProvider::EResultFormat::Skiff) {
            auto ytType =  ParseYTType(result.Input().Ref(), ctx, execCtx);

            type = ytType.first;
            rowSpec = ytType.second;
            skiffType = NodeToYsonString(TablesSpecToOutputSkiff(rowSpec));
        } else if (NCommon::HasResOrPullOption(result.Ref(), "type")) {
            TStringStream typeYson;
            ::NYson::TYsonWriter typeWriter(&typeYson);
            NCommon::WriteResOrPullType(typeWriter, result.Input().Ref().GetTypeAnn(), columns);
            type = typeYson.Str();
        }

        return session->Queue_->Async([lambda, hasListResult, extraUsage, tmpTablePath, execCtx, columns, rowSpec] () {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            execCtx->MakeUserFiles();

            switch (execCtx->Options_.FillSettings().Format) {
                case IDataProvider::EResultFormat::Yson:
                    return ExecCalc(lambda, extraUsage, tmpTablePath, execCtx, {},
                        TYsonExprResultFactory(execCtx->Options_.FillSettings().RowsLimitPerWrite,
                            execCtx->Options_.FillSettings().AllResultsBytesLimit,
                            columns,
                            hasListResult),
                        &columns,
                        execCtx->Options_.FillSettings().Format);
                case IDataProvider::EResultFormat::Skiff:
                    return ExecCalc(lambda, extraUsage, tmpTablePath, execCtx, {},
                        TSkiffExprResultFactory(execCtx->Options_.FillSettings().RowsLimitPerWrite,
                            execCtx->Options_.FillSettings().AllResultsBytesLimit,
                            hasListResult,
                            rowSpec,
                            execCtx->Options_.OptLLVM()),
                        &columns,
                        execCtx->Options_.FillSettings().Format);
                default:
                    YQL_LOG_CTX_THROW yexception() << "Invalid result type: " << execCtx->Options_.FillSettings().Format;
            }
        })
        .Apply([skiffType, type, execCtx, discard, pos, columns] (const TFuture<std::pair<TString, bool>>& f) {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            try {
                const std::pair<TString, bool>& value = f.GetValue();

                TResOrPullResult res;
                TStringStream out;

                auto fillSettings = execCtx->Options_.FillSettings();
                fillSettings.Format = IDataProvider::EResultFormat::Yson;

                ::NYson::TYsonWriter writer(discard ? (IOutputStream*)&Cnull : (IOutputStream*)&out, NCommon::GetYsonFormat(fillSettings), ::NYson::EYsonType::Node, true);
                writer.OnBeginMap();

                if (skiffType) {
                    writer.OnKeyedItem("SkiffType");
                    writer.OnRaw(skiffType, ::NYson::EYsonType::Node);


                    writer.OnKeyedItem("Columns");
                    writer.OnBeginList();
                    for (auto& column: columns) {
                        writer.OnListItem();
                        writer.OnStringScalar(column);
                    }
                    writer.OnEndList();
                }

                if (type) {
                    writer.OnKeyedItem("Type");
                    writer.OnRaw(type);
                }

                writer.OnKeyedItem("Data");
                switch (execCtx->Options_.FillSettings().Format) {
                    case IDataProvider::EResultFormat::Yson:
                        writer.OnRaw(value.first);
                        break;
                    case IDataProvider::EResultFormat::Skiff:
                        writer.OnStringScalar(value.first);
                        break;
                    default:
                        YQL_LOG_CTX_THROW yexception() << "Invalid result type: " << execCtx->Options_.FillSettings().Format;
                }

                if (value.second) {
                    writer.OnKeyedItem("Truncated");
                    writer.OnBooleanScalar(true);
                }

                writer.OnEndMap();
                if (!discard) {
                    res.Data = out.Str();
                }
                res.SetSuccess();

                return res;
            } catch (...) {
                return ResultFromCurrentException<TResOrPullResult>(pos);
            }
        });
    }

    TFuture<void> DoSort(TYtSort /*sort*/, const TExecContext<TRunOptions>::TPtr& execCtx) {
        YQL_ENSURE(execCtx->OutTables_.size() == 1);

        return execCtx->Session_->Queue_->Async([execCtx]() {
            return execCtx->LookupQueryCacheAsync().Apply([execCtx] (const auto& f) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                auto entry = execCtx->GetEntry();
                bool cacheHit = f.GetValue();
                TVector<TRichYPath> outYPaths = PrepareDestinations(execCtx->OutTables_, execCtx, entry, !cacheHit);
                if (cacheHit) {
                    execCtx->QueryCacheItem.Destroy();
                    return MakeFuture();
                }

                bool hasNonStrict = false;
                TSortOperationSpec sortOpSpec;
                for (const auto& table: execCtx->InputTables_) {
                    if (!table.Strict) {
                        hasNonStrict = true;
                    }
                    sortOpSpec.AddInput(table.Path);
                }

                sortOpSpec.Output(outYPaths.front());
                sortOpSpec.SortBy(execCtx->OutTables_.front().SortedBy);
                sortOpSpec.SchemaInferenceMode(ESchemaInferenceMode::FromOutput);

                NYT::TNode spec = execCtx->Session_->CreateSpecWithDesc(execCtx->CodeSnippets_);

                FillSpec(spec, *execCtx, entry, 0., Nothing(), EYtOpProp::IntermediateData);
                if (hasNonStrict) {
                    spec["schema_inference_mode"] = "from_output"; // YTADMINREQ-17692
                }

                return execCtx->RunOperation([entry, sortOpSpec = std::move(sortOpSpec), spec = std::move(spec)](){
                    return entry->Tx->Sort(sortOpSpec, TOperationOptions().StartOperationMode(TOperationOptions::EStartOperationMode::AsyncPrepare).Spec(spec));
                });
            });
        });
    }

    TFuture<void> DoCopy(TYtCopy /*copy*/, const TExecContext<TRunOptions>::TPtr& execCtx) {
        YQL_ENSURE(execCtx->InputTables_.size() == 1);
        YQL_ENSURE(execCtx->InputTables_.front().Temp);
        YQL_ENSURE(execCtx->OutTables_.size() == 1);

        return execCtx->Session_->Queue_->Async([execCtx]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            auto entry = execCtx->GetEntry();
            execCtx->QueryCacheItem.Destroy(); // Don't use cache for YtCopy
            TOutputInfo& out = execCtx->OutTables_.front();

            entry->DeleteAtFinalize(out.Path);

            entry->CreateDefaultTmpFolder();
            CreateParents({out.Path}, entry->CacheTx);
            entry->Tx->Copy(execCtx->InputTables_.front().Name, out.Path, TCopyOptions().Force(true));

        });
    }

    TFuture<void> DoMerge(TYtMerge merge, const TExecContext<TRunOptions>::TPtr& execCtx) {
        YQL_ENSURE(execCtx->OutTables_.size() == 1);
        bool forceTransform = NYql::HasSetting(merge.Settings().Ref(), EYtSettingType::ForceTransform);
        bool combineChunks = NYql::HasSetting(merge.Settings().Ref(), EYtSettingType::CombineChunks);
        TMaybe<ui64> limit = GetLimit(merge.Settings().Ref());

        return execCtx->Session_->Queue_->Async([forceTransform, combineChunks, limit, execCtx]() {
            return execCtx->LookupQueryCacheAsync().Apply([forceTransform, combineChunks, limit, execCtx] (const auto& f) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                auto entry = execCtx->GetEntry();
                bool cacheHit = f.GetValue();
                TVector<TRichYPath> outYPaths = PrepareDestinations(execCtx->OutTables_, execCtx, entry, !cacheHit);
                if (cacheHit) {
                    execCtx->QueryCacheItem.Destroy();
                    return MakeFuture();
                }

                bool hasNonStrict = false;
                TMergeOperationSpec mergeOpSpec;
                for (const auto& table: execCtx->InputTables_) {
                    if (!table.Strict) {
                        hasNonStrict = true;
                    }
                    mergeOpSpec.AddInput(table.Path);
                }

                if (execCtx->OutTables_.front().SortedBy.Parts_.empty()) {
                    mergeOpSpec.Mode(EMergeMode::MM_ORDERED);
                    if (limit) {
                        outYPaths.front().RowCountLimit(*limit);
                    }
                } else {
                    mergeOpSpec.Mode(EMergeMode::MM_SORTED);
                    mergeOpSpec.MergeBy(execCtx->OutTables_.front().SortedBy);
                }

                mergeOpSpec.Output(outYPaths.front());

                mergeOpSpec.ForceTransform(forceTransform);
                mergeOpSpec.CombineChunks(combineChunks);
                mergeOpSpec.SchemaInferenceMode(ESchemaInferenceMode::FromOutput);

                NYT::TNode spec = execCtx->Session_->CreateSpecWithDesc(execCtx->CodeSnippets_);
                EYtOpProps flags = EYtOpProp::AllowSampling;
                if (combineChunks) {
                    flags |= EYtOpProp::TemporaryChunkCombine;
                }
                FillSpec(spec, *execCtx, entry, 0., Nothing(), flags);
                if (hasNonStrict) {
                    spec["schema_inference_mode"] = "from_output"; // YTADMINREQ-17692
                }

                return execCtx->RunOperation([entry, mergeOpSpec = std::move(mergeOpSpec), spec = std::move(spec)](){
                    return entry->Tx->Merge(mergeOpSpec, TOperationOptions().StartOperationMode(TOperationOptions::EStartOperationMode::AsyncPrepare).Spec(spec));
                });
            });
        });
    }

    static TFuture<void> ExecMap(
        bool ordered,
        const TMaybe<ui64>& jobCount,
        const TMaybe<ui64>& limit,
        const TVector<TString>& sortLimitBy,
        TString mapLambda,
        const TString& inputType,
        const TExpressionResorceUsage& extraUsage,
        const TExecContext<TRunOptions>::TPtr& execCtx
    ) {
        const bool testRun = execCtx->Config_->GetLocalChainTest();
        TFuture<bool> ret = testRun ? MakeFuture<bool>(false) : execCtx->LookupQueryCacheAsync();
        return ret.Apply([ordered, jobCount, limit, sortLimitBy, mapLambda,
                          inputType, extraUsage, execCtx, testRun] (const auto& f) mutable
        {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            TTransactionCache::TEntry::TPtr entry;
            TVector<TRichYPath> outYPaths;
            if (testRun) {
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "Test mode support single output only.");
                const auto& out = execCtx->OutTables_.front();
                outYPaths.emplace_back(TRichYPath(out.Path).Schema(RowSpecToYTSchema(TestTables[out.Path].first = out.Spec[YqlRowSpecAttribute], NTCF_NONE)));
            } else {
                entry = execCtx->GetEntry();
                bool cacheHit = f.GetValue();
                outYPaths = PrepareDestinations(execCtx->OutTables_, execCtx, entry, !cacheHit);
                if (cacheHit) {
                    execCtx->QueryCacheItem.Destroy();
                    return MakeFuture();
                }
            }

            TRawMapOperationSpec mapOpSpec;
            auto job = MakeIntrusive<TYqlUserJob>();

            job->SetInputType(inputType);

            for (size_t i: xrange(execCtx->OutTables_.size())) {
                if (!execCtx->OutTables_[i].SortedBy.Parts_.empty()) {
                    mapOpSpec.Ordered(true);
                }
                else if (limit && sortLimitBy.empty()) {
                    outYPaths[i].RowCountLimit(*limit);
                }
                mapOpSpec.AddOutput(outYPaths[i]);
            }

            TVector<ui32> groups;
            TVector<TString> tables;
            TVector<ui64> rowOffsets;
            ui64 currentRowOffset = 0;
            TSet<TString> remapperAllFiles;
            TRemapperMap remapperMap;

            bool useSkiff = execCtx->Options_.Config()->UseSkiff.Get(execCtx->Cluster_).GetOrElse(DEFAULT_USE_SKIFF);
            bool hasTablesWithoutQB2Premapper = false;

            for (const TInputInfo& table: execCtx->InputTables_) {
                auto tablePath = table.Path;
                if (!table.QB2Premapper.IsUndefined()) {
                    bool tableUseSkiff = false;

                    ProcessTableQB2Premapper(table.QB2Premapper, table.Name, tablePath, mapOpSpec.GetInputs().size(),
                        remapperMap, remapperAllFiles, tableUseSkiff);

                    useSkiff = useSkiff && tableUseSkiff;
                }
                else {
                    hasTablesWithoutQB2Premapper = true;
                }

                if (!groups.empty() && groups.back() != table.Group) {
                    currentRowOffset = 0;
                }

                mapOpSpec.AddInput(tablePath);
                groups.push_back(table.Group);
                tables.push_back(table.Temp ? TString() : table.Name);
                rowOffsets.push_back(currentRowOffset);
                currentRowOffset += table.Records;
            }

            bool forceYsonInputFormat = false;

            if (useSkiff && !remapperMap.empty()) {
                // Disable skiff in case of mix of QB2 and normal tables
                if (hasTablesWithoutQB2Premapper) {
                    useSkiff = false;
                } else {
                    UpdateQB2PremapperUseSkiff(remapperMap, useSkiff);
                    forceYsonInputFormat = useSkiff;
                }
            }

            const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);
            job->SetInputSpec(execCtx->GetInputSpec(!useSkiff || forceYsonInputFormat, nativeTypeCompat, false));
            job->SetOutSpec(execCtx->GetOutSpec(!useSkiff, nativeTypeCompat));
            if (!groups.empty() && groups.back() != 0) {
                job->SetInputGroups(groups);
            }
            job->SetTableNames(tables);
            job->SetRowOffsets(rowOffsets);

            if (ordered) {
                mapOpSpec.Ordered(true);
            }

            job->SetYamrInput(execCtx->YamrInput);
            job->SetUseSkiff(useSkiff, testRun ? TMkqlIOSpecs::ESystemField(0) : TMkqlIOSpecs::ESystemField::RowIndex);

            auto tmpFiles = std::make_shared<TTempFiles>(execCtx->FileStorage_->GetTemp());
            {
                TUserJobSpec userJobSpec;
                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                    execCtx->FunctionRegistry_->SupportsSizedAllocators());
                alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                TNativeYtLambdaBuilder builder(alloc, execCtx->FunctionRegistry_, *execCtx->Session_);
                TProgramBuilder pgmBuilder(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_);
                TGatewayTransformer transform(execCtx, entry, pgmBuilder, *tmpFiles);
                size_t nodeCount = 0;
                builder.UpdateLambdaCode(mapLambda, nodeCount, transform);
                if (nodeCount > execCtx->Options_.Config()->LLVMNodeCountLimit.Get(execCtx->Cluster_).GetOrElse(DEFAULT_LLVM_NODE_COUNT_LIMIT)) {
                    execCtx->Options_.OptLLVM("OFF");
                }
                job->SetLambdaCode(mapLambda);
                job->SetOptLLVM(execCtx->Options_.OptLLVM());
                job->SetUdfValidateMode(execCtx->Options_.UdfValidateMode());
                transform.ApplyJobProps(*job);
                transform.ApplyUserJobSpec(userJobSpec, testRun);

                ui64 fileMemUsage = transform.GetUsedMemory();
                for (auto& f: remapperAllFiles) {
                    fileMemUsage += GetUncompressedFileSize(entry->Tx, f).GetOrElse(i64(1) << 10);
                    userJobSpec.AddFile(TRichYPath(f).Executable(true));
                }
                if (!remapperMap.empty()) {
                    fileMemUsage += 512_MB;
                }

                FillUserJobSpec(userJobSpec, execCtx, extraUsage, fileMemUsage, execCtx->EstimateLLVMMem(nodeCount), testRun,
                    GetQB2PremapperPrefix(remapperMap, useSkiff));

                mapOpSpec.MapperSpec(userJobSpec);
            }
            FillOperationSpec(mapOpSpec, execCtx);
            auto formats = job->GetIOFormats(execCtx->FunctionRegistry_);
            mapOpSpec.InputFormat(forceYsonInputFormat ? NYT::TFormat::YsonBinary() : formats.first);
            mapOpSpec.OutputFormat(formats.second);

            if (testRun) {
                YQL_ENSURE(execCtx->InputTables_.size() == 1U, "Support single input only.");
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "Support single output only.");

                const auto itI = TestTables.find(execCtx->InputTables_.front().Path.Path_);
                YQL_ENSURE(TestTables.cend() != itI);
                const auto itO = TestTables.find(execCtx->OutTables_.front().Path);
                YQL_ENSURE(TestTables.cend() != itO);

                TStringInput in(itI->second.second);
                TStringOutput out(itO->second.second);

                LocalRawMapReduce(mapOpSpec, job.Get(), &in, &out);
                DumpLocalTable(itO->second.second, execCtx->Config_->GetLocalChainFile());
                return MakeFuture();
            }

            NYT::TNode spec = execCtx->Session_->CreateSpecWithDesc(execCtx->CodeSnippets_);
            FillSpec(spec, *execCtx, entry, extraUsage.Cpu, Nothing(),
                EYtOpProp::TemporaryAutoMerge | EYtOpProp::WithMapper | EYtOpProp::WithUserJobs | EYtOpProp::AllowSampling);

            if (jobCount) {
                spec["job_count"] = static_cast<i64>(*jobCount);
            }

            TOperationOptions opOpts;
            FillOperationOptions(opOpts, execCtx, entry);
            opOpts.StartOperationMode(TOperationOptions::EStartOperationMode::AsyncPrepare).Spec(spec);

            return execCtx->RunOperation([entry, execCtx, job, mapOpSpec = std::move(mapOpSpec), opOpts = std::move(opOpts), tmpFiles]() {
                execCtx->SetNodeExecProgress("Uploading artifacts");
                return entry->Tx->RawMap(mapOpSpec, job, opOpts);
            });
        });
    }

    TFuture<void> DoMap(TYtMap map, const TExecContext<TRunOptions>::TPtr& execCtx, TExprContext& ctx) {
        const bool ordered = NYql::HasSetting(map.Settings().Ref(), EYtSettingType::Ordered);
        TMaybe<ui64> jobCount;
        if (auto setting = NYql::GetSetting(map.Settings().Ref(), EYtSettingType::JobCount)) {
            jobCount = FromString<ui64>(setting->Child(1)->Content());
        }

        TString mapLambda;
        {
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                execCtx->FunctionRegistry_->SupportsSizedAllocators());
            alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            TNativeYtLambdaBuilder builder(alloc, Services_, *execCtx->Session_);
            mapLambda = builder.BuildLambdaWithIO(*MkqlCompiler_, map.Mapper(), ctx);
        }

        TVector<TString> sortLimitBy = NYql::GetSettingAsColumnList(map.Settings().Ref(), EYtSettingType::SortLimitBy);
        TMaybe<ui64> limit = GetLimit(map.Settings().Ref());
        if (limit && !sortLimitBy.empty() && *limit > execCtx->Options_.Config()->TopSortMaxLimit.Get().GetOrElse(DEFAULT_TOP_SORT_LIMIT)) {
            limit.Clear();
        }
        auto extraUsage = execCtx->ScanExtraResourceUsage(map.Mapper().Body().Ref(), true);
        TString inputType = NCommon::WriteTypeToYson(GetSequenceItemType(map.Input().Size() == 1U ? TExprBase(map.Input().Item(0)) : TExprBase(map.Mapper().Args().Arg(0)), true));

        return execCtx->Session_->Queue_->Async([ordered, jobCount, limit, sortLimitBy, mapLambda, inputType, extraUsage, execCtx]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            execCtx->MakeUserFiles();
            return ExecMap(ordered, jobCount, limit, sortLimitBy, mapLambda, inputType, extraUsage, execCtx);
        });
    }

    static TFuture<void> ExecReduce(const TVector<std::pair<TString, bool>>& reduceBy,
        const TVector<std::pair<TString, bool>>& sortBy,
        bool joinReduce,
        const TMaybe<ui64>& maxDataSizePerJob,
        bool useFirstAsPrimary,
        const TMaybe<ui64>& limit,
        const TVector<TString>& sortLimitBy,
        TString reduceLambda,
        const TString& inputType,
        const TExpressionResorceUsage& extraUsage,
        const TExecContext<TRunOptions>::TPtr& execCtx
    ) {
        const bool testRun = execCtx->Config_->GetLocalChainTest();
        TFuture<bool> ret = testRun ? MakeFuture<bool>(false) : execCtx->LookupQueryCacheAsync();
        return ret.Apply([reduceBy, sortBy, joinReduce, maxDataSizePerJob, useFirstAsPrimary, limit,
                          sortLimitBy, reduceLambda, inputType, extraUsage, execCtx, testRun]
                         (const auto& f) mutable
        {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            TTransactionCache::TEntry::TPtr entry;
            TVector<TRichYPath> outYPaths;
            if (testRun) {
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "Test mode support single output only.");
                const auto& out = execCtx->OutTables_.front();
                outYPaths.emplace_back(TRichYPath(out.Path).Schema(RowSpecToYTSchema(TestTables[out.Path].first = out.Spec[YqlRowSpecAttribute], NTCF_NONE)));
            } else {
                entry = execCtx->GetEntry();
                const bool cacheHit = f.GetValue();
                outYPaths = PrepareDestinations(execCtx->OutTables_, execCtx, entry, !cacheHit);
                if (cacheHit) {
                    execCtx->QueryCacheItem.Destroy();
                    return MakeFuture();
                }
            }

            TRawReduceOperationSpec reduceOpSpec;
            auto job = MakeIntrusive<TYqlUserJob>();

            job->SetInputType(inputType);

            for (size_t i: xrange(execCtx->OutTables_.size())) {
                if (limit && sortLimitBy.empty()) {
                    outYPaths[i].RowCountLimit(*limit);
                }
                reduceOpSpec.AddOutput(outYPaths[i]);
            }

            TVector<ui32> groups;
            TVector<TString> tables;
            TVector<ui64> rowOffsets;
            ui64 currentRowOffset = 0;
            YQL_ENSURE(!execCtx->InputTables_.empty());
            const ui32 primaryGroup = useFirstAsPrimary ? execCtx->InputTables_.front().Group : execCtx->InputTables_.back().Group;
            for (const auto& table : execCtx->InputTables_) {
                if (joinReduce) {
                    auto yPath = table.Path;
                    if (table.Group == primaryGroup) {
                        yPath.Primary(true);
                    } else {
                        yPath.Foreign(true);
                    }
                    reduceOpSpec.AddInput(yPath);
                } else {
                    reduceOpSpec.AddInput(table.Path);
                }
                if (!groups.empty() && groups.back() != table.Group) {
                    currentRowOffset = 0;
                }

                groups.push_back(table.Group);
                tables.push_back(table.Temp ? TString() : table.Name);
                rowOffsets.push_back(currentRowOffset);
                currentRowOffset += table.Records;
            }

            const bool useSkiff = execCtx->Options_.Config()->UseSkiff.Get(execCtx->Cluster_).GetOrElse(DEFAULT_USE_SKIFF);

            const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);
            job->SetInputSpec(execCtx->GetInputSpec(!useSkiff, nativeTypeCompat, false));
            job->SetOutSpec(execCtx->GetOutSpec(!useSkiff, nativeTypeCompat));
            YQL_ENSURE(!groups.empty());
            if (groups.back() != 0) {
                job->SetInputGroups(groups);
            }
            job->SetTableNames(tables);
            job->SetRowOffsets(rowOffsets);

            if (joinReduce) {
                reduceOpSpec.JoinBy(ToYTSortColumns(reduceBy));
                reduceOpSpec.EnableKeyGuarantee(false);
            } else {
                reduceOpSpec.ReduceBy(ToYTSortColumns(reduceBy));
            }

            if (!sortBy.empty()) {
                reduceOpSpec.SortBy(ToYTSortColumns(sortBy));
            } else {
                reduceOpSpec.SortBy(ToYTSortColumns(reduceBy));
            }

            THashSet<TString> auxColumns;
            std::for_each(reduceBy.begin(), reduceBy.end(), [&auxColumns](const auto& it) { auxColumns.insert(it.first); });
            if (!sortBy.empty()) {
                std::for_each(sortBy.begin(), sortBy.end(), [&auxColumns](const auto& it) { auxColumns.insert(it.first); });
            }
            job->SetAuxColumns(auxColumns);

            job->SetUseSkiff(useSkiff, TMkqlIOSpecs::ESystemField::RowIndex | TMkqlIOSpecs::ESystemField::KeySwitch);
            job->SetYamrInput(execCtx->YamrInput);

            auto tmpFiles = std::make_shared<TTempFiles>(execCtx->FileStorage_->GetTemp());
            {
                TUserJobSpec userJobSpec;
                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                    execCtx->FunctionRegistry_->SupportsSizedAllocators());
                alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                TNativeYtLambdaBuilder builder(alloc, execCtx->FunctionRegistry_, *execCtx->Session_);
                TProgramBuilder pgmBuilder(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_);
                TGatewayTransformer transform(execCtx, entry, pgmBuilder, *tmpFiles);
                size_t nodeCount = 0;
                builder.UpdateLambdaCode(reduceLambda, nodeCount, transform);
                if (nodeCount > execCtx->Options_.Config()->LLVMNodeCountLimit.Get(execCtx->Cluster_).GetOrElse(DEFAULT_LLVM_NODE_COUNT_LIMIT)) {
                    execCtx->Options_.OptLLVM("OFF");
                }
                job->SetLambdaCode(reduceLambda);
                job->SetOptLLVM(execCtx->Options_.OptLLVM());
                job->SetUdfValidateMode(execCtx->Options_.UdfValidateMode());
                transform.ApplyJobProps(*job);
                transform.ApplyUserJobSpec(userJobSpec, testRun);
                FillUserJobSpec(userJobSpec, execCtx, extraUsage, transform.GetUsedMemory(), execCtx->EstimateLLVMMem(nodeCount), testRun);
                reduceOpSpec.ReducerSpec(userJobSpec);
            }
            FillOperationSpec(reduceOpSpec, execCtx);
            auto formats = job->GetIOFormats(execCtx->FunctionRegistry_);
            reduceOpSpec.InputFormat(formats.first);
            reduceOpSpec.OutputFormat(formats.second);

            if (testRun) {
                YQL_ENSURE(execCtx->InputTables_.size() == 1U, "Support single input only.");
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "Support single output only.");

                const auto itI = TestTables.find(execCtx->InputTables_.front().Path.Path_);
                YQL_ENSURE(TestTables.cend() != itI);
                const auto itO = TestTables.find(execCtx->OutTables_.front().Path);
                YQL_ENSURE(TestTables.cend() != itO);

                TStringInput in(itI->second.second);
                TStringOutput out(itO->second.second);

                LocalRawMapReduce(reduceOpSpec, job.Get(), &in, &out);
                DumpLocalTable(itO->second.second, execCtx->Config_->GetLocalChainFile());
                return MakeFuture();
            }

            NYT::TNode spec = execCtx->Session_->CreateSpecWithDesc(execCtx->CodeSnippets_);
            FillSpec(spec, *execCtx, entry, extraUsage.Cpu, Nothing(),
                EYtOpProp::TemporaryAutoMerge | EYtOpProp::WithReducer | EYtOpProp::WithUserJobs | EYtOpProp::AllowSampling);

            if (maxDataSizePerJob) {
                spec["max_data_size_per_job"] = static_cast<i64>(*maxDataSizePerJob);
            }

            TOperationOptions opOpts;
            FillOperationOptions(opOpts, execCtx, entry);
            opOpts.StartOperationMode(TOperationOptions::EStartOperationMode::AsyncPrepare).Spec(spec);

            return execCtx->RunOperation([entry, execCtx, job, reduceOpSpec = std::move(reduceOpSpec), opOpts = std::move(opOpts), tmpFiles]() {
                execCtx->SetNodeExecProgress("Uploading artifacts");
                return entry->Tx->RawReduce(reduceOpSpec, job, opOpts);
            });
        });
    }

    TFuture<void> DoReduce(TYtReduce reduce, const TExecContext<TRunOptions>::TPtr &execCtx, TExprContext& ctx) {
        auto reduceBy = NYql::GetSettingAsColumnPairList(reduce.Settings().Ref(), EYtSettingType::ReduceBy);
        auto sortBy = NYql::GetSettingAsColumnPairList(reduce.Settings().Ref(), EYtSettingType::SortBy);
        bool joinReduce = NYql::HasSetting(reduce.Settings().Ref(), EYtSettingType::JoinReduce);
        auto maxDataSizePerJob = NYql::GetMaxJobSizeForFirstAsPrimary(reduce.Settings().Ref());
        bool useFirstAsPrimary = NYql::HasSetting(reduce.Settings().Ref(), EYtSettingType::FirstAsPrimary);

        TString reduceLambda;
        {
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                execCtx->FunctionRegistry_->SupportsSizedAllocators());
            alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            TNativeYtLambdaBuilder builder(alloc, Services_, *execCtx->Session_);
            reduceLambda = builder.BuildLambdaWithIO(*MkqlCompiler_, reduce.Reducer(), ctx);
        }

        TVector<TString> sortLimitBy = NYql::GetSettingAsColumnList(reduce.Settings().Ref(), EYtSettingType::SortLimitBy);
        TMaybe<ui64> limit = GetLimit(reduce.Settings().Ref());
        if (limit && !sortLimitBy.empty() && *limit > execCtx->Options_.Config()->TopSortMaxLimit.Get().GetOrElse(DEFAULT_TOP_SORT_LIMIT)) {
            limit.Clear();
        }
        auto extraUsage = execCtx->ScanExtraResourceUsage(reduce.Reducer().Body().Ref(), true);
        const auto inputTypeSet = NYql::GetSetting(reduce.Settings().Ref(), EYtSettingType::ReduceInputType);
        TString inputType = NCommon::WriteTypeToYson(inputTypeSet
            ? inputTypeSet->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType()
            : GetSequenceItemType(reduce.Reducer().Args().Arg(0), true)
        );

        return execCtx->Session_->Queue_->Async([reduceBy, sortBy, joinReduce, maxDataSizePerJob, useFirstAsPrimary, limit, sortLimitBy, reduceLambda, inputType, extraUsage, execCtx]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            execCtx->MakeUserFiles();
            return ExecReduce(reduceBy, sortBy, joinReduce, maxDataSizePerJob, useFirstAsPrimary, limit,
                              sortLimitBy, reduceLambda, inputType, extraUsage, execCtx);
        });
    }

    static TFuture<void> ExecMapReduce(
        const TVector<std::pair<TString, bool>>& reduceBy,
        const TVector<std::pair<TString, bool>>& sortBy,
        const TMaybe<ui64>& limit,
        const TVector<TString>& sortLimitBy,
        TString mapLambda,
        const TString& mapInputType,
        size_t mapDirectOutputs,
        const TExpressionResorceUsage& mapExtraUsage,
        TString reduceLambda,
        const TString& reduceInputType,
        const TExpressionResorceUsage& reduceExtraUsage,
        NYT::TNode intermediateMeta,
        const NYT::TNode& intermediateSchema,
        const TExecContext<TRunOptions>::TPtr& execCtx
    ) {
        const bool testRun = execCtx->Config_->GetLocalChainTest();
        TFuture<bool> ret = testRun ? MakeFuture<bool>(false) : execCtx->LookupQueryCacheAsync();
        return ret.Apply([reduceBy, sortBy, limit, sortLimitBy, mapLambda, mapInputType, mapDirectOutputs,
                          mapExtraUsage, reduceLambda, reduceInputType, reduceExtraUsage,
                          intermediateMeta, intermediateSchema, execCtx, testRun]
                         (const auto& f) mutable
        {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            TTransactionCache::TEntry::TPtr entry;
            TVector<TRichYPath> outYPaths;

            if (testRun) {
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "Test mode support single output only.");
                const auto& out = execCtx->OutTables_.front();
                outYPaths.emplace_back(TRichYPath(out.Path).Schema(RowSpecToYTSchema(TestTables[out.Path].first = out.Spec[YqlRowSpecAttribute], NTCF_NONE)));
            } else {
                entry = execCtx->GetEntry();
                const bool cacheHit = f.GetValue();
                outYPaths = PrepareDestinations(execCtx->OutTables_, execCtx, entry, !cacheHit);
                if (cacheHit) {
                    execCtx->QueryCacheItem.Destroy();
                    return MakeFuture();
                }
            }

            TRawMapReduceOperationSpec mapReduceOpSpec;
            YQL_ENSURE(mapDirectOutputs < execCtx->OutTables_.size());

            for (size_t i: xrange(execCtx->OutTables_.size())) {
                if (limit && sortLimitBy.empty()) {
                    outYPaths[i].RowCountLimit(*limit);
                }
                if (i < mapDirectOutputs) {
                    mapReduceOpSpec.AddMapOutput(outYPaths[i]);
                } else {
                    mapReduceOpSpec.AddOutput(outYPaths[i]);
                }
            }

            bool useSkiff = execCtx->Options_.Config()->UseSkiff.Get(execCtx->Cluster_).GetOrElse(DEFAULT_USE_SKIFF);
            const bool reduceUseSkiff = useSkiff;
            bool hasTablesWithoutQB2Premapper = false;

            TVector<ui32> groups;
            TVector<TString> tables;
            TVector<ui64> rowOffsets;
            ui64 currentRowOffset = 0;
            TSet<TString> remapperAllFiles;
            TRemapperMap remapperMap;
            for (auto& table: execCtx->InputTables_) {
                auto tablePath = table.Path;
                if (!table.QB2Premapper.IsUndefined()) {
                    bool tableUseSkiff = false;

                    ProcessTableQB2Premapper(table.QB2Premapper, table.Name, tablePath, mapReduceOpSpec.GetInputs().size(),
                        remapperMap, remapperAllFiles, tableUseSkiff);

                    useSkiff = useSkiff && tableUseSkiff;
                }
                else {
                    hasTablesWithoutQB2Premapper = true;
                }
                if (!groups.empty() && groups.back() != table.Group) {
                    currentRowOffset = 0;
                }

                mapReduceOpSpec.AddInput(tablePath);
                groups.push_back(table.Group);
                tables.push_back(table.Temp ? TString() : table.Name);
                rowOffsets.push_back(currentRowOffset);
                currentRowOffset += table.Records;
            }

            bool forceYsonInputFormat = false;

            if (useSkiff && !remapperMap.empty()) {
                // Disable skiff in case of mix of QB2 and normal tables
                if (hasTablesWithoutQB2Premapper) {
                    useSkiff = false;
                } else {
                    UpdateQB2PremapperUseSkiff(remapperMap, useSkiff);
                    forceYsonInputFormat = useSkiff;
                }
            }

            NYT::TNode mapSpec = intermediateMeta;
            mapSpec.AsMap().erase(YqlSysColumnPrefix);

            const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);

            NYT::TNode mapOutSpec = NYT::TNode::CreateMap();
            mapOutSpec[YqlIOSpecTables] = NYT::TNode::CreateList();
            mapOutSpec[YqlIOSpecTables].Add(mapSpec);
            TString mapOutSpecStr;
            if (mapDirectOutputs) {
                mapOutSpecStr = execCtx->GetOutSpec(0, mapDirectOutputs, mapOutSpec, !reduceUseSkiff, nativeTypeCompat);
            } else {
                mapOutSpecStr = NYT::NodeToYsonString(mapOutSpec);
            }

            auto mapJob = MakeIntrusive<TYqlUserJob>();
            mapJob->SetInputType(mapInputType);
            mapJob->SetInputSpec(execCtx->GetInputSpec(!useSkiff || forceYsonInputFormat, nativeTypeCompat, false));
            mapJob->SetOutSpec(mapOutSpecStr);
            if (!groups.empty() && groups.back() != 0) {
                mapJob->SetInputGroups(groups);
            }
            mapJob->SetTableNames(tables);
            mapJob->SetRowOffsets(rowOffsets);
            mapJob->SetUseSkiff(useSkiff, TMkqlIOSpecs::ESystemField::RowIndex);
            mapJob->SetYamrInput(execCtx->YamrInput);

            auto reduceJob = MakeIntrusive<TYqlUserJob>();
            reduceJob->SetInputType(reduceInputType);
            reduceJob->SetInputSpec(NYT::NodeToYsonString(NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, NYT::TNode::CreateList().Add(intermediateMeta))));
            reduceJob->SetOutSpec(execCtx->GetOutSpec(mapDirectOutputs, execCtx->OutTables_.size(), {}, !reduceUseSkiff, nativeTypeCompat));

            mapReduceOpSpec.ReduceBy(ToYTSortColumns(reduceBy));
            if (!sortBy.empty()) {
                mapReduceOpSpec.SortBy(ToYTSortColumns(sortBy));
            } else {
                mapReduceOpSpec.SortBy(ToYTSortColumns(reduceBy));
            }

            THashSet<TString> auxColumns;
            std::for_each(reduceBy.begin(), reduceBy.end(), [&auxColumns](const auto& it) { auxColumns.insert(it.first); });
            if (!sortBy.empty()) {
                std::for_each(sortBy.begin(), sortBy.end(), [&auxColumns](const auto& it) { auxColumns.insert(it.first); });
            }
            reduceJob->SetAuxColumns(auxColumns);

            reduceJob->SetUseSkiff(reduceUseSkiff, TMkqlIOSpecs::ESystemField::KeySwitch);

            auto tmpFiles = std::make_shared<TTempFiles>(execCtx->FileStorage_->GetTemp());
            {
                TUserJobSpec mapUserJobSpec;
                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                    execCtx->FunctionRegistry_->SupportsSizedAllocators());
                alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                TNativeYtLambdaBuilder builder(alloc, execCtx->FunctionRegistry_, *execCtx->Session_);
                TProgramBuilder pgmBuilder(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_);
                TGatewayTransformer transform(execCtx, entry, pgmBuilder, *tmpFiles);
                size_t nodeCount = 0;
                builder.UpdateLambdaCode(mapLambda, nodeCount, transform);
                if (nodeCount > execCtx->Options_.Config()->LLVMNodeCountLimit.Get(execCtx->Cluster_).GetOrElse(DEFAULT_LLVM_NODE_COUNT_LIMIT)) {
                    execCtx->Options_.OptLLVM("OFF");
                }
                mapJob->SetLambdaCode(mapLambda);
                mapJob->SetOptLLVM(execCtx->Options_.OptLLVM());
                mapJob->SetUdfValidateMode(execCtx->Options_.UdfValidateMode());
                transform.ApplyJobProps(*mapJob);
                transform.ApplyUserJobSpec(mapUserJobSpec, testRun);

                for (auto& f: remapperAllFiles) {
                    mapUserJobSpec.AddFile(TRichYPath(f).Executable(true));
                }

                FillUserJobSpec(mapUserJobSpec, execCtx, mapExtraUsage, transform.GetUsedMemory(), execCtx->EstimateLLVMMem(nodeCount), testRun,
                    GetQB2PremapperPrefix(remapperMap, useSkiff));

                mapReduceOpSpec.MapperSpec(mapUserJobSpec);
            }

            {
                TUserJobSpec reduceUserJobSpec;
                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                    execCtx->FunctionRegistry_->SupportsSizedAllocators());
                alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                TNativeYtLambdaBuilder builder(alloc, execCtx->FunctionRegistry_, *execCtx->Session_);
                TProgramBuilder pgmBuilder(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_);
                TGatewayTransformer transform(execCtx, entry, pgmBuilder, *tmpFiles);
                size_t nodeCount = 0;
                builder.UpdateLambdaCode(reduceLambda, nodeCount, transform);
                if (nodeCount > execCtx->Options_.Config()->LLVMNodeCountLimit.Get(execCtx->Cluster_).GetOrElse(DEFAULT_LLVM_NODE_COUNT_LIMIT)) {
                    execCtx->Options_.OptLLVM("OFF");
                }
                reduceJob->SetLambdaCode(reduceLambda);
                reduceJob->SetOptLLVM(execCtx->Options_.OptLLVM());
                reduceJob->SetUdfValidateMode(execCtx->Options_.UdfValidateMode());
                transform.ApplyJobProps(*reduceJob);
                transform.ApplyUserJobSpec(reduceUserJobSpec, testRun);
                FillUserJobSpec(reduceUserJobSpec, execCtx, reduceExtraUsage, transform.GetUsedMemory(), execCtx->EstimateLLVMMem(nodeCount), testRun);
                mapReduceOpSpec.ReducerSpec(reduceUserJobSpec);
            }
            FillOperationSpec(mapReduceOpSpec, execCtx);
            auto formats = mapJob->GetIOFormats(execCtx->FunctionRegistry_);
            if (!intermediateSchema.IsUndefined() && formats.second.Config.AsString() == "skiff") {
                formats.second.Config.Attributes()["override_intermediate_table_schema"] = intermediateSchema;
            }
            mapReduceOpSpec.MapperInputFormat(forceYsonInputFormat ? NYT::TFormat::YsonBinary() : formats.first);
            mapReduceOpSpec.MapperOutputFormat(formats.second);
            formats = reduceJob->GetIOFormats(execCtx->FunctionRegistry_);
            if (!intermediateSchema.IsUndefined() && formats.first.Config.AsString() == "skiff") {
                formats.first.Config.Attributes()["override_intermediate_table_schema"] = intermediateSchema;
            }
            mapReduceOpSpec.ReducerInputFormat(formats.first);
            mapReduceOpSpec.ReducerOutputFormat(formats.second);

            if (testRun) {
                YQL_ENSURE(execCtx->InputTables_.size() == 1U, "Support single input only.");
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "Support single output only.");

                const auto itI = TestTables.find(execCtx->InputTables_.front().Path.Path_);
                YQL_ENSURE(TestTables.cend() != itI);
                const auto itO = TestTables.find(execCtx->OutTables_.front().Path);
                YQL_ENSURE(TestTables.cend() != itO);

                TStringInput in(itI->second.second);
                TStringOutput out(itO->second.second);

                LocalRawMapReduce(mapReduceOpSpec, reduceJob.Get(), &in, &out);
                DumpLocalTable(itO->second.second, execCtx->Config_->GetLocalChainFile());
                return MakeFuture();
            }

            NYT::TNode spec = execCtx->Session_->CreateSpecWithDesc(execCtx->CodeSnippets_);
            FillSpec(spec, *execCtx, entry, mapExtraUsage.Cpu, reduceExtraUsage.Cpu,
                EYtOpProp::IntermediateData | EYtOpProp::WithMapper | EYtOpProp::WithReducer | EYtOpProp::WithUserJobs | EYtOpProp::AllowSampling);

            TOperationOptions opOpts;
            FillOperationOptions(opOpts, execCtx, entry);
            opOpts.StartOperationMode(TOperationOptions::EStartOperationMode::AsyncPrepare).Spec(spec);

            return execCtx->RunOperation([entry, execCtx, mapJob, reduceJob, mapReduceOpSpec = std::move(mapReduceOpSpec), opOpts = std::move(opOpts), tmpFiles]() {
                execCtx->SetNodeExecProgress("Uploading artifacts");
                return entry->Tx->RawMapReduce(mapReduceOpSpec, mapJob, {}, reduceJob, opOpts);
            });
        });
    }

    static TFuture<void> ExecMapReduce(
        const TVector<std::pair<TString, bool>>& reduceBy,
        const TVector<std::pair<TString, bool>>& sortBy,
        const TMaybe<ui64>& limit,
        const TVector<TString>& sortLimitBy,
        TString reduceLambda,
        const TString& reduceInputType,
        const TExpressionResorceUsage& reduceExtraUsage,
        const NYT::TNode& intermediateSchema,
        const TExecContext<TRunOptions>::TPtr& execCtx
    ) {
        const bool testRun = execCtx->Config_->GetLocalChainTest();
        TFuture<bool> ret = testRun ? MakeFuture<bool>(false) : execCtx->LookupQueryCacheAsync();
        return ret.Apply([reduceBy, sortBy, limit, sortLimitBy, reduceLambda, reduceInputType,
                          reduceExtraUsage, intermediateSchema, execCtx, testRun]
                         (const auto& f) mutable
        {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            TTransactionCache::TEntry::TPtr entry;
            TVector<TRichYPath> outYPaths;
            if (testRun) {
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "Test mode support single output only.");
                const auto& out = execCtx->OutTables_.front();
                outYPaths.emplace_back(TRichYPath(out.Path).Schema(RowSpecToYTSchema(TestTables[out.Path].first = out.Spec[YqlRowSpecAttribute], NTCF_NONE)));
            } else {
                entry = execCtx->GetEntry();
                const bool cacheHit = f.GetValue();
                outYPaths = PrepareDestinations(execCtx->OutTables_, execCtx, entry, !cacheHit);
                if (cacheHit) {
                    execCtx->QueryCacheItem.Destroy();
                    return MakeFuture();
                }
            }

            TRawMapReduceOperationSpec mapReduceOpSpec;

            for (size_t i: xrange(execCtx->OutTables_.size())) {
                if (limit && sortLimitBy.empty()) {
                    outYPaths[i].RowCountLimit(*limit);
                }
                mapReduceOpSpec.AddOutput(outYPaths[i]);
            }

            TVector<ui32> groups;
            for (auto& table: execCtx->InputTables_) {
                mapReduceOpSpec.AddInput(table.Path);
                groups.push_back(table.Group);
            }

            auto reduceJob = MakeIntrusive<TYqlUserJob>();
            reduceJob->SetInputType(reduceInputType);
            if (!groups.empty() && groups.back() != 0) {
                reduceJob->SetInputGroups(groups);
            }

            const bool useSkiff = execCtx->Options_.Config()->UseSkiff.Get(execCtx->Cluster_).GetOrElse(DEFAULT_USE_SKIFF);

            const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);
            reduceJob->SetInputSpec(execCtx->GetInputSpec(!useSkiff, intermediateSchema.IsUndefined() ? 0ul : nativeTypeCompat, true)); // Explicitly disable native types for intermediate data because of YT limitations
            reduceJob->SetOutSpec(execCtx->GetOutSpec(!useSkiff, nativeTypeCompat));

            mapReduceOpSpec.ReduceBy(ToYTSortColumns(reduceBy));
            if (!sortBy.empty()) {
                mapReduceOpSpec.SortBy(ToYTSortColumns(sortBy));
            } else {
                mapReduceOpSpec.SortBy(ToYTSortColumns(reduceBy));
            }

            THashSet<TString> auxColumns;
            std::for_each(reduceBy.begin(), reduceBy.end(), [&auxColumns](const auto& it) { auxColumns.insert(it.first); });
            if (!sortBy.empty()) {
                std::for_each(sortBy.begin(), sortBy.end(), [&auxColumns](const auto& it) { auxColumns.insert(it.first); });
            }
            reduceJob->SetAuxColumns(auxColumns);

            reduceJob->SetUseSkiff(useSkiff, TMkqlIOSpecs::ESystemField::KeySwitch);
            reduceJob->SetYamrInput(execCtx->YamrInput);

            auto tmpFiles = std::make_shared<TTempFiles>(execCtx->FileStorage_->GetTemp());
            {
                TUserJobSpec reduceUserJobSpec;
                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                    execCtx->FunctionRegistry_->SupportsSizedAllocators());
                alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                TNativeYtLambdaBuilder builder(alloc, execCtx->FunctionRegistry_, *execCtx->Session_);
                TProgramBuilder pgmBuilder(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_);
                TGatewayTransformer transform(execCtx, entry, pgmBuilder, *tmpFiles);
                size_t nodeCount = 0;
                builder.UpdateLambdaCode(reduceLambda, nodeCount, transform);
                if (nodeCount > execCtx->Options_.Config()->LLVMNodeCountLimit.Get(execCtx->Cluster_).GetOrElse(DEFAULT_LLVM_NODE_COUNT_LIMIT)) {
                    execCtx->Options_.OptLLVM("OFF");
                }
                reduceJob->SetLambdaCode(reduceLambda);
                reduceJob->SetOptLLVM(execCtx->Options_.OptLLVM());
                reduceJob->SetUdfValidateMode(execCtx->Options_.UdfValidateMode());
                transform.ApplyJobProps(*reduceJob);
                transform.ApplyUserJobSpec(reduceUserJobSpec, testRun);
                FillUserJobSpec(reduceUserJobSpec, execCtx, reduceExtraUsage, transform.GetUsedMemory(), execCtx->EstimateLLVMMem(nodeCount), testRun);
                mapReduceOpSpec.ReducerSpec(reduceUserJobSpec);
            }
            FillOperationSpec(mapReduceOpSpec, execCtx);
            auto formats = reduceJob->GetIOFormats(execCtx->FunctionRegistry_);
            if (!intermediateSchema.IsUndefined() && formats.first.Config.AsString() == "skiff") {
                formats.first.Config.Attributes()["override_intermediate_table_schema"] = intermediateSchema;
            }
            mapReduceOpSpec.ReducerInputFormat(formats.first);
            mapReduceOpSpec.ReducerOutputFormat(formats.second);

            if (testRun) {
                YQL_ENSURE(execCtx->InputTables_.size() == 1U, "Support single input only.");
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "Support single output only.");

                const auto itI = TestTables.find(execCtx->InputTables_.front().Path.Path_);
                YQL_ENSURE(TestTables.cend() != itI);
                const auto itO = TestTables.find(execCtx->OutTables_.front().Path);
                YQL_ENSURE(TestTables.cend() != itO);

                TStringInput in(itI->second.second);
                TStringOutput out(itO->second.second);

                LocalRawMapReduce(mapReduceOpSpec, reduceJob.Get(), &in, &out);
                DumpLocalTable(itO->second.second, execCtx->Config_->GetLocalChainFile());
                return MakeFuture();
            }

            NYT::TNode spec = execCtx->Session_->CreateSpecWithDesc(execCtx->CodeSnippets_);
            FillSpec(spec, *execCtx, entry, 0., reduceExtraUsage.Cpu,
                EYtOpProp::IntermediateData | EYtOpProp::WithReducer | EYtOpProp::WithUserJobs | EYtOpProp::AllowSampling);

            TOperationOptions opOpts;
            FillOperationOptions(opOpts, execCtx, entry);
            opOpts.StartOperationMode(TOperationOptions::EStartOperationMode::AsyncPrepare).Spec(spec);

            return execCtx->RunOperation([entry, execCtx, reduceJob, mapReduceOpSpec = std::move(mapReduceOpSpec), opOpts = std::move(opOpts), tmpFiles]() {
                execCtx->SetNodeExecProgress("Uploading artifacts");
                return entry->Tx->RawMapReduce(mapReduceOpSpec, {}, {}, reduceJob, opOpts);
            });
        });
    }

    TFuture<void> DoMapReduce(TYtMapReduce mapReduce, const TExecContext<TRunOptions>::TPtr& execCtx, TExprContext& ctx) {
        auto reduceBy = NYql::GetSettingAsColumnPairList(mapReduce.Settings().Ref(), EYtSettingType::ReduceBy);
        auto sortBy = NYql::GetSettingAsColumnPairList(mapReduce.Settings().Ref(), EYtSettingType::SortBy);

        const bool useNativeTypes = execCtx->Options_.Config()->UseNativeYtTypes.Get().GetOrElse(DEFAULT_USE_NATIVE_YT_TYPES);
        const bool useIntermediateSchema = execCtx->Options_.Config()->UseIntermediateSchema.Get().GetOrElse(DEFAULT_USE_INTERMEDIATE_SCHEMA);
        const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);

        NYT::TNode intermediateMeta;
        NYT::TNode intermediateSchema;
        TString mapLambda;
        TExpressionResorceUsage mapExtraUsage;
        TString mapInputType;
        size_t mapDirectOutputs = 0;
        if (!mapReduce.Mapper().Maybe<TCoVoid>()) {
            const auto mapTypeSet = NYql::GetSetting(mapReduce.Settings().Ref(), EYtSettingType::MapOutputType);
            auto mapResultItem = mapTypeSet ?
                mapTypeSet->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType():
                GetSequenceItemType(mapReduce.Mapper(), true);

            if (mapResultItem->GetKind() == ETypeAnnotationKind::Variant) {
                auto items = mapResultItem->Cast<TVariantExprType>()->GetUnderlyingType()->Cast<TTupleExprType>()->GetItems();
                YQL_ENSURE(!items.empty());
                mapDirectOutputs = items.size() - 1;
                mapResultItem = items.front();
            }

            intermediateMeta = NYT::TNode::CreateMap();
            intermediateMeta[YqlRowSpecAttribute][RowSpecAttrType] = NCommon::TypeToYsonNode(mapResultItem);
            if (useIntermediateSchema && useNativeTypes) {
                intermediateMeta[YqlRowSpecAttribute][RowSpecAttrNativeYtTypeFlags] = (GetNativeYtTypeFlags(*mapResultItem->Cast<TStructExprType>()) & nativeTypeCompat);
                intermediateSchema = RowSpecToYTSchema(intermediateMeta[YqlRowSpecAttribute], nativeTypeCompat).ToNode();
            } else {
                intermediateMeta[YqlRowSpecAttribute][RowSpecAttrNativeYtTypeFlags] = 0ul; // Explicitly disable native types for intermediate data because of YT limitations
            }
            if (NYql::HasSetting(mapReduce.Settings().Ref(), EYtSettingType::KeySwitch)) {
                intermediateMeta[YqlSysColumnPrefix].Add("keyswitch");
            }
            mapExtraUsage = execCtx->ScanExtraResourceUsage(mapReduce.Mapper().Cast<TCoLambda>().Body().Ref(), true);

            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                execCtx->FunctionRegistry_->SupportsSizedAllocators());
            alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            TNativeYtLambdaBuilder builder(alloc, Services_, *execCtx->Session_);
            mapLambda = builder.BuildLambdaWithIO(*MkqlCompiler_, mapReduce.Mapper().Cast<TCoLambda>(), ctx);
            mapInputType = NCommon::WriteTypeToYson(GetSequenceItemType(mapReduce.Input().Size() == 1U ?
                TExprBase(mapReduce.Input().Item(0)) : TExprBase(mapReduce.Mapper().Cast<TCoLambda>().Args().Arg(0)), true));
        } else if (useIntermediateSchema && useNativeTypes) {
            YQL_ENSURE(mapReduce.Input().Size() == 1);
            const TTypeAnnotationNode* itemType = GetSequenceItemType(mapReduce.Input().Item(0), false);
            if (auto flags = GetNativeYtTypeFlags(*itemType->Cast<TStructExprType>())) {
                auto rowSpec = NYT::TNode::CreateMap();
                rowSpec[RowSpecAttrType] = NCommon::TypeToYsonNode(itemType);
                rowSpec[RowSpecAttrNativeYtTypeFlags] = (flags & nativeTypeCompat);
                intermediateSchema = RowSpecToYTSchema(rowSpec, nativeTypeCompat).ToNode();
            }
        }
        TString reduceLambda;
        {
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                execCtx->FunctionRegistry_->SupportsSizedAllocators());
            alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            TNativeYtLambdaBuilder builder(alloc, Services_, *execCtx->Session_);
            reduceLambda = builder.BuildLambdaWithIO(*MkqlCompiler_, mapReduce.Reducer(), ctx);
        }
        TExpressionResorceUsage reduceExtraUsage = execCtx->ScanExtraResourceUsage(mapReduce.Reducer().Body().Ref(), false);

        const auto inputTypeSet = NYql::GetSetting(mapReduce.Settings().Ref(), EYtSettingType::ReduceInputType);
        TString reduceInputType = NCommon::WriteTypeToYson(inputTypeSet ?
            inputTypeSet->Tail().GetTypeAnn()->Cast<TTypeExprType>()->GetType():
            GetSequenceItemType(mapReduce.Reducer().Args().Arg(0), false)
        );

        TVector<TString> sortLimitBy = NYql::GetSettingAsColumnList(mapReduce.Settings().Ref(), EYtSettingType::SortLimitBy);
        TMaybe<ui64> limit = GetLimit(mapReduce.Settings().Ref());
        if (limit && !sortLimitBy.empty() && *limit > execCtx->Options_.Config()->TopSortMaxLimit.Get().GetOrElse(DEFAULT_TOP_SORT_LIMIT)) {
            limit.Clear();
        }

        return execCtx->Session_->Queue_->Async([reduceBy, sortBy, limit, sortLimitBy, mapLambda, mapInputType, mapDirectOutputs, mapExtraUsage, reduceLambda, reduceInputType, reduceExtraUsage, intermediateMeta, intermediateSchema, execCtx]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            execCtx->MakeUserFiles();
            if (mapLambda) {
                return ExecMapReduce(reduceBy, sortBy, limit, sortLimitBy, mapLambda, mapInputType, mapDirectOutputs, mapExtraUsage,
                    reduceLambda, reduceInputType, reduceExtraUsage, intermediateMeta, intermediateSchema, execCtx);
            } else {
                return ExecMapReduce(reduceBy, sortBy, limit, sortLimitBy, reduceLambda, reduceInputType, reduceExtraUsage, intermediateSchema, execCtx);
            }
        });
    }

    static void ExecSafeFill(const TVector<TRichYPath>& outYPaths,
        TRuntimeNode root,
        const TString& outSpec,
        const TExecContext<TRunOptions>::TPtr& execCtx,
        const TTransactionCache::TEntry::TPtr& entry,
        const TNativeYtLambdaBuilder& builder,
        TScopedAlloc& alloc,
        TString filePrefix
    ) {
        NYT::TTableWriterOptions writerOptions;
        auto maxRowWeight = execCtx->Options_.Config()->MaxRowWeight.Get(execCtx->Cluster_);
        auto maxKeyWeight = execCtx->Options_.Config()->MaxKeyWeight.Get(execCtx->Cluster_);
        bool hasSecureParams = !execCtx->Options_.SecureParams().empty();

        if (maxRowWeight || maxKeyWeight || hasSecureParams) {
            NYT::TNode config;
            if (maxRowWeight) {
                config["max_row_weight"] = static_cast<i64>(*maxRowWeight);
            }
            if (maxKeyWeight) {
                config["max_key_weight"] = static_cast<i64>(*maxKeyWeight);
            }
            if (hasSecureParams) {
                FillSecureVault(config, execCtx->Options_.SecureParams());
            }
            writerOptions.Config(config);
        }

        NCommon::TCodecContext codecCtx(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_);
        TMkqlIOSpecs specs;
        if (execCtx->Options_.Config()->UseSkiff.Get(execCtx->Cluster_).GetOrElse(DEFAULT_USE_SKIFF)) {
            specs.SetUseSkiff(execCtx->Options_.OptLLVM());
        }
        specs.Init(codecCtx, outSpec);

        TVector<TRawTableWriterPtr> writers;
        for (size_t i: xrange(outYPaths.size())) {
            auto writer = entry->Tx->CreateRawWriter(outYPaths[i], specs.MakeOutputFormat(i), writerOptions);
            writers.push_back(writer);
        }

        TMkqlWriterImpl mkqlWriter(writers, 4_MB);
        mkqlWriter.SetSpecs(specs);
        mkqlWriter.SetWriteLimit(alloc.GetLimit());

        TExploringNodeVisitor explorer;
        auto localGraph = builder.BuildLocalGraph(GetGatewayNodeFactory(&codecCtx, &mkqlWriter, execCtx->UserFiles_, filePrefix),
            execCtx->Options_.UdfValidateMode(),
            NUdf::EValidatePolicy::Exception, "OFF" /* don't use LLVM locally */, EGraphPerProcess::Multi, explorer, root);
        auto& graph = std::get<0>(localGraph);
        const TBindTerminator bind(graph->GetTerminator());
        graph->Prepare();
        auto value = graph->GetValue();

        if (root.GetStaticType()->IsStream()) {
            NUdf::TUnboxedValue item;
            const auto status = value.Fetch(item);
            YQL_ENSURE(NUdf::EFetchStatus::Finish == status);
        } else {
            YQL_ENSURE(value.IsFinish());
        }

        mkqlWriter.Finish();
        for (auto& writer: writers) {
            writer->Finish();
        }
    }

    static TFuture<void> ExecFill(TString lambda,
        const TExpressionResorceUsage& extraUsage,
        const TString& tmpTable,
        const TExecContext<TRunOptions>::TPtr& execCtx)
    {
        const bool testRun = execCtx->Config_->GetLocalChainTest();
        TFuture<bool> ret = testRun ? MakeFuture<bool>(false) : execCtx->LookupQueryCacheAsync();
        return ret.Apply([lambda, extraUsage, tmpTable, execCtx, testRun] (const auto& f) mutable {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            TTransactionCache::TEntry::TPtr entry;
            TVector<TRichYPath> outYPaths;
            if (testRun) {
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "Test mode support single output only.");
                const auto& out = execCtx->OutTables_.front();
                outYPaths.emplace_back(TRichYPath(out.Path).Schema(RowSpecToYTSchema(TestTables[out.Path].first = out.Spec[YqlRowSpecAttribute], NTCF_NONE)));
            } else {
                entry = execCtx->GetEntry();
                bool cacheHit = f.GetValue();
                outYPaths = PrepareDestinations(execCtx->OutTables_, execCtx, entry, !cacheHit);
                if (cacheHit) {
                    execCtx->QueryCacheItem.Destroy();
                    return MakeFuture();
                }
            }

            const bool useSkiff = execCtx->Options_.Config()->UseSkiff.Get(execCtx->Cluster_).GetOrElse(DEFAULT_USE_SKIFF);

            TIntrusivePtr<TYqlUserJob> job;
            TRawMapOperationSpec mapOpSpec;

            auto tmpFiles = std::make_shared<TTempFiles>(execCtx->FileStorage_->GetTemp());

            bool localRun = !testRun &&
                (execCtx->Config_->HasExecuteUdfLocallyIfPossible()
                    ? execCtx->Config_->GetExecuteUdfLocallyIfPossible() : false);
            {
                TUserJobSpec userJobSpec;
                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                    execCtx->FunctionRegistry_->SupportsSizedAllocators());
                alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                TNativeYtLambdaBuilder builder(alloc, execCtx->FunctionRegistry_, *execCtx->Session_);
                TProgramBuilder pgmBuilder(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_);
                TGatewayTransformer transform(execCtx, entry, pgmBuilder, *tmpFiles);
                transform.SetTwoPhaseTransform();

                TRuntimeNode root = builder.Deserialize(lambda);
                root = builder.TransformAndOptimizeProgram(root, transform);
                if (transform.HasSecondPhase()) {
                    root = builder.TransformAndOptimizeProgram(root, transform);
                }
                size_t nodeCount = 0;
                std::tie(lambda, nodeCount) = builder.Serialize(root);
                if (nodeCount > execCtx->Options_.Config()->LLVMNodeCountLimit.Get(execCtx->Cluster_).GetOrElse(DEFAULT_LLVM_NODE_COUNT_LIMIT)) {
                    execCtx->Options_.OptLLVM("OFF");
                }

                if (transform.CanExecuteInternally() && !testRun) {
                    const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);
                    ExecSafeFill(outYPaths, root, execCtx->GetOutSpec(!useSkiff, nativeTypeCompat), execCtx, entry, builder, alloc, tmpFiles->TmpDir.GetPath() + '/');
                    return MakeFuture();
                }

                localRun = localRun && transform.CanExecuteLocally();

                job = MakeIntrusive<TYqlUserJob>();
                transform.ApplyJobProps(*job);
                transform.ApplyUserJobSpec(userJobSpec, localRun || testRun);

                FillUserJobSpec(userJobSpec, execCtx, extraUsage, transform.GetUsedMemory(),
                                execCtx->EstimateLLVMMem(nodeCount), localRun || testRun);
                mapOpSpec.MapperSpec(userJobSpec);
            }

            job->SetLambdaCode(lambda);
            job->SetOptLLVM(execCtx->Options_.OptLLVM());
            job->SetUdfValidateMode(execCtx->Options_.UdfValidateMode());
            const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(execCtx->Cluster_).GetOrElse(NTCF_LEGACY);
            job->SetOutSpec(execCtx->GetOutSpec(!useSkiff, nativeTypeCompat));
            job->SetUseSkiff(useSkiff, 0);

            mapOpSpec.AddInput(tmpTable);

            for (size_t i: xrange(execCtx->OutTables_.size())) {
                mapOpSpec.AddOutput(outYPaths[i]);
            }

            FillOperationSpec(mapOpSpec, execCtx);
            const auto formats = job->GetIOFormats(execCtx->FunctionRegistry_);
            mapOpSpec.InputFormat(formats.first);
            mapOpSpec.OutputFormat(formats.second);

            if (localRun && mapOpSpec.MapperSpec_.Files_.empty() && execCtx->OutTables_.size() == 1U) {
                return LocalFillJob(mapOpSpec, job.Get(), entry);
            } else if (testRun) {
                YQL_ENSURE(execCtx->OutTables_.size() == 1U, "Support single output only.");

                const TString dummy(NYT::NodeListToYsonString({NYT::TNode()("input", "dummy")}));
                TStringInput in(dummy);

                const auto itO = TestTables.find(execCtx->OutTables_.front().Path);
                YQL_ENSURE(TestTables.cend() != itO);

                TStringOutput out(itO->second.second);

                LocalRawMapReduce(mapOpSpec, job.Get(), &in, &out);
                DumpLocalTable(itO->second.second, execCtx->Config_->GetLocalChainFile());
                return MakeFuture();
            } else {
                PrepareTempDestination(tmpTable, execCtx, entry, entry->Tx);
                auto writer = entry->Tx->CreateTableWriter<NYT::TNode>(tmpTable);
                writer->AddRow(NYT::TNode()("input", "dummy"));
                writer->Finish();
            }

            NYT::TNode spec = execCtx->Session_->CreateSpecWithDesc(execCtx->CodeSnippets_);
            FillSpec(spec, *execCtx, entry, extraUsage.Cpu, Nothing(),
                EYtOpProp::TemporaryAutoMerge | EYtOpProp::WithMapper | EYtOpProp::WithUserJobs);

            TOperationOptions opOpts;
            FillOperationOptions(opOpts, execCtx, entry);
            opOpts.StartOperationMode(TOperationOptions::EStartOperationMode::AsyncPrepare).Spec(spec);

            return execCtx->RunOperation([entry, execCtx, job, mapOpSpec = std::move(mapOpSpec), opOpts = std::move(opOpts), tmpFiles]() {
                execCtx->SetNodeExecProgress("Uploading artifacts");
                return entry->Tx->RawMap(mapOpSpec, job, opOpts);
            })
            .Apply([tmpTable, entry](const TFuture<void>& f){
                f.GetValue();
                entry->RemoveInternal(tmpTable);
            });
        });
    }

    TFuture<void> DoFill(TYtFill fill, const TExecContext<TRunOptions>::TPtr& execCtx, TExprContext& ctx) {
        TString lambda;
        {
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                Services_.FunctionRegistry->SupportsSizedAllocators());
            alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            TNativeYtLambdaBuilder builder(alloc, Services_, *execCtx->Session_);
            lambda = builder.BuildLambdaWithIO(*MkqlCompiler_, fill.Content(), ctx);
        }
        auto extraUsage = execCtx->ScanExtraResourceUsage(fill.Content().Ref(), false);

        TString tmpFolder = GetTablesTmpFolder(*execCtx->Options_.Config());
        TString tmpTablePath = NYql::TransformPath(tmpFolder,
            TStringBuilder() << "tmp/" << GetGuidAsString(execCtx->Session_->RandomProvider_->GenGuid()), true, execCtx->Session_->UserName_);

        return execCtx->Session_->Queue_->Async([lambda, tmpTablePath, extraUsage, execCtx]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            execCtx->MakeUserFiles();
            return ExecFill(lambda, extraUsage, tmpTablePath, execCtx);
        });
    }

    TFuture<void> DoTouch(TYtOutputOpBase touch, const TExecContext<TRunOptions>::TPtr& execCtx) {
        Y_UNUSED(touch);
        return execCtx->Session_->Queue_->Async([execCtx]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            auto entry = execCtx->GetEntry();
            PrepareDestinations(execCtx->OutTables_, execCtx, entry, true);
        });
    }

    TFuture<bool> DoPrepare(TYtOutputOpBase write, const TExecContext<TPrepareOptions>::TPtr& execCtx) const {
        Y_UNUSED(write);
        return execCtx->Session_->Queue_->Async([execCtx]() {
            return execCtx->LookupQueryCacheAsync().Apply([execCtx](const auto& f) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                auto entry = execCtx->GetEntry();
                bool cacheHit = f.GetValue();
                PrepareDestinations(execCtx->OutTables_, execCtx, entry, !cacheHit);
                execCtx->QueryCacheItem.Destroy();
                return cacheHit;
            });
        });
    }

    TFuture<void> DoDrop(TYtDropTable drop, const TExecContext<TRunOptions>::TPtr& execCtx) {
        TString tmpFolder = GetTablesTmpFolder(*execCtx->Options_.Config());
        auto table = drop.Table();
        bool isAnonymous = NYql::HasSetting(table.Settings().Ref(), EYtSettingType::Anonymous);
        TString path = NYql::TransformPath(tmpFolder, table.Name().Value(), isAnonymous, execCtx->Session_->UserName_);
        YQL_CLOG(INFO, ProviderYt) << "Dropping: " << execCtx->Cluster_ << '.' << path;

        return execCtx->Session_->Queue_->Async([path, execCtx]() {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
            auto entry = execCtx->GetEntry();
            entry->Tx->Remove(path, TRemoveOptions().Force(true));
        });
    }

    TFuture<void> DoStatOut(TYtStatOut statOut, const TExecContext<TRunOptions>::TPtr& execCtx) {
        auto input = statOut.Input();

        TString tmpFolder = GetTablesTmpFolder(*execCtx->Options_.Config());

        TString ytTable = TString{GetOutTable(input).Cast<TYtOutTable>().Name().Value()};
        ytTable = NYql::TransformPath(tmpFolder, ytTable, true, execCtx->Session_->UserName_);
        ytTable = NYT::AddPathPrefix(ytTable, NYT::TConfig::Get()->Prefix);

        TVector<TString> replaceMask;
        for (const auto& item: statOut.ReplaceMask().Ptr()->Children()) {
            replaceMask.push_back(TString{TCoAtom(item).Value()});
        }

        auto statUploadOptions = TStatUploadOptions {
            TString{YtProviderName},
            execCtx->Options_.SessionId(),
            *execCtx->Options_.PublicId(),
            TString{statOut.Table().Cluster().Value()},
            TString{statOut.Table().Name().Value()},
            TString{statOut.Table().Scale().Value()},
            std::move(replaceMask),
            execCtx->YtServer_,
            std::move(ytTable),
            GetGuidAsString(execCtx->GetEntry()->Tx->GetId()),
            execCtx->GetAuth(),
            NNative::GetPool(*execCtx, execCtx->Options_.Config())
        };
        execCtx->SetNodeExecProgress("Running");
        return StatUploader_->Upload(std::move(statUploadOptions));
    }

    static ui64 CalcDataSize(const NYT::TRichYPath& ytPath, const NYT::TNode& attrs) {
        ui64 res = attrs["uncompressed_data_size"].IntCast<ui64>();
        const auto records = attrs["chunk_row_count"].IntCast<ui64>();
        if (auto usedRows = GetUsedRows(ytPath, records)) {
            res *= double(*usedRows) / double(records);
        }
        return res;
    }

    static bool AllPathColumnsAreInSchema(const NYT::TRichYPath& ytPath, const NYT::TNode& attrs) {
        YQL_ENSURE(ytPath.Columns_.Defined());

        if (!attrs.HasKey("schema")) {
            YQL_CLOG(INFO, ProviderYt) << "Missing YT schema for " << ytPath.Path_;
            return false;
        }

        TSet<TString> columns(ytPath.Columns_->Parts_.begin(), ytPath.Columns_->Parts_.end());

        for (const auto& schemaColumn : attrs["schema"].AsList()) {
            auto it = columns.find(schemaColumn["name"].AsString());
            if (it != columns.end()) {
                columns.erase(it);
            }
            if (columns.empty()) {
                break;
            }
        }

        if (!columns.empty()) {
            YQL_CLOG(INFO, ProviderYt) << "Columns {" << JoinSeq(", ", columns) << "} are missing in YT schema for table "
                                       << ytPath.Path_ << ", assuming uncompressed data size";
        }

        return columns.empty();
    }

    static TPathStatResult ExecPathStat(const TExecContext<TPathStatOptions>::TPtr& execCtx, bool onlyCached) {
        try {
            TPathStatResult res;
            res.DataSize.resize(execCtx->Options_.Paths().size(), 0);

            auto entry = execCtx->GetOrCreateEntry();
            auto tx = entry->Tx;
            const TString tmpFolder = GetTablesTmpFolder(*execCtx->Options_.Config());
            const NYT::EOptimizeForAttr tmpOptimizeFor = execCtx->Options_.Config()->OptimizeFor.Get(execCtx->Cluster_).GetOrElse(NYT::EOptimizeForAttr::OF_LOOKUP_ATTR);
            TVector<NYT::TRichYPath> ytPaths(Reserve(execCtx->Options_.Paths().size()));
            TVector<size_t> pathMap;

            auto extractSysColumns = [] (NYT::TRichYPath& ytPath) -> TVector<TString> {
                TVector<TString> res;
                if (ytPath.Columns_) {
                    auto it = std::remove_if(
                        ytPath.Columns_->Parts_.begin(),
                        ytPath.Columns_->Parts_.end(),
                        [] (const TString& col) { return col.StartsWith(YqlSysColumnPrefix); }
                    );
                    res.assign(it, ytPath.Columns_->Parts_.end());
                    ytPath.Columns_->Parts_.erase(it, ytPath.Columns_->Parts_.end());
                }
                return res;
            };

            for (size_t i: xrange(execCtx->Options_.Paths().size())) {
                auto& req = execCtx->Options_.Paths()[i];
                NYT::TRichYPath ytPath = req.Path();
                auto tablePath = NYql::TransformPath(tmpFolder, ytPath.Path_, req.IsTemp(), execCtx->Session_->UserName_);
                if (req.IsTemp() && !req.IsAnonymous()) {
                    ytPath.Path_ = NYT::AddPathPrefix(tablePath, NYT::TConfig::Get()->Prefix);
                    NYT::TNode attrs;
                    if (auto sysColumns = extractSysColumns(ytPath)) {
                        attrs = tx->Get(ytPath.Path_ + "/@", NYT::TGetOptions().AttributeFilter(
                            NYT::TAttributeFilter()
                                .AddAttribute(TString("uncompressed_data_size"))
                                .AddAttribute(TString("chunk_row_count"))
                        ));
                        auto records = attrs["chunk_row_count"].IntCast<ui64>();
                        records = GetUsedRows(ytPath, records).GetOrElse(records);
                        for (auto col: sysColumns) {
                            auto size = 0;
                            if (col == YqlSysColumnNum || col == YqlSysColumnRecord) {
                                size = sizeof(ui64);
                            } else if (col == YqlSysColumnIndex) {
                                size = sizeof(ui32);
                            }
                            // zero size for YqlSysColumnPath for temp tables
                            size *= records;
                            res.DataSize[i] += size;
                            YQL_CLOG(INFO, ProviderYt) << "Adding stat for " << col << ": " << size << " (virtual)";
                        }
                    }
                    if (auto val = entry->GetColumnarStat(ytPath)) {
                        res.DataSize[i] += *val;
                        YQL_CLOG(INFO, ProviderYt) << "Stat for " << req.Path().Path_ << ": " << res.DataSize[i] << " (from cache)";
                    } else if (onlyCached) {
                        YQL_CLOG(INFO, ProviderYt) << "Stat for " << req.Path().Path_ << " is missing in cache - sync path stat failed";
                        return res;
                    } else if (NYT::EOptimizeForAttr::OF_SCAN_ATTR == tmpOptimizeFor) {
                        pathMap.push_back(i);
                        ytPaths.push_back(ytPath);
                    } else {
                        // Use entire table size for lookup tables (YQL-7257)
                        if (attrs.IsUndefined()) {
                            attrs = tx->Get(ytPath.Path_ + "/@", NYT::TGetOptions().AttributeFilter(
                                NYT::TAttributeFilter()
                                    .AddAttribute(TString("uncompressed_data_size"))
                                    .AddAttribute(TString("chunk_row_count"))
                            ));
                        }
                        auto size = CalcDataSize(ytPath, attrs);
                        res.DataSize[i] += size;
                        entry->UpdateColumnarStat(ytPath, size);
                        YQL_CLOG(INFO, ProviderYt) << "Stat for " << req.Path().Path_ << ": " << res.DataSize[i] << " (uncompressed_data_size for lookup)";
                    }
                } else {
                    auto p = entry->Snapshots.FindPtr(std::make_pair(tablePath, req.Epoch()));
                    YQL_ENSURE(p, "Table " << tablePath << " (epoch=" << req.Epoch() << ") has no snapshot");
                    ytPath.Path(std::get<0>(*p)).TransactionId(std::get<1>(*p));
                    NYT::TNode attrs;
                    if (auto sysColumns = extractSysColumns(ytPath)) {
                        attrs = entry->Client->AttachTransaction(std::get<1>(*p))->Get(std::get<0>(*p) + "/@", NYT::TGetOptions().AttributeFilter(
                            NYT::TAttributeFilter()
                                .AddAttribute(TString("uncompressed_data_size"))
                                .AddAttribute(TString("optimize_for"))
                                .AddAttribute(TString("chunk_row_count"))
                                .AddAttribute(TString("schema"))
                        ));
                        auto records = attrs["chunk_row_count"].IntCast<ui64>();
                        records = GetUsedRows(ytPath, records).GetOrElse(records);
                        for (auto col: sysColumns) {
                            auto size = 0;
                            if (col == YqlSysColumnNum || col == YqlSysColumnRecord) {
                                size = sizeof(ui64);
                            } else if (col == YqlSysColumnIndex) {
                                size = sizeof(ui32);
                            } else if (col == YqlSysColumnPath && !req.IsTemp()) {
                                size = tablePath.size();
                            }
                            size *= records;
                            res.DataSize[i] += size;
                            YQL_CLOG(INFO, ProviderYt) << "Adding stat for " << col << ": " << size << " (virtual)";
                        }
                    }
                    if (auto val = entry->GetColumnarStat(ytPath)) {
                        res.DataSize[i] += *val;
                        YQL_CLOG(INFO, ProviderYt) << "Stat for " << req.Path().Path_ << " (epoch=" << req.Epoch() << "): " << res.DataSize[i] << " (from cache)";
                    } else if (onlyCached) {
                        YQL_CLOG(INFO, ProviderYt) << "Stat for " << req.Path().Path_ << " (epoch=" << req.Epoch() << ") is missing in cache - sync path stat failed";
                        return res;
                    } else {
                        if (attrs.IsUndefined()) {
                            attrs = entry->Client->AttachTransaction(std::get<1>(*p))->Get(std::get<0>(*p) + "/@", NYT::TGetOptions().AttributeFilter(
                                NYT::TAttributeFilter()
                                    .AddAttribute(TString("uncompressed_data_size"))
                                    .AddAttribute(TString("optimize_for"))
                                    .AddAttribute(TString("chunk_row_count"))
                                    .AddAttribute(TString("schema"))
                            ));
                        }
                        if (attrs.HasKey("optimize_for") && attrs["optimize_for"] == "scan" &&
                            AllPathColumnsAreInSchema(req.Path(), attrs))
                        {
                            pathMap.push_back(i);
                            ytPaths.push_back(ytPath);
                            YQL_CLOG(INFO, ProviderYt) << "Stat for " << req.Path().Path_ << " (epoch=" << req.Epoch() << ") add for request with path " << ytPath.Path_;
                        } else {
                            // Use entire table size for lookup tables (YQL-7257)
                            auto size = CalcDataSize(ytPath, attrs);
                            res.DataSize[i] += size;
                            entry->UpdateColumnarStat(ytPath, size);
                            YQL_CLOG(INFO, ProviderYt) << "Stat for " << req.Path().Path_ << " (epoch=" << req.Epoch() << "): " << res.DataSize[i] << " (uncompressed_data_size for lookup)";
                        }
                    }
                }
            }

            if (ytPaths) {
                YQL_ENSURE(!onlyCached);
                auto fetchMode = execCtx->Options_.Config()->JoinColumnarStatisticsFetcherMode.Get().GetOrElse(NYT::EColumnarStatisticsFetcherMode::Fallback);
                auto columnStats = tx->GetTableColumnarStatistics(ytPaths, NYT::TGetTableColumnarStatisticsOptions().FetcherMode(fetchMode));
                YQL_ENSURE(pathMap.size() == columnStats.size());
                    for (size_t i: xrange(columnStats.size())) {
                    auto& columnStat = columnStats[i];
                    const ui64 weight = columnStat.LegacyChunksDataWeight +
                        Accumulate(columnStat.ColumnDataWeight.begin(), columnStat.ColumnDataWeight.end(), 0ull,
                            [](ui64 sum, decltype(*columnStat.ColumnDataWeight.begin())& v) { return sum + v.second; });

                    res.DataSize[pathMap[i]] += weight;
                    entry->UpdateColumnarStat(ytPaths[i], columnStat);
                    YQL_CLOG(INFO, ProviderYt) << "Stat for " << execCtx->Options_.Paths()[pathMap[i]].Path().Path_ << ": " << weight << " (fetched)";
                }
            }

            res.SetSuccess();
            return res;
        } catch (...) {
            return ResultFromCurrentException<TPathStatResult>();
        }
    }

    static TRunResult MakeRunResult(const TVector<TOutputInfo>& outTables, const TTransactionCache::TEntry::TPtr& entry) {
        TRunResult res;
        res.SetSuccess();

        if (outTables.empty()) {
            return res;
        }

        auto batchGet = entry->Tx->CreateBatchRequest();
        TVector<TFuture<TYtTableStatInfo::TPtr>> batchRes(Reserve(outTables.size()));
        for (auto& out: outTables) {
            batchRes.push_back(
                batchGet->Get(out.Path + "/@", TGetOptions()
                    .AttributeFilter(TAttributeFilter()
                        .AddAttribute(TString("id"))
                        .AddAttribute(TString("dynamic"))
                        .AddAttribute(TString("row_count"))
                        .AddAttribute(TString("chunk_row_count"))
                        .AddAttribute(TString("uncompressed_data_size"))
                        .AddAttribute(TString("data_weight"))
                        .AddAttribute(TString("chunk_count"))
                        .AddAttribute(TString("modification_time"))
                        .AddAttribute(TString("schema"))
                        .AddAttribute(TString("revision"))
                        .AddAttribute(TString("content_revision"))
                    )
                ).Apply([out](const TFuture<NYT::TNode>& f) {

                    auto attrs = f.GetValue();

                    TString expectedSortedBy = ToColumnList(out.SortedBy.Parts_);
                    TString realSortedBy = TString("[]");
                    if (attrs.HasKey("schema")) {
                        auto keyColumns = KeyColumnsFromSchema(attrs["schema"]);
                        realSortedBy = ToColumnList(keyColumns.Keys);
                    }
                    YQL_ENSURE(expectedSortedBy == realSortedBy, "Output table " << out.Path
                        << " has unexpected \"sorted_by\" value. Expected: " << expectedSortedBy
                        << ", actual: " << realSortedBy);

                    auto statInfo = MakeIntrusive<TYtTableStatInfo>();
                    statInfo->Id = attrs["id"].AsString();
                    statInfo->RecordsCount = GetTableRowCount(attrs);
                    statInfo->DataSize = GetDataWeight(attrs).GetOrElse(0);
                    statInfo->ChunkCount = attrs["chunk_count"].AsInt64();
                    TString strModifyTime = attrs["modification_time"].AsString();
                    statInfo->ModifyTime = TInstant::ParseIso8601(strModifyTime).Seconds();
                    statInfo->TableRevision = attrs["revision"].IntCast<ui64>();
                    statInfo->Revision = GetContentRevision(attrs);
                    return statInfo;
                })
            );
        }

        batchGet->ExecuteBatch();

        for (size_t i: xrange(outTables.size())) {
            res.OutTableStats.emplace_back(outTables[i].Name, batchRes[i].GetValue());
        }

        return res;
    }

    template <class TSpec>
    static void LocalRawMapReduce(const TSpec& spec, IRawJob* mapper, IInputStream* in, IOutputStream* out)
    {
        YQL_ENSURE(GetRemoteFiles(spec).empty(), "Unexpected remote files in spec");
        const TTempDir tmp;
        for (const auto& f : GetLocalFiles(spec)) {
            TFsPath src(std::get<0U>(f));
            if (src.IsRelative()) {
                src = (TFsPath::Cwd() / src).Fix();
            }
            const TFsPath dst(tmp.Path().Child(src.GetName()));
            YQL_ENSURE(NFs::SymLink(src, dst), "Can't make symlink " << dst << " on " << src);
        }

        struct TJobBinaryPathVisitor {
            TFsPath operator()(const TJobBinaryDefault&) const {
                return GetPersistentExecPath();
            }
            TFsPath operator()(const TJobBinaryLocalPath& item) const {
                return item.Path;
            }
            TFsPath operator()(const TJobBinaryCypressPath&) const {
                ythrow yexception() << "LocalRawMap: unexpected TJobBinaryCypressPath";
            }
        };
        TFsPath src = std::visit(TJobBinaryPathVisitor(), GetJobBinary(spec));

        if (src.IsRelative()) {
            src = (TFsPath::Cwd() / src).Fix();
        }
        const TFsPath dst(tmp.Path().Child(src.GetName()));
        YQL_ENSURE(NFs::SymLink(src, dst), "Can't make symlink " << dst << " on " << src);

        TString jobstate;
        TStringOutput job(jobstate);
        mapper->Save(job);
        job.Finish();

        if (!jobstate.empty()) {
            TFile(tmp.Path().Child("jobstate"), CreateNew | WrOnly).Write(jobstate.data(), jobstate.size());
        }

        TShellCommandOptions opts;
        opts.SetUseShell(false).SetDetachSession(false).SetInputStream(in).SetOutputStream(out);

        opts.Environment.emplace("YQL_SUPPRESS_JOB_STATISTIC", '1');
        opts.Environment.emplace("YT_JOB_ID", '0');
        opts.Environment.emplace("YT_USE_CLIENT_PROTOBUF", TConfig::Get()->UseClientProtobuf ? '1' : '0');

        TList<TString> args;
        args.emplace_back("--yt-map");
        args.emplace_back(TJobFactory::Get()->GetJobName(mapper));
        args.emplace_back(ToString(spec.GetOutputs().size()));
        args.emplace_back(jobstate.empty() ? '0' : '1');

        TShellCommand command(dst.GetPath(), args, opts, tmp.Path());
        switch (const auto status = command.Run().GetStatus()) {
            case TShellCommand::SHELL_FINISHED: break;
            case TShellCommand::SHELL_ERROR: YQL_LOG_CTX_THROW yexception() << command.GetError();
            case TShellCommand::SHELL_INTERNAL_ERROR: YQL_LOG_CTX_THROW yexception() << command.GetInternalError();
            default: YQL_LOG_CTX_THROW yexception() << "Unexpected run status: " << int(status);
        }
        out->Finish();

        YQL_CLOG(INFO, ProviderYt) << command.GetError();

        YQL_ENSURE(command.GetExitCode() == 0, "Job returns: " << command.GetExitCode());
    }

    template <class TResultFactory>
    static TFuture<typename TResultFactory::TResult>
    LocalCalcJob(const TRawMapOperationSpec& spec, IRawJob* mapper, const TString& lambda, TResultFactory&& factory)
    {
        const auto& yson = NYT::NodeListToYsonString({NYT::TNode()("input", lambda)});
        TStringInput in(yson);
        TStringStream out;

        LocalRawMapReduce(spec, mapper, &in, &out);

        const auto& builder = factory.Create();
        for (const auto& reader = NYT::CreateTableReader<NYT::TNode>(&out, NYT::TTableReaderOptions()); reader->IsValid(); reader->Next()) {
            auto& row = reader->GetRow();
            if (!builder->WriteNext(row["output"])) {
                break;
            }
        }
        return MakeFuture(builder->Make());
    }

    static TFuture<void>
    LocalFillJob(const TRawMapOperationSpec& spec, IRawJob* mapper, TTransactionCache::TEntry::TPtr entry)
    {
        const auto& yson = NYT::NodeListToYsonString({NYT::TNode()("input", "dummy")});
        TStringInput in(yson);

        const auto writer = entry->Tx->CreateRawWriter(spec.GetOutputs().front(), *spec.OutputFormat_);

        LocalRawMapReduce(spec, mapper, &in, writer.Get());
        return MakeFuture();
    }

    template <class TExecParamsPtr, class TResultFactory>
    static TFuture<typename TResultFactory::TResult> ExecCalc(
        TString lambda,
        const TExpressionResorceUsage& extraUsage,
        const TString& tmpTable,
        const TExecParamsPtr& execCtx,
        TTransactionCache::TEntry::TPtr entry,
        TResultFactory&& factory,
        const TVector<TString>* columns = nullptr,
        IDataProvider::EResultFormat format = IDataProvider::EResultFormat::Yson
    )
    {
        TRawMapOperationSpec mapOpSpec;
        mapOpSpec.Format(TFormat::YsonBinary());
        TIntrusivePtr<TYqlCalcJob> job;
        auto tmpFiles = std::make_shared<TTempFiles>(execCtx->FileStorage_->GetTemp());

        bool localRun = execCtx->Config_->HasExecuteUdfLocallyIfPossible() ? execCtx->Config_->GetExecuteUdfLocallyIfPossible() : false;
        {
            TUserJobSpec userJobSpec;
            TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                execCtx->FunctionRegistry_->SupportsSizedAllocators());
            alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
            auto secureParamsProvider = MakeSimpleSecureParamsProvider(execCtx->Options_.SecureParams());
            TNativeYtLambdaBuilder builder(alloc, execCtx->FunctionRegistry_, *execCtx->Session_, secureParamsProvider.get());
            THolder<TCodecContext> codecCtx;
            TString pathPrefix;
            TProgramBuilder pgmBuilder(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_);
            TGatewayTransformer transform(execCtx, entry, pgmBuilder, *tmpFiles);
            transform.SetTwoPhaseTransform();
            TRuntimeNode root = builder.Deserialize(lambda);
            root = builder.TransformAndOptimizeProgram(root, transform);
            if (transform.HasSecondPhase()) {
                root = builder.TransformAndOptimizeProgram(root, transform);
                codecCtx.Reset(new TCodecContext(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_));
                pathPrefix = tmpFiles->TmpDir.GetPath() + '/';
            }
            size_t nodeCount = 0;
            std::tie(lambda, nodeCount) = builder.Serialize(root);
            if (nodeCount > execCtx->Options_.Config()->LLVMNodeCountLimit.Get(execCtx->Cluster_).GetOrElse(DEFAULT_LLVM_NODE_COUNT_LIMIT)) {
                execCtx->Options_.OptLLVM("OFF");
            }

            if (transform.CanExecuteInternally()) {
                TExploringNodeVisitor explorer;
                auto localGraph = builder.BuildLocalGraph(GetGatewayNodeFactory(codecCtx.Get(), nullptr, execCtx->UserFiles_, pathPrefix),
                    execCtx->Options_.UdfValidateMode(), NUdf::EValidatePolicy::Exception,
                    "OFF" /* don't use LLVM locally */, EGraphPerProcess::Multi, explorer, root);
                auto& graph = std::get<0>(localGraph);
                const TBindTerminator bind(graph->GetTerminator());
                graph->Prepare();
                auto value = graph->GetValue();

                switch (format) {
                    case IDataProvider::EResultFormat::Skiff: {
                        TMemoryUsageInfo memInfo("Calc");
                        THolderFactory holderFactory(alloc.Ref(), memInfo, execCtx->FunctionRegistry_);
                        TCodecContext codecCtx(builder.GetTypeEnvironment(), *execCtx->FunctionRegistry_, &holderFactory);

                        auto skiffBuilder = factory.Create(codecCtx, holderFactory);
                        skiffBuilder->WriteValue(value, root.GetStaticType());
                        return MakeFuture(skiffBuilder->Make());
                    }
                    case IDataProvider::EResultFormat::Yson: {
                        auto ysonBuilder = factory.Create();
                        ysonBuilder->WriteValue(value, root.GetStaticType());
                        return MakeFuture(ysonBuilder->Make());
                    }
                    default:
                        YQL_LOG_CTX_THROW yexception() << "Invalid result type: " << format;
                }
            }
            localRun = localRun && transform.CanExecuteLocally();
            {
                TStringInput in(lambda);
                TStringStream out;
                TBrotliCompress compressor(&out, 8);
                TransferData(&in, &compressor);
                compressor.Finish();
                lambda = out.Str();
            }
            job = MakeIntrusive<TYqlCalcJob>();
            transform.ApplyJobProps(*job);
            transform.ApplyUserJobSpec(userJobSpec, localRun);
            FillUserJobSpec(userJobSpec, execCtx, extraUsage, transform.GetUsedMemory(), execCtx->EstimateLLVMMem(nodeCount), localRun);
            mapOpSpec.MapperSpec(userJobSpec);
        }

        if (columns) {
            job->SetColumns(*columns);
        }
        job->SetUseResultYson(factory.UseResultYson());
        job->SetOptLLVM(execCtx->Options_.OptLLVM());
        job->SetUdfValidateMode(execCtx->Options_.UdfValidateMode());

        mapOpSpec.AddInput(tmpTable);
        mapOpSpec.AddOutput(tmpTable);
        FillOperationSpec(mapOpSpec, execCtx);

        if (localRun && mapOpSpec.MapperSpec_.Files_.empty()) {
            return LocalCalcJob(mapOpSpec, job.Get(), lambda, std::move(factory));
        }

        if (!entry) {
            entry = execCtx->GetOrCreateEntry();
        }

        const TString tmpFolder = GetTablesTmpFolder(*execCtx->Options_.Config());
        execCtx->SetCacheItem({tmpTable}, {NYT::TNode::CreateMap()}, tmpFolder);

        TFuture<bool> future = execCtx->Config_->GetLocalChainTest()
            ? MakeFuture<bool>(false)
            : execCtx->LookupQueryCacheAsync();
        return future
            .Apply([execCtx, entry, mapOpSpec = std::move(mapOpSpec), job, tmpTable, lambda, extraUsage, tmpFiles] (const TFuture<bool>& f) {
                if (f.GetValue()) {
                    execCtx->QueryCacheItem.Destroy();
                    return MakeFuture();
                }
                NYT::TNode spec = execCtx->Session_->CreateSpecWithDesc(execCtx->CodeSnippets_);
                FillSpec(spec, *execCtx, entry, extraUsage.Cpu, Nothing(), EYtOpProp::WithMapper);

                PrepareTempDestination(tmpTable, execCtx, entry, entry->Tx);

                {
                    auto writer = entry->Tx->CreateTableWriter<NYT::TNode>(tmpTable, NYT::TTableWriterOptions().Config(NYT::TNode()("max_row_weight", 128_MB)));
                    writer->AddRow(NYT::TNode()("input", lambda));
                    writer->Finish();
                }

                TOperationOptions opOpts;
                FillOperationOptions(opOpts, execCtx, entry);
                opOpts.StartOperationMode(TOperationOptions::EStartOperationMode::AsyncPrepare).Spec(spec);

                return execCtx->RunOperation([entry, execCtx, job, mapOpSpec = std::move(mapOpSpec), opOpts = std::move(opOpts), tmpFiles]() {
                    execCtx->SetNodeExecProgress("Uploading artifacts");
                    return entry->Tx->RawMap(mapOpSpec, job, opOpts);
                });
            })
            .Apply([execCtx, tmpTable, entry, factory = std::move(factory), format](const auto& f) {
                f.GetValue();

                auto reader = entry->Tx->CreateTableReader<NYT::TNode>(tmpTable);

                TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(),
                    execCtx->FunctionRegistry_->SupportsSizedAllocators());
                alloc.SetLimit(execCtx->Options_.Config()->DefaultCalcMemoryLimit.Get().GetOrElse(0));
                TMemoryUsageInfo memInfo("Calc");
                TTypeEnvironment env(alloc);
                THolderFactory holderFactory(alloc.Ref(), memInfo, execCtx->FunctionRegistry_);

                switch (format) {
                    case IDataProvider::EResultFormat::Skiff: {
                        TCodecContext codecCtx(env, *execCtx->FunctionRegistry_, &holderFactory);

                        auto skiffBuilder = factory.Create(codecCtx, holderFactory);
                        for (; reader->IsValid(); reader->Next()) {
                            auto& row = reader->GetRow();
                            if (!skiffBuilder->WriteNext(row["output"])) {
                                break;
                            }
                        }
                        return skiffBuilder->Make();
                    }
                    case IDataProvider::EResultFormat::Yson: {
                        auto ysonBuilder = factory.Create();
                        for (; reader->IsValid(); reader->Next()) {
                            auto& row = reader->GetRow();
                            if (!ysonBuilder->WriteNext(row["output"])) {
                                break;
                            }
                        }
                        return ysonBuilder->Make();
                    }
                    default:
                        YQL_LOG_CTX_THROW yexception() << "Unexpected result type: " << format;
                }
            })
            .Apply([tmpTable, execCtx, entry](const TFuture<typename TResultFactory::TResult>& f) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(execCtx->LogCtx_);
                auto res = f.GetValue(); // rethrow error if any
                execCtx->StoreQueryCache();
                entry->RemoveInternal(tmpTable);
                return res;
            });
    }

    template <class TExecParamsPtr>
    static void PrepareCommonAttributes(
            NYT::TNode& attrs,
            const TExecParamsPtr& execCtx,
            const TString& cluster,
            bool createTable)
    {
        if (auto compressionCodec = execCtx->Options_.Config()->TemporaryCompressionCodec.Get(cluster)) {
            attrs["compression_codec"] = *compressionCodec;
        }
        if (auto erasureCodec = execCtx->Options_.Config()->TemporaryErasureCodec.Get(cluster)) {
            attrs["erasure_codec"] = ToString(*erasureCodec);
        }
        if (auto optimizeFor = execCtx->Options_.Config()->OptimizeFor.Get(cluster)) {
            attrs["optimize_for"] = ToString(*optimizeFor);
        }
        if (auto ttl = execCtx->Options_.Config()->TempTablesTtl.Get().GetOrElse(TDuration::Zero())) {
            attrs["expiration_timeout"] = ttl.MilliSeconds();
        }

        if (createTable) {
            if (auto replicationFactor = execCtx->Options_.Config()->TemporaryReplicationFactor.Get(cluster)) {
                attrs["replication_factor"] = static_cast<i64>(*replicationFactor);
            }
            if (auto media = execCtx->Options_.Config()->TemporaryMedia.Get(cluster)) {
                attrs["media"] = *media;
            }
            if (auto primaryMedium = execCtx->Options_.Config()->TemporaryPrimaryMedium.Get(cluster)) {
                attrs["primary_medium"] = *primaryMedium;
            }
        }
    }

    template <class TExecParamsPtr>
    static void PrepareAttributes(
        NYT::TNode& attrs,
        const TOutputInfo& out,
        const TExecParamsPtr& execCtx,
        const TString& cluster,
        bool createTable)
    {
        PrepareCommonAttributes<TExecParamsPtr>(attrs, execCtx, cluster, createTable);

        NYT::MergeNodes(attrs, out.AttrSpec);

        if (createTable) {
            const auto nativeTypeCompat = execCtx->Options_.Config()->NativeYtTypeCompatibility.Get(cluster).GetOrElse(NTCF_LEGACY);
            attrs["schema"] = RowSpecToYTSchema(out.Spec[YqlRowSpecAttribute], nativeTypeCompat, out.ColumnGroups).ToNode();
        }
    }

    template <class TExecParamsPtr>
    static TVector<TRichYPath> PrepareDestinations(
        const TVector<TOutputInfo>& outTables,
        const TExecParamsPtr& execCtx,
        const TTransactionCache::TEntry::TPtr& entry,
        bool createTables)
    {
        auto cluster = execCtx->Cluster_;

        TVector<TRichYPath> res;
        for (auto& out: outTables) {
            res.push_back(TRichYPath(out.Path));
            entry->DeleteAtFinalize(out.Path);
        }

        if (createTables) {
            TVector<TString> outPaths;
            auto batchCreate = entry->Tx->CreateBatchRequest();
            TVector<TFuture<TLockId>> batchCreateRes;

            for (auto& out: outTables) {
                NYT::TNode attrs = NYT::TNode::CreateMap();

                PrepareAttributes(attrs, out, execCtx, cluster, true);

                YQL_CLOG(INFO, ProviderYt) << "Create tmp table " << out.Path << ", attrs: " << NYT::NodeToYsonString(attrs);

                // Force table recreation, because some tables may exist after query cache lookup
                batchCreateRes.push_back(batchCreate->Create(out.Path, NT_TABLE, TCreateOptions().Force(true).Attributes(attrs)));
                outPaths.push_back(out.Path);
            }
            entry->CreateDefaultTmpFolder();
            CreateParents(outPaths, entry->CacheTx);

            batchCreate->ExecuteBatch();
            WaitExceptionOrAll(batchCreateRes).GetValue();
        }
        else {
            // set attributes in transactions
            const auto multiSet = execCtx->Options_.Config()->_UseMultisetAttributes.Get().GetOrElse(DEFAULT_USE_MULTISET_ATTRS);
            if (multiSet) {
                for (auto& out: outTables) {
                    NYT::TNode attrs = NYT::TNode::CreateMap();
                    PrepareAttributes(attrs, out, execCtx, cluster, false);
                    YQL_CLOG(INFO, ProviderYt) << "Update tmp table " << out.Path << ", attrs: " << NYT::NodeToYsonString(attrs);
                    entry->Tx->MultisetAttributes(out.Path + "/@", attrs.AsMap(), NYT::TMultisetAttributesOptions());
                }
            } else {
                auto batchSet = entry->Tx->CreateBatchRequest();
                TVector<TFuture<void>> batchSetRes;

                for (auto& out: outTables) {
                    NYT::TNode attrs = NYT::TNode::CreateMap();

                    PrepareAttributes(attrs, out, execCtx, cluster, false);
                    YQL_CLOG(INFO, ProviderYt) << "Update tmp table " << out.Path << ", attrs: " << NYT::NodeToYsonString(attrs);
                    for (auto& attr: attrs.AsMap()) {
                        batchSetRes.push_back(batchSet->Set(TStringBuilder() << out.Path << "/@" << attr.first, attr.second));
                    }
                }

                batchSet->ExecuteBatch();
                WaitExceptionOrAll(batchSetRes).GetValue();
            }
        }

        return res;
    }

    template <class TExecParamsPtr>
    static void PrepareTempDestination(
        const TString& tmpTable,
        const TExecParamsPtr& execCtx,
        const TTransactionCache::TEntry::TPtr& entry,
        const NYT::ITransactionPtr tx)
    {
        auto cluster = execCtx->Cluster_;

        NYT::TNode attrs = NYT::TNode::CreateMap();
        PrepareCommonAttributes(attrs, execCtx, cluster, true);

        YQL_CLOG(INFO, ProviderYt) << "Table " << tmpTable << ", attrs: " << NYT::NodeToYsonString(attrs);

        entry->CreateDefaultTmpFolder();
        CreateParents(TVector<TString>{tmpTable}, entry->CacheTx);

        tx->Create(tmpTable, NT_TABLE, TCreateOptions().Force(true).Attributes(attrs));
        entry->DeleteAtFinalizeInternal(tmpTable);
    }

    TSession::TPtr GetSession(const TString& sessionId, bool failIfNotExists = true) const {
        auto guard = Guard(Mutex_);
        if (auto p = Sessions_.FindPtr(sessionId)) {
            return *p;
        }
        if (failIfNotExists) {
            YQL_LOG_CTX_THROW yexception() << "Session doesn't exist: " << sessionId;
        }
        return {};
    }

    template <class TOptions>
    typename TExecContext<TOptions>::TPtr MakeExecCtx(
        TOptions&& options,
        const TSession::TPtr& session,
        const TString& cluster,
        const TExprNode* root,
        TExprContext* exprCtx) const
    {
        auto ctx = MakeIntrusive<TExecContext<TOptions>>(Services_, Clusters_, MkqlCompiler_, std::move(options), session, cluster, UrlMapper_);
        if (root) {
            YQL_ENSURE(exprCtx);
            if (TYtTransientOpBase::Match(root)) {
                ctx->CodeSnippets_.emplace_back("settings",
                    ConvertToAst(*root->Child(TYtTransientOpBase::idx_Settings), *exprCtx, 0, true)
                        .Root->ToString(TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine | TAstPrintFlags::AdaptArbitraryContent));
                if (TYtMap::Match(root)) {
                    ctx->CodeSnippets_.emplace_back("mapper",
                        ConvertToAst(*root->Child(TYtMap::idx_Mapper), *exprCtx, 0, true)
                            .Root->ToString(TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine | TAstPrintFlags::AdaptArbitraryContent));
                } else if (TYtReduce::Match(root)) {
                    ctx->CodeSnippets_.emplace_back("reducer",
                        ConvertToAst(*root->Child(TYtReduce::idx_Reducer), *exprCtx, 0, true)
                            .Root->ToString(TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine | TAstPrintFlags::AdaptArbitraryContent));
                } else if (TYtMapReduce::Match(root)) {
                    ctx->CodeSnippets_.emplace_back("mapper",
                        ConvertToAst(*root->Child(TYtMapReduce::idx_Mapper), *exprCtx, 0, true)
                            .Root->ToString(TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine | TAstPrintFlags::AdaptArbitraryContent));
                    ctx->CodeSnippets_.emplace_back("reducer",
                        ConvertToAst(*root->Child(TYtMapReduce::idx_Reducer), *exprCtx, 0, true)
                            .Root->ToString(TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine | TAstPrintFlags::AdaptArbitraryContent));
                }
            } else if (TYtFill::Match(root)) {
                ctx->CodeSnippets_.emplace_back("lambda",
                    ConvertToAst(*root->Child(TYtFill::idx_Content), *exprCtx, 0, true)
                        .Root->ToString(TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine | TAstPrintFlags::AdaptArbitraryContent));
            } else if (TYtPublish::Match(root)) {
                ctx->CodeSnippets_.emplace_back("settings",
                    ConvertToAst(*root->Child(TYtPublish::idx_Settings), *exprCtx, 0, true)
                        .Root->ToString(TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine | TAstPrintFlags::AdaptArbitraryContent));
            } else {
                ctx->CodeSnippets_.emplace_back("code",
                    ConvertToAst(*root, *exprCtx, 0, true).Root->ToString(TAstPrintFlags::ShortQuote | TAstPrintFlags::PerLine | TAstPrintFlags::AdaptArbitraryContent));
            }
        }
        return ctx;
    }

private:
    const TYtNativeServices Services_;
    const TConfigClusters::TPtr Clusters_;
    TIntrusivePtr<NCommon::TMkqlCommonCallableCompiler> MkqlCompiler_;
    TMutex Mutex_;
    THashMap<TString, TSession::TPtr> Sessions_;
    const TYtUrlMapper UrlMapper_;
    IStatUploader::TPtr StatUploader_;
};

} // NNative

IYtGateway::TPtr CreateYtNativeGateway(const TYtNativeServices& services) {
    return MakeIntrusive<NNative::TYtNativeGateway>(services);
}

} // NYql
