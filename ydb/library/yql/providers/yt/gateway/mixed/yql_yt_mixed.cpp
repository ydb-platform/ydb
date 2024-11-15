#include "yql_yt_mixed.h"

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_native.h>
#include <ydb/library/yql/providers/yt/gateway/lib/yt_helpers.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <library/cpp/threading/future/wait/wait.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/ptr.h>
#include <util/generic/guid.h>


namespace NYql {
namespace {

using namespace NThreading;
using namespace NNodes;

class TMixedYtGateway final: public IYtGateway {
public:
    TMixedYtGateway(const TYtNativeServices& services)
        : FileServices_(NFile::TYtFileServices::Make(services.FunctionRegistry, {}, services.FileStorage))
        , FileGateway_(CreateYtFileGateway(FileServices_))
        , NativeGateway_(CreateYtNativeGateway(services))
    {
    }

    void OpenSession(TOpenSessionOptions&& options) final {
        FileGateway_->OpenSession(TOpenSessionOptions(options));
        NativeGateway_->OpenSession(std::move(options));
    }

    TFuture<void> CloseSession(TCloseSessionOptions&& options) final {
        return WaitAll(
            FileGateway_->CloseSession(TCloseSessionOptions(options)),
            NativeGateway_->CloseSession(std::move(options))
        );
    }

    TFuture<void> CleanupSession(TCleanupSessionOptions&& options) final {
        return WaitAll(
            FileGateway_->CleanupSession(TCleanupSessionOptions(options)),
            NativeGateway_->CleanupSession(std::move(options))
        );
    }

    TFuture<TFinalizeResult> Finalize(TFinalizeOptions&& options) final {
        for (auto& p : FileServices_->GetTablesMapping()) {
            try {
                NFs::Remove(p.second);
                NFs::Remove(p.second + ".attr");
            } catch (...) {
            }
        }

        return NativeGateway_->Finalize(std::move(options));
    }

    TFuture<TCanonizePathsResult> CanonizePaths(TCanonizePathsOptions&& options) final {
        return NativeGateway_->CanonizePaths(std::move(options));
    }

    TFuture<TTableInfoResult> GetTableInfo(TGetTableInfoOptions&& options) final {
        std::vector<size_t> nativeNdx;
        std::vector<size_t> fileNdx;
        TGetTableInfoOptions nativeOptions(options);
        nativeOptions.Tables().clear();
        TGetTableInfoOptions fileOptions(options);
        fileOptions.Tables().clear();
        for (size_t i = 0; i < options.Tables().size(); ++i) {
            if (options.Tables()[i].Anonymous()) {
                fileOptions.Tables().push_back(options.Tables()[i]);
                fileNdx.push_back(i);
            } else {
                nativeOptions.Tables().push_back(options.Tables()[i]);
                nativeNdx.push_back(i);
            }
        }
        TTableInfoResult dummyRes;
        dummyRes.SetSuccess();
        TFuture<TTableInfoResult> nativeResF = nativeOptions.Tables().empty() ? MakeFuture(dummyRes) : NativeGateway_->GetTableInfo(std::move(nativeOptions));
        TFuture<TTableInfoResult> fileResF = fileOptions.Tables().empty() ? MakeFuture(dummyRes) : FileGateway_->GetTableInfo(std::move(fileOptions));

        return WaitAll(nativeResF.IgnoreResult(), fileResF.IgnoreResult()).Apply([fs = FileServices_, ng = NativeGateway_, options = std::move(options), nativeResF, fileResF, nativeNdx = std::move(nativeNdx), fileNdx = std::move(fileNdx)](const TFuture<void>&) {
            auto nativeRes = nativeResF.GetValueSync();
            if (!nativeRes.Success()) {
                return nativeResF;
            }
            auto fileRes = fileResF.GetValueSync();
            if (!fileRes.Success()) {
                return fileResF;
            }

            TTableInfoResult res;
            res.SetStatus(nativeRes.Status());
            res.AddIssues(nativeRes.Issues());
            res.AddIssues(fileRes.Issues());
            res.Data.resize(options.Tables().size());
            for (size_t i = 0; i < fileNdx.size(); ++i) {
                res.Data.at(fileNdx[i]) = fileRes.Data.at(i);
            }
            TVector<TDownloadTablesReq> downloads;
            TVector<std::pair<TString, TString>> locks;
            TVector<std::pair<TString, TString>> attrs;
            for (size_t i = 0; i < nativeNdx.size(); ++i) {
                res.Data.at(nativeNdx[i]) = nativeRes.Data.at(i);
                const TTableReq& req = options.Tables().at(nativeNdx[i]);
                const auto fullTableName = TString(YtProviderName).append('.').append(req.Cluster()).append('.').append(req.Table());
                const auto tablePath = (TFsPath(fs->GetTmpDir()) / CreateGuidAsString()).GetPath();

                fs->GetTablesMapping()[fullTableName] = tablePath;
                if (nativeRes.Data[i].Meta->DoesExist && nativeRes.Data[i].Meta->SqlView.empty()) {
                    downloads.push_back(
                        TDownloadTablesReq()
                            .Cluster(req.Cluster())
                            .Table(req.Table())
                            .Anonymous(req.Anonymous())
                            .TargetPath(tablePath)
                        );
                    locks.emplace_back(tablePath, fullTableName);
                    NYT::TNode attrsContent = NYT::TNode::CreateMap();
                    for (const auto& p: nativeRes.Data[i].Meta->Attrs) {
                        if (READ_SCHEMA_ATTR_NAME == p.first || YqlRowSpecAttribute == p.first || SCHEMA_ATTR_NAME == p.first || FORMAT_ATTR_NAME == p.first) {
                            attrsContent[p.first] = NYT::NodeFromYsonString(p.second);
                        } else if (INFER_SCHEMA_ATTR_NAME == p.first) {
                            attrsContent["infer_schema"] = true;
                        } else {
                            attrsContent[p.first] = p.second;
                        }
                    }
                    attrs.emplace_back(tablePath + ".attr", NYT::NodeToYsonString(attrsContent, ::NYson::EYsonFormat::Pretty));
                }
            }
            if (!downloads.empty()) {
                auto downloadFuture = ng->DownloadTables(TDownloadTablesOptions(options.SessionId())
                    .Tables(std::move(downloads))
                    .Config(options.Config())
                    .Epoch(options.Epoch()));
                return downloadFuture.Apply([res = std::move(res), locks = std::move(locks), attrs = std::move(attrs), fs](const TFuture<TDownloadTablesResult>& f) mutable {
                    try {
                        auto downloadRes = f.GetValueSync();
                        if (!downloadRes.Success()) {
                            res.SetStatus(downloadRes.Status());
                            res.AddIssues(downloadRes.Issues());
                        } else {
                            for (auto& p: locks) {
                                fs->LockPath(p.first, p.second);
                            }
                            for (auto& p: attrs) {
                                TOFStream ofAttr(p.first);
                                ofAttr.Write(p.second);
                            }
                        }
                        return res;
                    } catch (...) {
                        return ResultFromCurrentException<TTableInfoResult>();
                    }
                });
            }
            return MakeFuture(std::move(res));
        });
    }

    TFuture<TTableRangeResult> GetTableRange(TTableRangeOptions&& options) final {
        return NativeGateway_->GetTableRange(std::move(options));
    }

    TFuture<TFolderResult> GetFolder(TFolderOptions&& options) final {
        return NativeGateway_->GetFolder(std::move(options));
    }

    TFuture<TBatchFolderResult> ResolveLinks(TResolveOptions&& options) final {
        return NativeGateway_->ResolveLinks(std::move(options));
    }

    TFuture<TBatchFolderResult> GetFolders(TBatchFolderOptions&& options) final {
        return NativeGateway_->GetFolders(std::move(options));
    }

    TFuture<TResOrPullResult> ResOrPull(const TExprNode::TPtr& node, TExprContext& ctx, TResOrPullOptions&& options) final {
        return FileGateway_->ResOrPull(node, ctx, std::move(options));
    }

    TFuture<TRunResult> Run(const TExprNode::TPtr& node, TExprContext& ctx, TRunOptions&& options) final {
        return FileGateway_->Run(node, ctx, std::move(options));
    }

    TFuture<TRunResult> Prepare(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) const final {
        return FileGateway_->Prepare(node, ctx, std::move(options));
    }

    TFuture<TCalcResult> Calc(const TExprNode::TListType& nodes, TExprContext& ctx, TCalcOptions&& options) final {
        return FileGateway_->Calc(nodes, ctx, std::move(options));
    }

    TFuture<TPublishResult> Publish(const TExprNode::TPtr& node, TExprContext& ctx, TPublishOptions&& options) final {
        TPublishOptions optionsCopy(options);
        auto publish = TYtPublish(node);
        auto cluster = publish.DataSink().Cluster().StringValue();
        auto table = publish.Publish().Name().StringValue();
        const bool isAnonymous = NYql::HasSetting(publish.Publish().Settings().Ref(), EYtSettingType::Anonymous);

        auto f = FileGateway_->Publish(node, ctx, std::move(options));
        if (!isAnonymous) {
            f = f.Apply([options = std::move(optionsCopy), cluster, table, fs = FileServices_, ng = NativeGateway_](const TFuture<TPublishResult>& f) {
                auto res = f.GetValueSync();
                if (!res.Success()) {
                    return f;
                }

                auto path = fs->GetTablePath(cluster, table, false, true);
                TString attrs = "{}";
                if (NFs::Exists(path + ".attr")) {
                    TIFStream input(path + ".attr");
                    attrs = input.ReadAll();
                }
                return ng->UploadTable(
                    TUploadTableOptions(options.SessionId())
                        .Cluster(cluster)
                        .Table(table)
                        .Path(path)
                        .Attrs(attrs)
                        .Config(options.Config())
                ).Apply([res = std::move(res)](const TFuture<TUploadTableResult>& f) mutable {
                    auto uploadRes = f.GetValueSync();
                    if (!uploadRes.Success()) {
                        res.SetStatus(uploadRes.Status());
                        res.AddIssues(uploadRes.Issues());
                    }
                    return res;
                });
            });
        }

        return f;
    }

    TFuture<TCommitResult> Commit(TCommitOptions&& options) final {
        return FileGateway_->Commit(std::move(options));
    }

    TFuture<TDropTrackablesResult> DropTrackables(TDropTrackablesOptions&& options) final {
        return FileGateway_->DropTrackables(std::move(options));
    }

    TFuture<TPathStatResult> PathStat(TPathStatOptions&& options) final {
        return FileGateway_->PathStat(std::move(options));
    }

    TPathStatResult TryPathStat(TPathStatOptions&& options) final {
        return FileGateway_->TryPathStat(std::move(options));
    }

    bool TryParseYtUrl(const TString& url, TString* cluster, TString* path) const final {
        return NativeGateway_->TryParseYtUrl(url, cluster, path);
    }

    TString GetDefaultClusterName() const final {
        return NativeGateway_->GetDefaultClusterName();
    }

    TString GetClusterServer(const TString& cluster) const final {
        return NativeGateway_->GetClusterServer(cluster);
    }

    NYT::TRichYPath GetRealTable(const TString& sessionId, const TString& cluster, const TString& table, ui32 epoch, const TString& tmpFolder) const final {
        return NativeGateway_->GetRealTable(sessionId, cluster, table, epoch, tmpFolder);
    }

    NYT::TRichYPath GetWriteTable(const TString& sessionId, const TString& cluster, const TString& table, const TString& tmpFolder) const final {
        return NativeGateway_->GetWriteTable(sessionId, cluster, table, tmpFolder);
    }

    TFuture<TDownloadTablesResult> DownloadTables(TDownloadTablesOptions&& options) final {
        return NativeGateway_->DownloadTables(std::move(options));
    }

    TFuture<TUploadTableResult> UploadTable(TUploadTableOptions&& options) final {
        return NativeGateway_->UploadTable(std::move(options));
    }

    TFuture<TRunResult> GetTableStat(const TExprNode::TPtr& node, TExprContext& ctx, TPrepareOptions&& options) final {
        return FileGateway_->GetTableStat(node, ctx, std::move(options));
    }

    TFullResultTableResult PrepareFullResultTable(TFullResultTableOptions&& options) final {
        return NativeGateway_->PrepareFullResultTable(std::move(options));
    }

    void SetStatUploader(IStatUploader::TPtr statUploader) final {
        NativeGateway_->SetStatUploader(statUploader);
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) final {
        FileGateway_->RegisterMkqlCompiler(compiler);
    }

    TGetTablePartitionsResult GetTablePartitions(TGetTablePartitionsOptions&& options) override {
        return NativeGateway_->GetTablePartitions(std::move(options));
    }

    void AddCluster(const TYtClusterConfig& cluster) override {
        NativeGateway_->AddCluster(cluster);
    }

private:
    NFile::TYtFileServices::TPtr FileServices_;
    IYtGateway::TPtr FileGateway_;
    IYtGateway::TPtr NativeGateway_;
};

} // namespace

IYtGateway::TPtr CreateYtMixedGateway(const TYtNativeServices& services) {
    return MakeIntrusive<TMixedYtGateway>(services);
}

} // namspace NYql
