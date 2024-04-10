#include "yql_dq_control.h"

#include <ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>
#include <ydb/library/yql/providers/dq/config/config.pb.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

#include <library/cpp/svnversion/svnversion.h>

#include <util/memory/blob.h>
#include <util/string/builder.h>
#include <util/system/file.h>

namespace NYql {

using TFileResource = Yql::DqsProto::TFile;

const TString DqStrippedSuffied = ".s";

class TDqControl : public IDqControl {

public:
    TDqControl(const NYdbGrpc::TGRpcClientConfig &grpcConf, int threads, const TVector<TFileResource> &files)
        : GrpcClient(threads)
        , Service(GrpcClient.CreateGRpcServiceConnection<Yql::DqsProto::DqService>(grpcConf))
        , Files(files)
    { }

    // after call, forking process in not allowed
    bool IsReady(const TMap<TString, TString>& additinalFiles) override {
        Yql::DqsProto::IsReadyRequest request;
        for (const auto& file : Files) {
            *request.AddFiles() = file;
        }

        for (const auto& [path, objectId] : additinalFiles){
            TFileResource r;
            r.SetLocalPath(path);
            r.SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
            r.SetObjectId(objectId);
            r.SetSize(TFile(path, OpenExisting | RdOnly).GetLength());
            *request.AddFiles() = r;
        }

        auto promise = NThreading::NewPromise<bool>();
        auto callback = [promise](NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::IsReadyResponse&& resp) mutable {
            Y_UNUSED(resp);

            promise.SetValue(status.Ok() && resp.GetIsReady());
        };

        NYdbGrpc::TCallMeta meta;
        meta.Timeout = TDuration::Seconds(1);
        Service->DoRequest<Yql::DqsProto::IsReadyRequest, Yql::DqsProto::IsReadyResponse>(
            request, callback, &Yql::DqsProto::DqService::Stub::AsyncIsReady, meta);

        try {
            return promise.GetFuture().GetValueSync();
        } catch (...) {
            YQL_CLOG(INFO, ProviderDq) << "DqControl IsReady Exception " << CurrentExceptionMessage();
            return false;
        }
    }

private:
    NYdbGrpc::TGRpcClientLow GrpcClient;
    std::unique_ptr<NYdbGrpc::TServiceConnection<Yql::DqsProto::DqService>> Service;
    const TVector<TFileResource> &Files;
};


class TDqControlFactory : public IDqControlFactory {

public:
    TDqControlFactory(
            const TString &host,
            int port,
            int threads,
            const TMap<TString, TString>& udfs,
            const TString& vanillaLitePath,
            const TString& vanillaLiteMd5,
            const THashSet<TString> &filter,
            bool enableStrip,
            const TFileStoragePtr& fileStorage
        )
        : Threads(threads)
        , GrpcConf(TStringBuilder() << host << ":" << port)
        , IndexedUdfFilter(filter)
        , EnableStrip(enableStrip)
        , FileStorage(fileStorage)
    {
        if (!vanillaLitePath.empty()) {
            TString path = vanillaLitePath;
            TString objectId = GetProgramCommitId();

            TString newPath, newObjectId;
            std::tie(newPath, newObjectId) = GetPathAndObjectId(path, objectId, vanillaLiteMd5);

            TFileResource vanillaLite;
            vanillaLite.SetLocalPath(newPath);
            vanillaLite.SetName(vanillaLitePath.substr(vanillaLitePath.rfind('/') + 1));
            vanillaLite.SetObjectType(Yql::DqsProto::TFile::EEXE_FILE);
            vanillaLite.SetObjectId(newObjectId);
            vanillaLite.SetSize(TFile(newPath, OpenExisting | RdOnly).GetLength());
            Files.push_back(vanillaLite);
        }

        for (const auto& [path, objectId] : udfs){
            YQL_CLOG(DEBUG, ProviderDq) << "DQ control, adding file: " << path << " with objectId " << objectId;
            TString newPath, newObjectId;
            std::tie(newPath, newObjectId) = GetPathAndObjectId(path, objectId, objectId);

            YQL_CLOG(DEBUG, ProviderDq) << "DQ control, rewrite path/objectId: " << newPath << ", " << newObjectId;
            TFileResource r;
            r.SetLocalPath(newPath);
            r.SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
            r.SetObjectId(newObjectId);
            r.SetSize(TFile(newPath, OpenExisting | RdOnly).GetLength());
            Files.push_back(r);
        }
    }

    IDqControlPtr GetControl() override {
        return new TDqControl(GrpcConf, Threads, Files);
    }

    const THashSet<TString>& GetIndexedUdfFilter() override {
        return IndexedUdfFilter;
    }

    bool StripEnabled() const override {
        return EnableStrip;
    }

private:
    std::tuple<TString, TString> GetPathAndObjectId(const TString& path, const TString& objectId, const TString& md5 = {}) {
        if (!EnableStrip) {
            return std::make_tuple(path, objectId);
        }

        TFileLinkPtr& fileLink = FileLinks[objectId];
        if (!fileLink) {
            fileLink = FileStorage->PutFileStripped(path, md5);
        }

        return std::make_tuple(fileLink->GetPath(), objectId + DqStrippedSuffied);
    }

    int Threads;
    TVector<TFileResource> Files;
    NYdbGrpc::TGRpcClientConfig GrpcConf;
    THashSet<TString> IndexedUdfFilter;
    THashMap<TString, TFileLinkPtr> FileLinks;
    bool EnableStrip;
    const TFileStoragePtr FileStorage;
};

IDqControlFactoryPtr CreateDqControlFactory(const NProto::TDqConfig& config, const TMap<TString, TString>& udfs, const TFileStoragePtr& fileStorage) {
    THashSet<TString> indexedUdfFilter(config.GetControl().GetIndexedUdfsToWarmup().begin(), config.GetControl().GetIndexedUdfsToWarmup().end());
    return CreateDqControlFactory(
        config.GetPort(),
        config.GetYtBackends()[0].GetVanillaJobLite(),
        config.GetYtBackends()[0].GetVanillaJobLiteMd5(),
        config.GetControl().GetEnableStrip(),
        indexedUdfFilter,
        udfs,
        fileStorage
    );
}

IDqControlFactoryPtr CreateDqControlFactory(
    const uint32_t port,
    const TString& vanillaJobLite,
    const TString& vanillaJobLiteMd5,
    const bool enableStrip,
    const THashSet<TString> indexedUdfFilter,
    const TMap<TString, TString>& udfs,
    const TFileStoragePtr& fileStorage)
{
    return new TDqControlFactory(
        "localhost",
        port,
        2,
        udfs,
        vanillaJobLite,
        vanillaJobLiteMd5,
        indexedUdfFilter,
        enableStrip,
        fileStorage
    );
}

} // namespace NYql
