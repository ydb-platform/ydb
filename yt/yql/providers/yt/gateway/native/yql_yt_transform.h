#pragma once

#include <yt/yql/providers/yt/gateway/lib/transform.h>
#include <yt/yql/providers/yt/gateway/lib/transaction_cache.h>

namespace NYql {

template<typename TExecContextPtr>
class TNativeGatewayTransformer: public TGatewayTransformer<TExecContextPtr> {
public:
    TNativeGatewayTransformer(TExecContextPtr execCtx, ITableDownloaderFunc downloader,
        NKikimr::NMiniKQL::TProgramBuilder& builder, TTransactionCache::TEntry::TPtr entry)
        : TGatewayTransformer<TExecContextPtr>(execCtx, downloader, builder)
        , Entry_(entry)
        , HasFilesToDump_(std::make_shared<bool>(false))
    {
    }

    void ApplyUserJobSpec(NYT::TUserJobSpec& spec, bool localRun);

    bool HasFilesToDump() const {
        return *HasFilesToDump_;
    }

private:
    NYT::ITransactionPtr GetTx();
    TTransactionCache::TEntry::TPtr GetEntry();

    void CalculateRemoteMemoryUsage(double remoteMemoryFactor, const NYT::TRichYPath& remoteFilePath) override;

    void HandleQContextCapture(TUserFiles::TFileInfo& fileInfo, const TString& alias) override;
    void ProcessLocalFileForDump(const TString& alias, const TString& localPath) override;
    void ProcessRemoteFileForDump(const TString& alias, const NYT::TRichYPath& remoteFile) override;

private:
    TTransactionCache::TEntry::TPtr Entry_;
    std::shared_ptr<bool> HasFilesToDump_;

};

template<typename TExecContextPtr>
TNativeGatewayTransformer<TExecContextPtr> MakeNativeGatewayTransformer(
    TExecContextPtr execCtx,
    TTransactionCache::TEntry::TPtr entry,
    TProgramBuilder& pgmBuilder,
    TTempFiles::TPtr tmpFiles
) {
    NYT::IClientPtr client;
    if (entry) {
        client = entry->Client;
    } else {
        client = execCtx->CreateYtClient(execCtx->Options_.Config());
    }
    auto tableToFileDownloader = MakeYtNativeFileDownloader(execCtx->Gateway, execCtx->GetSessionId(), execCtx->Cluster_, execCtx->Options_.Config(), client, tmpFiles);
    return TNativeGatewayTransformer(execCtx, tableToFileDownloader, pgmBuilder, entry);
}

} // namespace NYql

#include "yql_yt_transform-inl.h"
