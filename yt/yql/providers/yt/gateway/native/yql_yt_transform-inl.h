#pragma once

#include "yql_yt_transform.h"

#include <util/system/env.h>

namespace NYql {

template<typename TExecContextPtr>
void TNativeGatewayTransformer<TExecContextPtr>::ApplyUserJobSpec(NYT::TUserJobSpec& spec, bool localRun) {
    if (!this->RemoteFiles_->empty()) {
        YQL_ENSURE(!localRun, "Unexpected remote files");
        for (auto& file: *this->RemoteFiles_) {
            spec.AddFile(file);
        }
    }
    bool fakeChecksum = (GetEnv("YQL_LOCAL") == "1");  // YQL-15353
    for (auto& file: *this->LocalFiles_) {
        NYT::TAddLocalFileOptions opts;
        if (!fakeChecksum && file.second.Hash) {
            opts.MD5CheckSum(file.second.Hash);
        }
        opts.BypassArtifactCache(file.second.BypassArtifactCache);
        spec.AddLocalFile(file.first, opts);
    }
    const TString binTmpFolder = this->Settings_->BinaryTmpFolder.Get(this->ExecCtx_->Cluster_).GetOrElse(TString());
    const TString binCacheFolder = this->Settings_->_BinaryCacheFolder.Get(this->ExecCtx_->Cluster_).GetOrElse(TString());
    if (!localRun && binCacheFolder) {
        auto udfFiles = std::move(*this->DeferredUdfFiles_);
        TTransactionCache::TEntry::TPtr entry = GetEntry();
        for (auto& file: udfFiles) {
            YQL_ENSURE(!file.second.Hash.empty());
            if (auto snapshot = entry->GetBinarySnapshotFromCache(binCacheFolder, file.second.Hash, file.first)) {
                spec.AddFile(NYT::TRichYPath(snapshot->first).TransactionId(snapshot->second)
                                                        .FileName(TFsPath(file.first)
                                                        .GetName())
                                                        .Executable(true)
                                                        .BypassArtifactCache(file.second.BypassArtifactCache));
            } else {
                this->DeferredUdfFiles_->push_back(file);
            }
        }
    }
    if (!this->DeferredUdfFiles_->empty()) {
        if (localRun || !binTmpFolder) {
            for (auto& file: *this->DeferredUdfFiles_) {
                NYT::TAddLocalFileOptions opts;
                if (!fakeChecksum && file.second.Hash) {
                    opts.MD5CheckSum(file.second.Hash);
                }
                YQL_ENSURE(TFileStat(file.first).Size != 0);
                opts.BypassArtifactCache(file.second.BypassArtifactCache);
                spec.AddLocalFile(file.first, opts);
            }
        } else {
            const TDuration binExpiration = this->Settings_->BinaryExpirationInterval.Get().GetOrElse(TDuration());
            auto entry = GetEntry();
            for (auto& file: *this->DeferredUdfFiles_) {
                YQL_ENSURE(TFileStat(file.first).Size != 0);
                YQL_ENSURE(!file.second.Hash.empty());
                auto snapshot = entry->GetBinarySnapshot(binTmpFolder, file.second.Hash, file.first, binExpiration);
                spec.AddFile(NYT::TRichYPath(snapshot.first).TransactionId(snapshot.second).FileName(TFsPath(file.first).GetName()).Executable(true).BypassArtifactCache(file.second.BypassArtifactCache));
            }
        }
    }
    this->RemoteFiles_->clear();
    this->LocalFiles_->clear();
    this->DeferredUdfFiles_->clear();
}

template<typename TExecContextPtr>
NYT::ITransactionPtr TNativeGatewayTransformer<TExecContextPtr>::GetTx() {
    return GetEntry()->Tx;
}

template<typename TExecContextPtr>
TTransactionCache::TEntry::TPtr TNativeGatewayTransformer<TExecContextPtr>::GetEntry() {
    if (!Entry_) {
        Entry_ = this->ExecCtx_->TExecContextBase::GetOrCreateEntry(this->Settings_);
    }
    return Entry_;
}

template<typename TExecContextPtr>
void TNativeGatewayTransformer<TExecContextPtr>::CalculateRemoteMemoryUsage(double remoteMemoryFactor, const NYT::TRichYPath& remoteFilePath) {
    if (remoteMemoryFactor > 0.) {
        *this->UsedMem_ += remoteMemoryFactor * GetUncompressedFileSize(GetTx(), remoteFilePath.Path_).GetOrElse(ui64(1) << 10);
    }
}

template<typename TExecContextPtr>
void TNativeGatewayTransformer<TExecContextPtr>::HandleQContextCapture(TUserFiles::TFileInfo& fileInfo, const TString& alias) {
    auto& qContext = this->ExecCtx_->Session_->QContext_;
    if (qContext.CanRead() && qContext.CaptureMode() == EQPlayerCaptureMode::Full) {
        auto dumpPath = GetDumpPath(alias, this->ExecCtx_->Cluster_, YtGateway_FileDumpPath, qContext);
        YQL_ENSURE(dumpPath.Defined(), "Missing replay data");

        YQL_CLOG(INFO, ProviderYt) << "Substituting file " << alias << " with dump " << *dumpPath << " on cluster " << this->ExecCtx_->Cluster_;
        fileInfo.RemotePath = *dumpPath;
        fileInfo.Path = nullptr;
    }
}

template<typename TExecContextPtr>
void TNativeGatewayTransformer<TExecContextPtr>::ProcessLocalFileForDump(const TString& alias, const TString& localPath) {
    auto& qContext = this->ExecCtx_->Session_->QContext_;
    auto& fullCapture = this->ExecCtx_->Session_->FullCapture_;
    if (!fullCapture) {
        return;
    }
    try {
        auto basename = TFsPath(localPath).GetName();
        auto dumpPath = MakeDumpPath(basename, this->ExecCtx_->Cluster_, this->ExecCtx_->Session_->OperationOptions_, this->Settings_);
        auto dumpKey = MakeDumpKey(alias, this->ExecCtx_->Cluster_);
        this->ExecCtx_->JobFilesDumpPaths.emplace(basename, dumpPath);
        qContext.GetWriter()->Put({ YtGateway_FileDumpPath, dumpKey }, dumpPath).GetValueSync();
        YQL_CLOG(INFO, ProviderYt) << "Local file " << alias << " (" << basename << ") dump is " << dumpPath << " on cluster " << this->ExecCtx_->Cluster_;
        *HasFilesToDump_ = true;
    } catch (const std::exception& e) {
        fullCapture->ReportError(e);
    }
}

template<typename TExecContextPtr>
void TNativeGatewayTransformer<TExecContextPtr>::ProcessRemoteFileForDump(const TString& alias, const NYT::TRichYPath& remoteFile) {
    auto& qContext = this->ExecCtx_->Session_->QContext_;
    auto& fullCapture = this->ExecCtx_->Session_->FullCapture_;
    if (fullCapture) {
        try {
            auto dumpPath = MakeDumpPath(alias, this->ExecCtx_->Cluster_, this->ExecCtx_->Session_->OperationOptions_, this->Settings_);
            auto dumpKey = MakeDumpKey(alias, this->ExecCtx_->Cluster_);
            this->ExecCtx_->JobFilesDumpPaths.emplace(alias, dumpPath);
            qContext.GetWriter()->Put({ YtGateway_FileDumpPath, dumpKey }, dumpPath).GetValueSync();
            YQL_CLOG(INFO, ProviderYt) << "Remote file " << alias << " (" << remoteFile.Path_ << ") dump is " << dumpPath << " on cluster " << this->ExecCtx_->Cluster_;
            *HasFilesToDump_ = true;
        } catch (const std::exception& e) {
            fullCapture->ReportError(e);
        }
    }
}

} // namespace NYql
