#include "kv_body_store.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/library/actors/core/log.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/system/fs.h>

namespace NKikimr::NUdfStore {

//
// TKvBodyReadActor
//

void TKvBodyReadActor::Bootstrap() {
    if (ExpectedSize == 0) {
        ReplyError(TStringBuilder() << "UDF '" << Md5Key << "' has zero size in metadata");
        return;
    }
    Become(&TKvBodyReadActor::StateResolve);
    SendNavigateRequest();
}

void TKvBodyReadActor::SendNavigateRequest() {
    auto req = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    auto& entry = req->ResultSet.emplace_back();
    entry.Path = SplitPath(VolumePath);
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
    entry.ShowPrivatePath = true;
    entry.SyncVersion = false;
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(req.Release()));
}

void TKvBodyReadActor::HandleNavigateResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    NSchemeCache::TSchemeCacheNavigate* request = ev->Get()->Request.Get();

    if (request->ResultSet.size() != 1) {
        ReplyError("SchemeCache returned unexpected result set size");
        return;
    }

    auto& entry = request->ResultSet[0];
    if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        ReplyError(TStringBuilder() << "SchemeCache resolve failed for path '" << VolumePath
            << "', status: " << static_cast<int>(entry.Status));
        return;
    }

    if (!entry.SolomonVolumeInfo) {
        ReplyError(TStringBuilder() << "Path '" << VolumePath << "' is not a KeyValue volume");
        return;
    }

    const auto& desc = entry.SolomonVolumeInfo->Description;
    if (desc.PartitionsSize() == 0) {
        ReplyError("KeyValue volume has no partitions");
        return;
    }

    // Use partition 0
    KVTabletId = desc.GetPartitions(0).GetTabletId();
    if (!KVTabletId) {
        ReplyError("Failed to get tablet ID for partition 0");
        return;
    }

    Become(&TKvBodyReadActor::StateRead);
    CreatePipeAndSendRead();
}

void TKvBodyReadActor::CreatePipeAndSendRead() {
    // Prepare the temp file for incremental writing.
    if (!NFs::Exists(OutputDir)) {
        NFs::MakeDirectoryRecursive(OutputDir);
    }

    TFsPath finalPath = TFsPath(OutputDir) / Md5Key;
    TmpFilePath = finalPath.GetPath() + ".tmp";

    try {
        TmpFile = MakeHolder<TFile>(TmpFilePath, CreateAlways | WrOnly | Seq);
    } catch (const yexception& e) {
        ReplyError(TStringBuilder() << "Failed to open tmp file '" << TmpFilePath << "': " << e.what());
        return;
    }

    CurrentOffset = 0;
    Md5Ctx = MD5();

    NTabletPipe::TClientConfig cfg;
    cfg.RetryPolicy = {
        .RetryLimitCount = 3u
    };
    PipeClient = Register(NTabletPipe::CreateClient(SelfId(), KVTabletId, cfg));

    SendNextChunkRead();
}

void TKvBodyReadActor::SendNextChunkRead() {
    auto req = std::make_unique<TEvKeyValue::TEvRead>();
    req->Record.set_tablet_id(KVTabletId);
    req->Record.set_key(Md5Key);
    req->Record.set_offset(CurrentOffset);
    req->Record.set_size(ReadChunkSize);
    NTabletPipe::SendData(SelfId(), PipeClient, req.release());
    ALS_DEBUG(NKikimrServices::METADATA_PROVIDER)
        << "TKvBodyReadActor: sending read for key='" << Md5Key
        << "' offset=" << CurrentOffset << " size=" << ReadChunkSize;
}

void TKvBodyReadActor::HandleReadResponse(TEvKeyValue::TEvReadResponse::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    const auto status = record.status();

    // RSTATUS_NOT_FOUND at a non-zero offset means we've read past the end of
    // a value whose size is an exact multiple of ReadChunkSize.  Treat it as EOF.
    if (status == NKikimrKeyValue::Statuses::RSTATUS_NOT_FOUND && CurrentOffset > 0) {
        FinalizeAndSave();
        return;
    }

    if (status != NKikimrKeyValue::Statuses::RSTATUS_OK) {
        TString msg = TStringBuilder() << "KV read failed with status: "
            << static_cast<int>(status);
        if (!record.msg().empty()) {
            msg += " - " + record.msg();
        }
        CleanupTmpFile();
        ReplyError(msg);
        return;
    }

    // Extract the chunk data.
    TString chunk;
    if (ev->Get()->IsPayload()) {
        TRope rope = ev->Get()->GetBuffer();
        const TContiguousSpan span = rope.GetContiguousSpan();
        chunk = TString(span.data(), span.size());
    } else {
        chunk = record.value();
    }

    if (!chunk.empty()) {
        try {
            TmpFile->Write(chunk.data(), chunk.size());
        } catch (const yexception& e) {
            CleanupTmpFile();
            ReplyError(TStringBuilder() << "Failed to write to tmp file: " << e.what());
            return;
        }
        Md5Ctx.Update(chunk.data(), chunk.size());
        CurrentOffset += chunk.size();
    }

    // If we got fewer bytes than requested (or zero), this is the last chunk.
    if (chunk.size() < ReadChunkSize) {
        FinalizeAndSave();
    } else {
        SendNextChunkRead();
    }
}

void TKvBodyReadActor::FinalizeAndSave() {
    // Close the tmp file before renaming it to the final path.
    try {
        TmpFile->Close();
        TmpFile.Reset();
    } catch (const yexception& e) {
        CleanupTmpFile();
        ReplyError(TStringBuilder() << "Failed to close tmp file: " << e.what());
        return;
    }

    if (CurrentOffset != ExpectedSize) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TKvBodyReadActor: size mismatch for UDF '" << Md5Key
            << "': expected=" << ExpectedSize << ", actual=" << CurrentOffset;
        NFs::Remove(TmpFilePath);
        Send(ReplyTo, new TEvKvBodyStore::TEvReadBodyResponse(false, Md5Key,
            TStringBuilder() << "Size mismatch: expected=" << ExpectedSize
            << ", actual=" << CurrentOffset));
        PassAway();
        return;
    }

    // Verify MD5.
    char md5Buf[33];
    TString computedMd5 = Md5Ctx.End(md5Buf);

    if (!Md5Key.empty() && computedMd5 != Md5Key) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TKvBodyReadActor: MD5 mismatch for UDF '" << Md5Key
            << "': stored=" << Md5Key << ", computed=" << computedMd5;
        NFs::Remove(TmpFilePath);
        Send(ReplyTo, new TEvKvBodyStore::TEvReadBodyResponse(false, Md5Key,
            TStringBuilder() << "MD5 mismatch: stored=" << Md5Key << ", computed=" << computedMd5));
        PassAway();
        return;
    }

    TString finalPath = TFsPath(OutputDir) / Md5Key;
    if (!NFs::Rename(TmpFilePath, finalPath)) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TKvBodyReadActor: failed to rename tmp file for UDF '" << Md5Key << "'";
        NFs::Remove(TmpFilePath);
        Send(ReplyTo, new TEvKvBodyStore::TEvReadBodyResponse(false, Md5Key,
            TStringBuilder() << "Failed to rename tmp file to '" << finalPath << "'"));
        PassAway();
        return;
    }

    if (!LoadUdfIntoRegistry(finalPath)) {
        NFs::Remove(finalPath);
        Send(ReplyTo, new TEvKvBodyStore::TEvReadBodyResponse(false, Md5Key,
            TStringBuilder() << "Failed to load UDF '" << Md5Key << "' into function registry"));
        PassAway();
        return;
    }

    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TKvBodyReadActor: saved UDF '" << Md5Key
        << "' (" << CurrentOffset << " bytes) to " << finalPath;

    Send(ReplyTo, new TEvKvBodyStore::TEvReadBodyResponse(true, Md5Key));
    PassAway();
}

bool TKvBodyReadActor::LoadUdfIntoRegistry(const TString& finalPath) const {
    if (!FunctionRegistry) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TKvBodyReadActor: function registry is not available for UDF '" << Md5Key << "'";
        return false;
    }

    NMiniKQL::TUdfModuleRemappings remappings;
    THashSet<TString> modules;
    try {
        FunctionRegistry->LoadUdfs(finalPath, remappings, 0, {}, &modules);
    } catch (const std::exception& e) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TKvBodyReadActor: failed to load UDF '" << Md5Key
            << "' into function registry: " << e.what();
        return false;
    }

    if (modules.empty()) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TKvBodyReadActor: no UDF modules were registered from '" << finalPath << "'";
        return false;
    }

    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TKvBodyReadActor: loaded UDF '" << Md5Key
        << "' into function registry from " << finalPath;
    return true;
}

void TKvBodyReadActor::CleanupTmpFile() {
    if (TmpFile) {
        try { TmpFile->Close(); } catch (...) {}
        TmpFile.Reset();
    }
    if (!TmpFilePath.empty()) {
        NFs::Remove(TmpFilePath);
    }
}

void TKvBodyReadActor::HandlePipeConnected(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
    if (ev->Get()->Status != NKikimrProto::OK) {
        CleanupTmpFile();
        ReplyError(TStringBuilder() << "Failed to connect to KV tablet " << KVTabletId);
    }
}

void TKvBodyReadActor::HandlePipeDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& /*ev*/) {
    CleanupTmpFile();
    ReplyError(TStringBuilder() << "Connection to KV tablet " << KVTabletId << " was lost");
}

void TKvBodyReadActor::ReplyError(const TString& message) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER) << "TKvBodyReadActor: " << message;
    Send(ReplyTo, new TEvKvBodyStore::TEvReadBodyResponse(false, Md5Key, message));
    PassAway();
}

void TKvBodyReadActor::PassAway() {
    if (PipeClient) {
        NTabletPipe::CloseClient(SelfId(), PipeClient);
        PipeClient = {};
    }
    NActors::TActorBootstrapped<TKvBodyReadActor>::PassAway();
}

} // namespace NKikimr::NUdfStore
