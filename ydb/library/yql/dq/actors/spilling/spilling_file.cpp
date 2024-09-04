#include "spilling.h"
#include "spilling_file.h"

#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/events.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/thread/pool.h>
#include <util/generic/guid.h>
#include <util/folder/iterator.h>
#include <util/generic/vector.h>
#include <util/folder/dirut.h>
#include <util/system/user.h>

namespace NYql::NDq {

using namespace NActors;

namespace {

// Read, write, and execute by owner only
constexpr int DIR_MODE = S_IRWXU;

#define LOG_D(s) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, s)
#define LOG_I(s) LOG_INFO_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, s)
#define LOG_E(s) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, s)
#define LOG_C(s) LOG_CRIT_S(*TlsActivationContext,  NKikimrServices::KQP_COMPUTE, s)

#define A_LOG_D(s) LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, s)
#define A_LOG_E(s) LOG_ERROR_S(*ActorSystem, NKikimrServices::KQP_COMPUTE, s)

#define REMOVE_FILES 1

// Local File Storage Events
struct TEvDqSpillingLocalFile {
    enum EEv {
        EvOpenFile = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvCloseFile,

        LastEvent = EvCloseFile
    };

    struct TEvOpenFile : public TEventLocal<TEvOpenFile, EvOpenFile> {
        TTxId TxId;
        TString Description; // for viewer & logs only
        bool RemoveBlobsAfterRead;

        TEvOpenFile(TTxId txId, const TString& description, bool removeBlobsAfterRead)
            : TxId(txId), Description(description), RemoveBlobsAfterRead(removeBlobsAfterRead) {}
    };

    struct TEvCloseFile : public TEventLocal<TEvCloseFile, EvCloseFile> {
        TMaybe<TString> Error;

        TEvCloseFile() = default;
        TEvCloseFile(const TString& error)
            : Error(error) {}
    };
};


// It is a simple proxy between client and spilling service with one feature --
// it provides human-readable description for logs and viewer.
class TDqLocalFileSpillingActor : public TActorBootstrapped<TDqLocalFileSpillingActor> {
public:
    TDqLocalFileSpillingActor(TTxId txId, const TString& details, const TActorId& client, bool removeBlobsAfterRead)
        : TxId_(txId)
        , Details_(details)
        , ClientActorId_(client)
        , RemoveBlobsAfterRead_(removeBlobsAfterRead)
    {}

    void Bootstrap() {
        ServiceActorId_ = MakeDqLocalFileSpillingServiceID(SelfId().NodeId());
        YQL_ENSURE(ServiceActorId_);

        LOG_D("Register LocalFileSpillingActor " << SelfId() << " at service " << ServiceActorId_);
        Send(ServiceActorId_, new TEvDqSpillingLocalFile::TEvOpenFile(TxId_, Details_, RemoveBlobsAfterRead_), NActors::IEventHandle::FlagTrackDelivery);

        Become(&TDqLocalFileSpillingActor::WorkState);
    }

    static constexpr char ActorName[] = "DQ_LOCAL_FILE_SPILLING";

private:
    STRICT_STFUNC(WorkState,
        hFunc(TEvDqSpilling::TEvWrite, HandleWork)
        hFunc(TEvDqSpilling::TEvWriteResult, HandleWork)
        hFunc(TEvDqSpilling::TEvRead, HandleWork)
        hFunc(TEvDqSpilling::TEvReadResult, HandleWork)
        hFunc(TEvDqSpilling::TEvError, HandleWork)
        sFunc(NActors::TEvents::TEvUndelivered, HandleUndelivered)
        hFunc(TEvents::TEvPoison, HandleWork)
    );

    void HandleWork(TEvDqSpilling::TEvWrite::TPtr& ev) {
        ValidateSender(ev->Sender);

        Send(ServiceActorId_, ev->Release().Release(), NActors::IEventHandle::FlagTrackDelivery);
    }

    void HandleWork(TEvDqSpilling::TEvWriteResult::TPtr& ev) {
        if (!Send(ClientActorId_, ev->Release().Release())) {
            ClientLost();
        }
    }

    void HandleWork(TEvDqSpilling::TEvRead::TPtr& ev) {
        ValidateSender(ev->Sender);

        Send(ServiceActorId_, ev->Release().Release(), NActors::IEventHandle::FlagTrackDelivery);
    }

    void HandleWork(TEvDqSpilling::TEvReadResult::TPtr& ev) {
        if (!Send(ClientActorId_, ev->Release().Release())) {
            ClientLost();
        }
    }

    void HandleWork(TEvDqSpilling::TEvError::TPtr& ev) {
        Send(ClientActorId_, ev->Release().Release());
    }

    void HandleWork(TEvents::TEvPoison::TPtr& ev) {
        ValidateSender(ev->Sender);

        Send(ServiceActorId_, new TEvDqSpillingLocalFile::TEvCloseFile, NActors::IEventHandle::FlagTrackDelivery);
        PassAway();
    }

    void HandleUndelivered() {
        Send(ClientActorId_, new TEvDqSpilling::TEvError("Spilling Service not started"));
    }

private:
    void ValidateSender(const TActorId& sender) {
        YQL_ENSURE(ClientActorId_ == sender, "" << ClientActorId_ << " != " << sender);
    }

    void ClientLost() {
        Send(ServiceActorId_, new TEvDqSpillingLocalFile::TEvCloseFile("Client lost"), NActors::IEventHandle::FlagTrackDelivery);
        PassAway();
    }

private:
    const TTxId TxId_;
    const TString Details_;
    const TActorId ClientActorId_;
    const bool RemoveBlobsAfterRead_;
    TActorId ServiceActorId_;
};

class TDqLocalFileSpillingService : public TActorBootstrapped<TDqLocalFileSpillingService> {
private:
    struct TEvPrivate {
        enum EEv {
            EvCloseFileResponse = TEvDqSpillingLocalFile::EEv::LastEvent + 1,
            EvWriteFileResponse,
            EvReadFileResponse,
            EvRemoveOldTmp,

            LastEvent
        };

        static_assert(EEv::LastEvent - EventSpaceBegin(TEvents::ES_PRIVATE) < 16);

        struct TEvCloseFileResponse : public TEventLocal<TEvCloseFileResponse, EvCloseFileResponse> {
            TActorId Client;
            TDuration WaitTime;
            TDuration WorkTime;
        };

        struct TEvWriteFileResponse : public TEventLocal<TEvWriteFileResponse, EvWriteFileResponse> {
            TActorId Client;
            TDuration WaitTime;
            TDuration WorkTime;
            ui64 BlobId = 0;
            THolder<TFileHandle> NewFileHandle;
            TMaybe<TString> Error;
        };

        struct TEvReadFileResponse : public TEventLocal<TEvReadFileResponse, EvReadFileResponse> {
            TActorId Client;
            TDuration WaitTime;
            TDuration WorkTime;
            ui64 BlobId = 0;
            TBuffer Blob;
            bool Removed = false;
            TMaybe<TString> Error;
        };

        struct TEvRemoveOldTmp : public TEventLocal<TEvRemoveOldTmp, EvRemoveOldTmp> {
            TFsPath TmpRoot;
            ui32 NodeId;
            TString SpillingSessionId;

            TEvRemoveOldTmp(TFsPath tmpRoot, ui32 nodeId, TString spillingSessionId) 
                : TmpRoot(std::move(tmpRoot)), NodeId(nodeId), SpillingSessionId(std::move(spillingSessionId)) {}
        };
    };

    struct TFileDesc;
    using TFilesIt = __yhashtable_iterator<std::pair<const TActorId, TFileDesc>>;

public:
    TDqLocalFileSpillingService(const TFileSpillingServiceConfig& config,
        TIntrusivePtr<TSpillingCounters> counters)
        : Config_(config)
        , Counters_(counters)
    {
        IoThreadPool_ = CreateThreadPool(Config_.IoThreadPoolWorkersCount,
            Config_.IoThreadPoolQueueSize, IThreadPool::TParams().SetThreadNamePrefix("DqSpilling"));
    }

    void Bootstrap() {
        Root_ = Config_.Root;
        const auto rootToRemoveOldTmp = Root_;
        const auto sessionId = Config_.SpillingSessionId;
        const auto nodeId = SelfId().NodeId();

        Root_ /= (TStringBuilder() << NodePrefix_ << "_" << nodeId << "_" << sessionId);
        LOG_I("Init DQ local file spilling service at " << Root_ << ", actor: " << SelfId());

        try {
            if (Root_.IsSymlink()) {
                throw TIoException() << Root_ << " is a symlink, can not start Spilling Service";
            }
            Root_.ForceDelete();
            Root_.MkDirs(DIR_MODE);
        } catch (...) {
            LOG_E(CurrentExceptionMessage());
            Become(&TDqLocalFileSpillingService::BrokenState);
            return;
        }
        
        Send(SelfId(), MakeHolder<TEvPrivate::TEvRemoveOldTmp>(rootToRemoveOldTmp, nodeId, sessionId));

        Become(&TDqLocalFileSpillingService::WorkState);
    }

    static constexpr char ActorName[] = "DQ_LOCAL_FILE_SPILLING_SERVICE";

protected:
    void PassAway() override {
        IoThreadPool_->Stop();
        IActor::PassAway();
        if (Config_.CleanupOnShutdown) {
            Root_.ForceDelete();
        }
    }

private:
    STATEFN(BrokenState) {
        switch (ev->GetTypeRewrite()) {
            case TEvDqSpillingLocalFile::TEvOpenFile::EventType:
            case TEvDqSpillingLocalFile::TEvCloseFile::EventType:
            case TEvDqSpilling::TEvWrite::EventType:
            case TEvDqSpilling::TEvRead::EventType: {
                HandleBroken(ev->Sender);
                break;
            }
            hFunc(NMon::TEvHttpInfo, HandleBroken);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                Y_DEBUG_ABORT_UNLESS(false, "%s: unexpected message type 0x%08" PRIx32, __func__, ev->GetTypeRewrite());
        }
    }

    void HandleBroken(const TActorId& from) {
        LOG_E("Service is broken, send error to client " << from);
        Send(from, new TEvDqSpilling::TEvError("Service not started"));
    }

    void HandleBroken(NMon::TEvHttpInfo::TPtr& ev) {
        Send(ev->Sender, new NMon::TEvHttpInfoRes("<html><h2>Service is not started due to IO error</h2></html>"));
    }

private:
    STRICT_STFUNC(WorkState,
        hFunc(TEvDqSpillingLocalFile::TEvOpenFile, HandleWork)
        hFunc(TEvDqSpillingLocalFile::TEvCloseFile, HandleWork)
        hFunc(TEvPrivate::TEvCloseFileResponse, HandleWork)
        hFunc(TEvDqSpilling::TEvWrite, HandleWork)
        hFunc(TEvPrivate::TEvWriteFileResponse, HandleWork)
        hFunc(TEvDqSpilling::TEvRead, HandleWork)
        hFunc(TEvPrivate::TEvReadFileResponse, HandleWork)
        hFunc(TEvPrivate::TEvRemoveOldTmp, HandleWork)
        hFunc(NMon::TEvHttpInfo, HandleWork)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    );

    void HandleWork(TEvDqSpillingLocalFile::TEvOpenFile::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[OpenFile] TxId: " << msg.TxId << ", desc: " << msg.Description << ", from: " << ev->Sender
            << ", removeBlobsAfterRead: " << msg.RemoveBlobsAfterRead);

        auto it = Files_.find(ev->Sender);
        if (it != Files_.end()) {
            LOG_E("[OpenFile] Can not open file: already exists. TxId: " << msg.TxId << ", desc: " << msg.Description);

            Send(ev->Sender, new TEvDqSpilling::TEvError("File already exists"));
            return;
        }

        auto& fd = Files_[ev->Sender];
        fd.TxId = msg.TxId;
        fd.Description = std::move(msg.Description);
        fd.RemoveBlobsAfterRead = msg.RemoveBlobsAfterRead;
        fd.OpenAt = TInstant::Now();
    }

    void HandleWork(TEvDqSpillingLocalFile::TEvCloseFile::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[CloseFile] from: " << ev->Sender << ", error: " << msg.Error);

        auto it = Files_.find(ev->Sender);
        if (it == Files_.end()) {
            LOG_E("[CloseFile] Can not close file: not found. From: " << ev->Sender << ", error: " << msg.Error);
            return;
        }

        if (it->second.CloseAt) {
            LOG_E("[CloseFile] Can not close file: closing. From: " << ev->Sender << ", error: " << msg.Error);
            return;
        }

        CloseFile(it, msg.Error);
    }

    void CloseFile(TFilesIt it, TMaybe<TString>& error) {
        auto& fd = it->second;

        fd.CloseAt = TInstant::Now();
        fd.Error.Swap(error);

        if (!fd.PartsList.empty()) {
            auto closeOp = MakeHolder<TCloseFileOp>();
            closeOp->Client = it->first;
            closeOp->Service = SelfId();
            closeOp->ActorSystem = TlsActivationContext->ActorSystem();
            closeOp->FileHandles.reserve(fd.PartsList.size());
            closeOp->FileNames.reserve(fd.PartsList.size());
            for (auto& fp : fd.PartsList) {
                closeOp->FileHandles.emplace_back(std::move(fp.FileHandle));
                closeOp->FileNames.emplace_back(std::move(fp.FileName));
            }

            RunOp("CloseFile", std::move(closeOp), fd);
        } else {
            MoveFileToClosed(it);
        }
    }

    void HandleWork(TEvPrivate::TEvCloseFileResponse::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[CloseFileResponse] from: " << msg.Client);

        auto it = Files_.find(msg.Client);
        if (it == Files_.end()) {
            LOG_E("[CloseFileResponse] Can not find file from: " << msg.Client);
            return;
        }

        ui64 blobs = 0;
        for (auto& fp : it->second.PartsList) {
            blobs += fp.Blobs.size();
        }

        Counters_->SpillingStoredBlobs->Sub(blobs);
        Counters_->SpillingTotalSpaceUsed->Sub(it->second.TotalSize);

        MoveFileToClosed(it);
    }

    void HandleWork(TEvDqSpilling::TEvWrite::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[Write] from: " << ev->Sender << ", blobId: " << msg.BlobId << ", bytes: " << msg.Blob.size());

        auto it = Files_.find(ev->Sender);
        if (it == Files_.end()) {
            LOG_E("[Write] File not found. "
                << "From: " << ev->Sender << ", blobId: " << msg.BlobId << ", bytes: " << msg.Blob.size());

            Send(ev->Sender, new TEvDqSpilling::TEvError("File not found"));
            return;
        }

        auto& fd = it->second;

        if (fd.CloseAt) {
            LOG_E("[Write] File already closed. "
                << "From: " << ev->Sender << ", blobId: " << msg.BlobId << ", bytes: " << msg.Blob.size());

            Send(ev->Sender, new TEvDqSpilling::TEvError("File already closed"));
            return;
        }

        if (Config_.MaxFileSize && fd.TotalSize + msg.Blob.size() > Config_.MaxFileSize) {
            LOG_E("[Write] File size limit exceeded. "
                << "From: " << ev->Sender << ", blobId: " << msg.BlobId << ", bytes: " << msg.Blob.size());

            Send(ev->Sender, new TEvDqSpilling::TEvError("File size limit exceeded"));

            Counters_->SpillingTooBigFileErrors->Inc();
            return;
        }

        if (Config_.MaxTotalSize && TotalSize_ + msg.Blob.size() > Config_.MaxTotalSize) {
            LOG_E("[Write] Total size limit exceeded. "
                << "From: " << ev->Sender << ", blobId: " << msg.BlobId << ", bytes: " << msg.Blob.size());

            Send(ev->Sender, new TEvDqSpilling::TEvError("Total size limit exceeded"));

            Counters_->SpillingNoSpaceErrors->Inc();
            return;
        }

        fd.TotalSize += msg.Blob.size();
        TotalSize_ += msg.Blob.size();

        TFileDesc::TFilePart* fp = fd.PartsList.empty() ? nullptr : &fd.PartsList.back();

        bool newFile = false;
        if (!fp || (fd.RemoveBlobsAfterRead && (Config_.MaxFilePartSize && fp->Size + msg.Blob.size() > Config_.MaxFilePartSize))) {
            if (!fd.PartsList.empty()) {
                fd.PartsList.back().Last = false;
            }

            fd.PartsList.push_back({});
            fp = &fd.PartsList.back();
            fd.Parts.emplace(msg.BlobId, fp);

            auto fname = TStringBuilder() << fd.TxId << "_" << fd.Description << "_" << fd.NextPartListIndex++;
            fp->FileName = (Root_ / fname).GetPath();

            LOG_D("[Write] create new FilePart " << fp->FileName);
            newFile = true;
        } else {
            fd.Parts.emplace(msg.BlobId, fp);
        }

        auto& blobDesc = fp->Blobs[msg.BlobId];
        blobDesc.Size = msg.Blob.size();
        blobDesc.Offset = fp->Size;

        fp->Size += blobDesc.Size;

        auto writeOp = MakeHolder<TWriteFileOp>();
        writeOp->Client = ev->Sender;
        writeOp->Service = SelfId();
        writeOp->ActorSystem = TlsActivationContext->ActorSystem();
        writeOp->FileName = fp->FileName;
        writeOp->CreateFile = newFile;
        writeOp->BlobId = msg.BlobId;
        writeOp->Blob = std::move(msg.Blob);

        RunOp("Write", std::move(writeOp), fd);
    }

    void HandleWork(TEvPrivate::TEvWriteFileResponse::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[WriteFileResponse] from: " << msg.Client << ", blobId: " << msg.BlobId << ", error: " << msg.Error);

        auto it = Files_.find(msg.Client);
        if (it == Files_.end()) {
            LOG_E("[WriteFileResponse] Can not write file: not found. "
                << "From: " << msg.Client << ", blobId: " << msg.BlobId << ", error: " << msg.Error);

            Send(ev->Sender, new TEvDqSpilling::TEvError("Internal error"));
            return;
        }

        auto& fd = it->second;

        fd.Error = std::move(msg.Error);
        fd.TotalWaitTime += msg.WaitTime;
        fd.TotalWorkTime += msg.WorkTime;

        if (auto* fp = fd.Parts[msg.BlobId]) {
            auto& blobDesc = fp->Blobs[msg.BlobId];

            fp->WaitTime += msg.WaitTime;
            fp->WorkTime += msg.WorkTime;
            fp->WriteBytes += blobDesc.Size;

            fd.TotalWriteBytes += blobDesc.Size;

            Counters_->SpillingStoredBlobs->Inc();
            Counters_->SpillingTotalSpaceUsed->Add(blobDesc.Size);

            if (msg.NewFileHandle) {
                fp->FileHandle.Swap(msg.NewFileHandle);
            }
        } else {
            LOG_E("[WriteFileResponse] File part not found. From: " << msg.Client << ", blobId: " << msg.BlobId);
            if (!fd.Error) {
                fd.Error = "File part not found";
            }

            Counters_->SpillingIoErrors->Inc();
        }

        if (fd.Error) {
            Send(msg.Client, new TEvDqSpilling::TEvError(*fd.Error));

            fd.Ops.clear();
            CloseFile(it, fd.Error);
            return;
        }

        Counters_->SpillingWriteBlobs->Inc();

        Send(msg.Client, new TEvDqSpilling::TEvWriteResult(msg.BlobId));
        RunNextOp(fd);
    }

    void HandleWork(TEvDqSpilling::TEvRead::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[Read] from: " << ev->Sender << ", blobId: " << msg.BlobId);

        auto it = Files_.find(ev->Sender);
        if (it == Files_.end()) {
            LOG_E("[Read] Can not read file: not found. From: " << ev->Sender << ", blobId: " << msg.BlobId);

            Send(ev->Sender, new TEvDqSpilling::TEvError("File not found"));
            return;
        }

        auto& fd = it->second;

        if (fd.CloseAt) {
            LOG_E("[Read] Can not read file: closed. From: " << ev->Sender << ", blobId: " << msg.BlobId);

            Send(ev->Sender, new TEvDqSpilling::TEvError("Closed"));
            return;
        }

        auto partIt = fd.Parts.find(msg.BlobId);
        if (partIt == fd.Parts.end()) {
            LOG_E("[Read] Can not read file: part not found. From: " << ev->Sender << ", blobId: " << msg.BlobId);

            Send(ev->Sender, new TEvDqSpilling::TEvError("File part not found"));

            fd.Ops.clear();
            TMaybe<TString> err = "Part not found";
            CloseFile(it, err);
            return;
        }

        auto& fp = partIt->second;

        auto blobIt = fp->Blobs.find(msg.BlobId);
        if (blobIt == fp->Blobs.end()) {
            LOG_E("[Read] Can not read file: blob not found in the part. From: " << ev->Sender << ", blobId: " << msg.BlobId);

            Send(ev->Sender, new TEvDqSpilling::TEvError("Blob not found in the file part"));

            fd.Ops.clear();
            TMaybe<TString> err = "Blob not found in the file part";
            CloseFile(it, err);
            return;
        }

        blobIt->second.Read = true;

        bool remove = false;
        if (fd.RemoveBlobsAfterRead && !fp->Last) {
            remove = true;
            for (auto& b: partIt->second->Blobs) {
                if (!b.second.Read) {
                    remove = false;
                    break;
                }
            }
        }

        auto readOp = MakeHolder<TReadFileOp>();
        readOp->Client = ev->Sender;
        readOp->Service = SelfId();
        readOp->ActorSystem = TlsActivationContext->ActorSystem();
        readOp->FileName = partIt->second->FileName;
        readOp->BlobId = msg.BlobId;
        readOp->Offset = blobIt->second.Offset;
        readOp->Size = blobIt->second.Size;
        if (remove) {
            readOp->RemoveFile = std::move(fp->FileHandle);
        }

        RunOp("Read", std::move(readOp), fd);
    }

    void HandleWork(TEvPrivate::TEvReadFileResponse::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[ReadFileResponse] from: " << msg.Client << ", blobId: " << msg.BlobId << ", removed: " << msg.Removed
            << ", error: " << msg.Error);

        auto it = Files_.find(msg.Client);
        if (it == Files_.end()) {
            LOG_E("[ReadFileResponse] Can not read file: not found. "
                << "From: " << msg.Client << ", blobId: " << msg.BlobId << ", error: " << msg.Error);

            Send(ev->Sender, new TEvDqSpilling::TEvError("Internal error"));
            return;
        }

        auto& fd = it->second;

        fd.Error = std::move(msg.Error);
        fd.TotalWaitTime += msg.WaitTime;
        fd.TotalWorkTime += msg.WorkTime;
        fd.TotalReadBytes += msg.Blob.size();

        auto* fp = fd.Parts[msg.BlobId];
        if (fp) {
            fp->Blobs[msg.BlobId].Read = true;
            fp->WaitTime += msg.WaitTime;
            fp->WorkTime += msg.WorkTime;
            fp->ReadBytes += msg.Blob.size();

            if (msg.Removed) {
                fd.TotalSize -= fp->Size;
                TotalSize_ -= fp->Size;

                Counters_->SpillingTotalSpaceUsed->Sub(fp->Size);
                Counters_->SpillingStoredBlobs->Sub(fp->Blobs.size());

                fd.Parts.erase(msg.BlobId);
                fd.PartsList.remove_if([fp](const auto& x) { return &x == fp; });
            }
        } else {
            if (!fd.Error) {
                fd.Error = "Internal error";
            }
        }

        if (fd.Error) {
            Send(msg.Client, new TEvDqSpilling::TEvError(*fd.Error));

            fd.Ops.clear();
            CloseFile(it, fd.Error);
            return;
        }

        Counters_->SpillingReadBlobs->Inc();

        Send(msg.Client, new TEvDqSpilling::TEvReadResult(msg.BlobId, std::move(msg.Blob)));
        RunNextOp(fd);
    }

    void HandleWork(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream s;

        TMap<TTxId, TVector<const TFileDesc*>> byTx;
        for (const auto& fd : Files_) {
            byTx[fd.second.TxId].push_back(&fd.second);
        }

        HTML(s) {
            TAG(TH2) { s << "Configuration"; }
            PRE() {
                s << "  - Root: " << Config_.Root << Endl;
                s << "  - MaxTotalSize: " << Config_.MaxTotalSize << Endl;
                s << "  - MaxFileSize: " << Config_.MaxFileSize << Endl;
                s << "  - MaxFilePartSize: " << Config_.MaxFilePartSize << Endl;
                s << "  - IO thread pool, workers: " << Config_.IoThreadPoolWorkersCount
                    << ", queue: " << Config_.IoThreadPoolQueueSize << Endl;
            }

            TAG(TH2) { s << "Active files"; }
            PRE() { s << "Used space: " << TotalSize_ << Endl; }

            for (const auto& tx : byTx) {
                TAG(TH2) { s << "Transaction " << tx.first; }
                s << "Open files:" << Endl;
                UL() {
                    for (const auto* fd : tx.second) {
                        LI() { s << fd->Description; }
                        PRE() {
                            s << "  Remove blobs after read: " << fd->RemoveBlobsAfterRead << Endl;
                            s << "  Open at: " << fd->OpenAt << Endl;
                            s << "  Error: " << fd->Error << Endl;
                            s << "  Total size: " << fd->TotalSize << Endl;
                            s << "  Total work time: " << fd->TotalWorkTime << Endl;
                            s << "  Total wait time: " << fd->TotalWaitTime << Endl;
                            s << "  Total write bytes: " << fd->TotalWriteBytes << Endl;
                            s << "  Total read bytes: " << fd->TotalReadBytes << Endl;
                            ui64 blobs = 0;
                            for (const auto& fp : fd->PartsList) {
                                blobs += fp.Blobs.size();
                            }
                            s << "  Total blobs: " << blobs << Endl;
                            s << "  Total files: " << fd->PartsList.size() << Endl;
                            s << "  Files:" << Endl;
                            for (const auto& fp : fd->PartsList) {
                                s << "    - " << fp.FileName << Endl;
                                s << "      Size: " << fp.Size << Endl;
                                s << "      Blobs: " << fp.Blobs.size() << Endl;
                                s << "      Write bytes: " << fp.WriteBytes << Endl;
                                s << "      Read bytes: " << fp.WriteBytes << Endl;
                            }
                        }
                    }
                }
            }

            TAG(TH2) { s << "Last closed files"; }
            UL() {
                for (auto it = ClosedFiles_.rbegin(); it != ClosedFiles_.rend(); ++it) {
                    auto& fd = *it;
                    LI() { s << "Transaction: " << fd.TxId << ", " << fd.Description; }
                    PRE() {
                        s << "  Remove blobs after read: " << fd.RemoveBlobsAfterRead << Endl;
                        s << "  Open at: " << fd.OpenAt << Endl;
                        s << "  Close at: " << fd.CloseAt << Endl;
                        s << "  Error: " << fd.Error << Endl;
                        s << "  Total work time: " << fd.WorkTime << Endl;
                        s << "  Total wait time: " << fd.WaitTime << Endl;
                        s << "  Total write bytes: " << fd.WriteBytes << Endl;
                        s << "  Total read bytes: " << fd.ReadBytes << Endl;
                    }
                }
            }

        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(s.Str()));
    }

    void HandleWork(TEvPrivate::TEvRemoveOldTmp::TPtr& ev) {
        const auto& msg = *ev->Get();
        const auto& root = msg.TmpRoot;
        const auto nodeIdString = ToString(msg.NodeId);
        const auto& sessionId = msg.SpillingSessionId;
        const auto& nodePrefix = this->NodePrefix_;

        LOG_I("[RemoveOldTmp] removing at root: " << root);

        const auto isDirOldTmp = [&nodePrefix, &nodeIdString, &sessionId](const TString& dirName) -> bool {            
            // dirName: node_<nodeId>_<sessionId>
            TVector<TString> parts;
            StringSplitter(dirName).Split('_').Limit(3).Collect(&parts);

            if (parts.size() < 3) {
                return false;
            }
            return parts[0] == nodePrefix && parts[1] == nodeIdString && parts[2] != sessionId;
        };

        try {
            TDirIterator iter(root, TDirIterator::TOptions().SetMaxLevel(1));
            
            TVector<TString> oldTmps;
            for (const auto& dirEntry : iter) {
                if (dirEntry.fts_info == FTS_DP) {
                    continue;
                }
                
                const auto dirName = dirEntry.fts_name;
                if (isDirOldTmp(dirName)) {
                    LOG_D("[RemoveOldTmp] found old temporary at " << (root / dirName));
                    oldTmps.emplace_back(std::move(dirName));
                }
            }

            for (const auto& dirName : oldTmps) {
                (root / dirName).ForceDelete();
            }
        } catch (const yexception& e) {
            LOG_E("[RemoveOldTmp] removing failed due to: " << e.what());
        }
    }

private:
    void RunOp(TStringBuf opName, THolder<IObjectInQueue> op, TFileDesc& fd) {
        if (fd.HasActiveOp) {
            fd.Ops.emplace_back(opName, std::move(op));
        } else {
            fd.HasActiveOp = true;
            // TODO: retry if fails
            IoThreadPool_->SafeAddAndOwn(std::move(op));
        }
    }

    void RunNextOp(TFileDesc& fd) {
        fd.HasActiveOp = false;
        if (!fd.Ops.empty()) {
            auto op = std::move(fd.Ops.front().second);
            auto opName = fd.Ops.front().first;
            fd.Ops.pop_front();

            RunOp(opName, std::move(op), fd);
        }
    }

    void MoveFileToClosed(TFilesIt it) {
        TotalSize_ -= it->second.TotalSize;
        ClosedFiles_.emplace_back(TClosedFileDesc(std::move(it->second)));
        while (ClosedFiles_.size() > 100) {
            ClosedFiles_.pop_front();
        }
        Files_.erase(it);
    }

private:
    struct TCloseFileOp : public IObjectInQueue {
        TActorId Client;
        TActorId Service;
        TActorSystem* ActorSystem;
        TVector<THolder<TFileHandle>> FileHandles;
        TVector<TString> FileNames;
        TInstant Ts = TInstant::Now();

        void Process(void*) override {
            auto now = TInstant::Now();

            auto resp = MakeHolder<TEvPrivate::TEvCloseFileResponse>();
            resp->Client = Client;
            resp->WaitTime = now - Ts;

            A_LOG_D("[CloseFile async] from: " << Client << ", waitTime: " << resp->WaitTime);

            for (ui64 i = 0; i < FileHandles.size(); ++i) {
                try {
                    FileHandles[i].Reset();
#ifdef REMOVE_FILES
                    ::unlink(FileNames[i].c_str());
#endif
                } catch (const yexception& e) {
                    A_LOG_E("[CloseFile async] error while closing file " << FileNames[i] << ": " << e.what());
                }
            }
            resp->WorkTime = TInstant::Now() - now;

            ActorSystem->Send(Service, resp.Release());
        }
    };

    struct TWriteFileOp : public IObjectInQueue {
        TActorId Client;
        TActorId Service;
        TActorSystem* ActorSystem;
        TString FileName;
        bool CreateFile = false;
        ui64 BlobId = 0;
        TRope Blob;
        TInstant Ts = TInstant::Now();

        void Process(void*) override {
            auto now = TInstant::Now();
            A_LOG_D("[Write async] file: " << FileName << ", blobId: " << BlobId << ", bytes: " << Blob.size()
                << ", offset: " << (CreateFile ? 0 : GetFileLength(FileName)));

            auto resp = MakeHolder<TEvPrivate::TEvWriteFileResponse>();
            resp->Client = Client;
            resp->WaitTime = now - Ts;
            resp->BlobId = BlobId;

            try {
                TFile file;
                if (CreateFile) {
                    file = TFile(FileName, CreateAlways | WrOnly);
                    resp->NewFileHandle = MakeHolder<TFileHandle>(FileName, OpenExisting | RdWr);
                } else {
                    file = TFile::ForAppend(FileName);
                }
                for (auto it = Blob.Begin(); it.Valid(); ++it) {
                    file.Write(it.ContiguousData(), it.ContiguousSize());
                }
            } catch (const yexception& e) {
                A_LOG_E("[Write async] file: " << FileName << ", io error: " << e.what());
                resp->Error = e.what();
            }
            resp->WorkTime = TInstant::Now() - now;

            ActorSystem->Send(Service, resp.Release());
        }
    };

    struct TReadFileOp : public IObjectInQueue {
        TActorId Client;
        TActorId Service;
        TActorSystem* ActorSystem;
        TString FileName;
        ui64 BlobId;
        ui64 Offset;
        ui64 Size;
        THolder<TFileHandle> RemoveFile;
        TInstant Ts = TInstant::Now();

        void Process(void*) override {
            auto now = TInstant::Now();
            A_LOG_D("[Read async] file: " << FileName << ", blobId: " << BlobId << ", offset: " << Offset
                << ", size: " << Size << ", remove: " << (bool) RemoveFile);

            auto resp = MakeHolder<TEvPrivate::TEvReadFileResponse>();
            resp->Client = Client;
            resp->WaitTime = TInstant::Now() - now;
            resp->BlobId = BlobId;

            try {
                resp->Blob.Resize(Size);

                TFile f(FileName, OpenExisting | RdOnly);
                f.Seek(Offset, SeekDir::sSet);

                auto read = f.Read(resp->Blob.Data(), Size);
                YQL_ENSURE(read == Size, "" << read << " != " << Size);
                YQL_ENSURE(resp->Blob.size() == Size);

                if (RemoveFile) {
                    f.Close();
                    RemoveFile.Reset();
#ifdef REMOVE_FILES
                    ::unlink(FileName.c_str());
#endif
                    resp->Removed = true;
                }
            } catch (const yexception& e) {
                A_LOG_E("[Read async] file: " << FileName << ", blobId: " << BlobId << ", offset: " << Offset
                    << ", error: " << e.what());
                resp->Error = e.what();
            }
            resp->WorkTime = TInstant::Now() - now;

            ActorSystem->Send(Service, resp.Release());
        }
    };

private:
    struct TFileDesc {
        TTxId TxId;
        TString Description;
        bool RemoveBlobsAfterRead = false;
        TInstant OpenAt;

        TInstant CloseAt;
        TMaybe<TString> Error;

        struct TBlobDesc {
            ui64 Offset = 0;
            ui64 Size = 0;
            bool Read = false;
        };

        struct TFilePart {
            TString FileName;
            THolder<TFileHandle> FileHandle;

            THashMap<ui64, TBlobDesc> Blobs;

            ui64 WriteBytes = 0;
            ui64 ReadBytes = 0;
            ui64 Size = 0; // also it is an offset for the next blob
            TDuration WorkTime;
            TDuration WaitTime;
            bool Last = true;
        };

        ui64 TotalSize = 0;

        TDuration TotalWorkTime;
        TDuration TotalWaitTime;
        ui64 TotalWriteBytes = 0;
        ui64 TotalReadBytes = 0;

        THashMap<ui64, TFilePart*> Parts;
        TList<TFilePart> PartsList;
        ui32 NextPartListIndex = 0;

        TList<std::pair<TString, THolder<IObjectInQueue>>> Ops;
        bool HasActiveOp = false;
    };

    struct TClosedFileDesc {
        TTxId TxId;
        TString Description;
        bool RemoveBlobsAfterRead;
        TInstant OpenAt;
        TInstant CloseAt;
        TDuration WorkTime;
        TDuration WaitTime;
        ui64 WriteBytes;
        ui64 ReadBytes;
        TMaybe<TString> Error;

        TClosedFileDesc(TFileDesc&& fd)
            : TxId(fd.TxId)
            , Description(std::move(fd.Description))
            , RemoveBlobsAfterRead(fd.RemoveBlobsAfterRead)
            , OpenAt(fd.OpenAt)
            , CloseAt(fd.CloseAt)
            , WorkTime(fd.TotalWorkTime)
            , WaitTime(fd.TotalWaitTime)
            , WriteBytes(fd.TotalWriteBytes)
            , ReadBytes(fd.TotalReadBytes)
            , Error(std::move(fd.Error))
        {}
    };

private:
    const TFileSpillingServiceConfig Config_;
    const TString NodePrefix_ = "node";
    TFsPath Root_;
    TIntrusivePtr<TSpillingCounters> Counters_;

    THolder<IThreadPool> IoThreadPool_;
    THashMap<TActorId, TFileDesc> Files_;
    TList<const TClosedFileDesc> ClosedFiles_;
    ui64 TotalSize_ = 0;
};

} // anonymous namespace

TFsPath GetTmpSpillingRootForCurrentUser() {
    auto root = TFsPath{GetSystemTempDir()};
    root /= "spilling-tmp-" + GetUsername();
    return root;
}

IActor* CreateDqLocalFileSpillingActor(TTxId txId, const TString& details, const TActorId& client,
    bool removeBlobsAfterRead)
{
    return new TDqLocalFileSpillingActor(txId, details, client, removeBlobsAfterRead);
}

IActor* CreateDqLocalFileSpillingService(const TFileSpillingServiceConfig& config, TIntrusivePtr<TSpillingCounters> counters)
{
    return new TDqLocalFileSpillingService(config, counters);
}

} // namespace NYql::NDq
