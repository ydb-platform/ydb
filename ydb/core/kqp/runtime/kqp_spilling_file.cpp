#include "kqp_spilling.h"
#include "kqp_spilling_file.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/mon/mon.h>

#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/thread/pool.h>

namespace NKikimr::NKqp {

using namespace NActors;

namespace {

// Read, write, and execute by owner only
constexpr int DIR_MODE = S_IRWXU;

#define LOG_D(s) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_BLOBS_STORAGE, s)
#define LOG_I(s) LOG_INFO_S(*TlsActivationContext,  NKikimrServices::KQP_BLOBS_STORAGE, s)
#define LOG_E(s) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_BLOBS_STORAGE, s)
#define LOG_C(s) LOG_CRIT_S(*TlsActivationContext,  NKikimrServices::KQP_BLOBS_STORAGE, s)

#define A_LOG_D(s) LOG_DEBUG_S(*ActorSystem, NKikimrServices::KQP_BLOBS_STORAGE, s)
#define A_LOG_E(s) LOG_ERROR_S(*ActorSystem, NKikimrServices::KQP_BLOBS_STORAGE, s)

#define REMOVE_FILES 1

// Local File Storage Events
struct TEvKqpSpillingLocalFile {
    enum EEv {
        EvOpenFile = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        EvCloseFile,

        LastEvent = EvCloseFile
    };

    struct TEvOpenFile : public TEventLocal<TEvOpenFile, EvOpenFile> {
        ui64 TxId;
        TString Description; // for viewer & logs only
        bool RemoveBlobsAfterRead;

        TEvOpenFile(ui64 txId, const TString& description, bool removeBlobsAfterRead)
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
class TKqpLocalFileSpillingActor : public TActorBootstrapped<TKqpLocalFileSpillingActor> {
public:
    TKqpLocalFileSpillingActor(ui64 txId, const TString& details, const TActorId& client, bool removeBlobsAfterRead)
        : TxId(txId)
        , Details(details)
        , ClientActorId(client)
        , RemoveBlobsAfterRead(removeBlobsAfterRead) {}

    void Bootstrap() {
        ServiceActorId = MakeKqpLocalFileSpillingServiceID(SelfId().NodeId());
        YQL_ENSURE(ServiceActorId);

        LOG_D("Register LocalFileSpillingActor " << SelfId() << " at service " << ServiceActorId);
        Send(ServiceActorId, new TEvKqpSpillingLocalFile::TEvOpenFile(TxId, Details, RemoveBlobsAfterRead));

        Become(&TKqpLocalFileSpillingActor::WorkState);
    }

    static constexpr char ActorName[] = "KQP_LOCAL_FILE_SPILLING";

private:
    STRICT_STFUNC(WorkState,
        hFunc(TEvKqpSpilling::TEvWrite, HandleWork)
        hFunc(TEvKqpSpilling::TEvWriteResult, HandleWork)
        hFunc(TEvKqpSpilling::TEvRead, HandleWork)
        hFunc(TEvKqpSpilling::TEvReadResult, HandleWork)
        hFunc(TEvKqpSpilling::TEvError, HandleWork)
        hFunc(TEvents::TEvPoison, HandleWork)
    );

    void HandleWork(TEvKqpSpilling::TEvWrite::TPtr& ev) {
        ValidateSender(ev->Sender);

        Send(ServiceActorId, ev->Release().Release());
    }

    void HandleWork(TEvKqpSpilling::TEvWriteResult::TPtr& ev) {
        if (!Send(ClientActorId, ev->Release().Release())) {
            ClientLost();
        }
    }

    void HandleWork(TEvKqpSpilling::TEvRead::TPtr& ev) {
        ValidateSender(ev->Sender);

        Send(ServiceActorId, ev->Release().Release());
    }

    void HandleWork(TEvKqpSpilling::TEvReadResult::TPtr& ev) {
        if (!Send(ClientActorId, ev->Release().Release())) {
            ClientLost();
        }
    }

    void HandleWork(TEvKqpSpilling::TEvError::TPtr& ev) {
        Send(ClientActorId, ev->Release().Release());
    }

    void HandleWork(TEvents::TEvPoison::TPtr& ev) {
        ValidateSender(ev->Sender);

        Send(ServiceActorId, new TEvKqpSpillingLocalFile::TEvCloseFile);
        PassAway();
    }

private:
    void ValidateSender(const TActorId& sender) {
        YQL_ENSURE(ClientActorId == sender, "" << ClientActorId << " != " << sender);
    }

    void ClientLost() {
        Send(ServiceActorId, new TEvKqpSpillingLocalFile::TEvCloseFile("Client lost"));
        PassAway();
    }

private:
    const ui64 TxId;
    const TString Details;
    const TActorId ClientActorId;
    const bool RemoveBlobsAfterRead;
    TActorId ServiceActorId;
};

class TKqpLocalFileSpillingService : public TActorBootstrapped<TKqpLocalFileSpillingService> {
private:
    struct TEvPrivate {
        enum EEv {
            EvCloseFileResponse = TEvKqpSpillingLocalFile::EEv::LastEvent + 1,
            EvWriteFileResponse,
            EvReadFileResponse,

            LastEvent
        };

        static_assert(EEv::LastEvent - EventSpaceBegin(TKikimrEvents::ES_PRIVATE) < 16);

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
    };

    struct TFileDesc;
    using TFilesIt = __yhashtable_iterator<std::pair<const TActorId, TFileDesc>>;

public:
    TKqpLocalFileSpillingService(const NKikimrConfig::TTableServiceConfig::TSpillingServiceConfig::TLocalFileConfig& config,
        TIntrusivePtr<TKqpCounters> counters)
        : Config(config)
        , Counters(counters)
    {
        IoThreadPool = CreateThreadPool(Config.GetIoThreadPool().GetWorkersCount(),
            Config.GetIoThreadPool().GetQueueSize(), IThreadPool::TParams().SetThreadNamePrefix("KqpSpilling"));
    }

    void Bootstrap() {
        Root = Config.GetRoot();
        Root /= (TStringBuilder() << "node_" << SelfId().NodeId());

        LOG_I("Init KQP local file spilling service at " << Root << ", actor: " << SelfId());

        try {
            if (Root.IsSymlink()) {
                throw TIoException() << Root << " is a symlink, can not start Spilling Service";
            }
            Root.ForceDelete();
            Root.MkDirs(DIR_MODE);
        } catch (...) {
            LOG_E(CurrentExceptionMessage());
            Become(&TKqpLocalFileSpillingService::BrokenState);
            return;
        }

        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "kqp_spilling_file", "KQP Local File Spilling Service", false,
                TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
        }

        Become(&TKqpLocalFileSpillingService::WorkState);
    }

    static constexpr char ActorName[] = "KQP_LOCAL_FILE_SPILLING_SERVICE";

protected:
    void PassAway() override {
        IoThreadPool->Stop();
        IActor::PassAway();
    }

private:
    STATEFN(BrokenState) {
        switch (ev->GetTypeRewrite()) {
            case TEvKqpSpillingLocalFile::TEvOpenFile::EventType:
            case TEvKqpSpillingLocalFile::TEvCloseFile::EventType:
            case TEvKqpSpilling::TEvWrite::EventType:
            case TEvKqpSpilling::TEvRead::EventType: {
                HandleBroken(ev->Sender);
                break;
            }
            hFunc(NMon::TEvHttpInfo, HandleBroken);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                Y_VERIFY_DEBUG(false, "%s: unexpected message type 0x%08" PRIx32, __func__, ev->GetTypeRewrite());
        }
    }

    void HandleBroken(const TActorId& from) {
        LOG_E("Service is broken, send error to client " << from);
        Send(from, new TEvKqpSpilling::TEvError("Service not started"));
    }

    void HandleBroken(NMon::TEvHttpInfo::TPtr& ev) {
        Send(ev->Sender, new NMon::TEvHttpInfoRes("<html><h2>Service is not started due to IO error</h2></html>"));
    }

private:
    STRICT_STFUNC(WorkState,
        hFunc(TEvKqpSpillingLocalFile::TEvOpenFile, HandleWork)
        hFunc(TEvKqpSpillingLocalFile::TEvCloseFile, HandleWork)
        hFunc(TEvPrivate::TEvCloseFileResponse, HandleWork)
        hFunc(TEvKqpSpilling::TEvWrite, HandleWork)
        hFunc(TEvPrivate::TEvWriteFileResponse, HandleWork)
        hFunc(TEvKqpSpilling::TEvRead, HandleWork)
        hFunc(TEvPrivate::TEvReadFileResponse, HandleWork)
        hFunc(NMon::TEvHttpInfo, HandleWork)
        cFunc(TEvents::TEvPoison::EventType, PassAway)
    );

    void HandleWork(TEvKqpSpillingLocalFile::TEvOpenFile::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[OpenFile] TxId: " << msg.TxId << ", desc: " << msg.Description << ", from: " << ev->Sender
            << ", removeBlobsAfterRead: " << msg.RemoveBlobsAfterRead);

        auto it = Files.find(ev->Sender);
        if (it != Files.end()) {
            LOG_E("[OpenFile] Can not open file: already exists. TxId: " << msg.TxId << ", desc: " << msg.Description);

            Send(ev->Sender, new TEvKqpSpilling::TEvError("File already exists"));
            return;
        }

        auto& fd = Files[ev->Sender];
        fd.TxId = msg.TxId;
        fd.Description = std::move(msg.Description);
        fd.RemoveBlobsAfterRead = msg.RemoveBlobsAfterRead;
        fd.OpenAt = TInstant::Now();
    }

    void HandleWork(TEvKqpSpillingLocalFile::TEvCloseFile::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[CloseFile] from: " << ev->Sender << ", error: " << msg.Error);

        auto it = Files.find(ev->Sender);
        if (it == Files.end()) {
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

        auto it = Files.find(msg.Client);
        if (it == Files.end()) {
            LOG_E("[CloseFileResponse] Can not find file from: " << msg.Client);
            return;
        }

        ui64 blobs = 0;
        for (auto& fp : it->second.PartsList) {
            blobs += fp.Blobs.size();
        }

        Counters->SpillingStoredBlobs->Sub(blobs);
        Counters->SpillingTotalSpaceUsed->Sub(it->second.TotalSize);

        MoveFileToClosed(it);
    }

    void HandleWork(TEvKqpSpilling::TEvWrite::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[Write] from: " << ev->Sender << ", blobId: " << msg.BlobId << ", bytes: " << msg.Blob.size());

        auto it = Files.find(ev->Sender);
        if (it == Files.end()) {
            LOG_E("[Write] File not found. "
                << "From: " << ev->Sender << ", blobId: " << msg.BlobId << ", bytes: " << msg.Blob.size());

            Send(ev->Sender, new TEvKqpSpilling::TEvError("File not found"));
            return;
        }

        auto& fd = it->second;

        if (fd.CloseAt) {
            LOG_E("[Write] File already closed. "
                << "From: " << ev->Sender << ", blobId: " << msg.BlobId << ", bytes: " << msg.Blob.size());

            Send(ev->Sender, new TEvKqpSpilling::TEvError("File already closed"));
            return;
        }

        if (fd.TotalSize + msg.Blob.size() > Config.GetMaxFileSize()) {
            LOG_E("[Write] File size limit exceeded. "
                << "From: " << ev->Sender << ", blobId: " << msg.BlobId << ", bytes: " << msg.Blob.size());

            Send(ev->Sender, new TEvKqpSpilling::TEvError("File size limit exceeded"));

            Counters->SpillingTooBigFileErrors->Inc();
            return;
        }

        if (TotalSize + msg.Blob.size() > Config.GetMaxTotalSize()) {
            LOG_E("[Write] Total size limit exceeded. "
                << "From: " << ev->Sender << ", blobId: " << msg.BlobId << ", bytes: " << msg.Blob.size());

            Send(ev->Sender, new TEvKqpSpilling::TEvError("Total size limit exceeded"));

            Counters->SpillingNoSpaceErrors->Inc();
            return;
        }

        fd.TotalSize += msg.Blob.size();
        TotalSize += msg.Blob.size();

        TFileDesc::TFilePart* fp = fd.PartsList.empty() ? nullptr : &fd.PartsList.back();

        bool newFile = false;
        if (!fp || (fd.RemoveBlobsAfterRead && (fp->Size + msg.Blob.size() > Config.GetMaxFilePartSize()))) {
            if (!fd.PartsList.empty()) {
                fd.PartsList.back().Last = false;
            }

            fd.PartsList.push_back({});
            fp = &fd.PartsList.back();
            fd.Parts.emplace(msg.BlobId, fp);

            auto fname = TStringBuilder() << fd.TxId << "_" << fd.Description << "_" << fd.NextPartListIndex++;
            fp->FileName = (Root / fname).GetPath();

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

        auto it = Files.find(msg.Client);
        if (it == Files.end()) {
            LOG_E("[WriteFileResponse] Can not write file: not found. "
                << "From: " << msg.Client << ", blobId: " << msg.BlobId << ", error: " << msg.Error);

            Send(ev->Sender, new TEvKqpSpilling::TEvError("Internal error"));
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

            Counters->SpillingStoredBlobs->Inc();
            Counters->SpillingTotalSpaceUsed->Add(blobDesc.Size);

            if (msg.NewFileHandle) {
                fp->FileHandle.Swap(msg.NewFileHandle);
            }
        } else {
            LOG_E("[WriteFileResponse] File part not found. From: " << msg.Client << ", blobId: " << msg.BlobId);
            if (!fd.Error) {
                fd.Error = "File part not found";
            }

            Counters->SpillingIoErrors->Inc();
        }

        if (fd.Error) {
            Send(msg.Client, new TEvKqpSpilling::TEvError(*fd.Error));

            fd.Ops.clear();
            CloseFile(it, fd.Error);
            return;
        }

        Counters->SpillingWriteBlobs->Inc();

        Send(msg.Client, new TEvKqpSpilling::TEvWriteResult(msg.BlobId));
        RunNextOp(fd);
    }

    void HandleWork(TEvKqpSpilling::TEvRead::TPtr& ev) {
        auto& msg = *ev->Get();
        LOG_D("[Read] from: " << ev->Sender << ", blobId: " << msg.BlobId);

        auto it = Files.find(ev->Sender);
        if (it == Files.end()) {
            LOG_E("[Read] Can not read file: not found. From: " << ev->Sender << ", blobId: " << msg.BlobId);

            Send(ev->Sender, new TEvKqpSpilling::TEvError("File not found"));
            return;
        }

        auto& fd = it->second;

        if (fd.CloseAt) {
            LOG_E("[Read] Can not read file: closed. From: " << ev->Sender << ", blobId: " << msg.BlobId);

            Send(ev->Sender, new TEvKqpSpilling::TEvError("Closed"));
            return;
        }

        auto partIt = fd.Parts.find(msg.BlobId);
        if (partIt == fd.Parts.end()) {
            LOG_E("[Read] Can not read file: part not found. From: " << ev->Sender << ", blobId: " << msg.BlobId);

            Send(ev->Sender, new TEvKqpSpilling::TEvError("File part not found"));

            fd.Ops.clear();
            TMaybe<TString> err = "Part not found";
            CloseFile(it, err);
            return;
        }

        auto& fp = partIt->second;

        auto blobIt = fp->Blobs.find(msg.BlobId);
        if (blobIt == fp->Blobs.end()) {
            LOG_E("[Read] Can not read file: blob not found in the part. From: " << ev->Sender << ", blobId: " << msg.BlobId);

            Send(ev->Sender, new TEvKqpSpilling::TEvError("Blob not found in the file part"));

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

        auto it = Files.find(msg.Client);
        if (it == Files.end()) {
            LOG_E("[ReadFileResponse] Can not read file: not found. "
                << "From: " << msg.Client << ", blobId: " << msg.BlobId << ", error: " << msg.Error);

            Send(ev->Sender, new TEvKqpSpilling::TEvError("Internal error"));
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
                TotalSize -= fp->Size;

                Counters->SpillingTotalSpaceUsed->Sub(fp->Size);
                Counters->SpillingStoredBlobs->Sub(fp->Blobs.size());

                fd.Parts.erase(msg.BlobId);
                fd.PartsList.remove_if([fp](const auto& x) { return &x == fp; });
            }
        } else {
            if (!fd.Error) {
                fd.Error = "Internal error";
            }
        }

        if (fd.Error) {
            Send(msg.Client, new TEvKqpSpilling::TEvError(*fd.Error));

            fd.Ops.clear();
            CloseFile(it, fd.Error);
            return;
        }

        Counters->SpillingReadBlobs->Inc();

        Send(msg.Client, new TEvKqpSpilling::TEvReadResult(msg.BlobId, std::move(msg.Blob)));
        RunNextOp(fd);
    }

    void HandleWork(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream s;

        TMap<ui64, TVector<const TFileDesc*>> byTx;
        for (const auto& fd : Files) {
            byTx[fd.second.TxId].push_back(&fd.second);
        }

        HTML(s) {
            TAG(TH2) { s << "Configuration"; }
            PRE() {
                s << "  - Root: " << Config.GetRoot() << Endl;
                s << "  - MaxTotalSize: " << Config.GetMaxTotalSize() << Endl;
                s << "  - MaxFileSize: " << Config.GetMaxFileSize() << Endl;
                s << "  - MaxFilePartSize: " << Config.GetMaxFilePartSize() << Endl;
                s << "  - IO thread pool, workers: " << Config.GetIoThreadPool().GetWorkersCount()
                    << ", queue: " << Config.GetIoThreadPool().GetQueueSize() << Endl;
            }

            TAG(TH2) { s << "Active files"; }
            PRE() { s << "Used space: " << TotalSize << Endl; }

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
                for (auto it = ClosedFiles.rbegin(); it != ClosedFiles.rend(); ++it) {
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

private:
    void RunOp(TStringBuf opName, THolder<IObjectInQueue> op, TFileDesc& fd) {
        if (fd.HasActiveOp) {
            fd.Ops.emplace_back(opName, std::move(op));
        } else {
            fd.HasActiveOp = true;
            // TODO: retry if fails
            IoThreadPool->SafeAddAndOwn(std::move(op));
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
        TotalSize -= it->second.TotalSize;
        ClosedFiles.emplace_back(TClosedFileDesc(std::move(it->second)));
        while (ClosedFiles.size() > 100) {
            ClosedFiles.pop_front();
        }
        Files.erase(it);
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
    const NKikimrConfig::TTableServiceConfig::TSpillingServiceConfig::TLocalFileConfig Config;
    TFsPath Root;
    TIntrusivePtr<TKqpCounters> Counters;

    struct TFileDesc {
        ui64 TxId = 0;
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
        ui64 TxId;
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
            , Error(std::move(fd.Error)) {}
    };

    THolder<IThreadPool> IoThreadPool;
    THashMap<TActorId, TFileDesc> Files;
    TList<const TClosedFileDesc> ClosedFiles;
    ui64 TotalSize = 0;
};

} // anonymous namespace

IActor* CreateKqpLocalFileSpillingActor(ui64 txId, const TString& details, const TActorId& client,
    bool removeBlobsAfterRead)
{
    return new TKqpLocalFileSpillingActor(txId, details, client, removeBlobsAfterRead);
}

IActor* CreateKqpLocalFileSpillingService(
    const NKikimrConfig::TTableServiceConfig::TSpillingServiceConfig::TLocalFileConfig& config,
    TIntrusivePtr<TKqpCounters> counters)
{
    return new TKqpLocalFileSpillingService(config, counters);
}

} // namespace NKikimr::NKqp
