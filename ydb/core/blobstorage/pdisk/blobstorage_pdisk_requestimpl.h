#pragma once
#include "defs.h"
#include "blobstorage_pdisk.h"
#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_data.h"
#include "blobstorage_pdisk_drivemodel.h"
#include "blobstorage_pdisk_internal_interface.h"
#include "blobstorage_pdisk_mon.h"
#include "blobstorage_pdisk_request_id.h"
#include "blobstorage_pdisk_impl_metadata.h"

#include <ydb/core/blobstorage/base/vdisk_priorities.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/crypto/secured_block.h>
#include <ydb/library/schlab/schine/job_kind.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/actors/wilson/wilson_span_stack.h>

#include <util/generic/utility.h>
#include <util/generic/ptr.h>

namespace NKikimr {
namespace NPDisk {

enum class EOwnerGroupType {
    Static,
    Dynamic
};

//
// TRequestBase
//
class TRequestBase : public TThrRefBase {
public:
    // Identification
    const TActorId Sender;
    const TReqId ReqId;
    TOwner Owner;
    TOwnerRound OwnerRound;
    ui8 PriorityClass;
    EOwnerGroupType OwnerGroupType;
    ui64 Cookie = 0;

    // Classification
    ui64 TotalCost = 0; // Total request cost in nanoseconds
    ui8 GateId = 0;
    bool IsSensitive = false; // QoS: sensitive or best-effort
    bool IsFast = false; // QoS: best-effort with improved latency

    // Scheduling
    NHPTimer::STime Deadline = 0; // Deadline from request input to rt-scheduler
    ui64 Cost = 0; // Remaining cost in nanoseconds
    NSchLab::EJobKind JobKind = NSchLab::EJobKind::JobKindRequest;

    // Monitoring
    const NHPTimer::STime CreationTime;
    NHPTimer::STime InputTime = 0; // Time of entrance to rt-scheduler
    NHPTimer::STime ScheduleTime = 0;

    // Tracing
    mutable NWilson::TSpan Span;
    mutable NLWTrace::TOrbit Orbit;
public:
    TRequestBase(const TActorId &sender, TReqId reqId, TOwner owner, TOwnerRound ownerRound, ui8 priorityClass,
            NWilson::TSpan span = {})
        : Sender(sender)
        , ReqId(reqId)
        , Owner(owner)
        , OwnerRound(ownerRound)
        , PriorityClass(priorityClass)
        , OwnerGroupType(EOwnerGroupType::Dynamic)
        , CreationTime(HPNow())
        , Span(std::move(span))
    {
        Span.EnableAutoEnd();
    }

    void SetOwnerGroupType(bool isStaticGroupOwner) {
        OwnerGroupType = (isStaticGroupOwner ? EOwnerGroupType::Static : EOwnerGroupType::Dynamic);
    }

    void SetCookie(ui64 cookie) {
        Cookie = cookie;
    }

    virtual void Abort(TActorSystem* /*actorSystem*/) {
    }

    virtual ~TRequestBase() = default;

    virtual ERequestType GetType() const = 0;

    virtual void EstimateCost(const TDriveModel &drive) {
        Cost = drive.SeekTimeNs();
    }

    double LifeDurationMs(NHPTimer::STime now) {
        return HPMilliSecondsFloat(now - CreationTime);
    }

    double GetCostMs() const {
        return Cost / 1.0e6; // since cost is in nanoseconds
    }

    ui64 GetCost() const {
        return Cost;
    }

    ui64 GetCostCycles() const {
        return HPCyclesNs(Cost);
    }

    // Takes slack (duration free to do non-sensitive requests)
    // and reserve it or part of it for this request execution
    // On return slack contains:
    //  - slack duration left after reservation
    //  - or TDuration::Zero() if all available slack was reserved
    // Returns true if reservation has occured, false otherwise
    virtual bool TryStealSlack(ui64& slackNs, const TDriveModel &drive, ui64 appendBlockSize, bool adhesion) {
        // Default implementation appropriate for all sensitive requests
        // Must be overriden for best-effort requests
        Y_UNUSED(slackNs); Y_UNUSED(drive); Y_UNUSED(appendBlockSize); Y_UNUSED(adhesion);
        return true;
    }

    static void AbortDelete(TRequestBase* request, TActorSystem* actorSystem);
};

//
// TYardInit
//
class TYardInit : public TRequestBase {
public:
    TVDiskID VDisk;
    ui64 PDiskGuid;
    TActorId CutLogId;
    TActorId WhiteboardProxyId;
    ui32 SlotId;
    ui32 GroupSizeInUnits;
    bool GetDiskFd;

    TYardInit(const NPDisk::TEvYardInit &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::YardInit, reqIdx), 0, ev.OwnerRound, NPriInternal::Other)
        , VDisk(ev.VDisk)
        , PDiskGuid(ev.PDiskGuid)
        , CutLogId(ev.CutLogID)
        , WhiteboardProxyId(ev.WhiteboardProxyId)
        , SlotId(ev.SlotId)
        , GroupSizeInUnits(ev.GroupSizeInUnits)
        , GetDiskFd(ev.GetDiskFd)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestYardInit;
    }

    TVDiskID VDiskIdWOGeneration() const {
        TVDiskID v = VDisk;
        v.GroupGeneration = -1;
        return v;
    }

    TString ToString() const {
        TStringStream str;
        str << "TYardInit {";
        str << "VDisk# " << VDisk.ToString();
        str << " PDiskGuid# " << PDiskGuid;
        str << " SlotId# " << SlotId;
        str << " GroupSizeInUnits# " << GroupSizeInUnits;
        str << " GetDiskFd# " << GetDiskFd;
        str << "}";
        return str.Str();
    }
};

//
// TYardResize
//
class TYardResize : public TRequestBase {
public:
    ui32 GroupSizeInUnits;

    TYardResize(const NPDisk::TEvYardResize &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::YardResize, reqIdx), ev.Owner, ev.OwnerRound, NPriInternal::Other)
        , GroupSizeInUnits(ev.GroupSizeInUnits)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestYardResize;
    }
};

//
// TCheckSpace
//
class TCheckSpace : public TRequestBase {
public:
    TCheckSpace(const NPDisk::TEvCheckSpace &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::CheckSpace, reqIdx), ev.Owner, ev.OwnerRound, NPriInternal::Other)
    {
        Y_UNUSED(ev);
    }

    ERequestType GetType() const override {
        return ERequestType::RequestCheckSpace;
    }
};

//
// TLogRead
//
class TLogRead : public TRequestBase {
public:
    TLogPosition Position;
    ui64 SizeLimit;

    TLogRead(const NPDisk::TEvReadLog::TPtr &ev, ui32 pdiskId, TAtomicBase reqIdx)
        : TRequestBase(ev->Sender, TReqId(TReqId::LogRead, reqIdx), ev->Get()->Owner, ev->Get()->OwnerRound, NPriInternal::LogRead,
                NWilson::TSpan(TWilson::PDiskTopLevel, std::move(ev->TraceId), "PDisk.LogRead"))
        , Position(ev->Get()->Position)
        , SizeLimit(ev->Get()->SizeLimit)
    {
        Span.Attribute("size_limit", static_cast<i64>(ev->Get()->SizeLimit))
            .Attribute("pdisk_id", pdiskId);
    }

    ERequestType GetType() const override {
        return ERequestType::RequestLogRead;
    }
};

//
// TLogReadContinue
//
class TLogReadContinue : public TRequestBase {
public:
    void *Data;
    ui32 Size;
    ui64 Offset;
    std::weak_ptr<TCompletionAction> CompletionAction;
    TReqId ReqId;

    TLogReadContinue(const NPDisk::TEvReadLogContinue::TPtr &ev, ui32 pdiskId, TAtomicBase /*reqIdx*/)
        : TRequestBase(ev->Sender, ev->Get()->ReqId, 0, 0, NPriInternal::LogRead,
                NWilson::TSpan(TWilson::PDiskTopLevel, std::move(ev->TraceId), "PDisk.LogReadContinue"))
        , Data(ev->Get()->Data)
        , Size(ev->Get()->Size)
        , Offset(ev->Get()->Offset)
        , CompletionAction(ev->Get()->CompletionAction)
        , ReqId(ev->Get()->ReqId)
    {
        Span.Attribute("size", ev->Get()->Size)
            .Attribute("offset", static_cast<i64>(ev->Get()->Offset))
            .Attribute("pdisk_id", pdiskId);
    }

    ERequestType GetType() const override {
        return ERequestType::RequestLogReadContinue;
    }
};

//
// TLogReadResultProcess
//
class TLogReadResultProcess : public TRequestBase {
public:
    NPDisk::TEvReadLogResult::TPtr ReadLogResult;

    TLogReadResultProcess(NPDisk::TEvReadLogResult::TPtr &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::LogReadResultProcess, reqIdx), 0, 0, NPriInternal::LogRead)
        , ReadLogResult(std::move(ev))
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestLogReadResultProcess;
    }
};

//
// TLogSectorRestore
//
class TLogSectorRestore : public TRequestBase {
public:
    void *Data;
    ui32 Size;
    ui64 Offset;
    TCompletionAction *CompletionAction;

    TLogSectorRestore(const NPDisk::TEvLogSectorRestore &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::LogSectorRestore, reqIdx), 0, 0, NPriInternal::LogRead)
        , Data(ev.Data)
        , Size(ev.Size)
        , Offset(ev.Offset)
        , CompletionAction(ev.CompletionAction)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestLogSectorRestore;
    }
};

//
// TLogWrite
//
class TLogWrite : public TRequestBase {
public:
    TLogWrite *NextInBatch = nullptr;
    TLogWrite *BatchTail; // Valid only for the head of the batch
    using TCallback = NPDisk::TEvLog::TCallback;

    TLogSignature Signature;
    ui32 EstimatedChunkIdx;
    TRcBuf Data;
    ui64 LsnSegmentStart; // Additional data, for sanity checks only.
    ui64 Lsn; // Log sequence number
    void *Cookie;
    TCallback LogCallback;
    NPDisk::TCommitRecord CommitRecord;
    THolder<NPDisk::TEvLogResult> Result;
    std::function<void()> OnDestroy;

    bool Replied = false;

    TLogWrite(NPDisk::TEvLog &ev, const TActorId &sender, ui32 estimatedChunkIdx, TReqId reqId, NWilson::TSpan span)
        : TRequestBase(sender, reqId, ev.Owner, ev.OwnerRound, NPriInternal::LogWrite, std::move(span))
        , Signature(ev.Signature)
        , EstimatedChunkIdx(estimatedChunkIdx)
        , Data(ev.Data)
        , LsnSegmentStart(ev.LsnSegmentStart)
        , Lsn(ev.Lsn)
        , Cookie(ev.Cookie)
        , LogCallback(std::move(ev.LogCallback))
        , CommitRecord(ev.CommitRecord)
    {
        CommitRecord.Validate();
        BatchTail = this;
    }

    virtual ~TLogWrite() {
        Y_VERIFY_DEBUG(Replied);
        if (OnDestroy) {
            OnDestroy();
        }
    }

    ERequestType GetType() const override {
        return ERequestType::RequestLogWrite;
    }

    void EstimateCost(const TDriveModel &drive) override {
        ui64 totalBytes = (Data.size() + sizeof(TLogRecordHeader)) * (LogErasureDataParts + 1) / LogErasureDataParts;
        Cost = drive.TimeForSizeNs(totalBytes, EstimatedChunkIdx, TDriveModel::OP_TYPE_WRITE);
    }

    void AddToBatch(TLogWrite *req) {
        Y_VERIFY(BatchTail->NextInBatch == nullptr);
        Y_VERIFY(req->NextInBatch == nullptr);
        BatchTail->NextInBatch = req;
        BatchTail = req;
    }

    TLogWrite *PopFromBatch() {
        TLogWrite *next = NextInBatch;
        NextInBatch = nullptr;
        return next;
    }

    void SetOnDestroy(std::function<void()> onDestroy) {
        OnDestroy = std::move(onDestroy);
    }

    void Abort(TActorSystem* actorSystem) override {
        auto *result = new NPDisk::TEvLogResult(NKikimrProto::CORRUPTED,
            0, "TLogWrite is being aborted", 0);
        result->Results.emplace_back(Lsn, Cookie);
        actorSystem->Send(Sender, result);
        Replied = true;
    }

    TString ToString() const {
        TStringStream str;
        str << "TLogWrite {";
        str << "EstimatedChunkIdx# " << EstimatedChunkIdx;
        str << " LsnSegmentStart# " << LsnSegmentStart;
        str << " Lsn# " << Lsn;
        str << " Result# " << (!Result ? "is empty" : Result->ToString());
        str << " OnDestroy is " << (!OnDestroy ? "not " : "") << "set";
        str << "}";
        return str.Str();
    }
};

class TCompletionChunkRead;
//
// TChunkRead
//
class TChunkRead : public TRequestBase {
protected:
    static constexpr ui64 ReferenceCanary = 890461871990457885ull;
public:
    ui32 ChunkIdx;
    ui64 Offset;
    ui64 Size;
    void *Cookie;

    ui64 CurrentSector = 0;
    ui64 RemainingSize;
    TCompletionChunkRead *FinalCompletion = nullptr;
    bool ChunkEncrypted = true;
    bool IsReplied = false;

    ui64 SlackSize;
    ui64 FirstSector = 0;
    ui64 LastSector = 0;

    // Request is placed in scheduler's queues as raw pointer. To avoid deletion
    // in such situation request will take owning to self when pushed to
    // scheduler and drop owning when poped from scheduler
    TIntrusivePtr<TChunkRead> SelfPointer;

    const ui64 DoubleFreeCanary;

    std::function<TString()> DebugInfoGenerator;

    TChunkRead(const NPDisk::TEvChunkRead &ev, const TActorId &sender, TReqId reqId, NWilson::TSpan span)
        : TRequestBase(sender, reqId, ev.Owner, ev.OwnerRound, ev.PriorityClass, std::move(span))
        , ChunkIdx(ev.ChunkIdx)
        , Offset(ev.Offset)
        , Size(ev.Size)
        , Cookie(ev.Cookie)
        , RemainingSize(ev.Size)
        , SlackSize(Max<ui32>())
        , DoubleFreeCanary(ReferenceCanary)
    {
    }

    virtual ~TChunkRead() {
        Y_VERIFY(DoubleFreeCanary == ReferenceCanary, "DoubleFreeCanary in TChunkRead is dead");
        // Set DoubleFreeCanary to 0 and make sure compiler will not eliminate that action
        SecureWipeBuffer((ui8*)&DoubleFreeCanary, sizeof(DoubleFreeCanary));
        Y_VERIFY(!SelfPointer);
        Y_VERIFY(IsReplied, "Unreplied read request, chunkIdx# %" PRIu32 " Offset# %" PRIu32 " Size# %" PRIu32
            " CurrentSector# %" PRIu32 " RemainingSize# %" PRIu32,
            (ui32)ChunkIdx, (ui32)Offset, (ui32)Size, (ui32)CurrentSector, (ui32)RemainingSize);
    }

    ERequestType GetType() const override {
        return ERequestType::RequestChunkRead;
    }

    void Abort(TActorSystem* actorSystem) override;

    void EstimateCost(const TDriveModel &drive) override {
        Cost = drive.SeekTimeNs() + drive.TimeForSizeNs((ui64)RemainingSize, ChunkIdx, TDriveModel::OP_TYPE_READ);
    }

    bool TryStealSlack(ui64& slackNs, const TDriveModel &drive, ui64 appendBlockSize, bool adhesion) override {
        Y_UNUSED(appendBlockSize); Y_UNUSED(adhesion);
        // Calculate how many bytes can we read within given slack (with single seek)
        SlackSize = (ui32)drive.SizeForTimeNs(slackNs > drive.SeekTimeNs()? slackNs - drive.SeekTimeNs(): 0,
                ChunkIdx, TDriveModel::OP_TYPE_READ);
        if (SlackSize > 0) { // TODO[serxa]: actually there is some lower bound,
                             //              because we are not reading less than some number of bytes
            SlackSize = Min(SlackSize, RemainingSize);
            ui64 costNs = drive.SeekTimeNs() + drive.TimeForSizeNs((ui64)SlackSize, ChunkIdx, TDriveModel::OP_TYPE_READ);
            slackNs -= costNs;
            return true;
        } else {
            return false;
        }
    }
};

//
// TChunkReadPiece
//
class TChunkReadPiece : public TRequestBase {
public:
    TIntrusivePtr<TChunkRead> ChunkRead;
    ui64 PieceCurrentSector;
    ui64 PieceSizeLimit;
    bool IsTheLastPiece;

    // Request is placed in scheduler's queues as raw pointer. To avoid deletion
    // in such situation request will take owning to self when pushed to
    // scheduler and drop owning when poped from scheduler
    TIntrusivePtr<TChunkReadPiece> SelfPointer;

    TChunkReadPiece(TIntrusivePtr<TChunkRead> &read, ui64 pieceCurrentSector, ui64 pieceSizeLimit, bool isTheLastPiece);

    virtual ~TChunkReadPiece() {
        Y_VERIFY(!SelfPointer);
    }

    void OnSuccessfulDestroy(TActorSystem* actorSystem);

    ERequestType GetType() const override {
        return ERequestType::RequestChunkReadPiece;
    }

    void Abort(TActorSystem* actorSystem) override;

    void EstimateCost(const TDriveModel &drive) override {
        Cost = drive.SeekTimeNs() +
            drive.TimeForSizeNs((ui64)PieceSizeLimit, ChunkRead->ChunkIdx, TDriveModel::OP_TYPE_READ);
    }
};


class TCompletionChunkWrite;
//
// TChunkWrite
//
class TChunkWrite : public TRequestBase {
public:
    ui32 ChunkIdx;
    ui32 Offset;
    NPDisk::TEvChunkWrite::TPartsPtr PartsPtr;
    void *Cookie;
    bool DoFlush;
    bool IsSeqWrite;
    bool IsReplied = false;
    bool ChunkEncrypted = true;

    ui32 TotalSize;
    ui32 CurrentPart = 0;
    ui32 CurrentPartOffset = 0;
    ui32 RemainingSize = 0;

    ui32 SlackSize;
    ui32 BytesWritten = 0;

    TAtomic Pieces = 0;
    TAtomic Aborted = 0;

    THolder<TCompletionChunkWrite> Completion;

    TChunkWrite(const NPDisk::TEvChunkWrite &ev, const TActorId &sender, TReqId reqId, NWilson::TSpan span);

    void RegisterPiece() {
        AtomicIncrement(Pieces);
    }

    void AbortPiece(TActorSystem *actorSystem) {
        if (AtomicDecrement(Pieces) == 0) {
            this->Abort(actorSystem);
        }
    }

    ERequestType GetType() const override {
        return ERequestType::RequestChunkWrite;
    }

    void EstimateCost(const TDriveModel &drive) override {
        Cost = drive.SeekTimeNs() + drive.TimeForSizeNs((ui64)TotalSize, ChunkIdx, TDriveModel::OP_TYPE_WRITE);
    }

    bool TryStealSlack(ui64& slackNs, const TDriveModel &drive, ui64 appendBlockSize, bool adhesion) override {
        // Calculate how many bytes can we write within given slack (with single seek)
        // TODO[serxa]: use write speed? but there is no write speed in drive model!
        SlackSize = (ui32)drive.SizeForTimeNs(slackNs > drive.SeekTimeNs()? slackNs - drive.SeekTimeNs(): 0,
                ChunkIdx, TDriveModel::OP_TYPE_WRITE);
        // actually there is some lower bound, because we are not writing less than appendBlockSize bytes
        if (SlackSize >= appendBlockSize) {
            SlackSize = Min(
                SlackSize / appendBlockSize * appendBlockSize,
                (TotalSize + appendBlockSize - 1) / appendBlockSize * appendBlockSize);
            ui64 costNs = (adhesion? 0: drive.SeekTimeNs()) + drive.TimeForSizeNs((ui64)SlackSize, ChunkIdx, TDriveModel::OP_TYPE_WRITE);
            slackNs -= costNs;
            return true;
        } else {
            return false;
        }
    }

    void Abort(TActorSystem* actorSystem) override {
        if (!AtomicSwap(&Aborted, true)) {
            actorSystem->Send(Sender, new NPDisk::TEvChunkWriteResult(NKikimrProto::CORRUPTED, ChunkIdx, Cookie, 0, "TChunkWrite is being aborted"));
        }
    }
};

//
// TChunkWritePiece
//
class TChunkWritePiece : public TRequestBase {
public:
    TIntrusivePtr<TChunkWrite> ChunkWrite;
    ui32 PieceShift;
    ui32 PieceSize;

    TChunkWritePiece(TIntrusivePtr<TChunkWrite> &write, ui32 pieceShift, ui32 pieceSize, NWilson::TSpan span)
        : TRequestBase(write->Sender, write->ReqId, write->Owner, write->OwnerRound, write->PriorityClass, std::move(span))
        , ChunkWrite(write)
        , PieceShift(pieceShift)
        , PieceSize(pieceSize)
    {
        ChunkWrite->RegisterPiece();
    }

    ERequestType GetType() const override {
        return ERequestType::RequestChunkWritePiece;
    }

    void EstimateCost(const TDriveModel &drive) override {
        Cost = drive.SeekTimeNs() +
            drive.TimeForSizeNs((ui64)PieceSize, ChunkWrite->ChunkIdx, TDriveModel::OP_TYPE_WRITE);
    }

    void Abort(TActorSystem* actorSystem) override {
        if (ChunkWrite) {
            ChunkWrite->AbortPiece(actorSystem);
        }
    }
};

//
// TChunkTrim
//
class TChunkTrim : public TRequestBase {
public:
    ui32 ChunkIdx;
    ui32 Offset;
    ui64 Size;

    TChunkTrim(ui32 chunkIdx, ui32 offset, ui64 size, TAtomicBase reqIdx)
        : TRequestBase(TActorId(), TReqId(TReqId::ChunkTrim, reqIdx), OwnerUnallocated,
                TOwnerRound(0), NPriInternal::Trim)
        , ChunkIdx(chunkIdx)
        , Offset(offset)
        , Size(size)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestChunkTrim;
    }

    void EstimateCost(const TDriveModel &drive) override {
        Cost = drive.TrimTimeForSizeNs(Size);
    }

    bool TryStealSlack(ui64& slackNs, const TDriveModel &drive, ui64 appendBlockSize, bool adhesion) override {
        Y_UNUSED(drive); Y_UNUSED(appendBlockSize); Y_UNUSED(adhesion);
        if (slackNs > Cost) {
            slackNs -= Cost;
            return true;
        } else {
            return true;
        }
    }
};


//
// THarakiri
//
class THarakiri : public TRequestBase {
public:
    THarakiri(const NPDisk::TEvHarakiri &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::Harakiri, reqIdx), ev.Owner, ev.OwnerRound, NPriInternal::Other)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestHarakiri;
    }
};

//
// TSlay
//
class TSlay : public TRequestBase {
public:
    TVDiskID VDiskId;
    TOwnerRound SlayOwnerRound;
    ui32 PDiskId;
    ui32 VSlotId;
    TSlay(const NPDisk::TEvSlay &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::Slay, reqIdx), OwnerUnallocated, ev.SlayOwnerRound, NPriInternal::Other)
        , VDiskId(ev.VDiskId)
        , SlayOwnerRound(ev.SlayOwnerRound)
        , PDiskId(ev.PDiskId)
        , VSlotId(ev.VSlotId)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestYardSlay;
    }
};

//
// TChunkLock
//
class TChunkLock : public TRequestBase {
public:
    NPDisk::TEvChunkLock::ELockFrom LockFrom;
    bool ByVDiskId;
    TOwner Owner;
    TVDiskID VDiskId;
    bool IsGenerationSet;
    ui32 Count;
    NKikimrBlobStorage::TPDiskSpaceColor::E Color;

    TChunkLock(const NPDisk::TEvChunkLock &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::ChunkLock, reqIdx), 0, 0, NPriInternal::Other)
        , LockFrom(ev.LockFrom)
        , ByVDiskId(ev.ByVDiskId)
        , Owner(ev.Owner)
        , VDiskId(ev.VDiskId)
        , IsGenerationSet(ev.IsGenerationSet)
        , Count(ev.Count)
        , Color(ev.Color)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestChunkLock;
    }
};

//
// TChunkUnlock
//
class TChunkUnlock : public TRequestBase {
public:
    NPDisk::TEvChunkLock::ELockFrom LockFrom;
    bool ByVDiskId;
    TOwner Owner;
    TVDiskID VDiskId;
    bool IsGenerationSet;

    TChunkUnlock(const NPDisk::TEvChunkUnlock &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::ChunkUnlock, reqIdx), 0, 0, NPriInternal::Other)
        , LockFrom(ev.LockFrom)
        , ByVDiskId(ev.ByVDiskId)
        , Owner(ev.Owner)
        , VDiskId(ev.VDiskId)
        , IsGenerationSet(ev.IsGenerationSet)
    {
    }

    ERequestType GetType() const override {
        return ERequestType::RequestChunkUnlock;
    }
};

//
// TChunkReserve
//
class TChunkReserve : public TRequestBase {
public:
    ui32 SizeChunks;

    TChunkReserve(const NPDisk::TEvChunkReserve &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::ChunkReserve, reqIdx), ev.Owner, ev.OwnerRound, NPriInternal::Other)
        , SizeChunks(ev.SizeChunks)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestChunkReserve;
    }
};

//
// TChunkForget
//
class TChunkForget : public TRequestBase {
public:
    TVector<TChunkIdx> ForgetChunks;

    TChunkForget(const NPDisk::TEvChunkForget &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::ChunkForget, reqIdx), ev.Owner, ev.OwnerRound, NPriInternal::LogWrite)
        , ForgetChunks(std::move(ev.ForgetChunks))
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestChunkForget;
    }

    void EstimateCost(const TDriveModel &) override {
        Cost = 1;
    }
};

//
// TWhiteboardReport
//
class TWhiteboardReport : public TRequestBase {
public:
    TAutoPtr<TEvWhiteboardReportResult> Response;

    TWhiteboardReport(const TActorId &sender, TEvWhiteboardReportResult *response, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::WhiteboardReport, reqIdx), 0u, 0u, NPriInternal::Other)
        , Response(response)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestWhiteboartReport;
    }
};

//
// THttpInfo
//
class THttpInfo : public TRequestBase {
public:
    const TActorId EndCustomer;
    TStringStream OutputString;
    TString DeviceFlagStr;
    TString RealtimeFlagStr;
    TString FairSchedulerStr;
    TString ErrorStr;
    bool DoGetSchedule;

    THttpInfo(const TActorId &sender, const TActorId &endCustomer, TStringStream outputString,
            TString deviceFlagStr, TString realtimeFlagStr, TString fairSchedulerStr, TString errorStr,
            bool doGetSchedule, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::HttpInfo, reqIdx), 0u, 0u, NPriInternal::Other)
        , EndCustomer(endCustomer)
        , OutputString(outputString)
        , DeviceFlagStr(deviceFlagStr)
        , RealtimeFlagStr(realtimeFlagStr)
        , FairSchedulerStr(fairSchedulerStr)
        , ErrorStr(errorStr)
        , DoGetSchedule(doGetSchedule)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestHttpInfo;
    }

    void Abort(TActorSystem* actorSystem) override {
        TEvHttpInfoResult *reportResult = new TEvHttpInfoResult(EndCustomer);
        if (DoGetSchedule) {
            reportResult->HttpInfoRes = new NMon::TEvHttpInfoRes("", 0, NMon::IEvHttpInfoRes::EContentType::Custom);
            actorSystem->Send(Sender, reportResult);
        } else {
            reportResult->HttpInfoRes = new NMon::TEvHttpInfoRes("");
            actorSystem->Send(Sender, reportResult);
        }
    }
};

//
// TUndelivered
//
class TUndelivered : public TRequestBase {
public:
    TAutoPtr<TEvents::TEvUndelivered> Event;

    TUndelivered(TEvents::TEvUndelivered::TPtr ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::Undelivered, reqIdx), 0u, 0u, NPriInternal::Other)
        , Event(ev->Release())
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestUndelivered;
    }
};

//
// TYardControl
//
class TYardControl : public TRequestBase {
public:
    ui32 Action;
    void *Cookie;

    TYardControl(const NPDisk::TEvYardControl &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::YardControl, reqIdx), 0, 0, NPriInternal::Other)
        , Action(ev.Action)
        , Cookie(ev.Cookie)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestYardControl;
    }
};

//
// TAskForCutLog
//
class TAskForCutLog : public TRequestBase {
public:
    TAskForCutLog(const NPDisk::TEvAskForCutLog::TPtr &ev, ui32 /*pdiskId*/, TAtomicBase reqIdx)
        : TRequestBase(ev->Sender, TReqId(TReqId::AskForCutLog, reqIdx), ev->Get()->Owner, ev->Get()->OwnerRound, NPriInternal::Other)
    {
    }

    ERequestType GetType() const override {
        return ERequestType::RequestAskForCutLog;
    }
};

//
// TConfigureScheduler
//
class TConfigureScheduler : public TRequestBase {
public:
    TOwner OwnerId;
    TOwnerRound OwnerRound;

    TPDiskSchedulerConfig SchedulerCfg;

    TConfigureScheduler(const NPDisk::TEvConfigureScheduler &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::ConfigureScheduler, reqIdx), 0, 0, NPriInternal::Other)
        , OwnerId(ev.Owner)
        , OwnerRound(ev.OwnerRound)
        , SchedulerCfg(ev.SchedulerCfg)
    {}

    TConfigureScheduler(TOwner ownerId, TOwnerRound ownerRound)
        : TRequestBase(TActorId(), TReqId(TReqId::InnerConfigureScheduler, 0), 0, 0, NPriInternal::Other)
        , OwnerId(ownerId)
        , OwnerRound(ownerRound)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestConfigureScheduler;
    }
};


//
// TCommitLogChunks
//
class TCommitLogChunks : public TRequestBase {
public:
    TVector<ui32> CommitedLogChunks;

    TCommitLogChunks(TVector<ui32>&& commitedLogChunks, TAtomicBase reqIdx)
        : TRequestBase(TActorId(), TReqId(TReqId::CommitLogChunks, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , CommitedLogChunks(std::move(commitedLogChunks))
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestCommitLogChunks;
    }
};

//
// TReleaseChunks
//
class TReleaseChunks : public TRequestBase {
public:
    TMaybe<TLogChunkInfo> GapStart;
    TMaybe<TLogChunkInfo> GapEnd;
    TVector<TChunkIdx> ChunksToRelease;
    bool IsChunksFromLogSplice;

    TReleaseChunks(const TLogChunkInfo& gapStart, const TLogChunkInfo& gapEnd, TVector<TChunkIdx> chunksToRelease,
            TAtomicBase reqIdx)
        : TRequestBase(TActorId(), TReqId(TReqId::ReleaseChunks, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , GapStart(gapStart)
        , GapEnd(gapEnd)
        , ChunksToRelease(std::move(chunksToRelease))
        , IsChunksFromLogSplice(true)
    {}

    TReleaseChunks(TVector<TChunkIdx> chunksToRelease, TAtomicBase reqIdx)
        : TRequestBase(TActorId(), TReqId(TReqId::ReleaseChunks, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , ChunksToRelease(std::move(chunksToRelease))
        , IsChunksFromLogSplice(false)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestReleaseChunks;
    }
};

//
// TLogCommitDone
//
class TLogCommitDone : public TRequestBase {
public:
    TOwner OwnerId;
    TOwnerRound OwnerRound;
    ui64 Lsn;
    TVector<TChunkIdx> CommitedChunks;
    TVector<TChunkIdx> DeletedChunks;

    TLogCommitDone(const TLogWrite& reqLog, TAtomicBase reqIdx)
        : TRequestBase({}, TReqId(TReqId::LogCommitDone, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , OwnerId(reqLog.Owner)
        , OwnerRound(reqLog.OwnerRound)
        , Lsn(reqLog.Lsn)
        , CommitedChunks(std::move(reqLog.CommitRecord.CommitChunks))
        , DeletedChunks(std::move(reqLog.CommitRecord.DeleteChunks))
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestLogCommitDone;
    }

    TString ToString() const {
        TStringStream str;
        str << "TLogCommitDone {";
        str << "OwnerId# " << (ui32)OwnerId;
        str << " OwnerRound# " << OwnerRound;
        str << " Lsn# " << Lsn;
        str << " CommitedChunks.size()# " << CommitedChunks.size();
        str << " DeletedChunks.size()# " << DeletedChunks.size();
        str << "}";
        return str.Str();
    }
};

//
// TTryTrimChunk
//
class TTryTrimChunk : public TRequestBase {
public:
    ui64 TrimSize;

    TTryTrimChunk(ui64 trimSize, TAtomicBase reqIdx)
        : TRequestBase(TActorId(), TReqId(TReqId::TryTrimChunk, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , TrimSize(trimSize)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestTryTrimChunk;
    }
};

//
// TChunkShredResult
//
class TChunkShredResult : public TRequestBase {
public:
    TChunkIdx Chunk;
    ui32 SectorIdx;
    ui64 ShredSize;

    TChunkShredResult(TChunkIdx chunk, ui32 sectorIdx, ui64 shredSize, TAtomicBase reqIdx)
        : TRequestBase(TActorId(), TReqId(TReqId::ChunkShredResult, reqIdx), OwnerSystem, 0, NPriInternal::Other, {})
        , Chunk(chunk)
        , SectorIdx(sectorIdx)
        , ShredSize(shredSize)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestChunkShredResult;
    }

    TString ToString() const {
        TStringStream str;
        str << "TChunkShredResult { Chunk# " << Chunk
        << " SectorIdx# " << SectorIdx
        << " ShredSize# " << ShredSize << "}";
        return str.Str();
    }
};


class TStopDevice : public TRequestBase {
public:
    TStopDevice(TAtomicBase reqIdx)
        : TRequestBase(TActorId(), TReqId(TReqId::StopDevice, reqIdx), OwnerSystem, 0, NPriInternal::Other)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestStopDevice;
    }
};

class TReadMetadata : public TRequestBase {
public:
    const TMainKey MainKey;
    std::optional<TMetadataFormatSector> Format;

    TReadMetadata(TActorId sender, const TMainKey& mainKey, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::ReadMetadata, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , MainKey(mainKey)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestReadMetadata;
    }

    void Abort(TActorSystem *actorSystem) override {
        actorSystem->Send(Sender, new TEvReadMetadataResult(EPDiskMetadataOutcome::ERROR, std::nullopt));
    }
};

class TInitialReadMetadataResult : public TRequestBase {
public:
    const NMeta::TSlotKey Key;
    std::optional<TString> ErrorReason;
    TMetadataHeader Header;
    TRcBuf Payload;

    TInitialReadMetadataResult(NMeta::TSlotKey key, TAtomicBase reqIdx)
        : TRequestBase({}, TReqId(TReqId::InitialReadMetadataResult, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , Key(key)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestInitialReadMetadataResult;
    }
};

class TWriteMetadata : public TRequestBase {
public:
    TRcBuf Metadata;
    TMainKey MainKey;

    TWriteMetadata(TActorId sender, TRcBuf&& metadata, const TMainKey& mainKey, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::WriteMetadata, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , Metadata(std::move(metadata))
        , MainKey(mainKey)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestWriteMetadata;
    }

    void Abort(TActorSystem *actorSystem) override {
        actorSystem->Send(Sender, new TEvWriteMetadataResult(EPDiskMetadataOutcome::ERROR, std::nullopt));
    }
};

class TWriteMetadataResult : public TRequestBase {
public:
    const bool Success;

    TWriteMetadataResult(bool success, TActorId sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::WriteMetadataResult, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , Success(success)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestWriteMetadataResult;
    }

    void Abort(TActorSystem *actorSystem) override {
        actorSystem->Send(Sender, new TEvWriteMetadataResult(EPDiskMetadataOutcome::ERROR, std::nullopt));
    }
};

class TPushUnformattedMetadataSector : public TRequestBase {
public:
    const std::optional<TMetadataFormatSector> Format;
    const bool WantEvent;

    TPushUnformattedMetadataSector(const std::optional<TMetadataFormatSector>& format, bool wantEvent, TAtomicBase reqIdx)
        : TRequestBase({}, TReqId(TReqId::PushUnformattedMetadataSector, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , Format(format)
        , WantEvent(wantEvent)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestPushUnformattedMetadataSector;
    }
};

class TContinueReadMetadata : public TRequestBase {
    std::function<void(bool, TActorSystem*)> Callback;

public:
    TContinueReadMetadata(std::function<void(bool, TActorSystem*)> callback, TAtomicBase reqIdx)
        : TRequestBase({}, TReqId(TReqId::ContinueReadMetadata, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , Callback(std::move(callback))
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestContinueReadMetadata;
    }

    void Execute(TActorSystem *actorSystem) {
        Callback(true, actorSystem);
    }

    void Abort(TActorSystem *actorSystem) override {
        Callback(false, actorSystem);
    }
};

class TShredPDisk : public TRequestBase {
public:
    ui64 ShredGeneration;
    ui64 Cookie;

    TShredPDisk(NPDisk::TEvShredPDisk& ev, TActorId sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::ShredPDisk, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , ShredGeneration(ev.ShredGeneration)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestShredPDisk;
    }

    TString ToString() const {
        TStringStream str;
        str << "TShredPDisk {";
        str << " Owner# " << Owner;
        str << " OwnerRound# " << OwnerRound;
        str << " ShredGeneration# " << ShredGeneration;
        str << "}";
        return str.Str();
    }
};

class TPreShredCompactVDiskResult : public TRequestBase {
public:
    NKikimrProto::EReplyStatus Status;
    ui64 ShredGeneration;
    TString ErrorReason;

    TPreShredCompactVDiskResult(NPDisk::TEvPreShredCompactVDiskResult& ev, TActorId sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::PreShredCompactVDiskResult, reqIdx), ev.Owner, ev.OwnerRound, NPriInternal::Other)
        , Status(ev.Status)
        , ShredGeneration(ev.ShredGeneration)
        , ErrorReason(ev.ErrorReason)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestPreShredCompactVDiskResult;
    }

    TString ToString() const {
        TStringStream str;
        str << "TPreShredCompactVDiskResult {";
        str << " Owner# " << Owner;
        str << " OwnerRound# " << OwnerRound;
        str << " Status# " << Status;
        str << " ShredGeneration# " << ShredGeneration;
        str << " ErrorReason# " << ErrorReason;
        str << "}";
        return str.Str();
    }
};

class TShredVDiskResult : public TRequestBase {
public:
    NKikimrProto::EReplyStatus Status;
    ui64 ShredGeneration;
    TString ErrorReason;

    TShredVDiskResult(NPDisk::TEvShredVDiskResult& ev, TActorId sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::ShredVDiskResult, reqIdx), ev.Owner, ev.OwnerRound, NPriInternal::Other)
        , Status(ev.Status)
        , ShredGeneration(ev.ShredGeneration)
        , ErrorReason(ev.ErrorReason)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestShredVDiskResult;
    }

    TString ToString() const {
        TStringStream str;
        str << "TShredVDiskResult {";
        str << " Owner# " << Owner;
        str << " OwnerRound# " << OwnerRound;
        str << " Status# " << Status;
        str << " ShredGeneration# " << ShredGeneration;
        str << " ErrorReason# " << ErrorReason;
        str << "}";
        return str.Str();
    }
};

class TContinueShred : public TRequestBase {
public:
    TContinueShred(NPDisk::TEvContinueShred& ev, TActorId sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::ContinueShred, reqIdx), OwnerUnallocated, 0, NPriInternal::Other)
    {
        Y_UNUSED(ev);
    }

    ERequestType GetType() const override {
        return ERequestType::RequestContinueShred;
    }

    TString ToString() const {
        TStringStream str;
        str << "TContinueShred {";
        str << "}";
        return str.Str();
    }
};

class TChangeExpectedSlotCount : public TRequestBase {
public:
    ui32 ExpectedSlotCount;
    ui32 SlotSizeInUnits;

    TChangeExpectedSlotCount(const NPDisk::TEvChangeExpectedSlotCount &ev, const TActorId &sender, TAtomicBase reqIdx)
        : TRequestBase(sender, TReqId(TReqId::ChangeExpectedSlotCount, reqIdx), OwnerSystem, 0, NPriInternal::Other)
        , ExpectedSlotCount(ev.ExpectedSlotCount)
        , SlotSizeInUnits(ev.SlotSizeInUnits)
    {}

    ERequestType GetType() const override {
        return ERequestType::RequestChangeExpectedSlotCount;
    }

    TString ToString() const {
        TStringStream str;
        str << "TChangeExpectedSlotCount {"
            << " ExpectedSlotCount# " << ExpectedSlotCount
            << " SlotSizeInUnits# " << SlotSizeInUnits
            << " }";
        return str.Str();
    }
};

class TChunkReadRaw : public TRequestBase {
public:
    TChunkIdx ChunkIdx;
    ui32 Offset;
    ui32 Size;

public:
    TChunkReadRaw(const NPDisk::TEvChunkReadRaw& ev, const TActorId& sender, ui64 cookie, TAtomicBase reqIdx, NWilson::TSpan span)
        : TRequestBase(sender, TReqId(TReqId::ChunkReadRaw, reqIdx), ev.Owner, ev.OwnerRound, NPriInternal::Other, std::move(span))
        , ChunkIdx(ev.ChunkIdx)
        , Offset(ev.Offset)
        , Size(ev.Size)
    {
        SetCookie(cookie);
    }

    ERequestType GetType() const override {
        return ERequestType::RequestChunkReadRaw;
    }
};

class TChunkWriteRaw : public TRequestBase {
public:
    TChunkIdx ChunkIdx;
    ui32 Offset;
    TRope Data;

public:
    TChunkWriteRaw(NPDisk::TEvChunkWriteRaw& ev, const TActorId& sender, ui64 cookie, TAtomicBase reqIdx, NWilson::TSpan span)
        : TRequestBase(sender, TReqId(TReqId::ChunkWriteRaw, reqIdx), ev.Owner, ev.OwnerRound, NPriInternal::Other, std::move(span))
        , ChunkIdx(ev.ChunkIdx)
        , Offset(ev.Offset)
        , Data(std::move(ev.Data))
    {
        SetCookie(cookie);
    }

    ERequestType GetType() const override {
        return ERequestType::RequestChunkWriteRaw;
    }
};

} // NPDisk
} // NKikimr
