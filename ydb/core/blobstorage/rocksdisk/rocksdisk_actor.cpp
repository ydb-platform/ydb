#include "rocksdisk_actor.h"

#include "rocksdisk.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <cinttypes>

namespace NKikimr {

namespace {

class TRocksDiskActor final : public NActors::TActorBootstrapped<TRocksDiskActor> {
public:
    TRocksDiskActor(TString dbPath, TVDiskID vdiskId, ui64 incarnationGuid)
        : DbPath(std::move(dbPath))
        , VDiskId(vdiskId)
        , IncarnationGuid(incarnationGuid)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        Y_UNUSED(ctx);
        TString error;
        Y_ABORT_UNLESS(Disk.Open(DbPath, &error), "failed to open rocksdisk dbPath# %s error# %s",
            DbPath.c_str(), error.c_str());
        Become(&TThis::StateWork);
    }

private:
    template <typename TProtoBlobId>
    static TString MakeBlobKey(const TProtoBlobId& blobId) {
        TString key;
        Y_ABORT_UNLESS(blobId.SerializeToString(&key));
        return key;
    }

    static TString MakeBlockKey(ui64 tabletId) {
        return ToString(tabletId);
    }

    static TString MakeBarrierKey(const NKikimrBlobStorage::TEvVCollectGarbage& record) {
        return TStringBuilder()
            << record.GetTabletId() << ':'
            << record.GetChannel() << ':'
            << record.GetRecordGeneration() << ':'
            << record.GetPerGenerationCounter() << ':'
            << (record.GetHard() ? 1 : 0);
    }

    static TString MakeBarrierValue(const NKikimrBlobStorage::TEvVCollectGarbage& record) {
        const ui32 collectGeneration = record.HasCollectGeneration() ? record.GetCollectGeneration() : 0;
        const ui32 collectStep = record.HasCollectStep() ? record.GetCollectStep() : 0;
        return TStringBuilder() << collectGeneration << ':' << collectStep;
    }

    bool TryReadBlockedGeneration(ui64 tabletId, ui32* generation) {
        TString value;
        TString error;
        if (!Disk.Get(TRocksDisk::EColumnFamily::Blocks, MakeBlockKey(tabletId), &value, &error)) {
            if (error == "key not found") {
                return false;
            }
            Y_ABORT("failed to read block record tablet# %" PRIu64 " error# %s", tabletId, error.c_str());
        }

        ui32 parsed = 0;
        Y_ABORT_UNLESS(TryFromString<ui32>(value, parsed));
        *generation = parsed;
        return true;
    }

    bool IsBlocked(ui64 tabletId, ui32 generation) {
        ui32 blockedGeneration = 0;
        return TryReadBlockedGeneration(tabletId, &blockedGeneration) && generation <= blockedGeneration;
    }

    void Handle(TEvBlobStorage::TEvVPut::TPtr ev) {
        const auto& request = ev->Get()->Record;
        const TLogoBlobID id = LogoBlobIDFromLogoBlobID(request.GetBlobID());

        NKikimrProto::EReplyStatus status = NKikimrProto::OK;
        if (!request.GetIgnoreBlock()) {
            if (IsBlocked(id.TabletID(), id.Generation())) {
                status = NKikimrProto::BLOCKED;
            }
            for (const auto& extra : request.GetExtraBlockChecks()) {
                if (IsBlocked(extra.GetTabletId(), extra.GetGeneration())) {
                    status = NKikimrProto::BLOCKED;
                    break;
                }
            }
        }

        if (status == NKikimrProto::OK) {
            TString error;
            const TString value = ev->Get()->GetBuffer().ConvertToString();
            const bool ok = Disk.PutAsync(TRocksDisk::EColumnFamily::Blobs, MakeBlobKey(request.GetBlobID()), value, &error);
            Y_ABORT_UNLESS(ok, "failed to put blob id# %s error# %s", id.ToString().c_str(), error.c_str());
        }

        auto response = std::make_unique<TEvBlobStorage::TEvVPutResult>();
        response->Record.SetStatus(status);
        response->Record.MutableBlobID()->CopyFrom(request.GetBlobID());
        response->Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
        if (request.HasCookie()) {
            response->Record.SetCookie(request.GetCookie());
        }
        if (request.HasTimestamps()) {
            response->Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
        }
        if (status == NKikimrProto::OK) {
            response->Record.SetIncarnationGuid(IncarnationGuid);
        } else {
            response->Record.SetErrorReason("blocked");
        }

        Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    void Handle(TEvBlobStorage::TEvVGet::TPtr ev) {
        const auto& request = ev->Get()->Record;

        auto response = std::make_unique<TEvBlobStorage::TEvVGetResult>();
        response->Record.SetStatus(NKikimrProto::OK);
        response->Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
        if (request.HasCookie()) {
            response->Record.SetCookie(request.GetCookie());
        }
        if (request.HasTimestamps()) {
            response->Record.MutableTimestamps()->CopyFrom(request.GetTimestamps());
        }
        response->Record.SetIncarnationGuid(IncarnationGuid);

        for (const auto& query : request.GetExtremeQueries()) {
            auto* result = response->Record.AddResult();
            result->MutableBlobID()->CopyFrom(query.GetId());
            if (query.HasCookie()) {
                result->SetCookie(query.GetCookie());
            }
            if (query.HasShift()) {
                result->SetShift(query.GetShift());
            }

            const TLogoBlobID id = LogoBlobIDFromLogoBlobID(query.GetId());
            TString value;
            TString error;
            if (!Disk.Get(TRocksDisk::EColumnFamily::Blobs, MakeBlobKey(query.GetId()), &value, &error)) {
                if (error == "key not found") {
                    result->SetStatus(NKikimrProto::NODATA);
                    result->SetSize(0);
                    result->SetFullDataSize(id.BlobSize());
                    continue;
                }
                Y_ABORT("failed to read blob id# %s error# %s", id.ToString().c_str(), error.c_str());
            }

            ui32 shift = query.HasShift() ? query.GetShift() : 0;
            if (shift > value.size()) {
                shift = value.size();
            }
            ui32 size = query.HasSize() && query.GetSize() ? query.GetSize() : (value.size() - shift);
            if (shift + size > value.size()) {
                size = value.size() - shift;
            }

            result->SetStatus(NKikimrProto::OK);
            result->SetBufferData(value.substr(shift, size));
            result->SetSize(size);
            result->SetFullDataSize(id.BlobSize());
        }

        Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    void Handle(TEvBlobStorage::TEvVBlock::TPtr ev) {
        const auto& request = ev->Get()->Record;

        ui32 currentGeneration = 0;
        const bool hasCurrentGeneration = TryReadBlockedGeneration(request.GetTabletId(), &currentGeneration);
        const ui32 actualGeneration = hasCurrentGeneration
            ? std::max(currentGeneration, request.GetGeneration())
            : request.GetGeneration();

        NKikimrProto::EReplyStatus status = NKikimrProto::OK;
        if (hasCurrentGeneration && request.GetGeneration() <= currentGeneration) {
            status = NKikimrProto::ALREADY;
        } else {
            TString error;
            const bool ok = Disk.PutSync(
                TRocksDisk::EColumnFamily::Blocks,
                MakeBlockKey(request.GetTabletId()),
                ToString(actualGeneration),
                &error);
            Y_ABORT_UNLESS(ok, "failed to store block tablet# %" PRIu64 " error# %s",
                request.GetTabletId(), error.c_str());
        }

        auto response = std::make_unique<TEvBlobStorage::TEvVBlockResult>();
        response->Record.SetStatus(status);
        response->Record.SetTabletId(request.GetTabletId());
        response->Record.SetGeneration(actualGeneration);
        response->Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
        if (status == NKikimrProto::OK) {
            response->Record.SetIncarnationGuid(IncarnationGuid);
        }

        Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    void Handle(TEvBlobStorage::TEvVCollectGarbage::TPtr ev) {
        const auto& request = ev->Get()->Record;

        NKikimrProto::EReplyStatus status = NKikimrProto::OK;
        if (IsBlocked(request.GetTabletId(), request.GetRecordGeneration())) {
            status = NKikimrProto::BLOCKED;
        } else {
            TString error;
            const bool barrierOk = Disk.PutSync(
                TRocksDisk::EColumnFamily::GarbageBarriers,
                MakeBarrierKey(request),
                MakeBarrierValue(request),
                &error);
            Y_ABORT_UNLESS(barrierOk, "failed to store barrier tablet# %" PRIu64 " error# %s",
                request.GetTabletId(), error.c_str());

            for (const auto& item : request.GetDoNotKeep()) {
                TString deleteError;
                const bool deleted = Disk.DeleteSync(TRocksDisk::EColumnFamily::Blobs, MakeBlobKey(item), &deleteError);
                if (!deleted) {
                    Y_ABORT_UNLESS(deleteError == "key not found", "failed to delete blob from DoNotKeep error# %s",
                        deleteError.c_str());
                }
            }
        }

        auto response = std::make_unique<TEvBlobStorage::TEvVCollectGarbageResult>();
        response->Record.SetStatus(status);
        response->Record.SetTabletId(request.GetTabletId());
        response->Record.SetRecordGeneration(request.GetRecordGeneration());
        response->Record.SetChannel(request.GetChannel());
        response->Record.MutableVDiskID()->CopyFrom(request.GetVDiskID());
        if (status == NKikimrProto::OK) {
            response->Record.SetIncarnationGuid(IncarnationGuid);
        }

        Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvBlobStorage::TEvVPut, Handle);
        hFunc(TEvBlobStorage::TEvVGet, Handle);
        hFunc(TEvBlobStorage::TEvVBlock, Handle);
        hFunc(TEvBlobStorage::TEvVCollectGarbage, Handle);
    )

private:
    const TString DbPath;
    const TVDiskID VDiskId;
    const ui64 IncarnationGuid;
    TRocksDisk Disk;
};

} // namespace

NActors::IActor* CreateRocksDiskActor(const TString& dbPath, const TVDiskID& vdiskId, ui64 incarnationGuid) {
    return new TRocksDiskActor(dbPath, vdiskId, incarnationGuid);
}

} // namespace NKikimr
