#include "blobstorage.h"
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr {

NKikimrBlobStorage::EPDiskType PDiskTypeToPDiskType(const NPDisk::EDeviceType type) {
    switch (type) {
        case NPDisk::DEVICE_TYPE_ROT:
            return NKikimrBlobStorage::EPDiskType::ROT;
        case NPDisk::DEVICE_TYPE_SSD:
            return NKikimrBlobStorage::EPDiskType::SSD;
        case NPDisk::DEVICE_TYPE_NVME:
            return NKikimrBlobStorage::EPDiskType::NVME;
        case NPDisk::DEVICE_TYPE_UNKNOWN:
            return NKikimrBlobStorage::EPDiskType::UNKNOWN_TYPE;
        default:
            Y_ABORT("Device type is unknown; type# %" PRIu64, (ui64)type);
    }
}

NPDisk::EDeviceType PDiskTypeToPDiskType(const NKikimrBlobStorage::EPDiskType type) {
    switch (type) {
        case NKikimrBlobStorage::EPDiskType::ROT:
            return NPDisk::DEVICE_TYPE_ROT;
        case NKikimrBlobStorage::EPDiskType::SSD:
            return NPDisk::DEVICE_TYPE_SSD;
        case NKikimrBlobStorage::EPDiskType::NVME:
            return NPDisk::DEVICE_TYPE_NVME;
        case NKikimrBlobStorage::EPDiskType::UNKNOWN_TYPE:
            return NPDisk::DEVICE_TYPE_UNKNOWN;
        default:
            Y_ABORT("Device type is unknown; type# %" PRIu64, (ui64)type);
    }
}

bool operator==(const TPDiskCategory x, const TPDiskCategory y) {
    return x.Kind() == y.Kind() && x.Type() == y.Type();
}

bool operator!=(const TPDiskCategory x, const TPDiskCategory y) {
    return !(x == y);
}

bool operator<(const TPDiskCategory x, const TPDiskCategory y) {
    return std::make_tuple(x.Type(), x.Kind()) < std::make_tuple(y.Type(), y.Kind());
}

void TEvBlobStorage::TEvPut::ToSpan(NWilson::TSpan& span) const {
    span
        .Attribute("Id", Id.ToString())
        .Attribute("PutHandleClass", NKikimrBlobStorage::EPutHandleClass_Name(HandleClass));
}

std::unique_ptr<TEvBlobStorage::TEvPutResult> TEvBlobStorage::TEvPut::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId groupId) {
    auto res = std::make_unique<TEvPutResult>(status, Id, TStorageStatusFlags(), groupId, 0.0f);
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvGet::ToSpan(NWilson::TSpan& span) const {
    i64 totalSize = 0;
    for (ui32 i = 0; i < QuerySize; ++i) {
        const auto& q = Queries[i];
        if (q.Shift < q.Id.BlobSize()) {
            totalSize += Min<size_t>(q.Id.BlobSize() - q.Shift, q.Size ? q.Size : Max<size_t>());
        }
    }

    span
        .Attribute("TotalSize", totalSize)
        .Attribute("GetHandleClass", NKikimrBlobStorage::EGetHandleClass_Name(GetHandleClass))
        .Attribute("MustRestoreFirst", MustRestoreFirst)
        .Attribute("IsIndexOnly", IsIndexOnly);

    if (span.GetTraceId().GetVerbosity() >= TWilson::DsProxyInternals) {
        NWilson::TArrayValue queries;
        queries.reserve(QuerySize);
        for (ui32 i = 0; i < QuerySize; ++i) {
            const auto& q = Queries[i];
            queries.emplace_back(NWilson::TKeyValueList{{
                {"Id", q.Id.ToString()},
                {"Shift", q.Shift},
                {"Size", q.Size},
            }});
        }
        span.Attribute("Queries", std::move(queries));
    }
}

std::unique_ptr<TEvBlobStorage::TEvGetResult> TEvBlobStorage::TEvGet::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId groupId) {
    auto res = std::make_unique<TEvGetResult>(status, QuerySize, groupId);
    for (ui32 i = 0; i < QuerySize; ++i) {
        const auto& from = Queries[i];
        auto& to = res->Responses[i];
        to.Status = status;
        to.Id = from.Id;
        to.Shift = from.Shift;
        to.RequestedSize = from.Size;
        to.LooksLikePhantom = PhantomCheck ? std::make_optional(false) : std::nullopt;
    }
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvBlock::ToSpan(NWilson::TSpan& span) const {
    span
        .Attribute("TabletId", ::ToString(TabletId))
        .Attribute("Generation", Generation);
}

std::unique_ptr<TEvBlobStorage::TEvBlockResult> TEvBlobStorage::TEvBlock::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId /*groupId*/) {
    auto res = std::make_unique<TEvBlockResult>(status);
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvPatch::ToSpan(NWilson::TSpan& span) const {
    span
        .Attribute("OriginalGroupId", OriginalGroupId)
        .Attribute("OriginalId", OriginalId.ToString())
        .Attribute("PatchedId", PatchedId.ToString());
}

std::unique_ptr<TEvBlobStorage::TEvPatchResult> TEvBlobStorage::TEvPatch::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId groupId) {
    auto res = std::make_unique<TEvPatchResult>(status, PatchedId, TStorageStatusFlags(), groupId, 0.0f);
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvInplacePatch::ToSpan(NWilson::TSpan& /*span*/) const {
}

std::unique_ptr<TEvBlobStorage::TEvInplacePatchResult> TEvBlobStorage::TEvInplacePatch::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason) {
    auto res = std::make_unique<TEvInplacePatchResult>(status, PatchedId, TStorageStatusFlags(), 0.0f);
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvDiscover::ToSpan(NWilson::TSpan& span) const {
    span
        .Attribute("TabletId", ::ToString(TabletId))
        .Attribute("ReadBody", ReadBody);
}

std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> TEvBlobStorage::TEvDiscover::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId/*groupId*/) {
    auto res = std::make_unique<TEvDiscoverResult>(status, MinGeneration, 0);
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvRange::ToSpan(NWilson::TSpan& span) const {
    span
        .Attribute("TabletId", ::ToString(TabletId))
        .Attribute("From", From.ToString())
        .Attribute("To", To.ToString())
        .Attribute("MustRestoreFirst", MustRestoreFirst)
        .Attribute("IsIndexOnly", IsIndexOnly);
}

std::unique_ptr<TEvBlobStorage::TEvRangeResult> TEvBlobStorage::TEvRange::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId groupId) {
    auto res = std::make_unique<TEvRangeResult>(status, From, To, groupId);
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvCollectGarbage::ToSpan(NWilson::TSpan& span) const {
    span
        .Attribute("TabletId", ::ToString(TabletId))
        .Attribute("RecordGeneration", RecordGeneration)
        .Attribute("PerGenerationCounter", PerGenerationCounter)
        .Attribute("Channel", Channel);

    if (Collect) {
        span
            .Attribute("CollectGeneration", CollectGeneration)
            .Attribute("CollectStep", CollectStep);
    }

    if (span.GetTraceId().GetVerbosity() >= TWilson::DsProxyInternals) {
        auto vector = [&](const auto& name, const auto& v) {
            if (v) {
                NWilson::TArrayValue items;
                items.reserve(v->size());
                for (const TLogoBlobID& id : *v) {
                    items.emplace_back(id.ToString());
                }
                span.Attribute(name, std::move(items));
            }
        };
        vector("Keep", Keep);
        vector("DoNotKeep", DoNotKeep);
    } else {
        if (Keep) {
            span.Attribute("NumKeep", static_cast<i64>(Keep->size()));
        }
        if (DoNotKeep) {
            span.Attribute("NumDoNotKeep", static_cast<i64>(DoNotKeep->size()));
        }
    }
}

std::unique_ptr<TEvBlobStorage::TEvCollectGarbageResult> TEvBlobStorage::TEvCollectGarbage::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId /*groupId*/) {
    auto res = std::make_unique<TEvCollectGarbageResult>(status, TabletId, RecordGeneration, PerGenerationCounter, Channel);
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvStatus::ToSpan(NWilson::TSpan& /*span*/) const
{}

std::unique_ptr<TEvBlobStorage::TEvStatusResult> TEvBlobStorage::TEvStatus::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId /*groupId*/) {
    auto res = std::make_unique<TEvStatusResult>(status, TStorageStatusFlags());
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvAssimilate::ToSpan(NWilson::TSpan& /*span*/) const
{}

std::unique_ptr<TEvBlobStorage::TEvAssimilateResult> TEvBlobStorage::TEvAssimilate::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId/*groupId*/) {
    return std::make_unique<TEvBlobStorage::TEvAssimilateResult>(status, errorReason);
}

};

template<>
void Out<NKikimr::TStorageStatusFlags>(IOutputStream& o,
        typename TTypeTraits<NKikimr::TStorageStatusFlags>::TFuncParam x) {
    return x.Output(o);
}

template<>
void Out<NKikimr::TPDiskCategory>(IOutputStream &str, const NKikimr::TPDiskCategory &value) {
    str << value.ToString();
}
