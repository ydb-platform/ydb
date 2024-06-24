#include "blobstorage.h"

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

std::unique_ptr<TEvBlobStorage::TEvPutResult> TEvBlobStorage::TEvPut::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId groupId) {
    auto res = std::make_unique<TEvPutResult>(status, Id, TStorageStatusFlags(), groupId, 0.0f);
    res->ErrorReason = errorReason;
    return res;
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

std::unique_ptr<TEvBlobStorage::TEvBlockResult> TEvBlobStorage::TEvBlock::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId /*groupId*/) {
    auto res = std::make_unique<TEvBlockResult>(status);
    res->ErrorReason = errorReason;
    return res;
}

std::unique_ptr<TEvBlobStorage::TEvPatchResult> TEvBlobStorage::TEvPatch::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId groupId) {
    auto res = std::make_unique<TEvPatchResult>(status, PatchedId, TStorageStatusFlags(), groupId, 0.0f);
    res->ErrorReason = errorReason;
    return res;
}

std::unique_ptr<TEvBlobStorage::TEvInplacePatchResult> TEvBlobStorage::TEvInplacePatch::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason) {
    auto res = std::make_unique<TEvInplacePatchResult>(status, PatchedId, TStorageStatusFlags(), 0.0f);
    res->ErrorReason = errorReason;
    return res;
}

std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> TEvBlobStorage::TEvDiscover::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId/*groupId*/) {
    auto res = std::make_unique<TEvDiscoverResult>(status, MinGeneration, 0);
    res->ErrorReason = errorReason;
    return res;
}

std::unique_ptr<TEvBlobStorage::TEvRangeResult> TEvBlobStorage::TEvRange::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId groupId) {
    auto res = std::make_unique<TEvRangeResult>(status, From, To, groupId);
    res->ErrorReason = errorReason;
    return res;
}

std::unique_ptr<TEvBlobStorage::TEvCollectGarbageResult> TEvBlobStorage::TEvCollectGarbage::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId /*groupId*/) {
    auto res = std::make_unique<TEvCollectGarbageResult>(status, TabletId, RecordGeneration, PerGenerationCounter, Channel);
    res->ErrorReason = errorReason;
    return res;
}

std::unique_ptr<TEvBlobStorage::TEvStatusResult> TEvBlobStorage::TEvStatus::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId /*groupId*/) {
    auto res = std::make_unique<TEvStatusResult>(status, TStorageStatusFlags());
    res->ErrorReason = errorReason;
    return res;
}

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
