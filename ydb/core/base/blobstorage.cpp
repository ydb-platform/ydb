#include "blobstorage.h"
#include <util/stream/str.h>
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

void TEvBlobStorage::TEvCheckIntegrity::ToSpan(NWilson::TSpan& span) const {
    span
        .Attribute("Id", Id.ToString())
        .Attribute("GetHandleClass", NKikimrBlobStorage::EGetHandleClass_Name(GetHandleClass));
}

std::unique_ptr<TEvBlobStorage::TEvCheckIntegrityResult> TEvBlobStorage::TEvCheckIntegrity::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId /*groupId*/) {
    auto res = std::make_unique<TEvCheckIntegrityResult>(status);
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvBlock::ToSpan(NWilson::TSpan& span) const {
    span
        .Attribute("TabletId", ::ToString(TabletId))
        .Attribute("Generation", Generation);
    if (Version) {
        span.Attribute("Version", *Version);
    }
}

std::unique_ptr<TEvBlobStorage::TEvBlockResult> TEvBlobStorage::TEvBlock::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId /*groupId*/) {
    auto res = std::make_unique<TEvBlockResult>(status);
    res->ErrorReason = errorReason;
    return res;
}

void TEvBlobStorage::TEvGetBlock::ToSpan(NWilson::TSpan& span) const {
    span
        .Attribute("TabletId", ::ToString(TabletId));
}

std::unique_ptr<TEvBlobStorage::TEvGetBlockResult> TEvBlobStorage::TEvGetBlock::MakeErrorResponse(
        NKikimrProto::EReplyStatus status, const TString& errorReason, TGroupId /*groupId*/) {
    auto res = std::make_unique<TEvGetBlockResult>(status, TabletId, 0);
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

template<>
void Out<NKikimr::TBridgePileId>(IOutputStream& str, const NKikimr::TBridgePileId& bridgePileId) {
    str << bridgePileId.ToString();
}

namespace NKikimr {

TString TEvBlobStorage::TEvRequestCommon::GetRequestName(ui32 eventType) {
    switch (eventType) {
    case EvPut:
        return "Put";
    case EvGet:
        return "Get";
    case EvBlock:
        return "Block";
    case EvCollectGarbage:
        return "CollectGarbase";
    case EvDiscover:
        return "Discover";
    case EvPatch:
        return "Patch";
    case EvInplacePatch:
        return "InplacePatch";
    case EvCheckIntegrity:
        return "CheckIntegrity";
    case EvRange:
        return "Range";
    case EvStatus:
        return "Status";
    case EvAssimilate:
        return "Assimilate";
    case EvGetBlock:
        return "GetBlock";
    default:
        return "Unknown";
    }
}

const char* TEvBlobStorage::TEvPut::TacticName(ETactic tactic) {
    switch (tactic) {
        case TacticMaxThroughput:
            return "MaxThroughput";
        case TacticMinLatency:
            return "MinLatency";
        case TacticDefault:
            return "Default";
        default:
            return "unknown";
    }
}

TEvBlobStorage::TEvPut::TEvPut(TParameters parameters)
    : TEvRequestCommon(parameters.DataKind)
    , Id(parameters.BlobId)
    , Buffer(std::move(parameters.Buffer))
    , Deadline(parameters.Deadline)
    , HandleClass(parameters.HandleClass)
    , Tactic(parameters.Tactic)
    , WriteSource(parameters.WriteSource)
    , IssueKeepFlag(parameters.IssueKeepFlag)
    , IgnoreBlock(parameters.IgnoreBlock)
    , AlreadyEncrypted(parameters.AlreadyEncrypted)
    , ReduceInterpileTraffic(parameters.ReduceInterpileTraffic)
    , IsZeroEntry(parameters.IsZeroEntry)
    , FailOnSlowDown(parameters.FailOnSlowDown)
    , ExternalRelevanceWatcher(std::move(parameters.ExternalRelevanceWatcher))
{
    Y_ABORT_UNLESS(Id, "EvPut invalid: LogoBlobId must have non-zero tablet field, id# %s", Id.ToString().c_str());
    Y_ABORT_UNLESS(Buffer.size() < (40 * 1024 * 1024),
           "EvPut invalid: LogoBlobId# %s buffer.Size# %zu",
           Id.ToString().data(), Buffer.size());
    Y_ABORT_UNLESS(Buffer.size() == Id.BlobSize(),
           "EvPut invalid: LogoBlobId# %s buffer.Size# %zu",
           Id.ToString().data(), Buffer.size());
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Id, sizeof(Id));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(Buffer.GetContiguousSpan().Data(), Buffer.size());
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Deadline, sizeof(Deadline));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&HandleClass, sizeof(HandleClass));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&Tactic, sizeof(Tactic));
}

TString TEvBlobStorage::TEvPut::Print(bool isFull) const {
    TStringStream str;
    str << "TEvPut {Id# " << Id.ToString();
    str << " Size# " << Buffer.GetSize();
    if (isFull) {
        str << " Buffer# " << Buffer.ExtractUnderlyingContainerOrCopy<TString>().Quote();
    }
    str << " Deadline# " << Deadline.MilliSeconds();
    str << " HandleClass# " << HandleClass;
    str << " Tactic# " << TacticName(Tactic);
    if (DataKind != NKikimrBlobStorage::TDataKind::USER) {
        str << " DataKind# " << NKikimrBlobStorage::TDataKind::E_Name(DataKind);
    }
    if (IssueKeepFlag) {
        str << " IssueKeepFlag# " << IssueKeepFlag;
    }
    if (IgnoreBlock) {
        str << " IgnoreBlock# " << IgnoreBlock;
    }
    if (AlreadyEncrypted) {
        str << " AlreadyEncrypted# " << AlreadyEncrypted;
    }
    if (ReduceInterpileTraffic) {
        str << " ReduceInterpileTraffic# " << ReduceInterpileTraffic;
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvPutResult::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringStream str;
    str << "TEvPutResult {Id# " << Id.ToString();
    str << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
    str << " StatusFlags# " << StatusFlags;
    if (ErrorReason.size()) {
        str << " ErrorReason# \"" << ErrorReason << "\"";
    }
    str << " ApproximateFreeSpaceShare# " << ApproximateFreeSpaceShare;
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvGet::TQuery::ToString() const {
    TStringStream str;
    str << "TQuery {Id# " << Id.ToString();
    str << " Shift# " << Shift;
    str << " Size# " << Size;
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvGet::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringStream str;
    str << "TEvGet {MustRestoreFirst# " << (MustRestoreFirst ? "true" : "false");
    if (DataKind != NKikimrBlobStorage::TDataKind::USER) {
        str << " DataKind# " << NKikimrBlobStorage::TDataKind::E_Name(DataKind);
    }
    str << " GetHandleClass# " << NKikimrBlobStorage::EGetHandleClass_Name(GetHandleClass);
    str << " IsVerboseNoDataEnabled# " << (IsVerboseNoDataEnabled ? "true" : "false");
    str << " Deadline# " << Deadline.MilliSeconds();
    str << " QuerySize# " << QuerySize;
    for (ui32 i = 0; i < QuerySize; ++i) {
        TQuery &query = Queries[i];
        str << " {Id# " << query.Id.ToString();
        if (query.Shift) {
            str << " Shift# " << query.Shift;
        }
        if (query.Size) {
            str << " Size# " << query.Size;
        }
        str << "}";
    }
    if (ForceBlockTabletData) {
        str << " ForceBlockTabletId# : " << ForceBlockTabletData->Id;
        str << " ForceBlockTabletGeneration# : " << ForceBlockTabletData->Generation;
    }
    if (ReaderTabletData) {
        str << " ReaderTabletId# " << ReaderTabletData->Id;
        str << " ReaderTabletGeneration# " << ReaderTabletData->Generation;
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvGetResult::Print(bool isFull) const {
    TStringStream str;
    str << "TEvGetResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
    str << " ResponseSz# " << ResponseSz;
    for (ui32 i = 0; i < ResponseSz; ++i) {
        TResponse &response = Responses[i];
        str << " {" << response.Id.ToString();
        str << " " << NKikimrProto::EReplyStatus_Name(response.Status).data();
        if (response.Keep) {
            str << " Keep";
        }
        if (response.DoNotKeep) {
            str << " DoNotKeep";
        }
        if (response.Shift) {
            str << " Shift# " << response.Shift;
        }
        str << " Size# " << response.Buffer.size();
        if (response.RequestedSize) {
            str << " RequestedSize# " << response.RequestedSize;
        }
        if (isFull) {
            str << " Buffer# " << response.Buffer.ConvertToString().Quote();
        }
        str << "}";
        if (ErrorReason.size()) {
            str << " ErrorReason# \"" << ErrorReason << "\"";
        }
    }
    if (BlockedGeneration) {
        str << " BlockedGeneration# " << BlockedGeneration;
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvCheckIntegrity::Print(bool /*isFull*/) const {
    TStringStream str;
    str << "TEvCheckIntegrity {"
        << " Id# " << Id
        << " Deadline# " << Deadline
        << " GetHandleClass# " << NKikimrBlobStorage::EGetHandleClass_Name(GetHandleClass)
        << " }";
    return str.Str();
}

TString TEvBlobStorage::TEvCheckIntegrityResult::PlacementStatusToString(EPlacementStatus status) {
    switch (status) {
        case PS_OK:
            return "PS_OK";
        case PS_REPLICATION_IN_PROGRESS:
            return "PS_REPLICATION_IN_PROGRESS";
        case PS_UNKNOWN:
            return "PS_UNKNOWN";
        case PS_BLOB_IS_RECOVERABLE:
            return "PS_BLOB_IS_RECOVERABLE";
        case PS_BLOB_IS_LOST:
            return "PS_BLOB_IS_LOST";
        default:
            return "BAD_PLACEMENT_STATUS";
    }
}

TString TEvBlobStorage::TEvCheckIntegrityResult::DataStatusToString(EDataStatus status) {
    switch (status) {
        case DS_OK:
            return "DS_OK";
        case DS_UNKNOWN:
            return "DS_UNKNOWN";
        case DS_ERROR:
            return "DS_ERROR";
        default:
            return "BAD_DATA_STATUS";
    }
}

TString TEvBlobStorage::TEvCheckIntegrityResult::Print(bool /*isFull*/) const {
    TStringStream str;
    str << "TEvCheckIntegrityResult {"
        << " Id# " << Id
        << " Status# " << NKikimrProto::EReplyStatus_Name(Status)
        << " ErrorReason# " << ErrorReason
        << " Erasure# " << TBlobStorageGroupType::ErasureSpeciesName(Erasure)
        << " PlacementStatus# " << PlacementStatusToString(PlacementStatus)
        << " DataStatus# " << DataStatusToString(DataStatus)
        << " DataInfo# " << DataInfo
        << " }";
    return str.Str();
}

TString TEvBlobStorage::TEvGetBlock::Print(bool /*isFull*/) const {
    TStringStream str;
    str << "TEvGetBlock {TabletId# " << TabletId
        << " Deadline# " << Deadline
        << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvGetBlockResult::Print(bool /*isFull*/) const {
    TStringStream str;
    str << "TEvGetBlockResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
    str << " TabletId# " << TabletId << " BlockedGeneration# " << BlockedGeneration;
    if (ErrorReason.size()) {
        str << " ErrorReason# \"" << ErrorReason << "\"";
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvBlock::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringStream str;
    str << "TEvBlock {TabletId# " << TabletId
        << " Generation# " << Generation;
    if (Version) {
        str << " Version# " << *Version;
    }
    str << " Deadline# " << Deadline.MilliSeconds()
        << " IsMonitored# " << IsMonitored
        << " IsIdempotent# " << IsIdempotent
        << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvBlockResult::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringStream str;
    str << "TEvBlockResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
    str << " IsTabletStorageInfoVersionObsolete# " << IsTabletStorageInfoVersionObsolete;
    str << " ActualGeneration# " << ActualGeneration;
    if (ErrorReason.size()) {
        str << " ErrorReason# \"" << ErrorReason << "\"";
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvPatch::TDiff::ToString() const {
    TStringBuilder str;
    Output(str);
    return str;
}

void TEvBlobStorage::TEvPatch::CheckContructorArgs(const TLogoBlobID &originalId, const TLogoBlobID &patchedId,
        const TArrayHolder<TDiff> &diffs, ui64 diffCount)
{
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&originalId, sizeof(originalId));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&patchedId, sizeof(patchedId));
    REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(diffs.Get(), sizeof(*diffs.Get()) * diffCount);

    Y_VERIFY_S(originalId, "EvPatch invalid: original LogoBlobId must have non-zero tablet field,"
            << " OriginalId# " << originalId);
    Y_VERIFY_S(patchedId, "EvPatch invalid: patched LogoBlobId must have non-zero tablet field,"
            << " PatchedId# " << patchedId);
    Y_VERIFY_S(originalId != patchedId, "EvPatch invalid: OriginalId and PatchedId mustn't be equal"
            << " OriginalId# " << originalId
            << " PatchedId# " << patchedId);
    Y_VERIFY_S(patchedId.BlobSize(),
            "EvPatch invalid: LogoBlobId must have non-zero size field,"
            << " OriginalId# " << originalId
            << " PatchedId# " << patchedId);
    Y_VERIFY_S(originalId.BlobSize() == patchedId.BlobSize(),
            "EvPatch invalid: original and patched size must be equal,"
            << " OriginalId# " << originalId
            << " PatchedId# " << patchedId);

    for (ui32 idx = 0; idx < diffCount; ++idx) {
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(diffs[idx].Buffer.Data(), diffs[idx].Buffer.size());

        if (idx) {
            Y_VERIFY_S(diffs[idx - 1].Offset + diffs[idx - 1].Buffer.Size() <= diffs[idx].Offset,
                    "EvPatch invalid: Diffs must not overlap,"
                    << " [" << idx - 1 << "].Offset# " << diffs[idx - 1].Offset
                    << " [" << idx - 1 << "].Size# " << diffs[idx - 1].Buffer.Size()
                    << " [" << idx << "].Offset# " << diffs[idx].Offset
                    << " [" << idx << "].Size# " << diffs[idx].Buffer.Size());
        }
        Y_VERIFY_S(diffs[idx].Offset + diffs[idx].Buffer.Size() <= originalId.BlobSize(),
                "EvPatch invalid: Blob size bound was overflow by diff,"
                << " [" << idx << "].Offset# " << diffs[idx].Offset
                << " [" << idx << "].Size# " << diffs[idx].Buffer.Size()
                << " [" << idx << "].EndIdx# " << diffs[idx].Offset + diffs[idx].Buffer.Size()
                << " BlobSize# " << originalId.BlobSize());
        Y_VERIFY_S(diffs[idx].Buffer.Size(),
                "EvPatch invalid: Diff size must be non-zero,"
                << " [" << idx << "].Size# " << diffs[idx].Buffer.Size());
    }
}

bool TEvBlobStorage::TEvPatch::GetBlobIdWithSamePlacement(const TLogoBlobID &originalId, TLogoBlobID *patchedId,
        ui32 bitsForBruteForce, ui32 originalGroupId, ui32 currentGroupId)
{
    if (originalGroupId != currentGroupId) {
        return false;
    }

    ui32 expectedValue = originalId.Hash() % BaseDomainsCount;
    Y_ABORT_UNLESS(patchedId);
    if (patchedId->Hash() % BaseDomainsCount == expectedValue) {
        return true;
    }

    Y_ABORT_UNLESS(bitsForBruteForce <= TLogoBlobID::MaxCookie);
    ui32 baseCookie = ~bitsForBruteForce & patchedId->Cookie();
    ui32 extraCookie = TLogoBlobID::MaxCookie + 1;
    ui32 steps = 0;
    do {
        extraCookie = (extraCookie - 1) & bitsForBruteForce;
        ui32 cookie = baseCookie | (extraCookie ^ bitsForBruteForce);
        steps++;

        TLogoBlobID id(patchedId->TabletID(), patchedId->Generation(), patchedId->Step(), patchedId->Channel(),
                patchedId->BlobSize(), cookie, 0, patchedId->CrcMode());
        if (id.Hash() % BaseDomainsCount == expectedValue) {
            *patchedId = id;
            return true;
        }

    } while(extraCookie && steps < MaxStepsForFindingId);

    return false;
}

TString TEvBlobStorage::TEvPatch::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringBuilder str;
    str << "TEvPatch {OriginalGroupId# " << OriginalGroupId;
    str << " OriginalId# " << OriginalId;
    str << " PatchedId# " << PatchedId;
    str << " Deadline# " << Deadline.MilliSeconds();
    str << " DiffCount# " << DiffCount;
    for (ui32 idx = 0; idx < DiffCount; ++idx) {
        str << ' ';
        Diffs[idx].Output(str);
    }
    str << '}';
    return str;
}

TString TEvBlobStorage::TEvPatchResult::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringBuilder str;
    str << "TEvPatchResult {Id# " << Id;
    str << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
    str << " StatusFlags# " << StatusFlags;
    if (ErrorReason.size()) {
        str << " ErrorReason# \"" << ErrorReason << "\"";
    }
    str << " ApproximateFreeSpaceShare# " << ApproximateFreeSpaceShare;
    str << "}";
    return str;
}

TString TEvBlobStorage::TEvInplacePatch::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringBuilder str;
    str << "TEvInplacePatch {OriginalId# " << OriginalId;
    str << " PatchedId# " << PatchedId;
    str << " Deadline# " << Deadline.MilliSeconds();
    str << " DiffCount# " << DiffCount;
    for (ui32 idx = 0; idx < DiffCount; ++idx) {
        str << ' ';
        Diffs[idx].Output(str);
    }
    str << '}';
    return str;
}

TString TEvBlobStorage::TEvInplacePatchResult::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringBuilder str;
    str << "TEvPatchResult {Id# " << Id;
    str << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
    str << " StatusFlags# " << StatusFlags;
    if (ErrorReason.size()) {
        str << " ErrorReason# \"" << ErrorReason << "\"";
    }
    str << " ApproximateFreeSpaceShare# " << ApproximateFreeSpaceShare;
    str << "}";
    return str;
}

TString TEvBlobStorage::TEvDiscover::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringStream str;
    str << "TEvDiscover {TabletId# " << TabletId;
    str << " MinGeneration# " << MinGeneration;
    str << " ReadBody# " << (ReadBody ? "true" : "false");
    str << " DiscoverBlockedGeneration# " << (DiscoverBlockedGeneration ? "true" : "false");
    str << " ForceBlockedGeneration# " << ForceBlockedGeneration;
    str << " FromLeader# " << (FromLeader ? "true" : "false");
    str << " Deadline# " << Deadline.MilliSeconds();
    if (DataKind != NKikimrBlobStorage::TDataKind::USER) {
        str << " DataKind# " << NKikimrBlobStorage::TDataKind::E_Name(DataKind);
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvDiscoverResult::Print(bool isFull) const {
    TStringStream str;
    str << "TEvDiscoverResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
    str << " BlockedGeneration# " << BlockedGeneration;
    str << " Id# " << Id.ToString();
    str << " Size# " << Buffer.size();
    str << " MinGeneration# " << MinGeneration;
    if (isFull) {
        str << " Buffer# " << Buffer.Quote();
    }
    if (ErrorReason.size()) {
        str << " ErrorReason# \"" << ErrorReason << "\"";
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvRange::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringStream str;
    str << "TEvRange {TabletId# " << TabletId;
    str << " From# " << From.ToString();
    str << " To# " << To.ToString();
    str << " Deadline# " << Deadline.MilliSeconds();
    str << " MustRestoreFirst# " << (MustRestoreFirst ? "true" : "false");
    if (DataKind != NKikimrBlobStorage::TDataKind::USER) {
        str << " DataKind# " << NKikimrBlobStorage::TDataKind::E_Name(DataKind);
    }
    if (ForceBlockedGeneration)
        str << " ForceBlock: " << ForceBlockedGeneration;
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvRangeResult::Print(bool isFull) const {
    TStringStream str;
    str << "TEvRangeResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
    str << " From# " << From.ToString();
    str << " To# " << To.ToString();
    str << " Size# " << Responses.size();
    for (ui32 i = 0; i < Responses.size(); ++i) {
        const TResponse &response = Responses[i];
        str << " {" << response.Id.ToString();
        str << " Size# " << response.Buffer.size();
        if (isFull) {
            str << " Buffer# " << response.Buffer.Quote();
        }
        str << "}";
    }
    if (ErrorReason.size()) {
        str << " ErrorReason# \"" << ErrorReason << "\"";
    }
    str << "}";
    return str.Str();
}

THolder<TEvBlobStorage::TEvCollectGarbage> TEvBlobStorage::TEvCollectGarbage::CreateHardBarrier(ui64 tabletId, ui32 recordGeneration,
        ui32 perGenerationCounter, ui32 channel, ui32 collectGeneration, ui32 collectStep, TInstant deadline,
        TWriteSource writeSource) {
    return MakeHolder<TEvCollectGarbage>(tabletId, recordGeneration, perGenerationCounter, channel,
            true /*collect*/, collectGeneration, collectStep, nullptr /*keep*/, nullptr /*doNotKeep*/,
            deadline, false /*isMultiCollectAllowed*/, writeSource, true /*hard*/, false /*ignoreBlock*/);
}

TString TEvBlobStorage::TEvCollectGarbage::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringStream str;
    str << "TEvCollectGarbage {TabletId# " << TabletId;
    str << " RecordGeneration# " << RecordGeneration;
    str << " PerGenerationCounter# " << PerGenerationCounter;
    str << " Channel# " << Channel;
    str << " Deadline# " << Deadline.MilliSeconds();
    str << " Collect# " << (Collect ? "true" : "false");
    if (Collect) {
        str << " CollectGeneration# " << CollectGeneration;
        str << " CollectStep# " << CollectStep;
    }
    str << " Hard# " << (Hard ? "true" : "false");
    str << " IsMultiCollectAllowed# " << IsMultiCollectAllowed;
    str << " IsMonitored# " << IsMonitored;
    if (Keep.Get()) {
        str << " KeepSize# " << Keep->size() << " {";
        for (ui32 i = 0; i < Keep->size(); ++i) {
            str << "Id# " << (*Keep)[i].ToString();
        }
        str << "}";
    }
    if (DoNotKeep.Get()) {
        str << " DoNotKeepSize# " << DoNotKeep->size() << " {";
        for (ui32 i = 0; i < DoNotKeep->size(); ++i) {
            str << "Id# " << (*DoNotKeep)[i].ToString();
        }
        str << "}";
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvCollectGarbageResult::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringStream str;
    str << "TEvCollectGarbageResult {TabletId# " << TabletId;
    str << " RecordGeneration# " << RecordGeneration;
    str << " PerGenerationCounter# " << PerGenerationCounter;
    str << " Channel# " << Channel;
    str << " Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
    if (ErrorReason.size()) {
        str << " ErrorReason# \"" << ErrorReason << "\"";
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvStatus::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringStream str;
    str << "TEvStatus {Deadline# " << Deadline.MilliSeconds()
        << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvStatusResult::Print(bool isFull) const {
    Y_UNUSED(isFull);
    TStringStream str;
    str << "TEvStatusResult {Status# " << NKikimrProto::EReplyStatus_Name(Status).data();
    str << " StatusFlags# ";
    StatusFlags.Output(str);
    if (ErrorReason.size()) {
        str << " ErrorReason# \"" << ErrorReason << "\"";
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvAssimilate::ToString() const {
    TStringStream str;
    str << "TEvAssimilate {Reverse# " << Reverse;
    if (SkipBlocksUpTo) {
        str << " SkipBlocksUpTo# " << *SkipBlocksUpTo;
    }
    if (SkipBarriersUpTo) {
        auto& [tabletId, channel] = *SkipBarriersUpTo;
        str << " SkipBarriersUpTo# " << tabletId << ':' << (int)channel;
    }
    if (SkipBlobsUpTo) {
        str << " SkipBlobsUpTo# ";
        SkipBlobsUpTo->Out(str);
    }
    str << "}";
    return str.Str();
}

TString TEvBlobStorage::TEvAssimilateResult::TBlock::ToString() const {
    TStringStream str;
    Output(str);
    return str.Str();
}

void TEvBlobStorage::TEvAssimilateResult::TBarrier::TValue::Output(IOutputStream& s) const {
    if (RecordGeneration || PerGenerationCounter || CollectGeneration || CollectGeneration) {
        s << "{" << RecordGeneration << ":" << PerGenerationCounter << "=>" << CollectGeneration
            << ":" << CollectStep << "}";
    }
}

TString TEvBlobStorage::TEvAssimilateResult::TBarrier::ToString() const {
    TStringStream str;
    Output(str);
    return str.Str();
}

void TEvBlobStorage::TEvAssimilateResult::TBarrier::Output(IOutputStream& s) const {
    s << "{" << TabletId << ":" << int(Channel) << "=>soft";
    Soft.Output(s);
    s << "/hard";
    Hard.Output(s);
    s << "}";
}

TString TEvBlobStorage::TEvAssimilateResult::TBlob::ToString() const {
    TStringStream str;
    Output(str);
    return str.Str();
}

void TEvBlobStorage::TEvAssimilateResult::TBlob::Output(IOutputStream& s) const {
    Id.Out(s);
    if (Keep) {
        s << "k";
    }
    if (DoNotKeep) {
        s << "d";
    }
    if (!HasData) {
        s << "n";
    }
}

TString TEvBlobStorage::TEvAssimilateResult::Print(bool isFull) const {
    TStringStream str;
    str << "TEvAssimilateResult {"
        << "Status# " << NKikimrProto::EReplyStatus_Name(Status)
        << " ErrorReason# '" << ErrorReason << "'";

    auto out = [&](const char *name, auto& container) {
        str << " " << name << "# ";
        if (isFull) {
            str << "[";
            for (auto it = container.begin(); it != container.end(); ++it) {
                if (it != container.begin()) {
                    str << " ";
                }
                it->Output(str);
            }
            str << "]";
        } else {
            str << "size=" << container.size();
        }
    };

    out("Blocks", Blocks);
    out("Barriers", Barriers);
    out("Blobs", Blobs);

    str << "}";

    return str.Str();
}


TString TStorageStatusFlags::ToString() const {
    TStringStream str;
    Output(str);
    return str.Str();
}

TString TTabletChannelInfo::THistoryEntry::ToString() const {
    TStringStream str;
    str << "{FromGeneration# " << FromGeneration;
    str << " GroupID# " << GroupID;
    str << " Timestamp# " << Timestamp.ToString();
    str << " Version# " << Version;
    str << "}";
    return str.Str();
}

TString TTabletChannelInfo::ToString() const {
    TStringStream str;
    str << "{Channel# " << Channel;
    str << " Type# " << Type.ToString();
    str << " StoragePool# " << StoragePool;
    str << " History# {";
    const size_t historySize = History.size();
    for (size_t historyIdx = 0; historyIdx < historySize; ++historyIdx) {
        if (historyIdx != 0) {
            str <<", ";
        }
        str << historyIdx << ":" << History[historyIdx].ToString();
    }
    str << "}";
    return str.Str();
}

TString TTabletStorageInfo::ToString() const {
    TStringStream str;
    str << "{Version# " << Version;
    str << " TabletID# " << TabletID;
    str << " TabletType# " << TabletType;
    str << " Channels# {";
    const size_t channelsSize = Channels.size();
    for (size_t channelIdx = 0; channelIdx < channelsSize; ++channelIdx) {
        if (channelIdx != 0) {
            str <<", ";
        }
        str << channelIdx << ":" << Channels[channelIdx].ToString();
    }
    str << "}";
    if (TenantPathId)
        str << " Tenant: " << TenantPathId;
    str << " BootType: " << BootType;
    return str.Str();
}

} // namespace NKikimr
