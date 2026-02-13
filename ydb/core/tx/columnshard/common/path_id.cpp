#include "path_id.h"

#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/protos/pathid.pb.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/sessions.pb.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>
#include <ydb/core/tx/columnshard/transactions/protos/tx_event.pb.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

//Explicit specializations for protos that hold TInternalPathId

template <>
TInternalPathId TInternalPathId::FromProto(const NKikimrTxColumnShard::TTableVersionInfo& proto) {
    return TInternalPathId(proto.GetPathId());
}
template <>
void TInternalPathId::ToProto(NKikimrTxColumnShard::TTableVersionInfo& proto) const {
    proto.SetPathId(PathId);
}

template <>
TInternalPathId TInternalPathId::FromProto(const NKikimrTxColumnShard::TInternalOperationData& proto) {
    return TInternalPathId(proto.GetPathId());
}
template <>
void TInternalPathId::ToProto(NKikimrTxColumnShard::TInternalOperationData& proto) const {
    proto.SetPathId(PathId);
}

//TODO revise me when implementing reshading
template <>
TInternalPathId TInternalPathId::FromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto) {
    return TInternalPathId(proto.GetPathId());
}
template <>
void TInternalPathId::ToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const {
    proto.SetPathId(PathId);
}

//TODO revise me when implementing reshading
template <>
TInternalPathId TInternalPathId::FromProto(const NKikimrColumnShardDataSharingProto::TPathIdData& proto) {
    return TInternalPathId(proto.GetPathId());
}
template <>
void TInternalPathId::ToProto(NKikimrColumnShardDataSharingProto::TPathIdData& proto) const {
    proto.SetPathId(PathId);
}

//TODO revise me when implementing backup
template <>
TInternalPathId TInternalPathId::FromProto(const NKikimrColumnShardExportProto::TIdentifier& proto) {
    return TInternalPathId(proto.GetPathId());
}
template <>
void TInternalPathId::ToProto(NKikimrColumnShardExportProto::TIdentifier& proto) const {
    proto.SetPathId(PathId);
}

TString TInternalPathId::DebugString() const {
    return ToString(PathId);
}

//Explicit specialization for protos that hold SchemeShardLocalPathId

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TEvWrite& proto) {
    return TSchemeShardLocalPathId(proto.GetTableId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TEvWrite& proto) const {
    proto.SetTableId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TEvWriteResult& proto) {
    return TSchemeShardLocalPathId(proto.GetTableId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TEvWriteResult& proto) const {
    proto.SetTableId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TEvRead& proto) {
    return TSchemeShardLocalPathId(proto.GetTableId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TEvRead& proto) const {
    proto.SetTableId(PathId);
}
template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TEvReadResult& proto) {
    return TSchemeShardLocalPathId(proto.GetTableId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TEvReadResult& proto) const {
    proto.SetTableId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TInitShard& proto) {
    return TSchemeShardLocalPathId(proto.GetOwnerPathId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TInitShard& proto) const {
    proto.SetOwnerPathId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TCreateTable& proto) {
    return TSchemeShardLocalPathId(proto.GetPathId());
}

template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TCreateTable& proto) const {
    proto.SetPathId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TAlterStore& proto) {
    return TSchemeShardLocalPathId(proto.GetStorePathId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TAlterStore& proto) const {
    proto.SetStorePathId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TAlterTable& proto) {
    return TSchemeShardLocalPathId(proto.GetPathId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TAlterTable& proto) const {
    proto.SetPathId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TDropTable& proto) {
    return TSchemeShardLocalPathId(proto.GetPathId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TDropTable& proto) const {
    proto.SetPathId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TInternalOperationData& proto) {
    return TSchemeShardLocalPathId(proto.GetSchemeShardLocalPathId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TInternalOperationData& proto) const {
    proto.SetSchemeShardLocalPathId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrSchemeOp::TGranuleShardingInfo& proto) {
    return TSchemeShardLocalPathId(proto.GetPathId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrSchemeOp::TGranuleShardingInfo& proto) const {
    proto.SetPathId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxDataShard::TEvKqpScan& proto) {
    return TSchemeShardLocalPathId(proto.GetLocalPathId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrTxDataShard::TEvKqpScan& proto) const {
    proto.SetLocalPathId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrDataEvents::TTableId& proto) {
    return TSchemeShardLocalPathId(proto.GetTableId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrDataEvents::TTableId& proto) const {
    proto.SetTableId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrDataEvents::TLock& proto) {
    return TSchemeShardLocalPathId(proto.GetPathId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrDataEvents::TLock& proto) const {
    proto.SetPathId(PathId);
}

template <>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrProto::TPathID& proto) {
    return TSchemeShardLocalPathId(proto.GetLocalId());
}
template <>
void TSchemeShardLocalPathId::ToProto(NKikimrProto::TPathID& proto) const {
    proto.SetLocalId(PathId);
}

TString TSchemeShardLocalPathId::DebugString() const {
    return ToString(PathId);
}

TUnifiedPathId TUnifiedPathId::BuildValid(const TInternalPathId internalPathId, const TSchemeShardLocalPathId externalPathId) {
    AFL_VERIFY(internalPathId && externalPathId);
    return TUnifiedPathId(internalPathId, externalPathId);
}

TInternalPathId TUnifiedOptionalPathId::GetInternalPathIdVerified() const {
    AFL_VERIFY(!!InternalPathId);
    return *InternalPathId;
}

TSchemeShardLocalPathId TUnifiedOptionalPathId::GetSchemeShardLocalPathIdVerified() const {
    AFL_VERIFY(!!SchemeShardLocalPathId);
    return *SchemeShardLocalPathId;
}

}   //namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {

std::set<NColumnShard::TUnifiedPathId> IPathIdTranslator::GetUnifiedPathIdsByInternalVerified(const TInternalPathId internalPathId) const {
    std::set<NColumnShard::TUnifiedPathId> paths;
    for (const auto& schemeShardLocalPathId: ResolveSchemeShardLocalPathIdsVerified(internalPathId)) {
        paths.insert(NColumnShard::TUnifiedPathId::BuildValid(internalPathId, schemeShardLocalPathId));
    }
    return paths;
}

std::set<NColumnShard::TSchemeShardLocalPathId> IPathIdTranslator::ResolveSchemeShardLocalPathIdsVerified(const TInternalPathId internalPathId) const {
    auto result = ResolveSchemeShardLocalPathIds(internalPathId);
    AFL_VERIFY(result)("path_id", internalPathId.DebugString());
    return *result;
}

NOlap::TInternalPathId IPathIdTranslator::ResolveInternalPathIdVerified(
    const NColumnShard::TSchemeShardLocalPathId schemeShardLocalPathId, const bool withTabletPathId) const {
    auto result = ResolveInternalPathIdOptional(schemeShardLocalPathId, withTabletPathId);
    AFL_VERIFY(result);
    return *result;
}

}   // namespace NKikimr::NOlap

template <>
void Out<NKikimr::NColumnShard::TInternalPathId>(IOutputStream& s, const NKikimr::NColumnShard::TInternalPathId& v) {
    s << v.GetRawValue();
}

template <>
void Out<NKikimr::NColumnShard::TSchemeShardLocalPathId>(IOutputStream& s, const NKikimr::NColumnShard::TSchemeShardLocalPathId& v) {
    s << v.GetRawValue();
}

template <>
void Out<NKikimr::NColumnShard::TUnifiedPathId>(IOutputStream& s, const NKikimr::NColumnShard::TUnifiedPathId& v) {
    s << "{internal: " << v.InternalPathId << ", ss: " << v.SchemeShardLocalPathId << "}";
}

template <>
void Out<NKikimr::NColumnShard::TUnifiedOptionalPathId>(IOutputStream& s, const NKikimr::NColumnShard::TUnifiedOptionalPathId& v) {
    s << "{";
    if (v.HasSchemeShardLocalPathId() && v.HasInternalPathId()) {
        s << "internal: " << v.GetInternalPathIdVerified() << ", ss:" << v.GetSchemeShardLocalPathIdVerified();
    } else if (v.HasInternalPathId()) {
        s << "internal:" << v.GetInternalPathIdVerified();
    } else if (v.HasSchemeShardLocalPathId()) {
        s << "ss:" << v.GetSchemeShardLocalPathIdVerified();
    }
    s << "}";
}
