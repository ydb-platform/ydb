#include "path_id.h"
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/data_events.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/protos/pathid.pb.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/data.pb.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/sessions.pb.h>
#include <ydb/core/tx/columnshard/export/protos/task.pb.h>
#include <ydb/core/tx/columnshard/transactions/protos/tx_event.pb.h>

namespace NKikimr::NColumnShard {

//Explicit specializations for protos that hold TInternalPathId

template<>
TInternalPathId TInternalPathId::FromProto(const NKikimrTxColumnShard::TTableVersionInfo& proto) {
    return TInternalPathId(proto.GetPathId());
}
template<>
void TInternalPathId::ToProto(NKikimrTxColumnShard::TTableVersionInfo& proto) const {
    proto.SetPathId(PathId);
}

template<>
TInternalPathId TInternalPathId::FromProto(const NKikimrTxColumnShard::TInternalOperationData& proto) {
    return TInternalPathId(proto.GetPathId());
}
template<>
void TInternalPathId::ToProto(NKikimrTxColumnShard::TInternalOperationData& proto) const {
    proto.SetPathId(PathId);
}

//TODO revise me when implementing reshading
template<>
TInternalPathId TInternalPathId::FromProto(const NKikimrColumnShardDataSharingProto::TPortionInfo& proto) {
    return TInternalPathId(proto.GetPathId());
}
template<>
void TInternalPathId::ToProto(NKikimrColumnShardDataSharingProto::TPortionInfo& proto) const {
    proto.SetPathId(PathId);
}

//TODO revise me when implementing reshading
template<>
TInternalPathId TInternalPathId::FromProto(const NKikimrColumnShardDataSharingProto::TPathIdData& proto) {
    return TInternalPathId(proto.GetPathId());
}
template<>
void TInternalPathId::ToProto(NKikimrColumnShardDataSharingProto::TPathIdData& proto) const {
    proto.SetPathId(PathId);
}

//TODO revise me when implementing backup
template<>
TInternalPathId TInternalPathId::FromProto(const NKikimrColumnShardExportProto::TIdentifier& proto) {
    return TInternalPathId(proto.GetPathId());
}
template<>
void TInternalPathId::ToProto(NKikimrColumnShardExportProto::TIdentifier& proto) const {
    proto.SetPathId(PathId);
}


//Explicit specialization for protos that hold SchemeShardLocalPathId

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TEvWrite& proto) {
    return TSchemeShardLocalPathId(proto.GetTableId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TEvWrite& proto) const {
    proto.SetTableId(PathId);
}

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TEvWriteResult& proto) {
    return TSchemeShardLocalPathId(proto.GetTableId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TEvWriteResult& proto) const {
    proto.SetTableId(PathId);
}

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TEvRead& proto) {
    return TSchemeShardLocalPathId(proto.GetTableId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TEvRead& proto) const {
    proto.SetTableId(PathId);
}
template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TEvReadResult& proto) {
    return TSchemeShardLocalPathId(proto.GetTableId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TEvReadResult& proto) const {
    proto.SetTableId(PathId);
}

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TCreateTable& proto) {
    return TSchemeShardLocalPathId(proto.GetPathId());
}

template<>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TCreateTable& proto) const {
    proto.SetPathId(PathId);
}

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TAlterTable& proto) {
    return TSchemeShardLocalPathId(proto.GetPathId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TAlterTable& proto) const {
    proto.SetPathId(PathId);
}

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxColumnShard::TDropTable& proto) {
    return TSchemeShardLocalPathId(proto.GetPathId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrTxColumnShard::TDropTable& proto) const {
    proto.SetPathId(PathId);
}

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrSchemeOp::TGranuleShardingInfo& proto) {
    return TSchemeShardLocalPathId(proto.GetPathId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrSchemeOp::TGranuleShardingInfo& proto) const {
    proto.SetPathId(PathId);
}

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrTxDataShard::TEvKqpScan& proto) {
    return TSchemeShardLocalPathId(proto.GetLocalPathId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrTxDataShard::TEvKqpScan& proto) const {
    proto.SetLocalPathId(PathId);
}

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrDataEvents::TTableId& proto) {
    return TSchemeShardLocalPathId(proto.GetTableId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrDataEvents::TTableId& proto) const {
    proto.SetTableId(PathId);
}

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrDataEvents::TLock& proto) {
    return TSchemeShardLocalPathId(proto.GetPathId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrDataEvents::TLock& proto) const {
    proto.SetPathId(PathId);
}

template<>
TSchemeShardLocalPathId TSchemeShardLocalPathId::FromProto(const NKikimrProto::TPathID& proto) {
    return TSchemeShardLocalPathId(proto.GetLocalId());
}
template<>
void TSchemeShardLocalPathId::ToProto(NKikimrProto::TPathID& proto) const {
    proto.SetLocalId(PathId);
}
} //namespace NKikimr::NColumnShard

template<>
void Out<NKikimr::NColumnShard::TInternalPathId>(IOutputStream& s, const NKikimr::NColumnShard::TInternalPathId& v) {
    s << v.GetRawValue();
}

template<>
void Out<NKikimr::NColumnShard::TSchemeShardLocalPathId>(IOutputStream& s, const NKikimr::NColumnShard::TSchemeShardLocalPathId& v) {
    s << v.GetRawValue();
}

template<>
void Out<NKikimr::NColumnShard::TUnifiedPathId>(IOutputStream& s, const NKikimr::NColumnShard::TUnifiedPathId& v) {
    s << "{internal: " << v.InternalPathId << ", ss: " << v.SchemeShardLocalPathId << "}";
}
