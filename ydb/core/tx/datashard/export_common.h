#pragma once

#include "datashard_user_table.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#if defined EXPORT_LOG_T || \
    defined EXPORT_LOG_D || \
    defined EXPORT_LOG_I || \
    defined EXPORT_LOG_N || \
    defined EXPORT_LOG_W || \
    defined EXPORT_LOG_E || \
    defined EXPORT_LOG_C
#error log macro redefinition
#endif

#define EXPORT_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
#define EXPORT_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)

namespace NKikimr {
namespace NDataShard {

TMaybe<Ydb::Table::CreateTableRequest> GenYdbScheme(
    const TMap<ui32, TUserTable::TUserColumn>& columns,
    const NKikimrSchemeOp::TPathDescription& pathDesc);

TMaybe<Ydb::Scheme::ModifyPermissionsRequest> GenYdbPermissions(
    const NKikimrSchemeOp::TPathDescription& pathDesc);

TString DecimalToString(const std::pair<ui64, i64>& loHi);
TString DyNumberToString(TStringBuf data);
bool DecimalToStream(const std::pair<ui64, i64>& loHi, IOutputStream& out, TString& err);
bool DyNumberToStream(TStringBuf data, IOutputStream& out, TString& err);
bool PgToStream(TStringBuf data, void* typeDesc, IOutputStream& out, TString& err);
bool UuidToStream(const std::pair<ui64, ui64>& loHi, IOutputStream& out, TString& err);

} // NDataShard
} // NKikimr
