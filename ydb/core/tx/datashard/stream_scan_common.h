#pragma once

#include <ydb/core/tablet_flat/flat_update_op.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>

#include <util/generic/vector.h>

namespace NKikimrTxDataShard {

class TEvCdcStreamScanRequest_TLimits;

} // namespace NKikimrTxDataShard

namespace NKikimr::NDataShard::NStreamScan {

TVector<TRawTypeValue> MakeKey(TArrayRef<const TCell> cells, const TVector<NScheme::TTypeInfo>& keyColumnTypes);


} // namespace NKikimr::NDataShard::NStreamScan
