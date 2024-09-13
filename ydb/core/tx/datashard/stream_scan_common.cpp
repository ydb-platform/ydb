#include "stream_scan_common.h"

#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimr::NDataShard::NStreamScan {

using namespace NTable;

TLimits::TLimits(const NKikimrTxDataShard::TEvCdcStreamScanRequest_TLimits& proto)
    : BatchMaxBytes(proto.GetBatchMaxBytes())
    , BatchMinRows(proto.GetBatchMinRows())
    , BatchMaxRows(proto.GetBatchMaxRows())
{
}

TVector<TRawTypeValue> MakeKey(TArrayRef<const TCell> cells, TUserTable::TCPtr table) {
    TVector<TRawTypeValue> key(Reserve(cells.size()));

    Y_ABORT_UNLESS(cells.size() == table->KeyColumnTypes.size());
    for (TPos pos = 0; pos < cells.size(); ++pos) {
        key.emplace_back(cells.at(pos).AsRef(), table->KeyColumnTypes.at(pos));
    }

    return key;
}

} // namespace NKikimr::NDataShard::NStreamScan
