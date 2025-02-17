#include "stream_scan_common.h"

#include <ydb/core/protos/tx_datashard.pb.h>

namespace NKikimr::NDataShard::NStreamScan {

using namespace NTable;

TVector<TRawTypeValue> MakeKey(TArrayRef<const TCell> cells, const TVector<NScheme::TTypeInfo>& keyColumnTypes) {
    TVector<TRawTypeValue> key(Reserve(cells.size()));

    Y_ABORT_UNLESS(cells.size() == keyColumnTypes.size());
    for (TPos pos = 0; pos < cells.size(); ++pos) {
        key.emplace_back(cells.at(pos).AsRef(), keyColumnTypes.at(pos).GetTypeId());
    }

    return key;
}

} // namespace NKikimr::NDataShard::NStreamScan
