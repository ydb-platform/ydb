#pragma once

#include <ydb/core/tablet_flat/flat_update_op.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>

#include <util/generic/vector.h>

namespace NKikimrTxDataShard {

class TEvCdcStreamScanRequest_TLimits;

} // namespace NKikimrTxDataShard

namespace NKikimr::NDataShard::NStreamScan {

TVector<TRawTypeValue> MakeKey(TArrayRef<const TCell> cells, const TVector<NScheme::TTypeInfo>& keyColumnTypes);

struct TLimits {
    ui32 BatchMaxBytes;
    ui32 BatchMinRows;
    ui32 BatchMaxRows;

    TLimits(const NKikimrTxDataShard::TEvCdcStreamScanRequest_TLimits& proto);
    TLimits() = default;
};

class TBuffer {
public:
    inline void AddRow(TArrayRef<const TCell> key, TArrayRef<const TCell> value) {
        const auto& [k, v] = Data.emplace_back(
            TSerializedCellVec(key),
            TSerializedCellVec(value)
        );
        ByteSize += k.GetBuffer().size() + v.GetBuffer().size();
    }

    inline auto&& Flush() {
        ByteSize = 0;
        return std::move(Data);
    }

    inline ui64 Bytes() const {
        return ByteSize;
    }

    inline ui64 Rows() const {
        return Data.size();
    }

    inline explicit operator bool() const {
        return !Data.empty();
    }

private:
    TVector<std::pair<TSerializedCellVec, TSerializedCellVec>> Data; // key & value (if any)
    ui64 ByteSize = 0;
};

} // namespace NKikimr::NDataShard::NStreamScan
