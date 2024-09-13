#pragma once

#include <ydb/core/tablet_flat/flat_update_op.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>

#include <util/generic/vector.h>

namespace NKikimrTxDataShard {

class TEvCdcStreamScanRequest_TLimits;

} // namespace NKikimrTxDataShard

namespace NKikimr::NDataShard::NStreamScan {

TVector<TRawTypeValue> MakeKey(TArrayRef<const TCell> cells, TUserTable::TCPtr table);

struct TLimits {
    ui32 BatchMaxBytes;
    ui32 BatchMinRows;
    ui32 BatchMaxRows;

    TLimits(const NKikimrTxDataShard::TEvCdcStreamScanRequest_TLimits& proto);
};

class TBuffer {
public:
    void AddRow(TArrayRef<const TCell> key, TArrayRef<const TCell> value) {
        const auto& [k, v] = Data.emplace_back(
            TSerializedCellVec(key),
            TSerializedCellVec(value)
        );
        ByteSize += k.GetBuffer().size() + v.GetBuffer().size();
    }

    auto&& Flush() {
        ByteSize = 0;
        return std::move(Data);
    }

    ui64 Bytes() const {
        return ByteSize;
    }

    ui64 Rows() const {
        return Data.size();
    }

    explicit operator bool() const {
        return !Data.empty();
    }

private:
    TVector<std::pair<TSerializedCellVec, TSerializedCellVec>> Data; // key & value (if any)
    ui64 ByteSize = 0;
};

struct TChange {
    ui64 Order;
    ui64 Group;
    ui64 Step;
    ui64 TxId;
    TPathId PathId;
    ui64 BodySize;
    TPathId TableId;
    ui64 SchemaVersion;
    ui64 LockId = 0;
    ui64 LockOffset = 0;

    TInstant CreatedAt() const {
        return Group
            ? TInstant::MicroSeconds(Group)
            : TInstant::MilliSeconds(Step);
    }
};

} // namespace NKikimr::NDataShard::NStreamScan
