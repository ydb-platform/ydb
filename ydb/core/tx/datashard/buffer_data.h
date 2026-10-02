#pragma once

#include "scan_common.h"
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

namespace NKikimr::NDataShard {

// TODO: move to tx/datashard/build_index/
class TBufferData: public TNonCopyable {
public:
    TBufferData()
        : Rows{std::make_shared<NTxProxy::TUploadRows>()} {
    }

    std::shared_ptr<NTxProxy::TUploadRows> GetRowsData() const {
        return Rows;
    }

    ui64 GetRows() const {
        return Rows->size();
    }

    bool IsEmpty() const {
        return Rows->empty();
    }

    ui64 GetBufferBytes() const {
        return BufferBytes;
    }

    ui64 GetRowCellBytes() const {
        return RowCellBytes;
    }

    void FlushTo(TBufferData& other) {
        Y_ENSURE(this != &other);
        Y_ENSURE(other.IsEmpty());
        other.Rows.swap(Rows);
        other.BufferBytes = std::exchange(BufferBytes, 0);
        other.RowCellBytes = std::exchange(RowCellBytes, 0);
        other.LastKey = std::exchange(LastKey, {});
    }

    void Clear() {
        Rows->clear();
        BufferBytes = 0;
        RowCellBytes = 0;
        LastKey = {};
    }

    void AddRow(TConstArrayRef<TCell> rowKey, TConstArrayRef<TCell> rowValue, TConstArrayRef<TCell> originalKey = {}) {
        AddRow(rowKey, rowValue, TSerializedCellVec::Serialize(rowValue), originalKey);
    }

    void AddRow(TConstArrayRef<TCell> rowKey, TConstArrayRef<TCell> rowValue, TString&& rowValueSerialized,  TConstArrayRef<TCell> originalKey = {}) {
        RowCellBytes += CountRowCellBytes(rowKey, rowValue);
        Rows->emplace_back(TSerializedCellVec{rowKey}, std::move(rowValueSerialized));
        BufferBytes += Rows->back().first.GetBuffer().size() + Rows->back().second.size();
        LastKey = TSerializedCellVec{originalKey};
    }

    bool HasReachedLimits(const TIndexBuildScanSettings& scanSettings) const {
        return Rows->size() > scanSettings.GetMaxBatchRows() || BufferBytes > scanSettings.GetMaxBatchBytes();
    }

    auto&& ExtractLastKey() {
        return std::move(LastKey);
    }

    const auto& GetLastKey() const {
        return LastKey;
    }

private:
    std::shared_ptr<NTxProxy::TUploadRows> Rows;
    ui64 BufferBytes = 0;
    ui64 RowCellBytes = 0;
    TSerializedCellVec LastKey;
};

}
