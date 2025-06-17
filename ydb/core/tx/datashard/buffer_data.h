#pragma once

#include "ydb/core/scheme/scheme_tablecell.h"
#include "ydb/core/tx/datashard/upload_stats.h"
#include "ydb/core/tx/tx_proxy/upload_rows.h"

namespace NKikimr::NDataShard {

class TBufferData: public IStatHolder, public TNonCopyable {
public:
    TBufferData()
        : Rows{std::make_shared<NTxProxy::TUploadRows>()} {
    }

    ui64 GetRows() const override final {
        return Rows->size();
    }

    std::shared_ptr<NTxProxy::TUploadRows> GetRowsData() const {
        return Rows;
    }

    ui64 GetBytes() const override final {
        return ByteSize;
    }

    void FlushTo(TBufferData& other) {
        Y_ENSURE(this != &other);
        Y_ENSURE(other.IsEmpty());
        other.Rows.swap(Rows);
        other.ByteSize = std::exchange(ByteSize, 0);
        other.LastKey = std::exchange(LastKey, {});
    }

    void Clear() {
        Rows->clear();
        ByteSize = 0;
        LastKey = {};
    }

    void AddRow(TConstArrayRef<TCell> rowKey, TConstArrayRef<TCell> rowValue, TConstArrayRef<TCell> originalKey = {}) {
        AddRow(rowKey, rowValue, TSerializedCellVec::Serialize(rowValue), originalKey);
    }

    void AddRow(TConstArrayRef<TCell> rowKey, TConstArrayRef<TCell> rowValue, TString&& rowValueSerialized,  TConstArrayRef<TCell> originalKey = {}) {
        Y_UNUSED(rowValue);

        Rows->emplace_back(TSerializedCellVec{rowKey}, std::move(rowValueSerialized));
        ByteSize += Rows->back().first.GetBuffer().size() + Rows->back().second.size();
        LastKey = TSerializedCellVec{originalKey};
    }

    bool IsEmpty() const {
        return Rows->empty();
    }

    size_t Size() const {
        return Rows->size();
    }

    bool HasReachedLimits(size_t rowsLimit, ui64 bytesLimit) const {
        return Rows->size() > rowsLimit || ByteSize > bytesLimit;
    }

    auto&& ExtractLastKey() {
        return std::move(LastKey);
    }

    const auto& GetLastKey() const {
        return LastKey;
    }

private:
    std::shared_ptr<NTxProxy::TUploadRows> Rows;
    ui64 ByteSize = 0;
    TSerializedCellVec LastKey;
};

}
