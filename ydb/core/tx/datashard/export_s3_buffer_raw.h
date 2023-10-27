#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_s3_buffer.h"

#include <util/generic/buffer.h>

namespace NKikimr {
namespace NDataShard {

class TS3BufferRaw: public NExportScan::IBuffer {
    using TTagToColumn = IExport::TTableColumns;
    using TTagToIndex = THashMap<ui32, ui32>; // index in IScan::TRow

public:
    explicit TS3BufferRaw(const TTagToColumn& columns, ui64 rowsLimit, ui64 bytesLimit);

    void ColumnsOrder(const TVector<ui32>& tags) override;
    bool Collect(const NTable::IScan::TRow& row) override;
    IEventBase* PrepareEvent(bool last, NExportScan::IBuffer::TStats& stats) override;
    void Clear() override;
    bool IsFilled() const override;
    TString GetError() const override;

protected:
    inline ui64 GetRowsLimit() const { return RowsLimit; }
    inline ui64 GetBytesLimit() const { return BytesLimit; }

    bool Collect(const NTable::IScan::TRow& row, IOutputStream& out);
    virtual TMaybe<TBuffer> Flush(bool prepare);

private:
    const TTagToColumn Columns;
    const ui64 RowsLimit;
    const ui64 BytesLimit;

    TTagToIndex Indices;

protected:
    ui64 Rows;
    ui64 BytesRead;
    TBuffer Buffer;

    TString ErrorString;
}; // TS3BufferRaw

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
