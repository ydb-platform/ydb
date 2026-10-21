#pragma once

#include <util/system/types.h>

#include <functional>
#include <mutex>
#include <utility>

namespace NYdb::NConsoleClient::NPrivate {

// Reports source-file bytes, before any clamping or throttling by the UI.
class TParquetImportProgress {
public:
    using TReadCallback = std::function<void(ui64, ui64)>;
    using TConfirmCallback = std::function<void(ui64)>;

    TParquetImportProgress(ui64 totalRows, ui64 fileSize,
                           TReadCallback readCallback, TConfirmCallback confirmCallback)
        : TotalRows(totalRows)
        , FileSize(fileSize)
        , ReadCallback(std::move(readCallback))
        , ConfirmCallback(std::move(confirmCallback))
    {}

    // Called only by the reader thread; reports cumulative buffered bytes.
    void OnRead(ui64 rows) {
        ReadRows += rows;
        if (ReadCallback) {
            ReadCallback(RowsToBytes(ReadRows), FileSize);
        }
    }

    // Batches may finish concurrently and out of order. Serialize both the
    // counters and the callback, which receives only newly confirmed bytes.
    void OnConfirm(ui64 rows) {
        std::lock_guard<std::mutex> lock(ConfirmedProgressLock);
        ConfirmedRows += rows;
        const ui64 currentBytes = RowsToBytes(ConfirmedRows);
        if (ConfirmCallback) {
            ConfirmCallback(currentBytes - ConfirmedBytes);
        }
        ConfirmedBytes = currentBytes;
    }

private:
    ui64 RowsToBytes(ui64 rows) const {
        // Estimate source-file bytes, including compression, from row counts.
        // Round cumulative progress so that no bytes are lost between batches.
        // Empty files still contain metadata.
        if (TotalRows == 0 || rows == TotalRows) {
            return FileSize;
        }
        return static_cast<ui64>(static_cast<long double>(FileSize) * rows / TotalRows);
    }

    const ui64 TotalRows;
    const ui64 FileSize;
    const TReadCallback ReadCallback;
    const TConfirmCallback ConfirmCallback;
    ui64 ReadRows = 0;
    ui64 ConfirmedRows = 0;
    ui64 ConfirmedBytes = 0;
    std::mutex ConfirmedProgressLock;
};

} // namespace NYdb::NConsoleClient::NPrivate
