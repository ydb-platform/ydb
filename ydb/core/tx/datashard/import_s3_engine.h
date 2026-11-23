#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "backup_restore_traits.h"
#include "import_common.h"
#include "import_data_parser.h"

#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/protos/datashard_backup.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <compare>
#include <expected>
#include <functional>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/memory/pool.h>

namespace NKikimr::NDataShard {

// A half-open byte range of the source object. It also correlates NextRange() with
// PutRange(): the storage wrapper keeps the requested interval in its reply, not the cookie.
struct TImportRange {
    ui64 Offset = 0;
    ui64 Length = 0;

    ui64 End() const {
        return Offset + Length;
    }

    auto operator<=>(const TImportRange&) const = default;
};

class IImportS3Engine {
public:
    using TPtr = THolder<IImportS3Engine>;
    using TAddRowFn = IDataParser::TAddRowFn;
    using TAddChecksumChunkFn = std::function<void(TStringBuf)>;

    enum class ENextRangeStatus {
        Ready,
        Blocked,
        Exhausted,
    };

    struct TNextRangeResult {
        ENextRangeStatus Status = ENextRangeStatus::Blocked;
        TImportRange Range;
    };

    enum class EDataStatus {
        Ready,
        NeedInput,
        WaitingForCommit,
        Finished,
    };

    struct TDataBatch {
        ui64 Id = 0;
        ui64 ProcessedBytesAfter = 0;
        // Deltas to the durable progress counters; may include rows of earlier batches that
        // waited for a restartable boundary.
        ui64 DataBytes = 0;
        ui64 Rows = 0;
        // True when the commit moves the durable resume position; the checksum state is
        // snapshotted only then, so it always matches the position.
        bool Checkpoint = false;
        NKikimrBackup::TS3DownloadState DownloadStateAfter;
    };

    struct TDataResult {
        EDataStatus Status = EDataStatus::NeedInput;
        TDataBatch Batch;
    };

    virtual ~IImportS3Engine() = default;

    // Reserves the next source range; another call may return Blocked until PutRange()
    // supplies it.
    virtual std::expected<TNextRangeResult, TString> NextRange() = 0;

    // Supplies a completed range: the reserved one, with the exact length.
    virtual std::expected<void, TString> PutRange(const TImportRange& range, TString data) = 0;

    // Releases a reservation the transport gave up on, so a retry can request the range again.
    virtual std::expected<void, TString> FailRange(const TImportRange& range) = 0;

    // Produces at most one batch. The cells passed to addRow are borrowed: the sink must
    // consume them within the call.
    virtual std::expected<TDataResult, TString> GetData(
        TMemoryPool& pool,
        const TAddRowFn& addRow,
        const TAddChecksumChunkFn& addChecksumChunk) = 0;

    // Releases the batch once the sink owns its rows: after they are durable for UploadRows,
    // in the writer for direct part. No second batch while one is uncommitted.
    virtual std::expected<void, TString> Commit(ui64 batchId) = 0;

    virtual std::expected<void, TString> RestoreFromState(
        ui64 processedBytes,
        const NKikimrBackup::TS3DownloadState& state) = 0;

    virtual ui64 PendingBytes() const = 0;

    // True when the engine is ahead of the durable checkpoint: a retry must keep its
    // checksum state.
    virtual bool HasLiveState() const = 0;
};

struct TImportS3EngineSettings {
    NBackupRestoreTraits::EDataFormat DataFormat = NBackupRestoreTraits::EDataFormat::Invalid;
    NBackupRestoreTraits::ECompressionCodec CompressionCodec = NBackupRestoreTraits::ECompressionCodec::Invalid;
    ui64 ContentLength = 0;
    ui32 ReadBatchSize = 0;
    ui64 BufferSizeLimit = 0;
    ui64 ZstdBlockSize = 0;
    bool ValidateChecksum = false;
    TMaybe<NBackup::TEncryptionKey> EncryptionKey;
    TMaybe<NBackup::TEncryptionIV> EncryptionIV;
};

std::expected<IImportS3Engine::TPtr, TString> CreateImportS3Engine(
    const TImportS3EngineSettings& settings,
    const TTableInfo& tableInfo,
    const NKikimrSchemeOp::TTableDescription& scheme);

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
