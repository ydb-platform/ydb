#pragma once

#ifndef KIKIMR_DISABLE_S3_OPS

#include "backup_restore_traits.h"
#include "import_common.h"
#include "import_data_parser.h"

#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/protos/datashard_backup.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <expected>
#include <functional>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/memory/pool.h>

namespace NKikimr::NDataShard {

// A physical half-open byte range in the source object.
//
// Ranges are also the correlation token between NextRange() and PutRange():
// the external-storage wrapper preserves the requested interval in its reply,
// while actor event cookies are not preserved by that wrapper.
struct TImportRange {
    ui64 Offset = 0;
    ui64 Length = 0;

    ui64 End() const {
        return Offset + Length;
    }

    bool operator==(const TImportRange& other) const {
        return Offset == other.Offset && Length == other.Length;
    }

    bool operator!=(const TImportRange& other) const {
        return !(*this == other);
    }
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
        // Deltas to add to the durable progress counters. For sequential
        // transforms these may include rows emitted by earlier batches that
        // could not yet advance to a restartable input boundary.
        ui64 DataBytes = 0;
        ui64 Rows = 0;
        NKikimrBackup::TS3DownloadState DownloadStateAfter;
    };

    struct TDataResult {
        EDataStatus Status = EDataStatus::NeedInput;
        TDataBatch Batch;
    };

    virtual ~IImportS3Engine() = default;

    // Reserves and returns the next physical source range. Until PutRange()
    // supplies that exact range, another call may return Blocked.
    virtual std::expected<TNextRangeResult, TString> NextRange() = 0;

    // Supplies a completed range. Implementations validate that it was
    // reserved and that the response length is exact.
    virtual std::expected<void, TString> PutRange(const TImportRange& range, TString data) = 0;

    // Releases a reservation after the transport gives up on this attempt, so
    // a retry can request the same range without discarding parser state.
    virtual std::expected<void, TString> FailRange(const TImportRange& range) = 0;

    // Produces at most one bounded logical batch. TCell values passed to
    // addRow are borrowed and are valid only for the duration of this call;
    // the sink must serialize or copy them synchronously.
    virtual std::expected<TDataResult, TString> GetData(
        TMemoryPool& pool,
        const TAddRowFn& addRow,
        const TAddChecksumChunkFn& addChecksumChunk) = 0;

    // Releases the prepared batch after the output sink has accepted ownership
    // of its rows. For UploadRows this happens after the rows and checkpoint are
    // durable; the direct-part sink owns them in its writer until final attach.
    // Engines do not produce a second batch while one is uncommitted.
    virtual std::expected<void, TString> Commit(ui64 batchId) = 0;

    virtual std::expected<void, TString> RestoreFromState(
        ui64 processedBytes,
        const NKikimrBackup::TS3DownloadState& state) = 0;

    virtual ui64 PendingBytes() const = 0;

    // True when this live engine has made progress that is newer than the
    // durable checkpoint. A transport retry must preserve the matching
    // in-memory checksum state even when no source bytes are currently held.
    virtual bool HasLiveState() const = 0;

    // Direct-part import is a CSV-only sequential sink. The coordinator uses
    // this capability instead of branching on the concrete data format.
    virtual bool SupportsDirectPartImport() const = 0;
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
