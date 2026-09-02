#ifndef KIKIMR_DISABLE_S3_OPS

#include "import_s3_engine.h"

#include "import_parquet_s3_file.h"

#include <contrib/libs/zstd/include/zstd.h>

#include <algorithm>
#include <exception>
#include <utility>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {

namespace {

using namespace NBackupRestoreTraits;

struct TDestroyZstdContext {
    static void Destroy(::ZSTD_DCtx* context) noexcept {
        ZSTD_freeDCtx(context);
    }
};

constexpr ui64 SumWithSaturation(ui64 lhs, ui64 rhs) {
    return Max<ui64>() - lhs < rhs ? Max<ui64>() : lhs + rhs;
}

TString FormatRange(const TImportRange& range) {
    return TStringBuilder() << "[" << range.Offset << ", " << range.End() << ")";
}

class ISequentialReadController {
public:
    enum class EDataStatus {
        Ready,
        NeedInput,
    };

    struct TDataResult {
        EDataStatus Status = EDataStatus::NeedInput;
        TStringBuf Data;
    };

    virtual ~ISequentialReadController() = default;

    virtual void Feed(TString data, bool last) = 0;
    virtual std::expected<TDataResult, TString> TryGetData() = 0;
    virtual NKikimrBackup::TS3DownloadState Confirm() = 0;
    virtual ui64 PendingBytes() const = 0;
    virtual ui64 ReadyBytes() const = 0;
    virtual std::expected<void, TString> RestoreFromState(
        const NKikimrBackup::TS3DownloadState& state) = 0;
};

class TSequentialReadController : public ISequentialReadController {
public:
    TSequentialReadController(ui32 rangeSize, ui64 bufferSizeLimit)
        : RangeSize(rangeSize)
        , BufferSizeLimit(bufferSizeLimit)
    {
        Buffer.Reserve(RangeSize);
    }

    std::expected<void, TString> CanRequestNextRange(size_t size) const {
        if (size >= BufferSizeLimit || RangeSize >= BufferSizeLimit - size) {
            return std::unexpected("reached buffer size limit");
        }

        return {};
    }

protected:
    std::expected<void, TString> CanIncreaseBuffer(size_t size, size_t delta) const {
        if (size >= BufferSizeLimit || delta >= BufferSizeLimit - size) {
            return std::unexpected("reached buffer size limit");
        }

        return {};
    }

    TStringBuf AsStringBuf(size_t size) const {
        return TStringBuf(Buffer.Data(), size);
    }

protected:
    const ui32 RangeSize;
    const ui64 BufferSizeLimit;
    TBuffer Buffer;
};

class TRawReadController final : public TSequentialReadController {
public:
    using TSequentialReadController::TSequentialReadController;

    void Feed(TString data, bool /* last */) override {
        Buffer.Append(data.data(), data.size());
    }

    std::expected<TDataResult, TString> TryGetData() override {
        if (ReadyPos != 0) {
            return std::unexpected("raw reader has an unconfirmed block");
        }

        const ui64 pos = AsStringBuf(Buffer.Size()).rfind('\n');
        if (pos == TString::npos) {
            if (auto result = CanRequestNextRange(Buffer.Size()); !result) {
                return std::unexpected(std::move(result.error()));
            }

            return TDataResult{.Status = EDataStatus::NeedInput};
        }

        ReadyPos = pos + 1;
        return TDataResult{
            .Status = EDataStatus::Ready,
            .Data = AsStringBuf(ReadyPos),
        };
    }

    NKikimrBackup::TS3DownloadState Confirm() override {
        Buffer.ChopHead(ReadyPos);
        ReadyPos = 0;
        return {};
    }

    ui64 PendingBytes() const override {
        return Buffer.Size();
    }

    ui64 ReadyBytes() const override {
        return ReadyPos;
    }

    std::expected<void, TString> RestoreFromState(
        const NKikimrBackup::TS3DownloadState&) override
    {
        return {};
    }

private:
    ui64 ReadyPos = 0;
};

class TZstdReadController final : public TSequentialReadController {
public:
    TZstdReadController(ui32 rangeSize, ui64 bufferSizeLimit, ui64 zstdBlockSize)
        : TSequentialReadController(rangeSize, bufferSizeLimit)
        , Context(ZSTD_createDCtx())
        , ZstdBlockSize(zstdBlockSize ? zstdBlockSize : ZSTD_BLOCKSIZE_MAX)
    {
        ResetContext();
        Buffer.Reserve(ZstdBlockSize);
    }

    void Feed(TString data, bool /* last */) override {
        Y_ENSURE(Portion.Empty());
        Portion.Assign(data.data(), data.size());
    }

    std::expected<TDataResult, TString> TryGetData() override {
        if (ReadyInputBytes != 0 || ReadyOutputPos != 0) {
            return std::unexpected("zstd reader has an unconfirmed block");
        }

        auto input = ZSTD_inBuffer{Portion.Data(), Portion.Size(), 0};
        size_t decompressionResult = 0;
        while (!ReadyOutputPos) {
            PendingInputBytes -= input.pos;

            auto output = ZSTD_outBuffer{Buffer.Data(), Buffer.Capacity(), Buffer.Size()};
            decompressionResult = ZSTD_decompressStream(Context.Get(), &output, &input);

            if (ZSTD_isError(decompressionResult)) {
                return std::unexpected(TString(ZSTD_getErrorName(decompressionResult)));
            }

            PendingInputBytes += input.pos;
            Buffer.Proceed(output.pos);

            if (decompressionResult == 0) {
                if (Buffer.Size() > 0 && AsStringBuf(Buffer.Size()).back() != '\n') {
                    return std::unexpected("cannot find new line symbol");
                }

                ReadyInputBytes = PendingInputBytes;
                ReadyOutputPos = Buffer.Size();
                ResetContext();
            } else {
                const ui64 pos = AsStringBuf(Buffer.Size()).rfind('\n');
                if (pos != TString::npos) {
                    ReadyOutputPos = pos + 1;
                }
            }

            if (input.pos >= input.size) {
                break;
            }

            if (!ReadyOutputPos && output.pos == output.size) {
                if (auto result = CanIncreaseBuffer(Buffer.Size(), ZstdBlockSize)) {
                    Buffer.Reserve(Buffer.Size() + ZstdBlockSize);
                } else {
                    return std::unexpected(std::move(result.error()));
                }
            }
        }

        Portion.ChopHead(input.pos);

        if (!ReadyOutputPos && decompressionResult != 0) {
            if (auto result = CanRequestNextRange(Buffer.Size()); !result) {
                return std::unexpected(std::move(result.error()));
            }

            return TDataResult{.Status = EDataStatus::NeedInput};
        }

        return TDataResult{
            .Status = EDataStatus::Ready,
            .Data = ReadyOutputPos ? AsStringBuf(ReadyOutputPos) : TStringBuf(),
        };
    }

    NKikimrBackup::TS3DownloadState Confirm() override {
        Buffer.ChopHead(ReadyOutputPos);
        ReadyOutputPos = 0;

        PendingInputBytes -= ReadyInputBytes;
        ReadyInputBytes = 0;
        return {};
    }

    ui64 PendingBytes() const override {
        return PendingInputBytes;
    }

    ui64 ReadyBytes() const override {
        return ReadyInputBytes;
    }

    std::expected<void, TString> RestoreFromState(
        const NKikimrBackup::TS3DownloadState&) override
    {
        return {};
    }

private:
    void ResetContext() {
        ZSTD_DCtx_reset(Context.Get(), ZSTD_reset_session_only);
        ZSTD_DCtx_refDDict(Context.Get(), nullptr);
    }

private:
    THolder<::ZSTD_DCtx, TDestroyZstdContext> Context;
    const ui64 ZstdBlockSize;
    TBuffer Portion;
    ui64 PendingInputBytes = 0;
    ui64 ReadyInputBytes = 0;
    ui64 ReadyOutputPos = 0;
};

class TEncryptionReadController final : public ISequentialReadController {
public:
    TEncryptionReadController(
        NBackup::TEncryptionKey key,
        NBackup::TEncryptionIV expectedIV,
        THolder<ISequentialReadController> child)
        : Deserializer(std::move(key), std::move(expectedIV))
        , ConfirmedDeserializerState(Deserializer.GetState())
        , Child(std::move(child))
    {
    }

    void Feed(TString data, bool last) override {
        if (FeedError) {
            return;
        }

        if (!data.empty() || last) {
            NewData = true;
        }
        Last = last;

        try {
            Deserializer.AddData(TBuffer(data.data(), data.size()), last);
            FeedUnprocessedBytes += data.size();
        } catch (const std::exception& ex) {
            FeedError = ex.what();
        }
    }

    std::expected<TDataResult, TString> TryGetData() override {
        if (FeedError) {
            return std::unexpected(*FeedError);
        }

        if (BytesFedToChild) {
            auto result = TryGetDataFromChild();
            if (!result || result->Status != EDataStatus::NeedInput || !NewData) {
                return result;
            }
        }

        bool lastEmptyBlock = false;
        if (NewData) {
            try {
                NewData = false;
                const ui64 processedBefore = Deserializer.GetProcessedInputBytes();
                TMaybe<TBuffer> block = Deserializer.GetNextBlock();
                const ui64 processedAfter = Deserializer.GetProcessedInputBytes();
                if (processedAfter < processedBefore ||
                    processedAfter - processedBefore > FeedUnprocessedBytes) {
                    return std::unexpected("encrypted reader reported an invalid input position");
                }

                ReadyInputBytes += processedAfter - processedBefore;
                if (block) {
                    if (block->Size()) {
                        Child->Feed(TString(block->Data(), block->Size()), Last);
                        BytesFedToChild += block->Size();
                    } else {
                        lastEmptyBlock = Last;
                    }
                }
            } catch (const std::exception& ex) {
                return std::unexpected(TString(ex.what()));
            }
        }

        if (BytesFedToChild) {
            return TryGetDataFromChild();
        }
        if (lastEmptyBlock) {
            return TDataResult{.Status = EDataStatus::Ready};
        }

        return TDataResult{.Status = EDataStatus::NeedInput};
    }

    NKikimrBackup::TS3DownloadState Confirm() override {
        if (const ui64 readyBytes = ReadyBytes()) {
            ConfirmedDeserializerState = Deserializer.GetState();
            FeedUnprocessedBytes -= readyBytes;
            ReadyInputBytes = 0;
        }

        auto state = Child->Confirm();
        state.SetEncryptedDeserializerState(ConfirmedDeserializerState);
        return state;
    }

    ui64 PendingBytes() const override {
        return FeedUnprocessedBytes;
    }

    ui64 ReadyBytes() const override {
        return BytesFedToChild == 0 ? ReadyInputBytes : 0;
    }

    std::expected<void, TString> RestoreFromState(
        const NKikimrBackup::TS3DownloadState& state) override
    {
        if (const TString& deserializerState = state.GetEncryptedDeserializerState()) {
            try {
                Deserializer = NBackup::TEncryptedFileDeserializer::RestoreFromState(deserializerState);
                ConfirmedDeserializerState = deserializerState;
                FeedUnprocessedBytes = 0;
                ReadyInputBytes = 0;
                BytesFedToChild = 0;
                NewData = false;
                Last = false;
                FeedError.Clear();
            } catch (const std::exception& ex) {
                return std::unexpected(TString(ex.what()));
            }
        }

        return Child->RestoreFromState(state);
    }

private:
    std::expected<TDataResult, TString> TryGetDataFromChild() {
        auto result = Child->TryGetData();
        if (!result) {
            return result;
        }
        if (result->Status == EDataStatus::Ready) {
            if (const ui64 ready = Child->ReadyBytes()) {
                if (ready > BytesFedToChild) {
                    return std::unexpected("child reader consumed more decrypted bytes than were supplied");
                }
                BytesFedToChild -= ready;
            }
        }
        return result;
    }

private:
    bool Last = false;
    ui64 FeedUnprocessedBytes = 0;
    ui64 ReadyInputBytes = 0;
    bool NewData = false;
    ui64 BytesFedToChild = 0;
    TMaybe<TString> FeedError;
    NBackup::TEncryptedFileDeserializer Deserializer;
    TString ConfirmedDeserializerState;
    THolder<ISequentialReadController> Child;
};

class TCsvImportS3Engine final : public IImportS3Engine {
public:
    TCsvImportS3Engine(
        const TImportS3EngineSettings& settings,
        THolder<ISequentialReadController> reader,
        IDataParser::TPtr parser)
        : ContentLength(settings.ContentLength)
        , ReadBatchSize(settings.ReadBatchSize)
        , ValidateChecksum(settings.ValidateChecksum)
        , Encrypted(settings.EncryptionKey.Defined())
        , Reader(std::move(reader))
        , Parser(std::move(parser))
    {
    }

    std::expected<TNextRangeResult, TString> NextRange() override {
        if (FatalError) {
            return MakeRangeError(FatalError);
        }
        if (WaitingBatch || OutstandingRange || !NeedInput) {
            return TNextRangeResult{.Status = ENextRangeStatus::Blocked};
        }

        const ui64 start = SumWithSaturation(ProcessedBytes, Reader->PendingBytes());
        if (start >= ContentLength) {
            return TNextRangeResult{.Status = ENextRangeStatus::Exhausted};
        }

        const ui64 length = Min<ui64>(ReadBatchSize, ContentLength - start);
        if (length == 0) {
            return SetRangeError("cannot reserve an empty source range");
        }

        OutstandingRange = TImportRange{start, length};
        NeedInput = false;
        return TNextRangeResult{
            .Status = ENextRangeStatus::Ready,
            .Range = *OutstandingRange,
        };
    }

    std::expected<void, TString> PutRange(const TImportRange& range, TString data) override {
        if (auto result = ValidateOutstandingRange(range, data.size()); !result) {
            return result;
        }

        const bool last = range.End() == ContentLength;
        OutstandingRange.Clear();
        Reader->Feed(std::move(data), last);
        NeedInput = false;
        return {};
    }

    std::expected<void, TString> FailRange(const TImportRange& range) override {
        if (auto result = ValidateOutstandingRange(range, Nothing()); !result) {
            return result;
        }

        OutstandingRange.Clear();
        NeedInput = true;
        return {};
    }

    std::expected<TDataResult, TString> GetData(
        TMemoryPool& pool,
        const TAddRowFn& addRow,
        const TAddChecksumChunkFn& addChecksumChunk) override
    {
        if (FatalError) {
            return MakeDataError(FatalError);
        }
        if (WaitingBatch) {
            return TDataResult{
                .Status = EDataStatus::WaitingForCommit,
                .Batch = *WaitingBatch,
            };
        }
        if (OutstandingRange) {
            return TDataResult{.Status = EDataStatus::NeedInput};
        }
        if (ProcessedBytes == ContentLength && Reader->PendingBytes() == 0) {
            return TDataResult{.Status = EDataStatus::Finished};
        }

        auto readResult = Reader->TryGetData();
        if (!readResult) {
            return SetDataError(std::move(readResult.error()));
        }
        const TStringBuf data = readResult->Data;
        switch (readResult->Status) {
        case ISequentialReadController::EDataStatus::NeedInput:
            if (SumWithSaturation(ProcessedBytes, Reader->PendingBytes()) < ContentLength) {
                NeedInput = true;
                return TDataResult{.Status = EDataStatus::NeedInput};
            }
            return SetDataError("reached end of file");

        case ISequentialReadController::EDataStatus::Ready:
            break;
        }

        IDataParser::TParsedData parsedData;
        try {
            if (data && ValidateChecksum) {
                addChecksumChunk(data);
            }

            if (data) {
                auto parseResult = Parser->ParseBlock(data, pool, addRow);
                if (!parseResult) {
                    return SetDataError(std::move(parseResult.error()));
                }
                parsedData = *parseResult;
            }
        } catch (const std::exception& ex) {
            return SetDataError(ex.what());
        }

        const ui64 readyBytes = Reader->ReadyBytes();
        if (!readyBytes && !parsedData.Rows && !parsedData.DataBytes) {
            return SetDataError("CSV reader produced an empty batch without making progress");
        }
        if (readyBytes > ContentLength - ProcessedBytes) {
            return SetDataError("CSV reader advanced past the end of the source object");
        }

        TDataBatch batch;
        batch.Id = NextBatchId++;
        batch.ProcessedBytesAfter = ProcessedBytes + readyBytes;
        UncheckpointedDataBytes += parsedData.DataBytes;
        UncheckpointedRows += parsedData.Rows;
        if (readyBytes) {
            batch.DataBytes = std::exchange(UncheckpointedDataBytes, 0);
            batch.Rows = std::exchange(UncheckpointedRows, 0);
        }
        batch.DownloadStateAfter = Reader->Confirm();

        ProcessedBytes = batch.ProcessedBytesAfter;
        WaitingBatch = batch;
        NeedInput = false;
        return TDataResult{
            .Status = EDataStatus::Ready,
            .Batch = std::move(batch),
        };
    }

    std::expected<void, TString> Commit(ui64 batchId) override {
        if (!WaitingBatch) {
            return std::unexpected("there is no import batch waiting for commit");
        }
        if (WaitingBatch->Id != batchId) {
            return std::unexpected(TStringBuilder() << "unexpected import batch " << batchId
                << ", expected " << WaitingBatch->Id);
        }

        WaitingBatch.Clear();
        NeedInput = false;
        return {};
    }

    std::expected<void, TString> RestoreFromState(
        ui64 processedBytes,
        const NKikimrBackup::TS3DownloadState& state) override
    {
        if (processedBytes > ContentLength) {
            return std::unexpected(TStringBuilder() << "processed byte position " << processedBytes
                << " exceeds source size " << ContentLength);
        }
        if (Encrypted && processedBytes > 0 && state.GetEncryptedDeserializerState().empty()) {
            return std::unexpected("encrypted CSV checkpoint has no deserializer state");
        }
        if (!Encrypted && !state.GetEncryptedDeserializerState().empty()) {
            return std::unexpected("unencrypted CSV checkpoint contains encrypted deserializer state");
        }

        const bool started = HasLiveState();
        if (started) {
            // Restart() reloads the last durable DataShard checkpoint even when
            // this live engine is intentionally preserved after FailRange().
            // Direct-part import does not persist per-batch progress, so that
            // checkpoint may be older than the engine's local position.
            if (processedBytes <= ProcessedBytes) {
                return {};
            }

            return std::unexpected("cannot advance the checkpoint of a CSV engine after processing has started");
        }
        if (auto result = Reader->RestoreFromState(state); !result) {
            return result;
        }

        ProcessedBytes = processedBytes;
        NeedInput = processedBytes < ContentLength;
        return {};
    }

    ui64 PendingBytes() const override {
        return Reader->PendingBytes();
    }

    bool HasLiveState() const override {
        return OutstandingRange || WaitingBatch ||
            Reader->PendingBytes() != 0 || ProcessedBytes != 0;
    }

    bool SupportsDirectPartImport() const override {
        return true;
    }

private:
    std::expected<void, TString> ValidateOutstandingRange(
        const TImportRange& range,
        TMaybe<size_t> dataSize) const
    {
        if (!OutstandingRange) {
            return std::unexpected(TStringBuilder() << "range " << FormatRange(range)
                << " was not reserved");
        }
        if (*OutstandingRange != range) {
            return std::unexpected(TStringBuilder() << "unexpected range " << FormatRange(range)
                << ", expected " << FormatRange(*OutstandingRange));
        }
        if (dataSize && *dataSize != range.Length) {
            return std::unexpected(TStringBuilder() << "range " << FormatRange(range)
                << " returned " << *dataSize << " bytes, expected " << range.Length);
        }

        return {};
    }

    std::expected<TNextRangeResult, TString> MakeRangeError(const TString& error) const {
        return std::unexpected(error);
    }

    std::expected<TNextRangeResult, TString> SetRangeError(TString error) {
        FatalError = std::move(error);
        return MakeRangeError(FatalError);
    }

    std::expected<TDataResult, TString> MakeDataError(const TString& error) const {
        return std::unexpected(error);
    }

    std::expected<TDataResult, TString> SetDataError(TString error) {
        FatalError = std::move(error);
        return MakeDataError(FatalError);
    }

private:
    const ui64 ContentLength;
    const ui32 ReadBatchSize;
    const bool ValidateChecksum;
    const bool Encrypted;
    THolder<ISequentialReadController> Reader;
    IDataParser::TPtr Parser;
    ui64 ProcessedBytes = 0;
    ui64 UncheckpointedDataBytes = 0;
    ui64 UncheckpointedRows = 0;
    ui64 NextBatchId = 1;
    TMaybe<TImportRange> OutstandingRange;
    TMaybe<TDataBatch> WaitingBatch;
    bool NeedInput = true;
    TString FatalError;
};

class TParquetImportS3Engine final : public IImportS3Engine {
    enum class EPhase {
        FooterTail,
        FooterMetadata,
        SequentialData,
        RowGroupData,
        ParseRowGroup,
        EmitFinal,
        Finished,
        Error,
    };

public:
    TParquetImportS3Engine(
        const TImportS3EngineSettings& settings,
        IDataParser::TPtr parser)
        : ContentLength(settings.ContentLength)
        , ReadBatchSize(settings.ReadBatchSize)
        , BufferSizeLimit(settings.BufferSizeLimit)
        , ValidateChecksum(settings.ValidateChecksum)
        , Parser(std::move(parser))
    {
        ResetDownload();
    }

    std::expected<TNextRangeResult, TString> NextRange() override {
        if (Phase == EPhase::Error) {
            return MakeRangeError(FatalError);
        }
        if (WaitingBatch || OutstandingRange) {
            return TNextRangeResult{.Status = ENextRangeStatus::Blocked};
        }
        if (Phase == EPhase::ParseRowGroup || Phase == EPhase::EmitFinal || Phase == EPhase::Finished) {
            return TNextRangeResult{.Status = ENextRangeStatus::Exhausted};
        }

        if (Phase == EPhase::SequentialData) {
            if (!ChecksumChunk.empty()) {
                return TNextRangeResult{.Status = ENextRangeStatus::Blocked};
            }

            const ui64 target = SequentialTarget();
            if (ChecksumOffset >= target) {
                return SetRangeError("Parquet sequential checksum scan has no remaining source range");
            }

            const ui64 length = Min<ui64>(ReadBatchSize, target - ChecksumOffset);
            if (auto result = CanReserveMore(length); !result) {
                return SetRangeError(std::move(result.error()));
            }

            OutstandingRange = TImportRange{ChecksumOffset, length};
            return TNextRangeResult{
                .Status = ENextRangeStatus::Ready,
                .Range = *OutstandingRange,
            };
        }

        if (auto result = DrainLoadedRanges(); !result) {
            return SetRangeError(std::move(result.error()));
        }
        if (Phase == EPhase::ParseRowGroup || Phase == EPhase::EmitFinal || Phase == EPhase::Finished) {
            return TNextRangeResult{.Status = ENextRangeStatus::Exhausted};
        }
        if (FetchQueue.empty()) {
            return SetRangeError("Parquet range planner has no active range");
        }

        const auto& active = FetchQueue.front();
        if (active.Fetched >= active.Length) {
            return SetRangeError("Parquet range planner has an invalid active range");
        }
        const ui64 offset = active.Offset + active.Fetched;
        const ui64 length = Min<ui64>(active.Length - active.Fetched, ReadBatchSize);
        if (length == 0 || offset > ContentLength || length > ContentLength - offset) {
            return SetRangeError("Parquet range planner produced an invalid source range");
        }
        if (auto result = CanReserveMore(length); !result) {
            return SetRangeError(std::move(result.error()));
        }

        OutstandingRange = TImportRange{offset, length};
        return TNextRangeResult{
            .Status = ENextRangeStatus::Ready,
            .Range = *OutstandingRange,
        };
    }

    std::expected<void, TString> PutRange(const TImportRange& range, TString data) override {
        if (auto result = ValidateOutstandingRange(range, data.size()); !result) {
            return result;
        }

        if (Phase == EPhase::SequentialData) {
            if (range.Offset != ChecksumOffset) {
                return std::unexpected(TStringBuilder() << "unexpected Parquet sequential range " << FormatRange(range)
                    << ", expected offset " << ChecksumOffset);
            }
            if (!ChecksumChunk.empty()) {
                return std::unexpected("Parquet checksum chunk has not been consumed");
            }

            ChecksumChunk = std::move(data);
            OutstandingRange.Clear();
            return {};
        }

        if (!SparseFile) {
            return std::unexpected("Parquet range response has no sparse file");
        }
        if (FetchQueue.empty()) {
            return std::unexpected("Parquet range response has no active planned range");
        }

        auto& active = FetchQueue.front();
        const ui64 expectedOffset = active.Offset + active.Fetched;
        if (range.Offset != expectedOffset || range.Length > active.Length - active.Fetched) {
            return std::unexpected(TStringBuilder() << "range " << FormatRange(range)
                << " does not belong to active Parquet range [" << active.Offset
                << ", " << active.Offset + active.Length << ")");
        }

        SparseFile->PutRange(range.Offset, std::move(data));
        active.Fetched += range.Length;
        OutstandingRange.Clear();

        if (active.Fetched == active.Length) {
            if (auto result = AdvanceAfterRange(); !result) {
                Phase = EPhase::Error;
                FatalError = std::move(result.error());
                return std::unexpected(FatalError);
            }
        }

        return {};
    }

    std::expected<void, TString> FailRange(const TImportRange& range) override {
        if (auto result = ValidateOutstandingRange(range, Nothing()); !result) {
            return result;
        }

        OutstandingRange.Clear();
        return {};
    }

    std::expected<TDataResult, TString> GetData(
        TMemoryPool& pool,
        const TAddRowFn& addRow,
        const TAddChecksumChunkFn& addChecksumChunk) override
    {
        if (Phase == EPhase::Error) {
            return MakeDataError(FatalError);
        }
        if (WaitingBatch) {
            return TDataResult{
                .Status = EDataStatus::WaitingForCommit,
                .Batch = *WaitingBatch,
            };
        }
        if (Phase == EPhase::Finished) {
            return TDataResult{.Status = EDataStatus::Finished};
        }
        if (OutstandingRange) {
            return TDataResult{.Status = EDataStatus::NeedInput};
        }

        if (Phase == EPhase::SequentialData) {
            if (!addChecksumChunk) {
                return SetDataError("Parquet checksum validation has no checksum sink");
            }

            if (!ChecksumChunk.empty()) {
                try {
                    addChecksumChunk(ChecksumChunk);
                } catch (const std::exception& ex) {
                    return SetDataError(ex.what());
                }

                if (UseOnePassChecksum && !RowGroupRanges.empty()) {
                    RouteChecksumChunkToCurrentRowGroup(ChecksumOffset, ChecksumChunk);
                }
                ChecksumOffset += ChecksumChunk.size();
                ChecksumChunk.clear();
            }

            if (auto result = AdvanceSequentialChecksum(addChecksumChunk); !result) {
                return SetDataError(std::move(result.error()));
            }
            if (Phase == EPhase::SequentialData) {
                return TDataResult{.Status = EDataStatus::NeedInput};
            }
        }

        if (Phase == EPhase::EmitFinal) {
            TDataBatch batch;
            batch.Id = NextBatchId++;
            batch.ProcessedBytesAfter = ContentLength;

            WaitingBatch = batch;
            WaitingBatchIsFinal = true;
            return TDataResult{
                .Status = EDataStatus::Ready,
                .Batch = std::move(batch),
            };
        }

        if (Phase != EPhase::ParseRowGroup) {
            return TDataResult{.Status = EDataStatus::NeedInput};
        }

        auto* parquetParser = AsParquetStreamParser(Parser.Get());
        if (!parquetParser) {
            return SetDataError("Parquet engine has an incompatible data parser");
        }

        if (!RowGroupParserOpen) {
            if (auto result = parquetParser->OpenRowGroup(CurrentRowGroup); !result) {
                auto error = std::move(result.error());
                return SetDataError(error.empty()
                    ? TString("failed to open Parquet row group")
                    : std::move(error));
            }
            RowGroupParserOpen = true;
        }

        while (true) {
            IParquetStreamParser::TParsedBatch parsedBatch;
            try {
                auto result = parquetParser->ProcessNextBatch(pool, addRow);
                if (!result) {
                    return SetDataError(std::move(result.error()));
                }
                parsedBatch = *result;
            } catch (const std::exception& ex) {
                return SetDataError(ex.what());
            }

            if (!parsedBatch.Rows && parsedBatch.HasMore) {
                continue;
            }

            const bool rowGroupFinished = !parsedBatch.HasMore;
            const bool finalBatch = rowGroupFinished && CurrentRowGroup + 1 == RowGroupRanges.size();

            TDataBatch batch;
            batch.Id = NextBatchId++;
            batch.ProcessedBytesAfter = finalBatch ? ContentLength : 0;
            batch.DataBytes = parsedBatch.DataBytes;
            batch.Rows = parsedBatch.Rows;

            WaitingBatch = batch;
            WaitingBatchFinishesRowGroup = rowGroupFinished;
            WaitingBatchIsFinal = finalBatch;
            return TDataResult{
                .Status = EDataStatus::Ready,
                .Batch = std::move(batch),
            };
        }
    }

    std::expected<void, TString> Commit(ui64 batchId) override {
        if (!WaitingBatch) {
            return std::unexpected("there is no import batch waiting for commit");
        }
        if (WaitingBatch->Id != batchId) {
            return std::unexpected(TStringBuilder() << "unexpected import batch " << batchId
                << ", expected " << WaitingBatch->Id);
        }

        CheckpointProcessedBytes = WaitingBatch->ProcessedBytesAfter;
        const bool finishesRowGroup = WaitingBatchFinishesRowGroup;
        const bool finalBatch = WaitingBatchIsFinal;
        WaitingBatch.Clear();
        WaitingBatchFinishesRowGroup = false;
        WaitingBatchIsFinal = false;

        if (finalBatch) {
            if (auto* parquetParser = AsParquetStreamParser(Parser.Get())) {
                parquetParser->ResetFile();
            }
            SparseFile.reset();
            FetchQueue.clear();
            RowGroupRanges.clear();
            RowGroupParserOpen = false;
            Phase = EPhase::Finished;
        } else if (finishesRowGroup) {
            auto* parquetParser = AsParquetStreamParser(Parser.Get());
            if (!parquetParser) {
                Phase = EPhase::Error;
                FatalError = "Parquet engine has an incompatible data parser";
                return std::unexpected(FatalError);
            }

            parquetParser->ResetRowGroup();
            RowGroupParserOpen = false;
            if (UseOnePassChecksum) {
                SparseFile->ClearBefore(FooterSuffixStart);
            } else {
                SparseFile->Clear();
            }
            ++CurrentRowGroup;
            if (auto result = PlanCurrentRowGroup(); !result) {
                Phase = EPhase::Error;
                FatalError = std::move(result.error());
                return std::unexpected(FatalError);
            }
        }
        return {};
    }

    std::expected<void, TString> RestoreFromState(
        ui64 processedBytes,
        const NKikimrBackup::TS3DownloadState& state) override
    {
        if (!state.GetEncryptedDeserializerState().empty()) {
            return std::unexpected("Parquet checkpoint contains unsupported sequential encryption state");
        }
        if (processedBytes != 0 && processedBytes != ContentLength) {
            return std::unexpected(TStringBuilder() << "unsupported partial Parquet checkpoint at byte "
                << processedBytes << " of " << ContentLength);
        }

        const bool started = HasLiveState();
        if (started && processedBytes == CheckpointProcessedBytes) {
            return {};
        }
        if (started) {
            return std::unexpected("cannot replace the checkpoint of a Parquet engine after processing has started");
        }

        if (processedBytes == ContentLength) {
            if (auto* parquetParser = AsParquetStreamParser(Parser.Get())) {
                parquetParser->ResetFile();
            }
            SparseFile.reset();
            FetchQueue.clear();
            RowGroupRanges.clear();
            ChecksumChunk.clear();
            RowGroupParserOpen = false;
            ChecksumComplete = true;
            Phase = EPhase::Finished;
        } else {
            ResetDownload();
        }
        CheckpointProcessedBytes = processedBytes;
        return {};
    }

    ui64 PendingBytes() const override {
        return ChecksumChunk.size() + (SparseFile ? SparseFile->BufferedBytes() : 0);
    }

    bool HasLiveState() const override {
        return OutstandingRange || WaitingBatch || ChecksumOffset != 0 || !ChecksumChunk.empty()
            || (SparseFile && SparseFile->BufferedBytes() != 0) || Phase != EPhase::FooterTail;
    }

    bool SupportsDirectPartImport() const override {
        return false;
    }

private:
    void ResetDownload() {
        if (auto* parquetParser = AsParquetStreamParser(Parser.Get())) {
            parquetParser->ResetFile();
        }
        SparseFile.reset();
        FetchQueue.clear();
        RowGroupRanges.clear();
        OutstandingRange.Clear();
        WaitingBatch.Clear();
        ChecksumChunk.clear();
        ChecksumOffset = 0;
        FooterSuffixStart = 0;
        CurrentRowGroup = 0;
        RowGroupParserOpen = false;
        WaitingBatchFinishesRowGroup = false;
        WaitingBatchIsFinal = false;
        UseOnePassChecksum = false;
        ChecksumComplete = false;
        FatalError.clear();
        StartFooterDownload();
    }

    void StartFooterDownload() {
        SparseFile = std::make_shared<TParquetSparseFile>(ContentLength);
        FetchQueue.clear();
        const auto tailRange = TParquetSparseFile::FooterTailRange(ContentLength);
        FooterSuffixStart = tailRange.Offset;
        FetchQueue.push_back(tailRange);
        Phase = EPhase::FooterTail;
    }

    std::expected<void, TString> ValidateOutstandingRange(
        const TImportRange& range,
        TMaybe<size_t> dataSize) const
    {
        if (!OutstandingRange) {
            return std::unexpected(TStringBuilder() << "range " << FormatRange(range)
                << " was not reserved");
        }
        if (*OutstandingRange != range) {
            return std::unexpected(TStringBuilder() << "unexpected range " << FormatRange(range)
                << ", expected " << FormatRange(*OutstandingRange));
        }
        if (dataSize && *dataSize != range.Length) {
            return std::unexpected(TStringBuilder() << "range " << FormatRange(range)
                << " returned " << *dataSize << " bytes, expected " << range.Length);
        }

        return {};
    }

    std::expected<void, TString> CanReserveMore(ui64 length) const {
        const ui64 buffered = PendingBytes();
        if (buffered >= BufferSizeLimit || length >= BufferSizeLimit - buffered) {
            TString error = TStringBuilder() << "reached buffer size limit"
                << ": buffered=" << buffered
                << ", requested=" << length
                << ", limit=" << BufferSizeLimit;
            if (Phase == EPhase::RowGroupData || Phase == EPhase::ParseRowGroup ||
                (Phase == EPhase::SequentialData && UseOnePassChecksum && !RowGroupRanges.empty())) {
                error += TStringBuilder() << ", rowGroup=" << CurrentRowGroup;
            }
            return std::unexpected(std::move(error));
        }
        return {};
    }

    std::expected<void, TString> DrainLoadedRanges() {
        while (!FetchQueue.empty() && SparseFile) {
            auto& active = FetchQueue.front();
            const ui64 offset = active.Offset + active.Fetched;
            const ui64 remaining = active.Length - active.Fetched;
            if (remaining != 0 && !SparseFile->HasBytes(offset, remaining)) {
                break;
            }

            active.Fetched = active.Length;
            if (auto result = AdvanceAfterRange(); !result) {
                return result;
            }
            if (Phase == EPhase::ParseRowGroup || Phase == EPhase::EmitFinal || Phase == EPhase::Finished) {
                break;
            }
        }
        return {};
    }

    std::expected<void, TString> AdvanceAfterRange() {
        if (FetchQueue.empty()) {
            return std::unexpected("Parquet range planner has no completed range");
        }
        FetchQueue.erase(FetchQueue.begin());

        if (Phase == EPhase::FooterTail) {
            auto metadataRange = SparseFile->TryParseFooterMetadataRange();
            if (!metadataRange) {
                return std::unexpected(std::move(metadataRange.error()));
            }

            if (*metadataRange) {
                FooterSuffixStart = (**metadataRange).Offset;
                FetchQueue.push_back(**metadataRange);
                Phase = EPhase::FooterMetadata;
            } else if (auto result = InitializeRowGroups(); !result) {
                return result;
            }
        } else if (Phase == EPhase::FooterMetadata) {
            auto metadataRange = SparseFile->TryParseFooterMetadataRange();
            if (!metadataRange) {
                return std::unexpected(std::move(metadataRange.error()));
            }
            if (*metadataRange) {
                return std::unexpected("Parquet footer metadata is still incomplete after fetching its planned range");
            }
            if (auto result = InitializeRowGroups(); !result) {
                return result;
            }
        } else if (Phase == EPhase::RowGroupData && FetchQueue.empty()) {
            Phase = EPhase::ParseRowGroup;
        }

        return {};
    }

    std::expected<void, TString> InitializeRowGroups() {
        auto* parquetParser = AsParquetStreamParser(Parser.Get());
        if (!parquetParser) {
            return std::unexpected("Parquet engine has an incompatible data parser");
        }
        if (auto result = parquetParser->OpenMetadata(SparseFile->MakeRandomAccessFile(SparseFile)); !result) {
            return result;
        }
        auto ranges = SparseFile->PlanColumnChunkRangesByRowGroup(SparseFile);
        if (!ranges) {
            parquetParser->ResetFile();
            return std::unexpected(std::move(ranges.error()));
        }
        RowGroupRanges = std::move(*ranges);

        CurrentRowGroup = 0;
        RowGroupParserOpen = false;

        if (ValidateChecksum) {
            auto onePass = CanUseOnePassChecksum();
            if (!onePass) {
                parquetParser->ResetFile();
                return std::unexpected(std::move(onePass.error()));
            }
            UseOnePassChecksum = *onePass;
        }

        if (!UseOnePassChecksum) {
            // Metadata is decoded by Arrow. The normal path and the compatibility
            // fallback can now evict the footer and fetch sparse row-group data.
            SparseFile->Clear();
        }

        return PlanCurrentRowGroup();
    }

    std::expected<void, TString> PlanCurrentRowGroup() {
        FetchQueue.clear();

        if (RowGroupRanges.empty()) {
            Phase = ValidateChecksum && !ChecksumComplete
                ? EPhase::SequentialData
                : EPhase::EmitFinal;
            return {};
        }

        if (CurrentRowGroup >= RowGroupRanges.size()) {
            return std::unexpected("Parquet row-group planner advanced past the last row group");
        }

        if (UseOnePassChecksum) {
            const bool finalRowGroup = CurrentRowGroup + 1 == RowGroupRanges.size();
            if (!ChecksumComplete && (finalRowGroup || ChecksumOffset < CurrentRowGroupPrefixEnd())) {
                Phase = EPhase::SequentialData;
                return {};
            }
            if (!CurrentRowGroupIsLoaded()) {
                return std::unexpected(TStringBuilder() << "Parquet row group " << CurrentRowGroup
                    << " is incomplete after its sequential source range was consumed");
            }

            Phase = EPhase::ParseRowGroup;
            return {};
        }

        if (ValidateChecksum && !ChecksumComplete) {
            Phase = EPhase::SequentialData;
            return {};
        }

        FetchQueue = RowGroupRanges[CurrentRowGroup];
        Phase = FetchQueue.empty() ? EPhase::ParseRowGroup : EPhase::RowGroupData;
        return {};
    }

    std::expected<bool, TString> CanUseOnePassChecksum() const {
        TMaybe<ui64> previousEnd;
        for (ui32 rowGroup = 0; rowGroup < RowGroupRanges.size(); ++rowGroup) {
            TMaybe<ui64> groupStart;
            ui64 groupEnd = 0;
            ui64 prefixBytes = 0;

            for (const auto& range : RowGroupRanges[rowGroup]) {
                if (range.Offset > ContentLength || range.Length > ContentLength - range.Offset) {
                    return std::unexpected(TStringBuilder() << "Parquet row group " << rowGroup
                        << " has an invalid source range [" << range.Offset << ", "
                        << range.Offset + range.Length << ") for a " << ContentLength << " byte file");
                }
                if (!range.Length) {
                    continue;
                }

                groupStart = groupStart ? Min(*groupStart, range.Offset) : range.Offset;
                groupEnd = Max(groupEnd, range.Offset + range.Length);
                if (range.Offset < FooterSuffixStart) {
                    prefixBytes = SumWithSaturation(
                        prefixBytes,
                        Min(range.Offset + range.Length, FooterSuffixStart) - range.Offset);
                }
            }

            if (!groupStart) {
                continue;
            }
            if (previousEnd && *groupStart < *previousEnd) {
                // Some valid files (notably those written before PARQUET-816)
                // have overlapping or interleaved row-group envelopes. They
                // retain the legacy checksum pass and sparse row-group reads.
                return false;
            }
            previousEnd = groupEnd;

            const ui64 suffixBytes = ContentLength - FooterSuffixStart;
            const ui64 pendingReadBytes = FooterSuffixStart ? ReadBatchSize : 0;
            const ui64 peakBytes = SumWithSaturation(
                SumWithSaturation(suffixBytes, prefixBytes),
                pendingReadBytes);
            if (peakBytes >= BufferSizeLimit) {
                return false;
            }
        }

        return true;
    }

    ui64 CurrentRowGroupPrefixEnd() const {
        ui64 end = 0;
        for (const auto& range : RowGroupRanges[CurrentRowGroup]) {
            if (range.Offset < FooterSuffixStart) {
                end = Max(end, Min(range.Offset + range.Length, FooterSuffixStart));
            }
        }
        return end;
    }

    bool CurrentRowGroupIsLoaded() const {
        if (!SparseFile || CurrentRowGroup >= RowGroupRanges.size()) {
            return false;
        }

        for (const auto& range : RowGroupRanges[CurrentRowGroup]) {
            if (!SparseFile->HasBytes(range.Offset, range.Length)) {
                return false;
            }
        }
        return true;
    }

    ui64 SequentialTarget() const {
        if (!UseOnePassChecksum) {
            return ContentLength;
        }
        if (RowGroupRanges.empty() || CurrentRowGroup + 1 == RowGroupRanges.size()) {
            return FooterSuffixStart;
        }
        return Max(ChecksumOffset, CurrentRowGroupPrefixEnd());
    }

    void RouteChecksumChunkToCurrentRowGroup(ui64 offset, TStringBuf data) {
        Y_ENSURE(UseOnePassChecksum);
        Y_ENSURE(CurrentRowGroup < RowGroupRanges.size());

        const ui64 chunkEnd = offset + data.size();
        for (const auto& range : RowGroupRanges[CurrentRowGroup]) {
            const ui64 rangeEnd = range.Offset + range.Length;
            const ui64 intersectionStart = Max(offset, range.Offset);
            const ui64 intersectionEnd = Min(chunkEnd, rangeEnd);
            if (intersectionStart < intersectionEnd) {
                SparseFile->PutRange(
                    intersectionStart,
                    TString(
                        data.data() + intersectionStart - offset,
                        intersectionEnd - intersectionStart));
            }
        }
    }

    std::expected<void, TString> AdvanceSequentialChecksum(
        const TAddChecksumChunkFn& addChecksumChunk)
    {
        const ui64 target = SequentialTarget();
        if (ChecksumOffset < target) {
            return {};
        }
        if (ChecksumOffset > target) {
            return std::unexpected(TStringBuilder() << "Parquet checksum offset " << ChecksumOffset
                << " advanced past sequential target " << target);
        }

        if (UseOnePassChecksum && !RowGroupRanges.empty() &&
            CurrentRowGroup + 1 < RowGroupRanges.size()) {
            if (!CurrentRowGroupIsLoaded()) {
                return std::unexpected(TStringBuilder() << "Parquet row group " << CurrentRowGroup
                    << " is incomplete at sequential offset " << ChecksumOffset);
            }
            Phase = EPhase::ParseRowGroup;
            return {};
        }

        if (UseOnePassChecksum) {
            if (ChecksumOffset != FooterSuffixStart ||
                !SparseFile->HasBytes(FooterSuffixStart, ContentLength - FooterSuffixStart)) {
                return std::unexpected("Parquet cached footer suffix is incomplete");
            }

            while (ChecksumOffset < ContentLength) {
                const ui64 length = Min<ui64>(ReadBatchSize, ContentLength - ChecksumOffset);
                auto data = SparseFile->ReadBytes(ChecksumOffset, length);
                if (!data) {
                    return std::unexpected(TStringBuilder() << "Parquet cached footer suffix is missing range ["
                        << ChecksumOffset << ", " << ChecksumOffset + length << ")");
                }
                try {
                    addChecksumChunk(*data);
                } catch (const std::exception& ex) {
                    return std::unexpected(TString(ex.what()));
                }
                ChecksumOffset += length;
            }
        } else if (ChecksumOffset != ContentLength) {
            return std::unexpected("Parquet fallback checksum pass ended before EOF");
        }

        ChecksumComplete = true;
        return PlanCurrentRowGroup();
    }

    std::expected<TNextRangeResult, TString> MakeRangeError(const TString& error) const {
        return std::unexpected(error);
    }

    std::expected<TNextRangeResult, TString> SetRangeError(TString error) {
        Phase = EPhase::Error;
        FatalError = std::move(error);
        return MakeRangeError(FatalError);
    }

    std::expected<TDataResult, TString> MakeDataError(const TString& error) const {
        return std::unexpected(error);
    }

    std::expected<TDataResult, TString> SetDataError(TString error) {
        Phase = EPhase::Error;
        FatalError = std::move(error);
        return MakeDataError(FatalError);
    }

private:
    const ui64 ContentLength;
    const ui32 ReadBatchSize;
    const ui64 BufferSizeLimit;
    const bool ValidateChecksum;
    IDataParser::TPtr Parser;
    std::shared_ptr<TParquetSparseFile> SparseFile;
    TVector<TParquetFetchRange> FetchQueue;
    TVector<TVector<TParquetFetchRange>> RowGroupRanges;
    TMaybe<TImportRange> OutstandingRange;
    TMaybe<TDataBatch> WaitingBatch;
    TString ChecksumChunk;
    ui64 ChecksumOffset = 0;
    ui64 FooterSuffixStart = 0;
    ui64 NextBatchId = 1;
    ui32 CurrentRowGroup = 0;
    bool RowGroupParserOpen = false;
    bool WaitingBatchFinishesRowGroup = false;
    bool WaitingBatchIsFinal = false;
    bool UseOnePassChecksum = false;
    bool ChecksumComplete = false;
    ui64 CheckpointProcessedBytes = 0;
    EPhase Phase = EPhase::FooterTail;
    TString FatalError;
};

std::expected<void, TString> ValidateCommonSettings(const TImportS3EngineSettings& settings) {
    if (!settings.ReadBatchSize) {
        return std::unexpected("S3 import read batch size must be greater than zero");
    }
    if (!settings.BufferSizeLimit) {
        return std::unexpected("S3 import buffer size limit must be greater than zero");
    }
    if (settings.EncryptionKey.Defined() != settings.EncryptionIV.Defined()) {
        return std::unexpected("S3 import encryption key and IV must be provided together");
    }
    if (settings.EncryptionKey && (!*settings.EncryptionKey || !*settings.EncryptionIV)) {
        return std::unexpected("S3 import encryption key and IV must not be empty");
    }
    if (!settings.ContentLength && settings.EncryptionKey) {
        return std::unexpected("encrypted S3 import object cannot be empty");
    }

    return {};
}

} // anonymous namespace

std::expected<IImportS3Engine::TPtr, TString> CreateImportS3Engine(
    const TImportS3EngineSettings& settings,
    const TTableInfo& tableInfo,
    const NKikimrSchemeOp::TTableDescription& scheme)
{
    if (auto result = ValidateCommonSettings(settings); !result) {
        return std::unexpected(std::move(result.error()));
    }

    IDataParser::TPtr parser;
    switch (settings.DataFormat) {
    case NBackupRestoreTraits::EDataFormat::YdbDump:
        parser = CreateCsvDataParser();
        break;

    case NBackupRestoreTraits::EDataFormat::Parquet:
        if (settings.EncryptionKey) {
            return std::unexpected("externally encrypted Parquet import is not supported because Parquet requires random access");
        }
        if (settings.CompressionCodec != NBackupRestoreTraits::ECompressionCodec::None) {
            return std::unexpected("external compression is not supported for Parquet import");
        }
        if (settings.ContentLength < 8) {
            return std::unexpected("Parquet file is too small");
        }
        parser = CreateParquetDataParser();
        break;

    case NBackupRestoreTraits::EDataFormat::Invalid:
        return std::unexpected("invalid S3 import data format");
    }

    if (auto result = parser->Configure(tableInfo, scheme); !result) {
        return std::unexpected(TStringBuilder() << "failed to configure import parser: " << result.error());
    }

    if (settings.DataFormat == NBackupRestoreTraits::EDataFormat::Parquet) {
        return MakeHolder<TParquetImportS3Engine>(settings, std::move(parser));
    }

    THolder<ISequentialReadController> reader;
    switch (settings.CompressionCodec) {
    case NBackupRestoreTraits::ECompressionCodec::None:
        reader = MakeHolder<TRawReadController>(settings.ReadBatchSize, settings.BufferSizeLimit);
        break;

    case NBackupRestoreTraits::ECompressionCodec::Zstd:
        reader = MakeHolder<TZstdReadController>(
            settings.ReadBatchSize,
            settings.BufferSizeLimit,
            settings.ZstdBlockSize);
        break;

    case NBackupRestoreTraits::ECompressionCodec::Invalid:
        return std::unexpected("invalid S3 import compression codec");
    }

    if (settings.EncryptionKey) {
        reader = MakeHolder<TEncryptionReadController>(
            *settings.EncryptionKey,
            *settings.EncryptionIV,
            std::move(reader));
    }

    return MakeHolder<TCsvImportS3Engine>(settings, std::move(reader), std::move(parser));
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS
