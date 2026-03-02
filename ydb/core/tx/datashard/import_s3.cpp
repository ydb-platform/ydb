#ifndef KIKIMR_DISABLE_S3_OPS

#include "backup_restore_traits.h"
#include "datashard_impl.h"
#include "extstorage_usage_config.h"
#include "import_common.h"
#include "import_s3.h"

#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/encryption.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/wrappers/retry_policy.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/s3_storage.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/io_formats/ydb_dump/csv_ydb_dump.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <contrib/libs/zstd/include/zstd.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/buffer.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>
#include <util/string/builder.h>

namespace {

    struct DestroyZCtx {
        static void Destroy(::ZSTD_DCtx* p) noexcept {
            ZSTD_freeDCtx(p);
        }
    };

    constexpr ui64 SumWithSaturation(ui64 a, ui64 b) {
        return Max<ui64>() - a < b ? Max<ui64>() : a + b;
    }

} // anonymous

namespace NKikimr {
namespace NDataShard {

using namespace NBackup;
using namespace NBackupRestoreTraits;

using namespace NResourceBroker;
using namespace NWrappers;

using namespace Aws::S3;
using namespace Aws;

template <typename TSettings>
class TS3Downloader: public TActorBootstrapped<TS3Downloader<TSettings>> {
    using TThis = TS3Downloader<TSettings>;
    // IReadController
    //
    // Work cycle:
    // 1. RestoreFromState() - optional. State was made from Confirm() call
    // 2. NextRange()
    // 3. Feed()
    // 4. while (TryGetData() == READY_DATA) {
    //       4.a. Add ReadyBytes() to current processed bytes state
    //       4.b. Confirm() - update state
    //    }
    // 5. Repeat from 1. until the whole file is processed
    class IReadController {
    public:
        enum EDataStatus {
            READY_DATA = 0,
            NOT_ENOUGH_DATA = 1,
            ERROR = 2,
        };

    public:
        virtual ~IReadController() = default;
        virtual void Feed(TString&& portion, bool last) = 0;
        // Returns data (points to internal buffer) or error
        virtual EDataStatus TryGetData(TStringBuf& data, TString& error) = 0;
        // Clears internal buffer & makes it ready for another Feed() and TryGetData()
        virtual void Confirm(NKikimrBackup::TS3DownloadState& state) = 0;
        // Bytes that were read from S3 and put into controller. In terms of input bytes
        virtual ui64 PendingBytes() const = 0;
        // Bytes that controller has given to processing. In terms of input bytes
        virtual ui64 ReadyBytes() const = 0;
        virtual std::pair<ui64, ui64> NextRange(ui64 contentLength, ui64 processedBytes) const = 0;
        virtual bool RestoreFromState(const NKikimrBackup::TS3DownloadState& state, TString& error) = 0;
    };

    class TReadController: public IReadController {
    public:
        explicit TReadController(ui32 rangeSize, ui64 bufferSizeLimit)
            : RangeSize(rangeSize)
            , BufferSizeLimit(bufferSizeLimit)
        {
            // able to contain at least one range
            Buffer.Reserve(RangeSize);
        }

        std::pair<ui64, ui64> NextRange(ui64 contentLength, ui64 processedBytes) const override {
            Y_ENSURE(contentLength > 0);
            Y_ENSURE(processedBytes < contentLength);

            const ui64 start = processedBytes + this->PendingBytes();
            const ui64 end = Min(SumWithSaturation(start, RangeSize), contentLength) - 1;
            return std::make_pair(start, end);
        }

        bool RestoreFromState(const NKikimrBackup::TS3DownloadState&, TString&) override {
            return true;
        }

    protected:
        bool CanIncreaseBuffer(size_t size, size_t delta, TString& reason) const {
            if ((size + delta) >= BufferSizeLimit) {
                reason = "reached buffer size limit";
                return false;
            }

            return true;
        }

        bool CanRequestNextRange(size_t size, TString& reason) const {
            return CanIncreaseBuffer(size, RangeSize, reason);
        }

        TStringBuf AsStringBuf(size_t size) const {
            return TStringBuf(Buffer.Data(), size);
        }

    private:
        const ui32 RangeSize;
        const ui64 BufferSizeLimit;

    protected:
        TBuffer Buffer;

    }; // TReadController

    class TReadControllerRaw: public TReadController {
    public:
        using TReadController::TReadController;

        void Feed(TString&& portion, bool /* last */) override {
            this->Buffer.Append(portion.data(), portion.size());
        }

        IReadController::EDataStatus TryGetData(TStringBuf& data, TString& error) override {
            Y_ENSURE(Pos == 0);

            const ui64 pos = this->AsStringBuf(this->Buffer.Size()).rfind('\n');
            if (TString::npos == pos) {
                if (!this->CanRequestNextRange(this->Buffer.Size(), error)) {
                    return IReadController::ERROR;
                } else {
                    return IReadController::NOT_ENOUGH_DATA;
                }
            }

            Pos = pos + 1 /* \n */;
            data = this->AsStringBuf(Pos);

            return IReadController::READY_DATA;
        }

        void Confirm(NKikimrBackup::TS3DownloadState&) override {
            this->Buffer.ChopHead(Pos);
            Pos = 0;
        }

        ui64 PendingBytes() const override {
            return this->Buffer.Size();
        }

        ui64 ReadyBytes() const override {
            return Pos;
        }

    private:
        ui64 Pos = 0;
    };

    class TReadControllerZstd: public TReadController {
        void Reset() {
            ZSTD_DCtx_reset(Context.Get(), ZSTD_reset_session_only);
            ZSTD_DCtx_refDDict(Context.Get(), NULL);
        }

    public:
        explicit TReadControllerZstd(ui32 rangeSize, ui64 bufferSizeLimit)
            : TReadController(rangeSize, bufferSizeLimit)
            , Context(ZSTD_createDCtx())
        {
            Reset();
            // able to contain at least one block
            // take effect if RangeSize < BlockSize
            this->Buffer.Reserve(AppData()->ZstdBlockSizeForTest.GetOrElse(ZSTD_BLOCKSIZE_MAX));
        }

        void Feed(TString&& portion, bool /* last */) override {
            Y_ENSURE(Portion.Empty());
            Portion.Assign(portion.data(), portion.size());
        }

        IReadController::EDataStatus TryGetData(TStringBuf& data, TString& error) override {
            Y_ENSURE(ReadyInputBytes == 0 && ReadyOutputPos == 0);

            auto input = ZSTD_inBuffer{Portion.Data(), Portion.Size(), 0};
            size_t decompressionResult = 0;
            while (!ReadyOutputPos) {
                PendingInputBytes -= input.pos; // dec before decompress

                auto output = ZSTD_outBuffer{this->Buffer.Data(), this->Buffer.Capacity(), this->Buffer.Size()};
                decompressionResult = ZSTD_decompressStream(Context.Get(), &output, &input);

                if (ZSTD_isError(decompressionResult)) {
                    error = ZSTD_getErrorName(decompressionResult);
                    return IReadController::ERROR;
                }

                PendingInputBytes += input.pos; // inc after decompress
                this->Buffer.Proceed(output.pos);

                if (decompressionResult == 0) {
                    // end of frame
                    if (this->Buffer.Size() > 0 && this->AsStringBuf(this->Buffer.Size()).back() != '\n') { // Handle also a special case: theoretically it is possible to have nonempty zstd block with empty output data (we created a new block and have not added any data yet)
                        error = "cannot find new line symbol";
                        return IReadController::ERROR;
                    }

                    ReadyInputBytes = PendingInputBytes;
                    ReadyOutputPos = this->Buffer.Size();
                    Reset();
                } else {
                    // try to find complete row
                    const ui64 pos = this->AsStringBuf(this->Buffer.Size()).rfind('\n');
                    if (TString::npos != pos) {
                        ReadyOutputPos = pos + 1 /* \n */;
                    }
                }

                if (input.pos >= input.size) {
                    // end of input
                    break;
                }

                if (!ReadyOutputPos && output.pos == output.size) {
                    const auto blockSize = AppData()->ZstdBlockSizeForTest.GetOrElse(ZSTD_BLOCKSIZE_MAX);
                    if (this->CanIncreaseBuffer(this->Buffer.Size(), blockSize, error)) {
                        this->Buffer.Reserve(this->Buffer.Size() + blockSize);
                    } else {
                        return IReadController::ERROR;
                    }
                }
            }

            Portion.ChopHead(input.pos);

            if (!ReadyOutputPos && decompressionResult != 0) {
                if (!this->CanRequestNextRange(this->Buffer.Size(), error)) {
                    return IReadController::ERROR;
                } else {
                    return IReadController::NOT_ENOUGH_DATA;
                }
            }

            if (ReadyOutputPos) {
                data = this->AsStringBuf(ReadyOutputPos);
            } else {
                data = TStringBuf();
            }
            return IReadController::READY_DATA;
        }

        void Confirm(NKikimrBackup::TS3DownloadState&) override {
            this->Buffer.ChopHead(ReadyOutputPos);
            ReadyOutputPos = 0;

            PendingInputBytes -= ReadyInputBytes;
            ReadyInputBytes = 0;
        }

        ui64 PendingBytes() const override {
            return PendingInputBytes;
        }

        ui64 ReadyBytes() const override {
            return ReadyInputBytes;
        }

    private:
        THolder<::ZSTD_DCtx, DestroyZCtx> Context;
        TBuffer Portion;
        ui64 PendingInputBytes = 0;
        ui64 ReadyInputBytes = 0;
        ui64 ReadyOutputPos = 0;
    };

    class TEncryptionDeserializerController: public IReadController {
    public:
        TEncryptionDeserializerController(
                NBackup::TEncryptionKey key,
                NBackup::TEncryptionIV expectedIV,
                THolder<IReadController> deserializedDataController,
                ui64 readBatchSize)
            : Deserializer(std::move(key), std::move(expectedIV))
            , DataController(std::move(deserializedDataController))
            , ReadBatchSize(readBatchSize)
        {
        }

        void Feed(TString&& portion, bool last) override {
            if (!portion.empty() || last) {
                NewData = true;
            }
            Last = last;
            FeedUnprocessedBytes += portion.size();
            Deserializer.AddData(TBuffer(portion.data(), portion.size()), last);
        }

        IReadController::EDataStatus TryGetData(TStringBuf& data, TString& error) override {
            if (BytesFedToChild) {
                auto status = TryGetDataFromChildController(data, error);
                if (status != IReadController::NOT_ENOUGH_DATA || !NewData) {
                    return status;
                }
            }
            bool lastEmptyBlock = false; // The case of empty file
            if (NewData) {
                try {
                    NewData = false;
                    const ui64 processedBefore = Deserializer.GetProcessedInputBytes();
                    TMaybe<TBuffer> block = Deserializer.GetNextBlock(); // Returns at least one encrypted block
                    const ui64 processedAfter = Deserializer.GetProcessedInputBytes();
                    Y_ENSURE(processedAfter - processedBefore <= FeedUnprocessedBytes);
                    ReadyInputBytes += processedAfter - processedBefore;
                    if (block) {
                        if (block->Size()) {
                            // Data is read by blocks from encrypted file.
                            // Each block contains at least one row of data with '\n',
                            // so we will always get some data from DataController.
                            DataController->Feed(TString(block->Data(), block->Size()), Last);
                            BytesFedToChild += block->Size();
                        } else {
                            lastEmptyBlock = Last;
                        }
                    }
                } catch (const std::exception& ex) {
                    error = ex.what();
                    return IReadController::ERROR;
                }
            }

            if (BytesFedToChild) {
                return TryGetDataFromChildController(data, error);
            }
            if (lastEmptyBlock) {
                data.Clear();
                return IReadController::READY_DATA;
            }
            return IReadController::NOT_ENOUGH_DATA;
        }

        IReadController::EDataStatus TryGetDataFromChildController(TStringBuf& data, TString& error) {
            const auto status = DataController->TryGetData(data, error);
            if (status == IReadController::READY_DATA) {
                if (ui64 ready = DataController->ReadyBytes()) {
                    BytesFedToChild -= ready;
                    Y_ENSURE(BytesFedToChild >= 0);
                }
            }
            return status;
        }

        void Confirm(NKikimrBackup::TS3DownloadState& state) override {
            state.SetEncryptedDeserializerState(Deserializer.GetState());
            if (ui64 readyBytes = ReadyBytes()) {
                FeedUnprocessedBytes -= readyBytes;
                ReadyInputBytes = 0;
            }
            DataController->Confirm(state);
        }

        ui64 PendingBytes() const override {
            return FeedUnprocessedBytes;
        }

        ui64 ReadyBytes() const override {
            return BytesFedToChild == 0 ? ReadyInputBytes : 0;
        }

        std::pair<ui64, ui64> NextRange(ui64 contentLength, ui64 processedBytes) const override {
            Y_ENSURE(contentLength > 0);
            Y_ENSURE(processedBytes < contentLength);

            const ui64 start = processedBytes + this->PendingBytes();
            const ui64 end = Min(SumWithSaturation(start, ReadBatchSize), contentLength) - 1;
            return std::make_pair(start, end);
        }

        bool RestoreFromState(const NKikimrBackup::TS3DownloadState& state, TString& error) override {
            if (const TString& deserializerState = state.GetEncryptedDeserializerState()) {
                try {
                    Deserializer = NBackup::TEncryptedFileDeserializer::RestoreFromState(deserializerState);
                    FeedUnprocessedBytes = 0;
                    ReadyInputBytes = 0;
                } catch (const std::exception& ex) {
                    error = ex.what();
                    return false;
                }
            }
            return DataController->RestoreFromState(state, error);
        }

    private:
        bool Last = false;
        ui64 FeedUnprocessedBytes = 0;
        ui64 ReadyInputBytes = 0;
        bool NewData = false;
        ui64 BytesFedToChild = 0;
        NBackup::TEncryptedFileDeserializer Deserializer;
        THolder<IReadController> DataController;
        const ui64 ReadBatchSize;
    };

    class TUploadRowsRequestBuilder {
    public:
        void New(const TTableInfo& tableInfo, const NKikimrSchemeOp::TTableDescription& scheme) {
            Record = std::make_shared<NKikimrTxDataShard::TEvUploadRowsRequest>();
            Record->SetTableId(tableInfo.GetId());

            TVector<TString> columnNames;
            for (const auto& column : scheme.GetColumns()) {
                columnNames.push_back(column.GetName());
            }

            auto& rowScheme = *Record->MutableRowScheme();
            for (ui32 id : tableInfo.GetKeyColumnIds()) {
                rowScheme.AddKeyColumnIds(id);
            }
            for (ui32 id : tableInfo.GetValueColumnIds(columnNames)) {
                rowScheme.AddValueColumnIds(id);
            }

            CellBytes = 0;
        }

        void AddRow(const TVector<TCell>& keys, const TVector<TCell>& values) {
            Y_ENSURE(Record);
            auto& row = *Record->AddRows();
            row.SetKeyColumns(TSerializedCellVec::Serialize(keys));
            row.SetValueColumns(TSerializedCellVec::Serialize(values));

            for (const auto& x : keys) {
                CellBytes += x.Size();
            }
            for (const auto& x : values) {
                CellBytes += x.Size();
            }
        }

        const std::shared_ptr<NKikimrTxDataShard::TEvUploadRowsRequest>& GetRecord() {
            Y_ENSURE(Record);
            return Record;
        }

        ui64 GetCellBytes() const {
            return CellBytes;
        }

    private:
        std::shared_ptr<NKikimrTxDataShard::TEvUploadRowsRequest> Record;
        ui64 CellBytes;

    }; // TUploadRowsRequestBuilder

    struct TCounters {
        struct TLatency {
            TInstant Begin;
            ::NMonitoring::THistogramPtr Counter;

            explicit TLatency(::NMonitoring::THistogramPtr counter)
                : Counter(counter)
            {
            }

            void Start(TInstant begin) {
                Begin = begin;
            }

            void Finish(TInstant end) {
                Counter->Collect((end - Begin).MilliSeconds());
                Begin = TInstant::Zero();
            }
        };

        ::NMonitoring::TDynamicCounters::TCounterPtr BytesReceived;
        ::NMonitoring::TDynamicCounters::TCounterPtr BytesWritten;
        TLatency LatencyRead;
        TLatency LatencyProcess;
        TLatency LatencyWrite;

        explicit TCounters(::NMonitoring::TDynamicCounterPtr counters)
            : BytesReceived(counters->GetCounter("BytesReceived", true))
            , BytesWritten(counters->GetCounter("BytesWritten", true))
            , LatencyRead(counters->GetHistogram("LatencyReadMs", ::NMonitoring::ExponentialHistogram(10, 4, 1)))
            , LatencyProcess(counters->GetHistogram("LatencyProcessMs", ::NMonitoring::ExponentialHistogram(10, 4, 1)))
            , LatencyWrite(counters->GetHistogram("LatencyWriteMs", ::NMonitoring::ExponentialHistogram(10, 4, 1)))
        {
        }

    }; // TCounters

    static TInstant Now() {
        return TlsActivationContext->Now();
    }

    void AllocateResource() {
        IMPORT_LOG_D("AllocateResource");

        const auto* appData = AppData();
        this->Send(MakeResourceBrokerID(), new TEvResourceBroker::TEvSubmitTask(
            TStringBuilder() << "Restore { " << TxId << ":" << DataShard << " }",
            {{ 1, 0 }},
            appData->DataShardConfig.GetRestoreTaskName(),
            appData->DataShardConfig.GetRestoreTaskPriority(),
            nullptr
        ));

        this->Become(&TThis::StateAllocateResource);
    }

    void Handle(TEvResourceBroker::TEvResourceAllocated::TPtr& ev) {
        IMPORT_LOG_I("Handle TEvResourceBroker::TEvResourceAllocated {"
            << " TaskId: " << ev->Get()->TaskId
        << " }");

        TaskId = ev->Get()->TaskId;
        Restart();
    }

    void Restart() {
        IMPORT_LOG_N("Restart"
            << ": attempt# " << Attempt);

        if (const TActorId client = std::exchange(Client, TActorId())) {
            this->Send(client, new TEvents::TEvPoisonPill());
        }

        Client = this->RegisterWithSameMailbox(CreateStorageWrapper(ExternalStorageConfig->ConstructStorageOperator()));

        HeadObject(Settings.GetDataKey(DataFormat, CompressionCodec));
        this->Become(&TThis::StateDownloadData);
    }

    void HeadObject(const TString& key) {
        IMPORT_LOG_D("HeadObject"
            << ": key# " << key);

        auto request = Model::HeadObjectRequest()
            .WithKey(key);

        this->Send(Client, new TEvExternalStorage::TEvHeadObjectRequest(request));
    }

    void GetObject(const TString& key, const std::pair<ui64, ui64>& range) {
        IMPORT_LOG_D("GetObject"
            << ": key# " << key
            << ", range# " << range.first << "-" << range.second);

        auto request = Model::GetObjectRequest()
            .WithKey(key)
            .WithRange(TStringBuilder() << "bytes=" << range.first << "-" << range.second);

        this->Send(Client, new TEvExternalStorage::TEvGetObjectRequest(request));
    }

    void Handle(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        IMPORT_LOG_D("Handle " << ev->Get()->ToString());

        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess()) {
            switch (result.GetError().GetErrorType()) {
            case S3Errors::RESOURCE_NOT_FOUND:
            case S3Errors::NO_SUCH_KEY:
                break;
            default:
                IMPORT_LOG_E("Error at 'HeadObject'"
                    << ": error# " << result);
                return RetryOrFinish(result.GetError());
            }

            CompressionCodec = NBackupRestoreTraits::NextCompressionCodec(CompressionCodec);
            if (CompressionCodec == NBackupRestoreTraits::ECompressionCodec::Invalid) {
                return Finish(false, TStringBuilder() << "Cannot find any supported data file with prefix"
                    << ": " << Settings.GetObjectKeyPattern());
            }

            return HeadObject(Settings.GetDataKey(DataFormat, CompressionCodec));
        }

        THolder<IReadController> reader;
        switch (CompressionCodec) {
        case NBackupRestoreTraits::ECompressionCodec::None:
            reader.Reset(new TReadControllerRaw(ReadBatchSize, ReadBufferSizeLimit));
            break;
        case NBackupRestoreTraits::ECompressionCodec::Zstd:
            reader.Reset(new TReadControllerZstd(ReadBatchSize, ReadBufferSizeLimit));
            break;
        case NBackupRestoreTraits::ECompressionCodec::Invalid:
            Y_ENSURE(false, "unreachable");
        }

        if (Settings.EncryptionSettings.EncryptedBackup) {
            NBackup::TEncryptionIV expectedIV = NBackup::TEncryptionIV::Combine(
                *Settings.EncryptionSettings.IV,
                NBackup::EBackupFileType::TableData,
                0 /* already combined */,
                Settings.Shard
            );
            Reader = MakeHolder<TEncryptionDeserializerController>(
                *Settings.EncryptionSettings.Key,
                expectedIV,
                std::move(reader),
                ReadBatchSize
            );
        } else {
            Reader = std::move(reader);
        }

        ETag = result.GetResult().GetETag();
        ContentLength = result.GetResult().GetContentLength();

        if (!ContentLength && Settings.EncryptionSettings.EncryptedBackup) {
            // Encrypted file can not have zero length
            const TString error = TStringBuilder() << Settings.GetDataKey(DataFormat, CompressionCodec)
                << ": file is corrupted";
            IMPORT_LOG_E(error);
            return Finish(false, error);
        }

        if (Checksum) {
            HeadObject(ChecksumKey(Settings.GetDataKey(DataFormat, ECompressionCodec::None)));
            this->Become(&TThis::StateDownloadChecksum);
        } else {
            this->Send(DataShard, new TEvDataShard::TEvGetS3DownloadInfo(TxId));
        }
    }

    void Handle(TEvDataShard::TEvS3DownloadInfo::TPtr& ev) {
        IMPORT_LOG_D("Handle " << ev->Get()->ToString());

        const auto& info = ev->Get()->Info;
        if (!info.DataETag) {
            this->Send(DataShard, new TEvDataShard::TEvStoreS3DownloadInfo(TxId, {
                ETag, ProcessedBytes, WrittenBytes, WrittenRows, ProcessedChecksumState, DownloadState
            }));
            return;
        }

        ProcessDownloadInfo(info, TStringBuf("DownloadInfo"), true);
    }

    void ProcessDownloadInfo(const TS3Download& info, const TStringBuf marker, bool loadState = false) {
        IMPORT_LOG_N("Process download info at '" << marker << "'"
            << ": info# " << info);

        Y_ENSURE(info.DataETag);
        if (!CheckETag(*info.DataETag, ETag, marker)) {
            return;
        }

        DownloadState = info.DownloadState;
        if (loadState) {
            if (Checksum) {
                Checksum->SetState(info.ProcessedChecksumState);
            }
            if (TString restoreErr; !Reader->RestoreFromState(DownloadState, restoreErr)) {
                const TString error = TStringBuilder() << Settings.GetDataKey(DataFormat, CompressionCodec)
                    << ": failed to restore reader state: " << restoreErr;
                IMPORT_LOG_E(error);
                return Finish(false, error);
            }
        }

        ProcessedBytes = info.ProcessedBytes;
        ProcessedChecksumState = info.ProcessedChecksumState;
        ReadBytes = ProcessedBytes + Reader->PendingBytes();
        WrittenBytes = info.WrittenBytes;
        WrittenRows = info.WrittenRows;

        if (!ContentLength || ProcessedBytes >= ContentLength) {
            if (!CheckChecksum()) {
                return;
            }
            return Finish();
        }

        Process();
    }

    void Handle(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        IMPORT_LOG_D("Handle " << ev->Get()->ToString());

        auto& msg = *ev->Get();
        const auto& result = msg.Result;
        const TStringBuf marker = "GetObject";

        if (!CheckResult(result, marker)) {
            return;
        }

        if (!CheckETag(ETag, result.GetResult().c_str(), marker)) {
            return;
        }

        IMPORT_LOG_T("Content size"
            << ": processed-bytes# " << ProcessedBytes
            << ", content-length# " << ContentLength
            << ", body-size# " << msg.Body.size());

        *Counters.BytesReceived += msg.Body.size();
        Counters.LatencyRead.Finish(Now());

        ReadBytes += msg.Body.size();
        Reader->Feed(std::move(msg.Body), ReadBytes >= ContentLength);
        Process();
    }

    void HandleChecksum(TEvExternalStorage::TEvHeadObjectResponse::TPtr& ev) {
        IMPORT_LOG_D("HandleChecksum " << ev->Get()->ToString());

        const auto& result = ev->Get()->Result;

        if (!CheckResult(result, "HeadObject")) {
            return;
        }

        const auto contentLength = result.GetResult().GetContentLength();
        const auto checksumKey = ChecksumKey(Settings.GetDataKey(DataFormat, ECompressionCodec::None));
        GetObject(checksumKey, std::make_pair(0, contentLength - 1));
    }

    void HandleChecksum(TEvExternalStorage::TEvGetObjectResponse::TPtr& ev) {
        IMPORT_LOG_D("HandleChecksum " << ev->Get()->ToString());

        auto& msg = *ev->Get();
        const auto& result = msg.Result;

        if (!CheckResult(result, "GetObject")) {
            return;
        }

        ExpectedChecksum = msg.Body.substr(0, msg.Body.find(' '));

        this->Send(DataShard, new TEvDataShard::TEvGetS3DownloadInfo(TxId));
        this->Become(&TThis::StateDownloadData);
    }

    void Process() {
        TStringBuf data;
        TString error;

        switch (Reader->TryGetData(data, error)) {
        case IReadController::READY_DATA:
            break;

        case IReadController::NOT_ENOUGH_DATA:
            if (SumWithSaturation(ProcessedBytes, Reader->PendingBytes()) < ContentLength) {
                Counters.LatencyRead.Start(Now());
                return GetObject(Settings.GetDataKey(DataFormat, CompressionCodec),
                    Reader->NextRange(ContentLength, ProcessedBytes));
            } else {
                error = "reached end of file";
            }
            [[fallthrough]];

        default: // ERROR
            return Finish(false, TStringBuilder() << Settings.GetDataKey(DataFormat, CompressionCodec)
                << ": cannot process data: " << error);
        }

        Counters.LatencyProcess.Start(Now());

        RequestBuilder.New(TableInfo, Scheme);

        // Special case:
        // in encrypted file we have nonzero bytes on input, but can still have zero bytes on output
        // In this case TryGetData() returns READY_DATA
        if (data) {
            if (Checksum) {
                Checksum->AddData(data);
            }

            TMemoryPool pool(256);
            while (ProcessData(data, pool));
        }

        if (const auto processed = Reader->ReadyBytes()) { // has progress
            ProcessedBytes += processed;
            if (Checksum) {
                ProcessedChecksumState = Checksum->GetState();
            }

            WrittenBytes += std::exchange(PendingBytes, 0);
            WrittenRows += std::exchange(PendingRows, 0);
        }

        DownloadState.Clear();
        Reader->Confirm(DownloadState);
        UploadRows();
    }

    bool ProcessData(TStringBuf& data, TMemoryPool& pool) {
        pool.Clear();

        TStringBuf line = data.NextTok('\n');
        const TStringBuf origLine = line;

        if (!line) {
            if (data) {
                return true; // skip empty line
            }

            return false;
        }

        std::vector<std::pair<i32, NScheme::TTypeInfo>> columnOrderTypes; // {keyOrder, PType}
        columnOrderTypes.reserve(Scheme.GetColumns().size());

        for (const auto& column : Scheme.GetColumns()) {
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
            columnOrderTypes.emplace_back(TableInfo.KeyOrder(column.GetName()), typeInfoMod.TypeInfo);
        }

        TVector<TCell> keys;
        TVector<TCell> values;
        TString strError;

        if (!NFormats::TYdbDump::ParseLine(line, columnOrderTypes, pool, keys, values, strError, PendingBytes)) {
            Finish(false, TStringBuilder() << Settings.GetDataKey(DataFormat, CompressionCodec)
                << ": " << strError << " on line: " << origLine);
            return false;
        }

        Y_ENSURE(!keys.empty());
        if (!TableInfo.IsMyKey(keys) /* TODO: maybe skip */) {
            Finish(false, TStringBuilder() << Settings.GetDataKey(DataFormat, CompressionCodec)
                << ": key is out of range on line: " << origLine);
            return false;
        }

        RequestBuilder.AddRow(keys, values);
        ++PendingRows;

        return true;
    }

    void UploadRows() {
        const auto& record = RequestBuilder.GetRecord();

        IMPORT_LOG_I("Upload rows"
            << ": count# " << record->RowsSize()
            << ", size# " << record->ByteSizeLong());

        Counters.LatencyProcess.Finish(Now());
        Counters.LatencyWrite.Start(Now());

        this->Send(DataShard, new TEvDataShard::TEvS3UploadRowsRequest(TxId, record, {
            ETag, ProcessedBytes, WrittenBytes, WrittenRows, ProcessedChecksumState, DownloadState
        }));
    }

    void Handle(TEvDataShard::TEvS3UploadRowsResponse::TPtr& ev) {
        IMPORT_LOG_D("Handle " << ev->Get()->ToString());

        *Counters.BytesWritten += RequestBuilder.GetCellBytes();
        Counters.LatencyWrite.Finish(Now());

        const auto& record = ev->Get()->Record;

        if (record.GetStatus() == NKikimrTxDataShard::TError::OK) {
            return ProcessDownloadInfo(ev->Get()->Info, TStringBuf("UploadResponse"));
        } else if (ev->Get()->IsRetriableError()) {
            return RetryOrFinish(record.GetErrorDescription());
        } else {
            return Finish(false, TStringBuilder() << Settings.GetDataKey(DataFormat, CompressionCodec)
                << ": " << record.GetErrorDescription());
        }
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }

        IMPORT_LOG_E("Error at '" << marker << "'"
            << ": error# " << result);
        RetryOrFinish(result.GetError());

        return false;
    }

    bool CheckETag(const TString& expected, const TString& got, const TStringBuf marker) {
        if (expected == got) {
            return true;
        }

        const TString error = TStringBuilder() << Settings.GetDataKey(DataFormat, CompressionCodec)
            << ": ETag mismatch at '" << marker << "'"
            << ": expected '" << expected << "'"
            << ", got '" << got << "'";

        IMPORT_LOG_E(error);
        Finish(false, error);

        return false;
    }

    bool CheckScheme() {
        auto finish = [this](const TString& error) -> bool {
            IMPORT_LOG_E(error);
            Finish(false, error);

            return false;
        };

        for (const auto& column : Scheme.GetColumns()) {
            if (!TableInfo.HasColumn(column.GetName())) {
                return finish(TStringBuilder() << Settings.GetObjectKeyPattern()
                    << ": cannot find column '" << column.GetName() << "'");
            }

            const auto type = TableInfo.GetColumnType(column.GetName());
            auto schemeType = NScheme::TypeInfoModFromProtoColumnType(column.GetTypeId(),
                column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
            if (type.first != schemeType.TypeInfo || type.second != schemeType.TypeMod) {
                return finish(TStringBuilder() << Settings.GetObjectKeyPattern()
                    << ": column '" << column.GetName() << "' type mismatch"
                    << ": expected '" << NScheme::TypeName(type.first, type.second) << "'"
                    << ", got '" << NScheme::TypeName(schemeType.TypeInfo, schemeType.TypeMod) << "'");
            }
        }

        if (TableInfo.GetKeyColumnIds().size() != (ui32)Scheme.KeyColumnNamesSize()) {
            return finish(TStringBuilder() << Settings.GetObjectKeyPattern()
                << ": key column count mismatch"
                << ": expected '" << TableInfo.GetKeyColumnIds().size() << "'"
                << ", got '" << Scheme.KeyColumnNamesSize() << "'");
        }

        for (ui32 i = 0; i < (ui32)Scheme.KeyColumnNamesSize(); ++i) {
            const auto& name = Scheme.GetKeyColumnNames(i);
            const ui32 keyOrder = TableInfo.KeyOrder(name);

            if (keyOrder != i) {
                return finish(TStringBuilder() << Settings.GetObjectKeyPattern()
                    << ": key column '" << name << "' order mismatch"
                    << ": expected '" << keyOrder << "'"
                    << ", got '" << i << "'");
            }
        }

        return true;
    }

    bool CheckChecksum() {
        if (!Checksum) {
            return true;
        }

        TString gotChecksum = Checksum->Finalize();
        if (gotChecksum == ExpectedChecksum) {
            return true;
        }

        const TString error = TStringBuilder() << Settings.GetDataKey(DataFormat, ECompressionCodec::None)
            << ": checksum mismatch"
            << ": expected '" << ExpectedChecksum << "'"
            << ", got '" << gotChecksum << "'";

        IMPORT_LOG_E(error);
        Finish(false, error);

        return false;
    }

    static bool ShouldRetry(const Aws::S3::S3Error& error) {
        return NWrappers::ShouldRetry(error);
    }

    static bool ShouldRetry(const TString&) {
        return true;
    }

    template <typename T>
    bool CanRetry(const T& error) const {
        return Attempt < Retries && ShouldRetry(error);
    }

    void Retry() {
        Delay = Min(Delay * ++Attempt, MaxDelay);
        const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());
        this->Schedule(Delay + random, new TEvents::TEvWakeup());
    }

    template <typename T>
    void RetryOrFinish(const T& error) {
        if (CanRetry(error)) {
            Retry();
        } else {
            if constexpr (std::is_same_v<T, Aws::S3::S3Error>) {
                Finish(false, TStringBuilder() << Settings.GetDataKey(DataFormat, CompressionCodec)
                    << ": S3 error: " << error.GetMessage().c_str());
            } else {
                Finish(false, TStringBuilder() << Settings.GetDataKey(DataFormat, CompressionCodec)
                    << ": " << error);
            }
        }
    }

    void Finish(bool success = true, const TString& error = TString()) {
        IMPORT_LOG_N("Finish"
            << ": success# " << success
            << ", error# " << error
            << ", writtenBytes# " << WrittenBytes
            << ", writtenRows# " << WrittenRows);

        TAutoPtr<IDestructable> prod = new TImportJobProduct(success, error, WrittenBytes, WrittenRows);
        this->Send(DataShard, new TEvDataShard::TEvAsyncJobComplete(prod), 0, TxId);

        Y_ENSURE(TaskId);
        this->Send(MakeResourceBrokerID(), new TEvResourceBroker::TEvFinishTask(TaskId));

        PassAway();
    }

    void NotifyDied() {
        this->Send(MakeResourceBrokerID(), new TEvResourceBroker::TEvNotifyActorDied());
        PassAway();
    }

    void PassAway() override {
        if (Client) {
            this->Send(Client, new TEvents::TEvPoisonPill());
        }

        TActorBootstrapped<TS3Downloader<TSettings>>::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IMPORT_S3_DOWNLOADER_ACTOR;
    }

    TStringBuf LogPrefix() const {
        return LogPrefix_;
    }

    static TSettings GetSettings(const NKikimrSchemeOp::TRestoreTask& task);

    static ui64 GetReadBatchSize(const NKikimrSchemeOp::TRestoreTask& task) {
        if constexpr (std::is_same_v<TSettings, NKikimrSchemeOp::TS3Settings>) {
            return GetSettings(task).GetLimits().GetReadBatchSize();
        } else {
            return 8388608; // Default 8MB for FS
        }
    }

    explicit TS3Downloader(const TActorId& dataShard, ui64 txId, const NKikimrSchemeOp::TRestoreTask& task, const TTableInfo& tableInfo)
        : ExternalStorageConfig(NWrappers::IExternalStorageConfig::Construct(AppData()->AwsClientConfig, GetSettings(task)))
        , DataShard(dataShard)
        , TxId(txId)
        , Settings(TStorageSettings::FromRestoreTask<TSettings>(task))
        , DataFormat(NBackupRestoreTraits::EDataFormat::Csv)
        , CompressionCodec(NBackupRestoreTraits::ECompressionCodec::None)
        , TableInfo(tableInfo)
        , Scheme(task.GetTableDescription())
        , LogPrefix_(TStringBuilder() << "s3:" << TxId)
        , Retries(task.GetNumberOfRetries())
        , ReadBatchSize(GetReadBatchSize(task))
        , ReadBufferSizeLimit(AppData()->DataShardConfig.GetRestoreReadBufferSizeLimit())
        , Checksum(task.GetValidateChecksums() ? CreateChecksum() : nullptr)
        , ProcessedChecksumState(Checksum ? Checksum->GetState() : NKikimrBackup::TChecksumState())
        , Counters(GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "import"))
    {
    }

    void Bootstrap() {
        IMPORT_LOG_D("Bootstrap"
            << ": attempt# " << Attempt);

        if (!CheckScheme()) {
            return;
        }

        AllocateResource();
    }

    STATEFN(StateAllocateResource) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvResourceBroker::TEvResourceAllocated, Handle);
            sFunc(TEvents::TEvPoisonPill, NotifyDied);
        }
    }

    STATEFN(StateDownloadData) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, Handle);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, Handle);

            hFunc(TEvDataShard::TEvS3DownloadInfo, Handle);
            hFunc(TEvDataShard::TEvS3UploadRowsResponse, Handle);

            sFunc(TEvents::TEvWakeup, Restart);
            sFunc(TEvents::TEvPoisonPill, NotifyDied);
        }
    }

    STATEFN(StateDownloadChecksum) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvHeadObjectResponse, HandleChecksum);
            hFunc(TEvExternalStorage::TEvGetObjectResponse, HandleChecksum);

            sFunc(TEvents::TEvWakeup, Restart);
            sFunc(TEvents::TEvPoisonPill, NotifyDied);
        }
    }

private:
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    const TActorId DataShard;
    const ui64 TxId;
    const TStorageSettings Settings;
    const NBackupRestoreTraits::EDataFormat DataFormat;
    NBackupRestoreTraits::ECompressionCodec CompressionCodec;
    const TTableInfo TableInfo;
    const NKikimrSchemeOp::TTableDescription Scheme;
    const TString LogPrefix_;

    const ui32 Retries;
    ui32 Attempt = 0;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    ui64 TaskId = 0;
    TActorId Client;

    TString ETag;
    ui64 ContentLength = 0;
    ui64 ProcessedBytes = 0;
    ui64 ReadBytes = 0;
    ui64 WrittenBytes = 0;
    ui64 WrittenRows = 0;
    ui64 PendingBytes = 0;
    ui64 PendingRows = 0;
    NKikimrBackup::TS3DownloadState DownloadState;

    const ui32 ReadBatchSize;
    const ui64 ReadBufferSizeLimit;
    THolder<IReadController> Reader;
    TUploadRowsRequestBuilder RequestBuilder;

    NBackup::IChecksum::TPtr Checksum;
    NKikimrBackup::TChecksumState ProcessedChecksumState;
    TString ExpectedChecksum;

    TCounters Counters;

}; // TS3Downloader

template <>
NKikimrSchemeOp::TS3Settings TS3Downloader<NKikimrSchemeOp::TS3Settings>::GetSettings(
    const NKikimrSchemeOp::TRestoreTask& task)
{
    return task.GetS3Settings();
}

template <>
NKikimrSchemeOp::TFSSettings TS3Downloader<NKikimrSchemeOp::TFSSettings>::GetSettings(
    const NKikimrSchemeOp::TRestoreTask& task)
{
    return task.GetFSSettings();
}

IActor* CreateDownloaderBySettingsType(
    const TActorId& dataShard,
    ui64 txId,
    const NKikimrSchemeOp::TRestoreTask& task,
    const TTableInfo& tableInfo)
{
    if (task.HasS3Settings()) {
        return new TS3Downloader<NKikimrSchemeOp::TS3Settings>(
            dataShard, txId, task, tableInfo);
    } else if (task.HasFSSettings()) {
        return new TS3Downloader<NKikimrSchemeOp::TFSSettings>(
            dataShard, txId, task, tableInfo);
    }

    Y_ABORT("Unsupported storage type in restore task");
}

IActor* CreateS3Downloader(const TActorId& dataShard, ui64 txId, const NKikimrSchemeOp::TRestoreTask& task, const TTableInfo& info) {
    return CreateDownloaderBySettingsType(dataShard, txId, task, info);
}

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
