#ifndef KIKIMR_DISABLE_S3_OPS

#include "datashard_impl.h"
#include "import_common.h"
#include "import_s3.h"
#include "s3_common.h"

#include <ydb/core/base/appdata.h> 
#include <ydb/core/protos/flat_scheme_op.pb.h> 
#include <ydb/core/protos/services.pb.h> 
#include <ydb/core/tablet/resource_broker.h> 
#include <ydb/core/wrappers/s3_wrapper.h> 
#include <ydb/core/io_formats/csv.h> 
#include <ydb/public/lib/scheme_types/scheme_type_id.h> 

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NDataShard { 

using namespace NResourceBroker;
using namespace NWrappers;

using namespace Aws::S3;
using namespace Aws;

class TS3Downloader: public TActorBootstrapped<TS3Downloader>, private TS3User {
    class TReadController {
        static constexpr ui64 SumWithSaturation(ui64 a, ui64 b) {
            return Max<ui64>() - a < b ? Max<ui64>() : a + b;
        }

    public:
        explicit TReadController(ui32 rangeSize)
            : RangeSize(rangeSize)
        {
        }

        std::pair<ui64, ui64> NextRange(ui64 contentLength, ui64 processedBytes) const {
            Y_VERIFY(contentLength > 0);
            Y_VERIFY(processedBytes < contentLength);

            const ui64 start = processedBytes + (Buffer.size() - Pos);
            const ui64 end = Min(SumWithSaturation(start, RangeSize), contentLength) - 1;
            return std::make_pair(start, end);
        }

        bool Feed(TString&& portion, TStringBuf& buf) {
            if (Buffer && Pos) {
                Buffer.remove(0, Pos);
            }

            if (!Buffer) {
                Buffer = std::move(portion);
            } else {
                Buffer.append(portion);
            }

            Pos = Buffer.rfind('\n');
            if (TString::npos == Pos) {
                return false;
            }

            Pos += 1; // for '\n'
            buf = TStringBuf(Buffer.data(), Pos);

            return true;
        }

    private:
        const ui32 RangeSize;

        TString Buffer;
        ui64 Pos = 0;

    }; // TReadController

    class TUploadRowsRequestBuilder {
    public:
        void New(const TTableInfo& tableInfo, const NKikimrSchemeOp::TTableDescription& scheme) {
            Request.Reset(new TEvDataShard::TEvUnsafeUploadRowsRequest());
            Request->Record.SetTableId(tableInfo.GetId());

            TVector<TString> columnNames;
            for (const auto& column : scheme.GetColumns()) {
                columnNames.push_back(column.GetName());
            }

            auto& rowScheme = *Request->Record.MutableRowScheme();
            for (ui32 id : tableInfo.GetKeyColumnIds()) {
                rowScheme.AddKeyColumnIds(id);
            }
            for (ui32 id : tableInfo.GetValueColumnIds(columnNames)) {
                rowScheme.AddValueColumnIds(id);
            }
        }

        void AddRow(const TVector<TCell>& keys, const TVector<TCell>& values) {
            Y_VERIFY(Request);
            auto& row = *Request->Record.AddRows();
            row.SetKeyColumns(TSerializedCellVec::Serialize(keys));
            row.SetValueColumns(TSerializedCellVec::Serialize(values));
        }

        const NKikimrTxDataShard::TEvUploadRowsRequest& GetRecord() const {
            Y_VERIFY(Request);
            return Request->Record;
        }

        THolder<TEvDataShard::TEvUnsafeUploadRowsRequest> Build() {
            Y_VERIFY(Request);
            return std::move(Request);
        }

    private:
        THolder<TEvDataShard::TEvUnsafeUploadRowsRequest> Request;

    }; // TUploadRowsRequestBuilder

    void AllocateResource() {
        IMPORT_LOG_D("AllocateResource"
            << ": self# " << SelfId());

        const auto* appData = AppData();
        Send(MakeResourceBrokerID(), new TEvResourceBroker::TEvSubmitTask(
            TStringBuilder() << "Restore { " << TxId << ":" << DataShard << " }",
            {{ 1, 0 }},
            appData->DataShardConfig.GetRestoreTaskName(),
            appData->DataShardConfig.GetRestoreTaskPriority(),
            nullptr
        ));

        Become(&TThis::StateAllocateResource);
    }

    void Handle(TEvResourceBroker::TEvResourceAllocated::TPtr& ev) {
        IMPORT_LOG_D("Handle TEvResourceBroker::TEvResourceAllocated"
            << ": self# " << SelfId()
            << ", taskId# " << ev->Get()->TaskId);

        TaskId = ev->Get()->TaskId;
        Restart();
    }

    void Restart() {
        IMPORT_LOG_D("Restart"
            << ": self# " << SelfId()
            << ", attempt# " << Attempt);

        if (const TActorId client = std::exchange(Client, TActorId())) {
            Send(client, new TEvents::TEvPoisonPill());
        }

        Client = RegisterWithSameMailbox(CreateS3Wrapper(Settings.Credentials, Settings.Config));

        HeadObject(Settings.DataKey);
        Become(&TThis::StateWork);
    }

    void HeadObject(const TString& key) {
        IMPORT_LOG_D("HeadObject"
            << ": self# " << SelfId()
            << ", key# " << key);

        auto request = Model::HeadObjectRequest()
            .WithBucket(Settings.Bucket)
            .WithKey(key);

        Send(Client, new TEvS3Wrapper::TEvHeadObjectRequest(request));
    }

    void GetObject(const TString& key, const std::pair<ui64, ui64>& range) {
        IMPORT_LOG_D("GetObject"
            << ": self# " << SelfId()
            << ", key# " << key
            << ", range# " << range.first << "-" << range.second);

        auto request = Model::GetObjectRequest()
            .WithBucket(Settings.Bucket)
            .WithKey(key)
            .WithRange(TStringBuilder() << "bytes=" << range.first << "-" << range.second);

        Send(Client, new TEvS3Wrapper::TEvGetObjectRequest(request));
    }

    void Handle(TEvS3Wrapper::TEvHeadObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;

        IMPORT_LOG_D("Handle TEvS3Wrapper::TEvHeadObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        if (!CheckResult(result, TStringBuf("HeadObject"))) {
            return;
        }

        ETag = result.GetResult().GetETag();
        ContentLength = result.GetResult().GetContentLength();

        Send(DataShard, new TEvDataShard::TEvGetS3DownloadInfo(SelfId(), TxId));
    }

    void Handle(TEvDataShard::TEvS3DownloadInfo::TPtr& ev) {
        const auto& info = ev->Get()->Info;

        IMPORT_LOG_D("Handle TEvDataShard::TEvS3DownloadInfo"
            << ": self# " << SelfId()
            << ", info# " << info.ToString());

        if (!info.DataETag) {
            Send(DataShard, new TEvDataShard::TEvStoreS3DownloadInfo(SelfId(),
                TxId, ETag, ProcessedBytes, WrittenBytes, WrittenRows));
            return;
        }

        if (!CheckETag(*info.DataETag, ETag, TStringBuf("DownloadInfo"))) {
            return;
        }

        ProcessedBytes = info.ProcessedBytes;
        WrittenBytes = info.WrittenBytes;
        WrittenRows = info.WrittenRows;

        if (!ContentLength || ProcessedBytes >= ContentLength) {
            return Finish();
        }

        GetObject(Settings.DataKey, Reader.NextRange(ContentLength, ProcessedBytes));
    }

    void Handle(TEvS3Wrapper::TEvGetObjectResponse::TPtr& ev) {
        auto& msg = *ev->Get();
        const auto& result = msg.Result;

        IMPORT_LOG_D("Handle TEvS3Wrapper::TEvGetObjectResponse"
            << ": self# " << SelfId()
            << ", result# " << result);

        const TStringBuf marker = "GetObject";

        if (!CheckResult(result, marker)) {
            return;
        }

        if (!CheckETag(ETag, result.GetResult().c_str(), marker)) {
            return;
        }

        IMPORT_LOG_T("Content size"
            << ": self# " << SelfId()
            << ", processed-bytes# " << ProcessedBytes
            << ", content-length# " << ContentLength
            << ", body-size# " << msg.Body.size());

        if (!Reader.Feed(std::move(msg.Body), Buffer)) {
            return Finish(false, "Cannot find new line symbol in data");
        }

        ProcessedBytes += Buffer.size();
        RequestBuilder.New(TableInfo, Scheme);

        TMemoryPool pool(256);
        while (ProcessData(pool));
    }

    bool ProcessData(TMemoryPool& pool) {
        pool.Clear();

        TStringBuf line = Buffer.NextTok('\n');
        const TStringBuf origLine = line;

        if (!line) {
            if (Buffer) {
                return true; // skip empty line
            }

            const auto& record = RequestBuilder.GetRecord();
            WrittenRows += record.RowsSize();

            IMPORT_LOG_D("Upload rows"
                << ": self# " << SelfId()
                << ", count# " << record.RowsSize()
                << ", size# " << record.ByteSizeLong());

            Send(DataShard, RequestBuilder.Build().Release());
            return false;
        }

        std::vector<std::pair<i32, ui32>> columnOrderTypes; // {keyOrder, PType}
        columnOrderTypes.reserve(Scheme.GetColumns().size());

        for (const auto& column : Scheme.GetColumns()) {
            columnOrderTypes.emplace_back(TableInfo.KeyOrder(column.GetName()), column.GetTypeId());
        }

        TVector<TCell> keys;
        TVector<TCell> values;
        TString strError;

        if (!NFormats::TYdbDump::ParseLine(line, columnOrderTypes, pool, keys, values, strError, WrittenBytes)) {
            Finish(false, TStringBuilder() << strError << " on line: " << origLine);
            return false;
        }

        Y_VERIFY(!keys.empty());
        if (!TableInfo.IsMyKey(keys) /* TODO: maybe skip */) {
            Finish(false, TStringBuilder() << "Key is out of range on line: " << origLine);
            return false;
        }

        RequestBuilder.AddRow(keys, values);
        return true;
    }

    void Handle(TEvDataShard::TEvUnsafeUploadRowsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        IMPORT_LOG_D("Handle TEvDataShard::TEvUnsafeUploadRowsResponse"
            << ": self# " << SelfId()
            << ", record# " << record.ShortDebugString());

        switch (record.GetStatus()) {
        case NKikimrTxDataShard::TError::OK:
            break;

        case NKikimrTxDataShard::TError::SCHEME_ERROR:
        case NKikimrTxDataShard::TError::BAD_ARGUMENT:
            return Finish(false, record.GetErrorDescription());

        default:
            return RetryOrFinish(record.GetErrorDescription());
        };

        Send(DataShard, new TEvDataShard::TEvStoreS3DownloadInfo(SelfId(),
            TxId, ETag, ProcessedBytes, WrittenBytes, WrittenRows));
    }

    template <typename TResult>
    bool CheckResult(const TResult& result, const TStringBuf marker) {
        if (result.IsSuccess()) {
            return true;
        }

        IMPORT_LOG_E("Error at '" << marker << "'"
            << ": self# " << SelfId()
            << ", error# " << result);
        RetryOrFinish(result.GetError().GetMessage().c_str());

        return false;
    }

    bool CheckETag(const TString& expected, const TString& got, const TStringBuf marker) {
        if (expected == got) {
            return true;
        }

        const TString error = TStringBuilder() << "ETag mismatch at '" << marker << "'"
            << ": self# " << SelfId()
            << ", expected# " << expected
            << ", got# " << got;

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
                return finish(TStringBuilder() << "Scheme mismatch: cannot find column"
                    << ": name# " << column.GetName());
            }

            const NScheme::TTypeId type = TableInfo.GetColumnType(column.GetName());
            if (type != static_cast<NScheme::TTypeId>(column.GetTypeId())) {
                return finish(TStringBuilder() << "Scheme mismatch: column type mismatch"
                    << ": name# " << column.GetName()
                    << ", expected# " << type
                    << ", got# " << static_cast<NScheme::TTypeId>(column.GetTypeId()));
            }
        }

        if (TableInfo.GetKeyColumnIds().size() != (ui32)Scheme.KeyColumnNamesSize()) {
            return finish(TStringBuilder() << "Scheme mismatch: key column count mismatch"
                << ": expected# " << TableInfo.GetKeyColumnIds().size()
                << ", got# " << Scheme.KeyColumnNamesSize());
        }

        for (ui32 i = 0; i < (ui32)Scheme.KeyColumnNamesSize(); ++i) {
            const auto& name = Scheme.GetKeyColumnNames(i);
            const ui32 keyOrder = TableInfo.KeyOrder(name);

            if (keyOrder != i) {
                return finish(TStringBuilder() << "Scheme mismatch: key order mismatch"
                    << ": name# " << name
                    << ", expected# " << keyOrder
                    << ", got# " << i);
            }
        }

        return true;
    }

    void RetryOrFinish(const TString& error) {
        if (Attempt++ < Retries) {
            Delay = Min(Delay * Attempt, MaxDelay);
            const TDuration random = TDuration::FromValue(TAppData::RandomProvider->GenRand64() % Delay.MicroSeconds());

            Schedule(Delay + random, new TEvents::TEvWakeup());
        } else {
            Finish(false, error);
        }
    }

    void Finish(bool success = true, const TString& error = TString()) {
        IMPORT_LOG_I("Finish"
            << ": self# " << SelfId()
            << ", success# " << success
            << ", error# " << error
            << ", writtenBytes# " << WrittenBytes
            << ", writtenRows# " << WrittenRows);

        TAutoPtr<IDestructable> prod = new TImportJobProduct(success, error, WrittenBytes, WrittenRows);
        Send(DataShard, new TDataShard::TEvPrivate::TEvAsyncJobComplete(prod), 0, TxId); 

        Y_VERIFY(TaskId);
        Send(MakeResourceBrokerID(), new TEvResourceBroker::TEvFinishTask(TaskId));

        PassAway();
    }

    void NotifyDied() {
        Send(MakeResourceBrokerID(), new TEvResourceBroker::TEvNotifyActorDied());
        PassAway();
    }

    void PassAway() override {
        if (Client) {
            Send(Client, new TEvents::TEvPoisonPill());
        }

        TActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IMPORT_S3_DOWNLOADER_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "s3";
    }

    explicit TS3Downloader(const TActorId& dataShard, ui64 txId, const NKikimrSchemeOp::TRestoreTask& task, const TTableInfo& tableInfo)
        : DataShard(dataShard)
        , TxId(txId)
        , Settings(TS3Settings::FromRestoreTask(task))
        , TableInfo(tableInfo)
        , Scheme(task.GetTableDescription())
        , Retries(task.GetNumberOfRetries())
        , Reader(task.GetS3Settings().GetLimits().GetReadBatchSize())
    {
    }

    void Bootstrap() {
        IMPORT_LOG_D("Bootstrap"
            << ": self# " << SelfId()
            << ", attempt# " << Attempt);

        if (!CheckScheme()) {
            return;
        }

        AllocateResource();
    }

    STATEFN(StateAllocateResource) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvResourceBroker::TEvResourceAllocated, Handle);
            cFunc(TEvents::TEvPoisonPill::EventType, NotifyDied);
        }
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvS3Wrapper::TEvHeadObjectResponse, Handle);
            hFunc(TEvS3Wrapper::TEvGetObjectResponse, Handle);

            hFunc(TEvDataShard::TEvS3DownloadInfo, Handle);
            hFunc(TEvDataShard::TEvUnsafeUploadRowsResponse, Handle);

            cFunc(TEvents::TEvWakeup::EventType, Restart);
            cFunc(TEvents::TEvPoisonPill::EventType, NotifyDied);
        }
    }

private:
    const TActorId DataShard;
    const ui64 TxId;
    const TS3Settings Settings;
    const TTableInfo TableInfo;
    const NKikimrSchemeOp::TTableDescription Scheme;

    const ui32 Retries;
    ui32 Attempt = 0;

    TDuration Delay = TDuration::Minutes(1);
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    ui64 TaskId = 0;
    TActorId Client;

    TString ETag;
    ui64 ContentLength = 0;
    ui64 ProcessedBytes = 0;
    ui64 WrittenBytes = 0;
    ui64 WrittenRows = 0;

    TReadController Reader;
    TStringBuf Buffer;
    TUploadRowsRequestBuilder RequestBuilder;

}; // TS3Downloader

IActor* CreateS3Downloader(const TActorId& dataShard, ui64 txId, const NKikimrSchemeOp::TRestoreTask& task, const TTableInfo& info) {
    return new TS3Downloader(dataShard, txId, task, info);
}

} // NDataShard 
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
