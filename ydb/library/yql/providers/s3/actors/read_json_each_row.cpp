#include "read_json_each_row.h"
#include "yql_s3_actors_util.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/library/yql/providers/s3/json/json_row_parser.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

#include <ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>

#include <zlib.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TS3ReadJsonEachRowActor: " << this->SelfId() << ", TxId: " << Context->TxId << ". " << stream)
#define LOG_W(stream) \
    LOG_WARN_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TS3ReadJsonEachRowActor: " << this->SelfId() << ", TxId: " << Context->TxId << ". " << stream)
#define LOG_I(stream) \
    LOG_INFO_S (*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TS3ReadJsonEachRowActor: " << this->SelfId() << ", TxId: " << Context->TxId << ". " << stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TS3ReadJsonEachRowActor: " << this->SelfId() << ", TxId: " << Context->TxId << ". " << stream)
#define LOG_T(stream) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "TS3ReadJsonEachRowActor: " << this->SelfId() << ", TxId: " << Context->TxId << ". " << stream)

namespace NYql::NDq {

void OnSplitStart(NActors::TActorSystem* actorSystem, const NActors::TActorId& self, CURLcode curlResponseCode, long httpResponseCode) {
    actorSystem->Send(new NActors::IEventHandle(self, NActors::TActorId{}, new TEvS3Provider::TEvDownloadStart(curlResponseCode, httpResponseCode)));
}

void OnSplitData(NActors::TActorSystem* actorSystem, const NActors::TActorId& self, IHTTPGateway::TCountedContent&& data) {
    actorSystem->Send(new NActors::IEventHandle(self, NActors::TActorId{}, new TEvS3Provider::TEvDownloadData(std::move(data))));
}

void OnSplitFinish(NActors::TActorSystem* actorSystem, const NActors::TActorId& self, CURLcode curlResponseCode, TIssues issues) {
    actorSystem->Send(new NActors::IEventHandle(self, NActors::TActorId{}, new TEvS3Provider::TEvDownloadFinish(0, curlResponseCode, std::move(issues))));
}

class TS3ReadJsonEachRowActor : public NActors::TActorBootstrapped<TS3ReadJsonEachRowActor> {

public:
    TS3ReadJsonEachRowActor(TSplitReadContext::TPtr context)
        : Context(context)
        , Parser(Context->SourceContext->Schema) {
    }

    void Bootstrap() {
        if (Context->SplitOffset != 0 || Context->SplitSize != Context->FileSize) {
            TIssues issues{TIssue{TStringBuilder() << "Partial file splits are not supported"}};
            Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::INTERNAL_ERROR));
            return;
        }
        Offset = Context->SplitOffset;
        Size = Context->FileSize;

        LOG_D("Bootstrap");
        Become(&TS3ReadJsonEachRowActor::StateFunc);

        if (Context->SourceContext->Compression && Context->SourceContext->Compression != "gzip") {
            TIssues issues{TIssue{TStringBuilder() << "Unsupported compression \"" << Context->SourceContext->Compression << "\""}};
            Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::BAD_REQUEST));
        } else if (Context->SourceContext->Format != "json_each_row") {
            TIssues issues{TIssue{TStringBuilder() << "Unexpected format \"" << Context->SourceContext->Format << "\" (\"json_each_row\" expected)"}};
            Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::BAD_REQUEST));
        } else {
            if (Context->SourceContext->Compression == "gzip") {
// Cerr << "GZIP INIT" << Endl;
                ProcessData = &TS3ReadJsonEachRowActor::ProcessGzipData;
                FinishData = &TS3ReadJsonEachRowActor::FinishGzip;
                Zero(ZStream);
                if (inflateInit2(&ZStream, 31) != Z_OK) {
                    TIssues issues{TIssue{TStringBuilder() << "Can't initialize ZLIB"}};
                    Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::INTERNAL_ERROR));
                    return;
                }
            } else {
                ProcessData = &TS3ReadJsonEachRowActor::ProcessRawData;
                FinishData = nullptr;
            }
            StartDownload();
        }
    }

    STRICT_STFUNC(StateFunc,
        cFunc(NActors::TEvents::TEvWakeup::EventType, StartDownload);
        hFunc(TEvS3Provider::TEvDownloadStart, Handle);
        hFunc(TEvS3Provider::TEvDownloadData, Handle);
        hFunc(TEvS3Provider::TEvDownloadFinish, Handle);
        cFunc(TEvS3Provider::TEvContinue::EventType, HandleContinue);
        cFunc(NActors::TEvents::TEvPoison::EventType, HandlePoison);
    )

private:
    static constexpr std::string_view TruncatedSuffix = "... [truncated]"sv;
private:
    void StartDownload() {
        Context->CancelHook = Context->Gateway->Download(
            Context->Url,
            Context->Headers,
            0,
            Context->FileSize,
            std::bind(&OnSplitStart,  Context->SourceContext->ActorSystem, SelfId(), std::placeholders::_1, std::placeholders::_2),
            std::bind(&OnSplitData,   Context->SourceContext->ActorSystem, SelfId(), std::placeholders::_1),
            std::bind(&OnSplitFinish, Context->SourceContext->ActorSystem, SelfId(), std::placeholders::_1, std::placeholders::_2),
            Context->SourceContext->HttpInflightSize);
    }

    void Handle(TEvS3Provider::TEvDownloadStart::TPtr& ev) {
        HttpResponseCode = ev->Get()->HttpResponseCode;
        CurlResponseCode = ev->Get()->CurlResponseCode;
        LOG_D("TEvDownloadStart, HTTP code: " << HttpResponseCode << ", CURL code: " << static_cast<i32>(CurlResponseCode));
    }

    void Handle(TEvS3Provider::TEvDownloadData::TPtr& ev) {
        if (Context->SourceContext->HttpDataRps) {
            Context->SourceContext->HttpDataRps->Inc();
        }
        if (HttpResponseCode == 200L || HttpResponseCode == 206L) {
            if (Paused || !DeferredDataParts.empty()) {
                DeferredDataParts.push(std::move(ev->Release()));
                if (Context->SourceContext->DeferredQueueSize) {
                    Context->SourceContext->DeferredQueueSize->Inc();
                }
            } else {
                ExtractDataPart(*ev->Get(), false);
            }
        } else if (HttpResponseCode && !Context->IsCancelled() && !Context->NextRetryDelay) {
            ServerReturnedError = true;
            if (ErrorText.size() < 256_KB)
                ErrorText.append(ev->Get()->Result.Extract());
            else if (!ErrorText.EndsWith(TruncatedSuffix))
                ErrorText.append(TruncatedSuffix);
            LOG_W("TEvDownloadData, ERROR: " << ErrorText << ", Offset: " << Offset);
        }
    }

    void Handle(TEvS3Provider::TEvDownloadFinish::TPtr& ev) {

        if (CurlResponseCode == CURLE_OK) {
            CurlResponseCode = ev->Get()->CurlResponseCode;
        }

        Issues.Clear();
        if (!ErrorText.empty()) {
            TString errorCode;
            TString message;
            if (!ParseS3ErrorResponse(ErrorText, errorCode, message)) {
                message = ErrorText;
            }
            Issues.AddIssues(BuildIssues(HttpResponseCode, errorCode, message));
        }

        if (ev->Get()->Issues) {
            Issues.AddIssues(ev->Get()->Issues);
        }

        if (HttpResponseCode >= 300) {
            ServerReturnedError = true;
            Issues.AddIssue(TIssue{TStringBuilder() << "HTTP error code: " << HttpResponseCode});
        }

        if (Issues) {
            Context->NextRetryDelay = Context->GetRetryState()->GetNextRetryDelay(CurlResponseCode, HttpResponseCode);
            LOG_D("TEvDownloadFinish with Issues (try to retry): " << Issues.ToOneLineString());
            if (Context->NextRetryDelay) {
                // inplace retry: report problem to TransientIssues and repeat
                // Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvAsyncInputError(InputIndex, Issues, NYql::NDqProto::StatusIds::UNSPECIFIED));
            } else {
                // can't retry here: fail download
                Context->RetryState = nullptr;
                // InputFinished = true;
                // LOG_W("ReadError: " << Issues.ToOneLineString() << ", Offset: " << Offset);
                // throw TS3ReadError(); // Don't pass control to data parsing, because it may validate eof and show wrong issues about incorrect data format
            }
        }

        if (!Context->IsCancelled() && Context->NextRetryDelay && Size > 0ULL) {
            Schedule(*Context->NextRetryDelay, new NActors::TEvents::TEvWakeup());
            if (DeferredDataParts.size()) {
                if (Context->SourceContext->DeferredQueueSize) {
                    Context->SourceContext->DeferredQueueSize->Sub(DeferredDataParts.size());
                }
                std::queue<THolder<TEvS3Provider::TEvDownloadData>> tmp;
                DeferredDataParts.swap(tmp);
            }
        } else {
            LOG_D("TEvDownloadFinish, Offset: " << Offset << ", Error: " << ServerReturnedError);
            if (ServerReturnedError) {
            //     throw TS3ReadError(); // Don't pass control to data parsing, because it may validate eof and show wrong issues about incorrect data format
            } else {
                // Cerr << "JER finished" << Endl;
                if (FinishData) {
                    (this->*FinishData)();
                }
                Send(Context->SourceContext->SourceId, new TEvS3Provider::TEvFileFinished(Context->PathIndex, TakeIngressDelta(), TakeCpuTimeDelta(), Context->FileSize));
            }
        }
    }

    void HandleContinue() {
        LOG_D("TEvContinue");
        Paused = false;
        if (!Paused && !DeferredDataParts.empty()) {
            ExtractDataPart(*DeferredDataParts.front(), true);
            DeferredDataParts.pop();
            if (Context->SourceContext->DeferredQueueSize) {
                Context->SourceContext->DeferredQueueSize->Dec();
            }
        }
    }

    void HandlePoison() {
        LOG_D("TEvPoison");
        Context->Cancel();
        PassAway();
    }

    void ExtractDataPart(TEvS3Provider::TEvDownloadData& event, bool deferred = false) {
        auto data = event.Result.Extract();
        IngressBytes += data.size();
        Offset += data.size();
        Size -= data.size();
        LOG_T("TEvDownloadData (" << (deferred ? "deferred" : "instant") << "), size: " << data.size());
        // Send(ComputeActorId, new IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(InputIndex));
        (this->*ProcessData)(data);
    }

    void ProcessGzipData(const TString& data) {
        ZStream.next_in = const_cast<unsigned char*>(reinterpret_cast<const unsigned char*>(data.data()));
        ZStream.avail_in = data.size();
        while (ZStream.avail_in) {
            TString output;
            output.resize(1_MB);
            ZStream.next_out = const_cast<unsigned char*>(reinterpret_cast<const unsigned char*>(output.data()));
            ZStream.avail_out = output.size();
            switch (const auto code = inflate(&ZStream, Z_SYNC_FLUSH)) {
                case Z_NEED_DICT: 
                {
                    TIssues issues{TIssue{TStringBuilder() << "Can't initialize ZLIB"}};
                    Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::BAD_REQUEST));
                }
                    return;
                case Z_OK:
                    output.resize(1_MB - ZStream.avail_out);
// Cerr << "GZIP OUTPUT " << output << Endl;
                    ProcessRawData(output);
                    break;
                case Z_STREAM_END:
                    output.resize(1_MB - ZStream.avail_out);
// Cerr << "GZIP FINAL " << output << Endl;
                    ProcessRawData(output);
                    return;
                default:
                {
                    TIssues issues{TIssue{TStringBuilder() << "Unexpected ZLIB code " << code}};
                    Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::BAD_REQUEST));
                }
                    return;
            }
        }
    }

    void FinishGzip() {
        if (inflateReset(&ZStream) != Z_OK) {
            TIssues issues{TIssue{TStringBuilder() << "Inflate reset error: " << (ZStream.msg ? ZStream.msg : "Unknown error.")}};
            Send(Context->SourceContext->SourceId, new NYql::NDq::IDqComputeActorAsyncInput::TEvAsyncInputError(0, issues, NYql::NDqProto::StatusIds::BAD_REQUEST));
        }
    }

    void ProcessRawData(const TString& data) {
        ParseRawData(data);

// Cerr << "JER take batch" << Endl;
        auto batch = Parser.TakeBatch();
        auto size = NUdf::GetSizeOfArrowBatchInBytes(*batch);
// Cerr << "JER got batch of " << size << "bytes" << Endl;
        Paused = Context->SourceContext->Add(size, SelfId(), Paused);
        Send(Context->SourceContext->SourceId, new TEvS3Provider::TEvNextRecordBatch(
            batch, Context->PathIndex, TakeIngressDelta(), TakeCpuTimeDelta()
        ));
    }

    void ParseRawData(const TString& data) {
// Cerr << "JER Parse " << data.size() << " bytes" << Endl;
        if (data.empty()) {
            return;
        }

        auto n = data.find('\n');
        if (n == data.npos) {
// Cerr << "JER no EOL" << Endl;
            // no EOL found - just wait for next data
            PreviousData += data;
            return;
        }

// Cerr << "JER found EOL at " << n << Endl;

        if (PreviousData) {
            TString mergedLine;
            mergedLine.reserve(PreviousData.size() - PreviousPos + n);
            std::copy(PreviousData.cbegin() + PreviousPos, PreviousData.cend(), std::back_inserter(mergedLine));
            std::copy(data.cbegin(), data.cbegin() + n, std::back_inserter(mergedLine));
            Parser.ParseNextRow(mergedLine);
            PreviousData.clear();
            PreviousPos = 0;
        } else {
            Parser.ParseNextRow(TStringBuf(data.begin(), n));
        }

        while (true) {
            auto n1 = data.find('\n', n + 1);
            if (n1 == data.npos) {
                PreviousData = data;
                PreviousPos = n + 1;
                return;
            }
// Cerr << "JER found EOL at " << n1 << Endl;
            Parser.ParseNextRow(TStringBuf(data.begin() + n + 1, n1 - n - 1));
            if (n1 + 1 == data.size())
            {
                // end of data
                return;
            }
            n = n1;
        }
    }

    ui64 TakeIngressDelta() {
        auto currentIngressBytes = IngressBytes;
        IngressBytes = 0;
        return currentIngressBytes;
    }

    TDuration TakeCpuTimeDelta() {
        auto currentCpuTime = CpuTime;
        CpuTime = TDuration::Zero();
        return currentCpuTime;
    }

    TSplitReadContext::TPtr Context;
    std::size_t Offset = 0;
    std::size_t Size = 0;
    NYql::NJson::TJsonRowParser Parser;
    long HttpResponseCode = 0L;
    CURLcode CurlResponseCode = CURLE_OK;
    bool Paused = false;
    bool ServerReturnedError = false;
    TString ErrorText;
    TIssues Issues;
    std::queue<THolder<TEvS3Provider::TEvDownloadData>> DeferredDataParts;
    ui64 IngressBytes = 0;
    TDuration CpuTime;
    TString PreviousData;
    std::size_t PreviousPos = 0;

    void (TS3ReadJsonEachRowActor::*ProcessData)(const TString& data) = nullptr;
    void (TS3ReadJsonEachRowActor::*FinishData)() = nullptr;
    z_stream ZStream;
};

NActors::IActor* CreateS3ReadJsonEachRowActor(TSplitReadContext::TPtr context) {
    return new TS3ReadJsonEachRowActor(context);
}

} // namespace NYql::NDq
