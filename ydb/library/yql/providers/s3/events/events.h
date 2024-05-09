#pragma once

#include <ydb/core/base/events.h>

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/providers/s3/proto/file_queue.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/Block.h>

#include <arrow/api.h>

namespace NYql::NDq {

struct TEvS3Provider {

    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_S3_PROVIDER),
        // Lister events
        EvUpdateConsumersCount = EvBegin,
        EvAck,
        EvGetNextBatch,
        EvObjectPathBatch,
        EvObjectPathReadError,
        // Reader events
        EvReadResult,
        EvDataPart,
        EvReadStarted,
        EvReadFinished,
        EvReadError,
        EvRetry,
        EvNextBlock,
        EvNextRecordBatch,
        EvFileFinished,
        EvContinue,
        EvReadResult2,
        // Writer events
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_S3_PROVIDER), "expect EvEnd < EventSpaceEnd(TEvents::ES_S3_PROVIDER)");
    
    struct TEvUpdateConsumersCount :
        public NActors::TEventPB<TEvUpdateConsumersCount, NS3::FileQueue::TEvUpdateConsumersCount, EvUpdateConsumersCount> {
        
        explicit TEvUpdateConsumersCount(ui64 consumersCountDelta = 0) {
            Record.SetConsumersCountDelta(consumersCountDelta);
        }
    };

    struct TEvAck :
        public NActors::TEventPB<TEvAck, NS3::FileQueue::TEvAck, EvAck> {
        
        TEvAck() = default;

        explicit TEvAck(const NDqProto::TMessageTransportMeta& transportMeta) {
            Record.MutableTransportMeta()->CopyFrom(transportMeta);
        }
    };

    struct TEvGetNextBatch :
        public NActors::TEventPB<TEvGetNextBatch, NS3::FileQueue::TEvGetNextBatch, EvGetNextBatch> {
    };

    struct TEvObjectPathBatch :
        public NActors::TEventPB<TEvObjectPathBatch, NS3::FileQueue::TEvObjectPathBatch, EvObjectPathBatch> {

        TEvObjectPathBatch() {
            Record.SetNoMoreFiles(false);
        }

        TEvObjectPathBatch(std::vector<NS3::FileQueue::TObjectPath> objectPaths, bool noMoreFiles, const NDqProto::TMessageTransportMeta& transportMeta) {
            Record.MutableObjectPaths()->Assign(
                std::make_move_iterator(objectPaths.begin()),
                std::make_move_iterator(objectPaths.end()));
            Record.SetNoMoreFiles(noMoreFiles);
            Record.MutableTransportMeta()->CopyFrom(transportMeta);
        }
    };

    struct TEvObjectPathReadError :
        public NActors::TEventPB<TEvObjectPathReadError, NS3::FileQueue::TEvObjectPathReadError, EvObjectPathReadError> {

        TEvObjectPathReadError() = default;

        TEvObjectPathReadError(TIssues issues, const NDqProto::TMessageTransportMeta& transportMeta) {
            NYql::IssuesToMessage(issues, Record.MutableIssues());
            Record.MutableTransportMeta()->CopyFrom(transportMeta);
        }
    };

    struct TEvReadResult : public NActors::TEventLocal<TEvReadResult, EvReadResult> {
        TEvReadResult(
            IHTTPGateway::TContent&& result,
            const TString& requestId,
            size_t pathInd,
            TString path)
            : Result(std::move(result))
            , RequestId(requestId)
            , PathIndex(pathInd)
            , Path(std::move(path)) { }

        IHTTPGateway::TContent Result;
        const TString RequestId;
        const size_t PathIndex;
        const TString Path;
    };

    struct TEvDataPart : public NActors::TEventLocal<TEvDataPart, EvDataPart> {
        TEvDataPart(IHTTPGateway::TCountedContent&& data) : Result(std::move(data)) {}
        IHTTPGateway::TCountedContent Result;
    };

    struct TEvReadStarted : public NActors::TEventLocal<TEvReadStarted, EvReadStarted> {
        TEvReadStarted(CURLcode curlResponseCode, long httpResponseCode)
            : CurlResponseCode(curlResponseCode), HttpResponseCode(httpResponseCode) {}
        const CURLcode CurlResponseCode;
        const long HttpResponseCode;
    };

    struct TEvReadFinished : public NActors::TEventLocal<TEvReadFinished, EvReadFinished> {
        TEvReadFinished(size_t pathIndex, CURLcode curlResponseCode, TIssues&& issues)
            : PathIndex(pathIndex), CurlResponseCode(curlResponseCode), Issues(std::move(issues)) {
        }
        const size_t PathIndex;
        const CURLcode CurlResponseCode;
        TIssues Issues;
    };

    struct TEvFileFinished : public NActors::TEventLocal<TEvFileFinished, EvFileFinished> {
        TEvFileFinished(size_t pathIndex, ui64 ingressDelta, TDuration cpuTimeDelta)
            : PathIndex(pathIndex), IngressDelta(ingressDelta), CpuTimeDelta(cpuTimeDelta) {
        }
        const size_t PathIndex;
        const ui64 IngressDelta;
        const TDuration CpuTimeDelta;
    };

    struct TEvReadError : public NActors::TEventLocal<TEvReadError, EvReadError> {
        TEvReadError(
            TIssues&& error,
            const TString& requestId,
            size_t pathInd,
            TString path)
            : Error(std::move(error))
            , RequestId(requestId)
            , PathIndex(pathInd)
            , Path(std::move(path)) { }

        const TIssues Error;
        const TString RequestId;
        const size_t PathIndex;
        const TString Path;
    };

    struct TEvRetryEventFunc : public NActors::TEventLocal<TEvRetryEventFunc, EvRetry> {
        explicit TEvRetryEventFunc(std::function<void()> functor) : Functor(std::move(functor)) {}
        const std::function<void()> Functor;
    };

    struct TEvNextBlock : public NActors::TEventLocal<TEvNextBlock, EvNextBlock> {
        TEvNextBlock(NDB::Block& block, size_t pathInd, ui64 ingressDelta, TDuration cpuTimeDelta)
            : PathIndex(pathInd), IngressDelta(ingressDelta), CpuTimeDelta(cpuTimeDelta) {
            Block.swap(block);
        }
        NDB::Block Block;
        const size_t PathIndex;
        const ui64 IngressDelta;
        const TDuration CpuTimeDelta;
    };

    struct TEvNextRecordBatch : public NActors::TEventLocal<TEvNextRecordBatch, EvNextRecordBatch> {
        TEvNextRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch, size_t pathInd, ui64 ingressDelta, TDuration cpuTimeDelta)
            : Batch(batch), PathIndex(pathInd), IngressDelta(ingressDelta), CpuTimeDelta(cpuTimeDelta) {
        }
        std::shared_ptr<arrow::RecordBatch> Batch;
        const size_t PathIndex;
        const ui64 IngressDelta;
        const TDuration CpuTimeDelta;
    };

    struct TEvContinue : public NActors::TEventLocal<TEvContinue, EvContinue> {
    };

    struct TReadRange {
        int64_t Offset;
        int64_t Length;
    };

    struct TEvReadResult2 : public NActors::TEventLocal<TEvReadResult2, EvReadResult2> {
        TEvReadResult2(TReadRange readRange, IHTTPGateway::TContent&& result) : ReadRange(readRange), Failure(false), Result(std::move(result)) { }
        TEvReadResult2(TReadRange readRange, TIssues&& issues) : ReadRange(readRange), Failure(true), Result(""), Issues(std::move(issues)) { }
        const TReadRange ReadRange;
        const bool Failure;
        IHTTPGateway::TContent Result;
        const TIssues Issues;
    };
};

} // namespace NYql::NDq
