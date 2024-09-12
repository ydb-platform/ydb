#pragma once

#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/providers/s3/proto/file_queue.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/Block.h>

#include <ydb/core/kqp/common/kqp_tx.h>

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
        // Gateway events
        EvReadResult,      // non-streaming download result
        EvDownloadStart,   // streaming started
        EvDownloadData,    // streaming data (part of)
        EvDownloadFinish,  // streaming finished
        // Reader events
        EvReadError,
        EvRetry,
        EvNextBlock,
        EvNextRecordBatch,
        EvFileFinished,
        EvContinue,
        EvReadResult2, // merge with EvReadResult and rename to EvDownloadResult
        // Writer events
        // Cache events
        EvCacheSourceStart,
        EvCacheCheckRequest,
        EvCacheCheckResult,
        EvCachePutRequest,
        EvCacheNotification,
        EvCacheSourceFinish,
        // Decompressor events
        EvDecompressDataRequest,
        EvDecompressDataResult,
        EvDecompressDataFinish,
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

    struct TEvDownloadStart : public NActors::TEventLocal<TEvDownloadStart, EvDownloadStart> {
        TEvDownloadStart(CURLcode curlResponseCode, long httpResponseCode)
            : CurlResponseCode(curlResponseCode), HttpResponseCode(httpResponseCode) {}
        const CURLcode CurlResponseCode;
        const long HttpResponseCode;
    };

    struct TEvDownloadData : public NActors::TEventLocal<TEvDownloadData, EvDownloadData> {
        TEvDownloadData(IHTTPGateway::TCountedContent&& data) : Result(std::move(data)) {}
        IHTTPGateway::TCountedContent Result;
    };

    struct TEvDownloadFinish : public NActors::TEventLocal<TEvDownloadFinish, EvDownloadFinish> {
        TEvDownloadFinish(size_t pathIndex, CURLcode curlResponseCode, TIssues&& issues)
            : PathIndex(pathIndex), CurlResponseCode(curlResponseCode), Issues(std::move(issues)) {
        }
        const size_t PathIndex;
        const CURLcode CurlResponseCode;
        TIssues Issues;
    };

    struct TEvFileFinished : public NActors::TEventLocal<TEvFileFinished, EvFileFinished> {
        TEvFileFinished(size_t pathIndex, ui64 ingressDelta, TDuration cpuTimeDelta, ui64 splitSize)
            : PathIndex(pathIndex), IngressDelta(ingressDelta), CpuTimeDelta(cpuTimeDelta), SplitSize(splitSize) {
        }
        const size_t PathIndex;
        const ui64 IngressDelta;
        const TDuration CpuTimeDelta;
        const ui64 SplitSize;
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
        TEvNextBlock(NDB::Block& block, size_t pathInd, ui64 ingressDelta, TDuration cpuTimeDelta, ui64 ingressDecompressedDelta = 0)
            : PathIndex(pathInd), IngressDelta(ingressDelta), CpuTimeDelta(cpuTimeDelta), IngressDecompressedDelta(ingressDecompressedDelta) {
            Block.swap(block);
        }
        NDB::Block Block;
        const size_t PathIndex;
        const ui64 IngressDelta;
        const TDuration CpuTimeDelta;
        const ui64 IngressDecompressedDelta;
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

    struct TEvDecompressDataRequest : public NActors::TEventLocal<TEvDecompressDataRequest, EvDecompressDataRequest> {
        TEvDecompressDataRequest(TString&& data) : Data(std::move(data)) {}
        TString Data;
    };

    struct TEvDecompressDataResult : public NActors::TEventLocal<TEvDecompressDataResult, EvDecompressDataResult> {
        TEvDecompressDataResult(TString&& data) : Data(std::move(data)) {}
        TEvDecompressDataResult(std::exception_ptr exception) : Exception(exception) {}
        TString Data;
        std::exception_ptr Exception;
    };

    struct TEvDecompressDataFinish : public NActors::TEventLocal<TEvDecompressDataFinish, EvDecompressDataFinish> {
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

    struct TEvCacheSourceStart : public NActors::TEventLocal<TEvCacheSourceStart, EvCacheSourceStart> {

        TEvCacheSourceStart(NActors::TActorId sourceId, const TTxId& txId, std::shared_ptr<arrow::Schema> schema)
            : SourceId(sourceId), TxId(txId), Schema(schema) {
        }
 
        NActors::TActorId SourceId;
        TTxId TxId;
        std::shared_ptr<arrow::Schema> Schema;
    };

    struct TEvCacheCheckRequest : public NActors::TEventLocal<TEvCacheCheckRequest, EvCacheCheckRequest> {

        TEvCacheCheckRequest(NActors::TActorId sourceId, const TString& path, ui64 rowGroup)
            : SourceId(sourceId), Path(path), RowGroup(rowGroup) {
        }

        NActors::TActorId SourceId;
        TString Path;
        const ui64 RowGroup;
    };

    struct TEvCacheCheckResult : public NActors::TEventLocal<TEvCacheCheckResult, EvCacheCheckResult> {
        TString Path;
        ui64 RowGroup;
        std::vector<std::shared_ptr<arrow::RecordBatch>> Batches;
        bool Hit = false;
    };

    struct TEvCachePutRequest : public NActors::TEventLocal<TEvCachePutRequest, EvCachePutRequest> {
        NActors::TActorId SourceId;
        TString Path;
        ui64 RowGroup = 0;
        std::vector<std::shared_ptr<arrow::RecordBatch>> Batches;
    };

    struct TEvCacheNotification : public NActors::TEventLocal<TEvCacheNotification, EvCacheNotification> {
        TString Path;
        ui64 RowGroup = 0;
        std::vector<std::shared_ptr<arrow::RecordBatch>> Batches;
    };

    struct TEvCacheSourceFinish : public NActors::TEventLocal<TEvCacheSourceFinish, EvCacheSourceFinish> {

        TEvCacheSourceFinish(NActors::TActorId sourceId)
            : SourceId(sourceId) {
        }

        NActors::TActorId SourceId;
    };
};

} // namespace NYql::NDq
