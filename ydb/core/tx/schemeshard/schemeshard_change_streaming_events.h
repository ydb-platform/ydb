#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/scheme/scheme_pathid.h>

namespace NKikimr {
namespace NSchemeShard {

// Extend existing TEvPrivate with DataShard-to-DataShard change streaming events
// These events complement the existing change exchange infrastructure and 
// leverage the existing TEvChangeExchange system for actual data transfer
struct TEvPrivateChangeStreaming {
    enum EEv {
        // Change streaming coordination events (SchemeShard level)
        EvStartDataShardStreaming = EventSpaceBegin(TKikimrEvents::ES_PRIVATE) + 1000, // Offset to avoid conflicts
        EvDataShardStreamingProgress,
        EvDataShardStreamingComplete,
        EvDataShardStreamingError,
        EvRetryDataShardStreaming,
        
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE),
                  "expected EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

    // Start DataShard-to-DataShard streaming operation
    struct TEvStartDataShardStreaming : TEventLocal<TEvStartDataShardStreaming, EvStartDataShardStreaming> {
        ui64 StreamingOperationId;
        TPathId SourcePathId;
        TPathId TargetPathId;
        TString StreamingConfig; // JSON config with parameters
        
        TEvStartDataShardStreaming(ui64 operationId, const TPathId& sourceId, const TPathId& targetId, const TString& config)
            : StreamingOperationId(operationId)
            , SourcePathId(sourceId)
            , TargetPathId(targetId)
            , StreamingConfig(config)
        {}
    };

    // Progress update from DataShard streaming operations
    struct TEvDataShardStreamingProgress : TEventLocal<TEvDataShardStreamingProgress, EvDataShardStreamingProgress> {
        ui64 StreamingOperationId;
        TPathId PathId;
        ui64 ProcessedRecords;
        ui64 LastProcessedLsn;
        TString Status;
        
        TEvDataShardStreamingProgress(ui64 operationId, const TPathId& pathId, ui64 records, ui64 lsn, const TString& status)
            : StreamingOperationId(operationId)
            , PathId(pathId)
            , ProcessedRecords(records)
            , LastProcessedLsn(lsn)
            , Status(status)
        {}
    };

    // DataShard-to-DataShard streaming operation completed
    struct TEvDataShardStreamingComplete : TEventLocal<TEvDataShardStreamingComplete, EvDataShardStreamingComplete> {
        ui64 StreamingOperationId;
        ui64 TotalRecordsProcessed;
        
        TEvDataShardStreamingComplete(ui64 operationId, ui64 totalRecords)
            : StreamingOperationId(operationId)
            , TotalRecordsProcessed(totalRecords)
        {}
    };

    // Error in DataShard streaming operation
    struct TEvDataShardStreamingError : TEventLocal<TEvDataShardStreamingError, EvDataShardStreamingError> {
        ui64 StreamingOperationId;
        TPathId PathId;
        TString ErrorMessage;
        bool IsRetryable;
        
        TEvDataShardStreamingError(ui64 operationId, const TPathId& pathId, const TString& error, bool retryable)
            : StreamingOperationId(operationId)
            , PathId(pathId)
            , ErrorMessage(error)
            , IsRetryable(retryable)
        {}
    };

    // Retry DataShard streaming operation
    struct TEvRetryDataShardStreaming : TEventLocal<TEvRetryDataShardStreaming, EvRetryDataShardStreaming> {
        ui64 StreamingOperationId;
        ui32 RetryCount;
        
        TEvRetryDataShardStreaming(ui64 operationId, ui32 retryCount)
            : StreamingOperationId(operationId)
            , RetryCount(retryCount)
        {}
    };
};

} // namespace NSchemeShard
} // namespace NKikimr
