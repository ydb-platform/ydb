#pragma once
#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/events/event_subspace.h>
#include <ydb/core/fq/libs/checkpoint_storage/proto/graph_description.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>

namespace NFq {

struct TEvCheckpointStorage {
    // Event ids.
    enum EEv : ui32 {
        EvRegisterCoordinatorRequest = YqEventSubspaceBegin(TYqEventSubspace::CheckpointStorage),
        EvRegisterCoordinatorResponse,
        EvCreateCheckpointRequest,
        EvCreateCheckpointResponse,
        EvSetCheckpointStatusPendingCommitRequest,
        EvSetCheckpointStatusPendingCommitResponse,
        EvCompleteCheckpointRequest,
        EvCompleteCheckpointResponse,
        EvAbortCheckpointRequest,
        EvAbortCheckpointResponse,
        EvGetCheckpointsMetadataRequest,
        EvGetCheckpointsMetadataResponse,

        // Internal Storage events.
        EvNewCheckpointSucceeded,

        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(TYqEventSubspace::CheckpointStorage), "All events must be in their subspace");

    // Events.

    struct TEvRegisterCoordinatorRequest
        : NActors::TEventLocal<TEvRegisterCoordinatorRequest, EvRegisterCoordinatorRequest> {
        explicit TEvRegisterCoordinatorRequest(TCoordinatorId coordinatorId)
            : CoordinatorId(std::move(coordinatorId)) {
        }

        TCoordinatorId CoordinatorId;
    };

    struct TEvRegisterCoordinatorResponse
        : NActors::TEventLocal<TEvRegisterCoordinatorResponse, EvRegisterCoordinatorResponse> {
        TEvRegisterCoordinatorResponse() = default;

        explicit TEvRegisterCoordinatorResponse(NYql::TIssues issues)
            : Issues(std::move(issues)) {
        }

        NYql::TIssues Issues;
    };

    struct TEvCreateCheckpointRequest : NActors::TEventLocal<TEvCreateCheckpointRequest, EvCreateCheckpointRequest> {
        TEvCreateCheckpointRequest(
            TCoordinatorId coordinatorId,
            TCheckpointId checkpointId,
            ui64 nodeCount,
            const NProto::TCheckpointGraphDescription& graphDesc)
            : CoordinatorId(std::move(coordinatorId))
            , CheckpointId(std::move(checkpointId))
            , NodeCount(nodeCount)
            , GraphDescription(graphDesc) {
        }

        TEvCreateCheckpointRequest(
            TCoordinatorId coordinatorId,
            TCheckpointId checkpointId,
            ui64 nodeCount,
            const TString& graphDescId)
            : CoordinatorId(std::move(coordinatorId))
            , CheckpointId(std::move(checkpointId))
            , NodeCount(nodeCount)
            , GraphDescription(graphDescId) {
        }

        TCoordinatorId CoordinatorId;
        TCheckpointId CheckpointId;
        ui64 NodeCount;
        std::variant<TString, NProto::TCheckpointGraphDescription> GraphDescription;
    };

    struct TEvCreateCheckpointResponse : NActors::TEventLocal<TEvCreateCheckpointResponse, EvCreateCheckpointResponse> {
        TEvCreateCheckpointResponse(TCheckpointId checkpointId, NYql::TIssues issues, TString graphDescId)
            : CheckpointId(std::move(checkpointId))
            , Issues(std::move(issues))
            , GraphDescId(std::move(graphDescId)) {
        }

        TCheckpointId CheckpointId;
        NYql::TIssues Issues;
        TString GraphDescId;
    };

    struct TEvSetCheckpointPendingCommitStatusRequest
        : NActors::TEventLocal<TEvSetCheckpointPendingCommitStatusRequest, EvSetCheckpointStatusPendingCommitRequest> {
        TEvSetCheckpointPendingCommitStatusRequest(TCoordinatorId coordinatorId, TCheckpointId checkpointId, ui64 stateSizeBytes)
            : CoordinatorId(std::move(coordinatorId))
            , CheckpointId(std::move(checkpointId))
            , StateSizeBytes(stateSizeBytes) {
        }

        TCoordinatorId CoordinatorId;
        TCheckpointId CheckpointId;
        ui64 StateSizeBytes;
    };

    struct TEvSetCheckpointPendingCommitStatusResponse
        : NActors::TEventLocal<TEvSetCheckpointPendingCommitStatusResponse, EvSetCheckpointStatusPendingCommitResponse> {
        TEvSetCheckpointPendingCommitStatusResponse(TCheckpointId checkpointId, NYql::TIssues issues)
            : CheckpointId(std::move(checkpointId))
            , Issues(std::move(issues)) {
        }

        TCheckpointId CheckpointId;
        NYql::TIssues Issues;
    };

    struct TEvCompleteCheckpointRequest
        : NActors::TEventLocal<TEvCompleteCheckpointRequest, EvCompleteCheckpointRequest> {
        TEvCompleteCheckpointRequest(
            TCoordinatorId coordinatorId,
            TCheckpointId checkpointId,
            ui64 stateSizeBytes,
            NYql::NDqProto::TCheckpoint::EType type)
            : CoordinatorId(std::move(coordinatorId))
            , CheckpointId(std::move(checkpointId))
            , StateSizeBytes(stateSizeBytes)
            , Type(type) {
        }

        TCoordinatorId CoordinatorId;
        TCheckpointId CheckpointId;
        ui64 StateSizeBytes;
        NYql::NDqProto::TCheckpoint::EType Type;
    };

    struct TEvCompleteCheckpointResponse
        : NActors::TEventLocal<TEvCompleteCheckpointResponse, EvCompleteCheckpointResponse> {
        TEvCompleteCheckpointResponse(TCheckpointId checkpointId, NYql::TIssues issues)
            : CheckpointId(std::move(checkpointId))
            , Issues(std::move(issues)) {
        }

        TCheckpointId CheckpointId;
        NYql::TIssues Issues;
    };

    struct TEvAbortCheckpointRequest
            : NActors::TEventLocal<TEvAbortCheckpointRequest, EvAbortCheckpointRequest> {
        TEvAbortCheckpointRequest(TCoordinatorId coordinatorId, TCheckpointId checkpointId, TString reason)
            : CoordinatorId(std::move(coordinatorId))
            , CheckpointId(std::move(checkpointId))
            , Reason(std::move(reason)) {
        }

        TCoordinatorId CoordinatorId;
        TCheckpointId CheckpointId;
        TString Reason;
    };

    struct TEvAbortCheckpointResponse
            : NActors::TEventLocal<TEvAbortCheckpointResponse, EvAbortCheckpointResponse> {
        TEvAbortCheckpointResponse(TCheckpointId checkpointId, NYql::TIssues issues)
                : CheckpointId(std::move(checkpointId))
                , Issues(std::move(issues)) {
        }

        TCheckpointId CheckpointId;
        NYql::TIssues Issues;
    };

    struct TEvGetCheckpointsMetadataRequest
        : NActors::TEventLocal<TEvGetCheckpointsMetadataRequest, EvGetCheckpointsMetadataRequest> {
        explicit TEvGetCheckpointsMetadataRequest(TString graphId, TVector<ECheckpointStatus> statuses = TVector<ECheckpointStatus>(), ui64 limit = std::numeric_limits<ui64>::max(), bool loadGraphDescription = false)
            : GraphId(std::move(graphId))
            , Statuses(std::move(statuses))
            , Limit(limit)
            , LoadGraphDescription(loadGraphDescription) {
        }

        TString GraphId;
        TVector<ECheckpointStatus> Statuses;
        ui64 Limit;
        bool LoadGraphDescription = false;
    };

    struct TEvGetCheckpointsMetadataResponse
        : NActors::TEventLocal<TEvGetCheckpointsMetadataResponse, EvGetCheckpointsMetadataResponse> {
        TEvGetCheckpointsMetadataResponse(TVector<TCheckpointMetadata> checkpoints, NYql::TIssues issues)
            : Checkpoints(std::move(checkpoints))
            , Issues(std::move(issues)) {
        }

        TCheckpoints Checkpoints;
        NYql::TIssues Issues;
    };

    // note that no response exists
    struct TEvNewCheckpointSucceeded : NActors::TEventLocal<TEvNewCheckpointSucceeded, EvNewCheckpointSucceeded> {
        TEvNewCheckpointSucceeded(
            TCoordinatorId coordinatorId,
            TCheckpointId checkpointId,
            NYql::NDqProto::TCheckpoint::EType type)
            : CoordinatorId(std::move(coordinatorId))
            , CheckpointId(std::move(checkpointId))
            , Type(type)
        {
        }

        TCoordinatorId CoordinatorId;
        TCheckpointId CheckpointId;
        NYql::NDqProto::TCheckpoint::EType Type;
    };
};

} // namespace NFq
