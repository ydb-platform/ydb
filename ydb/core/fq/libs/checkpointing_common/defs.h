#pragma once
#include <ydb/core/fq/libs/graph_params/proto/graph_params.pb.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/stream/str.h>

namespace NFq {

constexpr ui64 MaxYdbStringValueLength = 16 * 1000 * 1000;

////////////////////////////////////////////////////////////////////////////////

struct TCoordinatorId {
    TCoordinatorId(TString graphId, ui64 generation)
        : GraphId(std::move(graphId))
        , Generation(generation) {
    }

    TString GraphId;
    ui64 Generation;

    TString ToString() const;
    void PrintTo(IOutputStream& out) const;
};

using TCoordinators = TVector<TCoordinatorId>;

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointId {
    ui64 CoordinatorGeneration = 0;
    ui64 SeqNo = 0;

    TCheckpointId(ui64 gen, ui64 SeqNo)
        : CoordinatorGeneration(gen)
        , SeqNo(SeqNo)
    {
    }

    bool operator ==(const TCheckpointId& rhs) const {
        return CoordinatorGeneration == rhs.CoordinatorGeneration && SeqNo == rhs.SeqNo;
    }

    bool operator <(const TCheckpointId& rhs) const {
        return CoordinatorGeneration < rhs.CoordinatorGeneration ||
            (CoordinatorGeneration == rhs.CoordinatorGeneration && SeqNo < rhs.SeqNo);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointIdHash {
    size_t operator ()(const TCheckpointId& checkpointId);
};

////////////////////////////////////////////////////////////////////////////////

enum class ECheckpointStatus {
    Unspecified,
    Pending,
    PendingCommit,
    Completed,
    Aborted,
    GC,
};

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointMetadata {
    TCheckpointMetadata(TString graphId,
                        const TCheckpointId& checkpointId,
                        ECheckpointStatus status,
                        TInstant created,
                        TInstant modified)
        : GraphId(std::move(graphId))
        , CheckpointId(checkpointId)
        , Status(status)
        , Created(created)
        , Modified(modified)
    {
    }

    TString GraphId;
    TCheckpointId CheckpointId;
    ECheckpointStatus Status;

    TInstant Created;
    TInstant Modified;
    TMaybe<NProto::TGraphParams> Graph;
};

using TCheckpoints = TVector<TCheckpointMetadata>;

} // namespace NFq
