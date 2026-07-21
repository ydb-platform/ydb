#pragma once

#include <ydb/core/kqp/opt/rbo/verification/semantic_snapshot.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NKqp::NRBOPrefixCapture {

enum class ECaptureStatus {
    PrefixCaptured,
    PrefixUnsupported,
    OptimizerComplete,
    FinalUnsupported,
};

struct TCaptureOutput {
    ui64 RequestedOrdinal = 0;
    ECaptureStatus Status = ECaptureStatus::PrefixCaptured;
    TString InitialSnapshot;
    TString CandidateSnapshot;
    TString UnsupportedReason;
    TVector<TRBOTransformationEventV1> Events;
};

TCaptureOutput ClassifyCapture(
    ui64 requestedOrdinal,
    bool preparationSucceeded,
    TVector<TRBOSemanticSnapshotBoundaryResultV1> boundaries,
    TStringBuf preparationIssues = {});

TStringBuf StatusName(ECaptureStatus status) noexcept;
TStringBuf EventKindName(ERBOTransformationEventKindV1 kind);
TString RenderManifest(const TCaptureOutput& capture);

// These deliberately mirror benchmark_ut input preparation byte-for-byte.
TString RewriteBenchmarkSchemaToColumnStore(const TString& schema);
TString AddBenchmarkQueryPrelude(const TString& query);

} // namespace NKikimr::NKqp::NRBOPrefixCapture
