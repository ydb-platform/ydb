#pragma once

#include "transformation_event.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NKqp {

class TOpRoot;
class TRBOContext;

struct TSemanticSnapshotCatalogColumnV1 {
    TString Name;
    TString Type;
    bool Nullable = false;
};

struct TSemanticSnapshotCatalogKeyV1 {
    TVector<TString> Columns;
    bool NullsDistinct = false;
};

struct TSemanticSnapshotCatalogTableV1 {
    // Length-prefixed cluster/path/path-id/sys-view/schema-version identity.
    // This exact string is also used by scan nodes and as the witness table key.
    TString Name;
    // Internal exporter evidence; deliberately absent from snapshot JSON.
    // Zero means that no universal logical String-cell bound was established.
    ui64 MaximumStoredStringCellBytes = 0;
    TVector<TSemanticSnapshotCatalogColumnV1> Columns;
    TVector<TSemanticSnapshotCatalogKeyV1> UniqueKeys;
};

// Capture this once from the initial plan, then reuse it for both exports.  In
// particular, a table removed by optimization must remain in both snapshots.
struct TSemanticSnapshotCatalogV1 {
    TVector<TSemanticSnapshotCatalogTableV1> Tables;
};

struct TSemanticSnapshotCatalogCaptureResult {
    bool IsSupported() const noexcept {
        return UnsupportedReason.empty();
    }

    TSemanticSnapshotCatalogV1 Catalog;
    TString UnsupportedReason;
};

struct TSemanticSnapshotExportResult {
    bool IsSupported() const noexcept {
        return UnsupportedReason.empty();
    }

    TString Json;
    TString UnsupportedReason;
};

enum class ERBOSemanticSnapshotBoundaryV1 {
    Initial,
    Final,
    TransformationPrefix,
};

struct TRBOSemanticSnapshotBoundaryResultV1 {
    bool IsSupported() const noexcept {
        return UnsupportedReason.empty();
    }

    ERBOSemanticSnapshotBoundaryV1 Boundary;
    TString Json;
    TString UnsupportedReason;
    // Empty for Initial and for non-diagnostic runs. A diagnostic Prefix has
    // [1..target]; diagnostic Final has the complete shorter sequence.
    TVector<TRBOTransformationEventV1> TransformationEvents;
};

class IRBOSemanticSnapshotSink {
public:
    virtual ~IRBOSemanticSnapshotSink() = default;

    // The result is passed by value so the sink owns the JSON or diagnostic.
    virtual void OnSemanticSnapshot(TRBOSemanticSnapshotBoundaryResultV1 result) = 0;

    // Diagnostic-only opt-in. Transformations are numbered from one across the
    // complete optimizer pipeline.  The default cannot affect optimization.
    virtual std::optional<ui64> GetTransformationPrefixTarget() const {
        return std::nullopt;
    }
};

// Query-local helper that captures one shared initial catalog and uses it for
// either the normal Final boundary or a diagnostic Prefix boundary.  A null
// sink makes capture a no-op.  Export and sink failures never escape into
// normal query compilation.
class TSemanticSnapshotPairCaptureV1 {
public:
    explicit TSemanticSnapshotPairCaptureV1(IRBOSemanticSnapshotSink* sink) noexcept;

    TSemanticSnapshotPairCaptureV1(const TSemanticSnapshotPairCaptureV1&) = delete;
    TSemanticSnapshotPairCaptureV1& operator=(const TSemanticSnapshotPairCaptureV1&) = delete;

    void CaptureInitial(TOpRoot& root, TRBOContext& ctx) noexcept;
    void CaptureFinal(TOpRoot& root, const TRBOContext& ctx) noexcept;
    void CaptureTransformationPrefix(
        TOpRoot& root,
        TRBOContext& ctx,
        const TVector<TRBOTransformationEventV1>& events) noexcept;

    // Sink configuration is untrusted instrumentation.  Invalid values and
    // exceptions disable the debug hook without changing query compilation.
    std::optional<ui64> GetTransformationPrefixTarget() const noexcept;

private:
    void Deliver(TRBOSemanticSnapshotBoundaryResultV1 result) noexcept;

    IRBOSemanticSnapshotSink* Sink = nullptr;
    std::optional<TSemanticSnapshotCatalogV1> Catalog;
    TString CatalogFailure;
    bool InitialAttempted = false;
};

// Export the strict logical version-one snapshot consumed by rbo_verifier.
// The function performs no I/O and reports every unmodeled semantic construct
// as unsupported instead of omitting it from the snapshot.
TSemanticSnapshotCatalogCaptureResult CaptureSemanticSnapshotCatalogV1(
    TOpRoot& initialRoot, const TRBOContext& ctx);

TSemanticSnapshotExportResult ExportSemanticSnapshotV1(
    TOpRoot& root, const TRBOContext& ctx, const TSemanticSnapshotCatalogV1& catalog);

// Convenience overload for inspecting one plan.  Pairwise verification should
// use CaptureSemanticSnapshotCatalogV1 and the overload above.
TSemanticSnapshotExportResult ExportSemanticSnapshotV1(TOpRoot& root, const TRBOContext& ctx);

} // namespace NKikimr::NKqp
