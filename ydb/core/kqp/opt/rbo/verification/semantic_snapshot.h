#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

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
