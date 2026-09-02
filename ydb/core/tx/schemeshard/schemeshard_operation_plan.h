#pragma once

#include "schemeshard_database_relative_path.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
#include <optional>
#include <variant>

namespace NKikimr::NSchemeShard {

// SchemeShard's model of what one accepted operation intends to do, computed and sealed
// before any part of it is constructed or proposed. It holds facts, not filter rules: a
// consumer asks it a question rather than being handed a set someone else already filtered.
// It lives in memory for the life of the operation and is not persisted: its purpose is to
// give a consumer the paths a DDL touches, to be saved beside the request and patched when
// the request is applied to another database.
//
// Two members, deliberately separate:
//   - LogicalEffects: what the DDL means. A consumer reconstructing the request sees these.
//   - PhysicalWrites: which path rows SchemeShard may write while executing. This includes
//     bookkeeping a record never shows -- generated directories, a copy source whose state is
//     flipped and restored -- and is what a write cross-check is measured against.
//
// Planned so far: ESchemeOpCreateTable (bare and CopyFromTable) and ESchemeOpDropTable. Every
// other operation still derives its paths inside Propose.

using TPlanEffectId = ui32;
using TPhysicalWriteId = ui32;

// The logical change itself.
enum class EPlanEffect : ui8 {
    Create,
    Alter,
    Drop,
    MoveFrom,
    MoveTo,
    ChildrenChanged,
};

// Which side of the operation this path sits on. Metadata for a consumer, never identity:
// parts are bound to effects by id.
enum class EPlanRole : ui8 {
    Target,
    Source,
    Container,
};

// Whether the DDL is about this path, or it exists only because SchemeShard decomposes the
// operation internally. A consumer reconstructing the parent DDL sees RequestNamed and
// RequestImplied; PartDerived stays in the plan for verification.
enum class EPlanOrigin : ui8 {
    RequestNamed,     // a field of the DDL names this path
    RequestImplied,   // top-level semantics imply it, without naming it
    PartDerived,      // exists only because of internal decomposition
};

// Whether a path-row write is expected. Intent, not outcome.
enum class EPlanObservation : ui8 {
    MustWrite,
    MayWrite,
};

// Why a physical write exists when no logical effect explains it.
enum class EPhysicalWriteReason : ui8 {
    LogicalEffect,               // the row of a logical effect above
    GeneratedDirectory,          // a directory the operation creates for a relative Name
    GeneratedDirectoryContainer, // the directory that gains a generated directory
    SourceStateFlip,             // a copy source: state set to Copying, restored at completion
};

// A schema effect changes the object; a reference names or depends on it without changing
// it. A reference therefore carries no effect kind at all -- there is no field in which to
// claim a mutation nobody performs.
struct TSchemaEffect {
    EPlanEffect Effect;
    std::optional<TPlanEffectId> Related;   // the other half of a move or rename
};

struct TReference {
};

struct TLogicalPathEffect {
    TPlanEffectId Id;
    TDatabaseRelativePath Path;
    TString LeafName;                       // last path component
    std::optional<TPathId> PathId;          // absent while the object does not exist yet
    EPlanRole Role;
    EPlanOrigin Origin;
    std::variant<TSchemaEffect, TReference> Kind;

    bool IsSchemaEffect() const {
        return std::holds_alternative<TSchemaEffect>(Kind);
    }

    const TSchemaEffect* AsSchemaEffect() const {
        return std::get_if<TSchemaEffect>(&Kind);
    }
};

struct TPhysicalPathWrite {
    TPhysicalWriteId Id;
    TDatabaseRelativePath Path;
    TString LeafName;
    std::optional<TPathId> PathId;
    EPlanObservation Expect;
    EPhysicalWriteReason Reason;
    std::optional<TPlanEffectId> LogicalEffect;
};

// Typed bindings: what one constructed part is licensed to touch, by id. A part never
// searches the plan for something matching a role.
struct TCreateTablePartBindings {
    TPlanEffectId Target;
    TPlanEffectId Container;
};

struct TCopyTablePartBindings {
    TPlanEffectId Target;
    TPlanEffectId Container;
    TPlanEffectId Source;
    TVector<TPlanEffectId> DropStreams;     // in request order
};

// A generated directory has no logical effect: both of its paths are physical writes.
struct TMkDirPartBindings {
    TPhysicalWriteId Target;
    TPhysicalWriteId Container;
};

struct TCreateIndexPartBindings {
    TPlanEffectId Target;
    TPlanEffectId Container;
    TPlanEffectId Source;                   // the index being copied
};

struct TCopySequencePartBindings {
    TPlanEffectId Target;
    TPlanEffectId Container;
    TPlanEffectId Source;
};

// Every drop part -- table, column table, index, impl table, cdc stream, its topic, sequence
// -- has the same shape: the object it removes and the parent that loses it. One binding type
// serves them all; the part kind is the blueprint's operation type.
struct TDropPartBindings {
    TPlanEffectId Target;
    TPlanEffectId Container;
};

using TPartBindings = std::variant<
    TCreateTablePartBindings,
    TCopyTablePartBindings,
    TMkDirPartBindings,
    TCreateIndexPartBindings,
    TCopySequencePartBindings,
    TDropPartBindings
>;

struct TPartBlueprint {
    ui32 PartIdx;                           // TxPartId of the part built from this blueprint
    ui32 RequestIdx;                        // which request subplan it belongs to
    NKikimrSchemeOp::TModifyScheme Tx;      // the transaction the part is constructed from
    TPartBindings Bindings;
};

// One per transaction of the proposed request, after rewrite. Part indexes refer to Parts.
struct TRequestSubplan {
    ui32 RequestIdx;
    TVector<ui32> GeneratedDirParts;        // outermost first
    TVector<ui32> Parts;                    // construction order
};

// A resolved view of one planned path, whichever list it came from.
struct TPlannedPathView {
    TDatabaseRelativePath Path;
    TString LeafName;
    std::optional<TPathId> PathId;
};

class TOperationPlanBuilder;

class TSealedOperationPlan {
    friend class TOperationPlanBuilder;

public:
    const TString& GetDatabaseRoot() const {
        return DatabaseRoot;
    }

    // The plan's external form is database-relative; SchemeShard's own TPath is absolute.
    // This is the one place the two are bridged.
    TString Absolute(const TDatabaseRelativePath& path) const;

    const TVector<TLogicalPathEffect>& GetLogicalEffects() const {
        return LogicalEffects;
    }

    const TVector<TPhysicalPathWrite>& GetPhysicalWrites() const {
        return PhysicalWrites;
    }

    const TVector<TRequestSubplan>& GetRequests() const {
        return Requests;
    }

    const TVector<TPartBlueprint>& GetParts() const {
        return Parts;
    }

    const TLogicalPathEffect& Effect(TPlanEffectId id) const {
        return LogicalEffects.at(id);
    }

    const TPhysicalPathWrite& Write(TPhysicalWriteId id) const {
        return PhysicalWrites.at(id);
    }

    TPlannedPathView ViewOfEffect(TPlanEffectId id) const;
    TPlannedPathView ViewOfWrite(TPhysicalWriteId id) const;

    const TPartBlueprint* FindPart(ui32 partIdx) const;

    // Projections.

    // What a consumer reconstructing the parent DDL should see: schema effects the request
    // names or implies. References and decomposition artefacts are not in it.
    TVector<const TLogicalPathEffect*> SchemaEffectsForRecord() const;

    // What may legitimately write a path row.
    const TVector<TPhysicalPathWrite>& PathWriteAllowance() const {
        return PhysicalWrites;
    }

private:
    TString DatabaseRoot;
    TVector<TLogicalPathEffect> LogicalEffects;
    TVector<TPhysicalPathWrite> PhysicalWrites;
    TVector<TRequestSubplan> Requests;
    TVector<TPartBlueprint> Parts;
};

// The only way to make a plan. Seal() returns the immutable form and leaves the builder empty.
class TOperationPlanBuilder {
public:
    explicit TOperationPlanBuilder(TString databaseRoot)
    {
        Plan.DatabaseRoot = std::move(databaseRoot);
    }

    TPlanEffectId AddSchemaEffect(TDatabaseRelativePath path, TString leafName, std::optional<TPathId> pathId,
        EPlanEffect effect, EPlanRole role, EPlanOrigin origin);

    TPlanEffectId AddReference(TDatabaseRelativePath path, TString leafName, std::optional<TPathId> pathId,
        EPlanRole role, EPlanOrigin origin);

    TPhysicalWriteId AddPhysicalWrite(TDatabaseRelativePath path, TString leafName, std::optional<TPathId> pathId,
        EPlanObservation expect, EPhysicalWriteReason reason, std::optional<TPlanEffectId> logicalEffect);

    // Both directions, so neither half of a move has to be found by scanning.
    void Pair(TPlanEffectId a, TPlanEffectId b);

    ui32 AddRequest();

    // Returns the part index. The blueprint is appended to the request's generated-directory
    // list or its part list, in call order.
    ui32 AddGeneratedDirPart(ui32 requestIdx, NKikimrSchemeOp::TModifyScheme tx, TMkDirPartBindings bindings);
    ui32 AddPart(ui32 requestIdx, NKikimrSchemeOp::TModifyScheme tx, TPartBindings bindings);

    const TString& GetDatabaseRoot() const {
        return Plan.DatabaseRoot;
    }

    const TLogicalPathEffect& Effect(TPlanEffectId id) const {
        return Plan.LogicalEffects.at(id);
    }

    std::shared_ptr<const TSealedOperationPlan> Seal();

private:
    TSealedOperationPlan Plan;
};

// A request the planner refuses. Carries what SchemeShard's reject response carries: the
// status, the reason, and for a target that already exists or is already going away, the
// path and the transaction that created or dropped it.
struct TRejectedOperation {
    NKikimrScheme::EStatus Status;
    TString Reason;
    std::optional<TPathId> PathId;
    std::optional<TTxId> PathCreateTxId;
    std::optional<TTxId> PathDropTxId;
};

using TOperationPlanResult = std::variant<TRejectedOperation, std::shared_ptr<const TSealedOperationPlan>>;

} // namespace NKikimr::NSchemeShard
