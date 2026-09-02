#pragma once

#include "schemeshard_database_relative_path.h"

#include <ydb/core/scheme/scheme_pathid.h>

#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NSchemeShard {

// SchemeShard's model of what one accepted operation intends to do, computed before anything
// executes. Deliberately not named after any consumer: Schema CDC is one projection over this,
// and so are the outbox, write observation and the tx-target cross-check. Filter rules belong
// to the consumer; this structure holds facts.
//
// Pilot scope: ESchemeOpCreateTable only. The older TAffectedPath model in
// schemeshard_affected_paths.h still serves every other operation, and the two coexist only
// until this shape is judged fit to repeat.

// What kind of thing an effect is, for a consumer that filters.
enum class EPlanEffectClass : ui8 {
    SchemaEffect,          // a logical change to the object
    Reference,             // named or depended on, but not changed
    BookkeepingInternal,   // an internal row write carrying no logical meaning
};

// The logical change itself.
enum class EPlanEffect : ui8 {
    Create,
    Alter,
    Drop,
    MoveFrom,
    MoveTo,
    ChildrenChanged,
};

// Which side of the operation this path sits on.
enum class EPlanRole : ui8 {
    Target,
    Source,
    Container,
};

// Whether the DDL is about this path, or it exists only because SchemeShard decomposes the
// operation internally. A separate axis from class: an index impl table's creation is a real
// SchemaEffect *and* PartDerived.
//
// Consumers reconstruct the parent operation, not the parts it splits into -- replay reissues
// the DDL and the target database runs its own decomposition -- so a record carries
// RequestNamed and RequestImplied only. PartDerived stays in the plan, because verification
// needs it.
enum class EPlanOrigin : ui8 {
    RequestNamed,     // a field of the DDL names this path
    RequestImplied,   // top-level semantics imply it, without naming it
    PartDerived,      // exists only because of internal decomposition
};

// Whether a path-row write is expected. Intent, not outcome: what actually committed is known
// only after execution and is recorded separately, in memory, at completion.
enum class EPlanObservation : ui8 {
    MustWrite,
    MayWrite,
    ReferenceOnly,
};

using TPlanEffectId = ui32;

// One path effect.
//
// Class, Effect, Role and Origin are deliberately not defaulted and the type has no default
// constructor: every one of them is CDC-visible, and a permissive default is how an operation
// silently inherits a claim nobody made. In the older model a plain alter ended up asserting
// that its parent's child set had changed, purely because a helper defaulted the field.
// Making them required is what turns migrating the remaining operations into a set of compile
// errors rather than an audit.
struct TPlannedPathEffect {
    TPlanEffectId          Id;
    TDatabaseRelativePath  Path;
    std::optional<TPathId> PathId;   // absent while the object does not exist yet

    EPlanEffectClass Class;
    EPlanEffect      Effect;
    EPlanRole        Role;
    EPlanOrigin      Origin;
    EPlanObservation Expect;

    // Pairs the two halves of a move or rename, which are otherwise two unrelated effects.
    std::optional<TPlanEffectId> Related;

    TPlannedPathEffect() = delete;

    TPlannedPathEffect(TPlanEffectId id, TDatabaseRelativePath path,
            std::optional<TPathId> pathId, EPlanEffectClass cls, EPlanEffect effect,
            EPlanRole role, EPlanOrigin origin, EPlanObservation expect)
        : Id(id)
        , Path(std::move(path))
        , PathId(std::move(pathId))
        , Class(cls)
        , Effect(effect)
        , Role(role)
        , Origin(origin)
        , Expect(expect)
    {}
};

// What one constructed part is licensed to touch, by effect id.
struct TPartPlan {
    ui32 PartIdx;
    TVector<TPlanEffectId> Effects;
};

class TLogicalOperationPlan {
public:
    // The plan's external form is database-relative, because that is what lets a consumer
    // reattach it to another database. SchemeShard's own TPath is absolute, so every internal
    // consumption site would otherwise re-derive the prefix by hand -- which is the second
    // computation this whole design removes. The root is therefore part of the plan, and
    // Absolute() is the one place the two forms are bridged.
    void SetDatabaseRoot(TString root) {
        DatabaseRoot = std::move(root);
    }

    const TString& GetDatabaseRoot() const {
        return DatabaseRoot;
    }

    TString Absolute(const TPlannedPathEffect& effect) const {
        const TStringBuf relative = effect.Path.Value();
        if (relative == "/") {
            return DatabaseRoot;
        }
        return DatabaseRoot + relative;
    }

    TPlanEffectId Add(TDatabaseRelativePath path, std::optional<TPathId> pathId,
            EPlanEffectClass cls, EPlanEffect effect, EPlanRole role, EPlanOrigin origin,
            EPlanObservation expect)
    {
        const TPlanEffectId id = Effects.size();
        Effects.emplace_back(id, std::move(path), std::move(pathId), cls, effect, role, origin,
            expect);
        return id;
    }

    // Both directions, so neither half of a move has to be found by scanning.
    void Pair(TPlanEffectId a, TPlanEffectId b) {
        Effects[a].Related = b;
        Effects[b].Related = a;
    }

    const TVector<TPlannedPathEffect>& GetEffects() const {
        return Effects;
    }

    // Projections. The plan stays one complete set; a consumer asks it a question rather than
    // being handed a set someone else already filtered.

    // What a consumer reconstructing the parent DDL should see.
    TVector<const TPlannedPathEffect*> SchemaEffectsForRecord() const {
        return Select([](const TPlannedPathEffect& e) {
            return e.Class == EPlanEffectClass::SchemaEffect
                && e.Origin != EPlanOrigin::PartDerived;
        });
    }

    // What may legitimately write a path row, including the bookkeeping a record never shows.
    TVector<const TPlannedPathEffect*> PathWriteAllowance() const {
        return Select([](const TPlannedPathEffect& e) {
            return e.Expect != EPlanObservation::ReferenceOnly;
        });
    }

private:
    template <class TPredicate>
    TVector<const TPlannedPathEffect*> Select(TPredicate predicate) const {
        TVector<const TPlannedPathEffect*> selected;
        for (const auto& effect : Effects) {
            if (predicate(effect)) {
                selected.push_back(&effect);
            }
        }
        return selected;
    }

private:
    TString DatabaseRoot;
    TVector<TPlannedPathEffect> Effects;
    TVector<TPartPlan> Parts;
};

// Test-only observation point for the pilot. When non-null, an operation that plans under the
// new model copies its plan here, so a test can assert the model directly rather than inferring
// it from behaviour. Null in production; removed when the pilot either graduates or is dropped.
inline TLogicalOperationPlan* LastPlannedOperation = nullptr;

} // namespace NKikimr::NSchemeShard
