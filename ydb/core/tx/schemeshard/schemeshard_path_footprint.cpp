#include "schemeshard_path_footprint.h"

#include "schemeshard_impl.h"
#include "schemeshard_path.h"

#include <ydb/core/base/path.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NKikimr::NSchemeShard {

namespace {

using EKind = EPathRefKind;
using ERole = EPathRefRole;

////////////////////////////////////////////////////////////////////////////////
// The static columns of SCHEMESHARD_PATH_FIELDS, indexed by EPathField.

#define SCHEMESHARD_PATH_FIELD_TEMPLATE(name, tpl, proto, kind, role) TStringBuf(tpl),
#define SCHEMESHARD_PATH_FIELD_PROTO(name, tpl, proto, kind, role) TStringBuf(proto),
#define SCHEMESHARD_PATH_FIELD_KIND(name, tpl, proto, kind, role) EKind::kind,
#define SCHEMESHARD_PATH_FIELD_ROLE(name, tpl, proto, kind, role) ERole::role,

constexpr TStringBuf FieldTemplates[] = {
    SCHEMESHARD_PATH_FIELDS(SCHEMESHARD_PATH_FIELD_TEMPLATE)
};
constexpr TStringBuf FieldProtoNames[] = {
    SCHEMESHARD_PATH_FIELDS(SCHEMESHARD_PATH_FIELD_PROTO)
};
constexpr EKind FieldKinds[] = {
    SCHEMESHARD_PATH_FIELDS(SCHEMESHARD_PATH_FIELD_KIND)
};
constexpr ERole FieldRoles[] = {
    SCHEMESHARD_PATH_FIELDS(SCHEMESHARD_PATH_FIELD_ROLE)
};

#undef SCHEMESHARD_PATH_FIELD_TEMPLATE
#undef SCHEMESHARD_PATH_FIELD_PROTO
#undef SCHEMESHARD_PATH_FIELD_KIND
#undef SCHEMESHARD_PATH_FIELD_ROLE

constexpr size_t PathFieldCount = static_cast<size_t>(EPathField::Count);
static_assert(std::size(FieldTemplates) == PathFieldCount);
static_assert(std::size(FieldProtoNames) == PathFieldCount);
static_assert(std::size(FieldKinds) == PathFieldCount);
static_assert(std::size(FieldRoles) == PathFieldCount);

size_t FieldIndex(EPathField field) {
    const size_t index = static_cast<size_t>(field);
    Y_DEBUG_ABORT_UNLESS(index < PathFieldCount);
    return index < PathFieldCount ? index : 0;
}

}  // namespace

TStringBuf PathFieldName(EPathField field) {
    return FieldTemplates[FieldIndex(field)];
}

TStringBuf PathFieldProtoName(EPathField field) {
    return FieldProtoNames[FieldIndex(field)];
}

EPathRefKind PathFieldDefaultKind(EPathField field) {
    return FieldKinds[FieldIndex(field)];
}

EPathRefRole PathFieldDefaultRole(EPathField field) {
    return FieldRoles[FieldIndex(field)];
}

TString FieldPath(const TPathRef& ref) {
    const TStringBuf tpl = PathFieldName(ref.Field);
    if (tpl.find('{') == TStringBuf::npos) {
        return TString(tpl);
    }
    TStringBuilder rendered;
    size_t pos = 0;
    while (pos < tpl.size()) {
        const size_t open = tpl.find('{', pos);
        if (open == TStringBuf::npos) {
            rendered << tpl.substr(pos);
            break;
        }
        rendered << tpl.substr(pos, open - pos);
        const size_t close = tpl.find('}', open + 1);
        if (close == TStringBuf::npos) {
            rendered << tpl.substr(open);
            break;
        }
        const TStringBuf placeholder = tpl.substr(open + 1, close - open - 1);
        if (placeholder == "i") {
            rendered << ref.Index;
        } else if (placeholder == "j") {
            rendered << ref.SubIndex;
        } else {
            rendered << ref.MapKey;
        }
        pos = close + 1;
    }
    return rendered;
}

const TVector<TStringBuf>& KnownPathFieldNames() {
    static const TVector<TStringBuf> names = [] {
        TVector<TStringBuf> collected;
        collected.reserve(PathFieldCount);
        for (const TStringBuf name : FieldProtoNames) {
            // Empty for a synthetic Implicit marker, for the working dir, and
            // for an id-valued field: the descriptor walk classifies string
            // fields only.
            if (!name.empty()) {
                collected.push_back(name);
            }
        }
        // Several fields share one protobuf field (the same submessage read
        // under two prefixes, e.g. Replication and AlterReplication).
        SortUnique(collected);
        return collected;
    }();
    return names;
}

TStringBuf PathRefKindName(EPathRefKind kind) {
    switch (kind) {
    case EPathRefKind::LeafUnderWorkingDir: return "LeafUnderWorkingDir";
    case EPathRefKind::PathUnderWorkingDir: return "PathUnderWorkingDir";
    case EPathRefKind::PathUnderWorkingDirSplit: return "PathUnderWorkingDirSplit";
    case EPathRefKind::Absolute: return "Absolute";
    case EPathRefKind::LeafUnderSibling: return "LeafUnderSibling";
    case EPathRefKind::ById: return "ById";
    case EPathRefKind::Implicit: return "Implicit";
    }
    return "Unknown";
}

TStringBuf PathRefRoleName(EPathRefRole role) {
    switch (role) {
    case EPathRefRole::Target: return "Target";
    case EPathRefRole::Source: return "Source";
    case EPathRefRole::Parent: return "Parent";
    case EPathRefRole::Dependency: return "Dependency";
    }
    return "Unknown";
}

namespace {

// NKikimr::JoinPath always inserts the separator, so joining an empty leaf
// yields a trailing slash. An empty leaf here means "the directory itself":
// that is what a PathUnderWorkingDir/Absolute ref with no value stands for
// (CreateFullBackupOp's working dir), and TPath::Child would not add a segment
// for it either.
TString JoinLeafUnder(TStringBuf dir, TStringBuf leaf) {
    if (leaf.empty()) {
        return TString(dir);
    }
    if (dir.empty()) {
        return TString(leaf);
    }
    return TStringBuilder() << dir << '/' << leaf;
}

// The string form of ResolveRelativeOrAbsolute: a base that starts with a
// slash is already absolute, anything else hangs off the working dir.
TString JoinRelativeOrAbsolute(TStringBuf workingDir, TStringBuf value) {
    if (value.StartsWith('/')) {
        return TString(value);
    }
    return JoinLeafUnder(workingDir, value);
}

}  // namespace

TString JoinPathRef(TStringBuf workingDir, const TPathRef& ref, const TVector<TString>& joined) {
    switch (ref.Kind) {
    case EPathRefKind::LeafUnderWorkingDir:
        return JoinLeafUnder(workingDir, ref.Value);
    case EPathRefKind::PathUnderWorkingDir:
        return JoinRelativeOrAbsolute(workingDir, ref.Value);
    case EPathRefKind::PathUnderWorkingDirSplit:
        // TPath::Child(value, TSplitChildTag{}) dives the value one segment at
        // a time under the working dir, so a leading slash does not escape it.
        return JoinLeafUnder(workingDir,
            ref.Value.StartsWith('/') ? ref.Value.substr(1) : ref.Value);
    case EPathRefKind::Absolute:
        // Propose() resolves these on their own; the working dir is never
        // joined in. An empty value stands for the working dir itself.
        return ref.Value.empty() ? TString(workingDir) : TString(ref.Value);
    case EPathRefKind::LeafUnderSibling: {
        const TString base = ref.BasePath.empty() && ref.AnchorIndex >= 0
                && size_t(ref.AnchorIndex) < joined.size()
            ? joined[ref.AnchorIndex]
            : JoinRelativeOrAbsolute(workingDir, ref.BasePath);
        return base.empty() ? TString() : JoinLeafUnder(base, ref.Value);
    }
    case EPathRefKind::ById:
    case EPathRefKind::Implicit:
        // A path id and a runtime-derived set both need schemeshard state.
        return TString();
    }
    return TString();
}

}  // namespace NKikimr::NSchemeShard
