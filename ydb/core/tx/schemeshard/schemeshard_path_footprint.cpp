#include "schemeshard_path_footprint.h"

#include "schemeshard_impl.h"
#include "schemeshard_path.h"

#include <ydb/core/base/path.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

#include <array>
#include <string_view>

namespace NKikimr::NSchemeShard {

namespace {

using EKind = EPathRefKind;
using ERole = EPathRefRole;

// Metadata indexed by EPathField.

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

enum class EPlaceholder {
    None,
    Index,
    SubIndex,
    MapKey,
};

struct TTemplatePart {
    TStringBuf Literal;
    EPlaceholder Placeholder = EPlaceholder::None;
};

struct TTemplateRange {
    size_t Begin = 0;
    size_t End = 0;
};

constexpr size_t TemplatePartCount = [] {
    size_t count = PathFieldCount;
    for (const auto tpl : FieldTemplates) {
        for (const char c : tpl) {
            count += c == '{';
        }
    }
    return count;
}();

struct TCompiledTemplates {
    std::array<TTemplatePart, TemplatePartCount> Parts;
    std::array<TTemplateRange, PathFieldCount> Ranges;
};

consteval TCompiledTemplates CompileTemplates() {
    TCompiledTemplates result{};
    size_t next = 0;
    for (size_t field = 0; field < PathFieldCount; ++field) {
        const std::string_view tpl(FieldTemplates[field].data(), FieldTemplates[field].size());
        result.Ranges[field].Begin = next;
        size_t pos = 0;
        while (true) {
            const size_t open = tpl.find_first_of("{}", pos);
            if (open == std::string_view::npos) {
                result.Parts[next++] = {TStringBuf(tpl.data() + pos, tpl.size() - pos)};
                break;
            }
            const size_t close = tpl.find('}', open + 1);
            if (tpl[open] != '{' || close == std::string_view::npos) {
                throw "Unmatched brace in path field template";
            }
            const auto placeholder = tpl.substr(open + 1, close - open - 1);
            EPlaceholder kind;
            if (placeholder == "i") {
                kind = EPlaceholder::Index;
            } else if (placeholder == "j") {
                kind = EPlaceholder::SubIndex;
            } else if (placeholder == "key") {
                kind = EPlaceholder::MapKey;
            } else {
                throw "Unknown path field placeholder";
            }
            result.Parts[next++] = {TStringBuf(tpl.data() + pos, open - pos), kind};
            pos = close + 1;
        }
        result.Ranges[field].End = next;
    }
    return result;
}

constexpr auto CompiledTemplates = CompileTemplates();

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
    const auto range = CompiledTemplates.Ranges[FieldIndex(ref.Field)];
    if (range.End == range.Begin + 1) {
        return TString(CompiledTemplates.Parts[range.Begin].Literal);
    }
    TStringBuilder rendered;
    for (size_t i = range.Begin; i < range.End; ++i) {
        const auto& part = CompiledTemplates.Parts[i];
        rendered << part.Literal;
        switch (part.Placeholder) {
        case EPlaceholder::None:
            break;
        case EPlaceholder::Index:
            rendered << ref.Index;
            break;
        case EPlaceholder::SubIndex:
            rendered << ref.SubIndex;
            break;
        case EPlaceholder::MapKey:
            rendered << ref.MapKey;
            break;
        }
    }
    return rendered;
}

const TVector<TStringBuf>& KnownPathFieldNames() {
    static const TVector<TStringBuf> names = [] {
        TVector<TStringBuf> collected;
        collected.reserve(PathFieldCount);
        for (const TStringBuf name : FieldProtoNames) {
            // Synthetic and ID fields have no protobuf string field.
            if (!name.empty()) {
                collected.push_back(name);
            }
        }
        // Operations may share a protobuf field.
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

// An empty leaf denotes the directory itself, without a trailing slash.
TString JoinLeafUnder(TStringBuf dir, TStringBuf leaf) {
    if (leaf.empty()) {
        return TString(dir);
    }
    if (dir.empty()) {
        return TString(leaf);
    }
    return TStringBuilder() << dir << '/' << leaf;
}

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
        // A leading slash does not escape WorkingDir with TSplitChildTag.
        return JoinLeafUnder(workingDir,
            ref.Value.StartsWith('/') ? ref.Value.substr(1) : ref.Value);
    case EPathRefKind::Absolute:
        // An empty value denotes WorkingDir.
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
        return TString();
    }
    return TString();
}

}  // namespace NKikimr::NSchemeShard
