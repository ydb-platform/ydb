#include "schemeshard_path_footprint.h"
#include "schemeshard_operation_registry.h"

#include "schemeshard_impl.h"
#include "schemeshard_path.h"

#include <ydb/core/base/path.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

#include <array>
#include <string_view>
#include <type_traits>

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

namespace {

// Kept outside TRefSink so its default member initializers can be used
// in default arguments.
struct TRefAt {
    ui32 Index = Max<ui32>();
    ui32 SubIndex = Max<ui32>();
    TStringBuf Key;
};

class TRefSink {
public:
    using TAt = TRefAt;

    explicit TRefSink(TPathRefs& out)
        : Out(out)
    {}

    void Add(EPathField field, TStringBuf value, TAt at = {}) {
        Emit(field, value, PathFieldDefaultKind(field), PathFieldDefaultRole(field), {}, at);
    }

    // Override defaults when an operation resolves the field differently.
    void AddAs(EPathField field, TStringBuf value, EKind kind, ERole role, TAt at = {}) {
        Emit(field, value, kind, role, {}, at);
    }

    // A leaf under a path from another request field.
    void Sibling(EPathField field, TStringBuf value, TStringBuf base, TAt at = {}) {
        Emit(field, value, PathFieldDefaultKind(field), PathFieldDefaultRole(field), base, at);
    }

    // Anchor to an earlier ref when the base needs ID or split-path resolution.
    void SiblingOf(EPathField field, TStringBuf value, int anchorIndex, TAt at = {}) {
        Emit(field, value, PathFieldDefaultKind(field), PathFieldDefaultRole(field), {}, at)
            .AnchorIndex = anchorIndex;
    }

    void ById(EPathField field, ui64 ownerId, ui64 localPathId, TAt at = {}) {
        TPathRef& ref = Emit(field, {}, PathFieldDefaultKind(field),
            PathFieldDefaultRole(field), {}, at);
        ref.OwnerId = ownerId;
        ref.LocalPathId = localPathId;
    }

    // Paths discovered or generated during execution.
    void Implicit(EPathField field, int anchorIndex, TAt at = {}) {
        Emit(field, {}, PathFieldDefaultKind(field), PathFieldDefaultRole(field), {}, at)
            .AnchorIndex = anchorIndex;
    }

    int Last() const {
        return static_cast<int>(Out.Refs.size()) - 1;
    }

    // Keep computed base paths alive alongside the refs.
    TStringBuf Own(TString value) {
        Out.Owned.push_back(std::move(value));
        return Out.Owned.back();
    }

private:
    TPathRef& Emit(EPathField field, TStringBuf value, EKind kind, ERole role,
            TStringBuf base, TAt at) {
        TPathRef ref;
        ref.Field = field;
        ref.Index = at.Index;
        ref.SubIndex = at.SubIndex;
        ref.MapKey = at.Key;
        ref.Value = value;
        ref.Kind = kind;
        ref.Role = role;
        ref.BasePath = base;
        Out.Refs.push_back(ref);
        return Out.Refs.back();
    }

    TPathRefs& Out;
};

// Protobuf map iteration order is unspecified; sort so the footprint is stable.
template <class TMap>
TVector<const typename TMap::value_type*> SortedByKey(const TMap& m) {
    TVector<const typename TMap::value_type*> items;
    items.reserve(m.size());
    for (const auto& kv : m) {
        items.push_back(&kv);
    }
    Sort(items, [](const auto* l, const auto* r) { return l->first < r->first; });
    return items;
}

}  // namespace

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

template <NKikimrSchemeOp::EOperationType Type>
using TOperationTag = std::integral_constant<NKikimrSchemeOp::EOperationType, Type>;

class TOperationPathExtractor {
    using F = EPathField;
    using TAt = TRefSink::TAt;

public:
    TOperationPathExtractor(const NKikimrSchemeOp::TModifyScheme& tx, TRefSink& out)
        : Tx(tx)
        , Out(out)
    {}

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpMkDir>) {
        Out.Add(F::MkDir_Name, Tx.GetMkDir().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateTable>) {
        Out.Add(F::CreateTable_Name, Tx.GetCreateTable().GetName());
        // Copy sources are resolved absolutely.
        if (Tx.GetCreateTable().HasCopyFromTable()) {
            Out.Add(F::CreateTable_CopyFromTable, Tx.GetCreateTable().GetCopyFromTable());
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup>) {
        Out.Add(F::CreatePersQueueGroup_Name, Tx.GetCreatePersQueueGroup().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropTable>) {
        GenericDrop();
        Out.Implicit(F::Implicit_DropTable_Children, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropPersQueueGroup>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterTable>) {
        const auto& alter = Tx.GetAlterTable();
        if (alter.HasPathId()) {
            const auto pathId = TPathId::FromProto(alter.GetPathId());
            Out.ById(F::AlterTable_PathId, pathId.OwnerId, pathId.LocalPathId);
        } else if (alter.HasId_Deprecated()) {
            Out.ById(F::AlterTable_Id_Deprecated, 0, alter.GetId_Deprecated());
        } else {
            Out.Add(F::AlterTable_Name, alter.GetName());
        }
        const int alterTableIndex = Out.Last();
        // Relative sequence names use the table ref as their base, even for ID-addressed tables.
        for (size_t i = 0; i < alter.ColumnsSize(); ++i) {
            const auto& column = alter.GetColumns(i);
            if (!column.HasDefaultFromSequence()) {
                continue;
            }
            const TAt at{.Index = ui32(i)};
            const TString& value = column.GetDefaultFromSequence();
            if (value.StartsWith('/')) {
                Out.AddAs(F::AlterTable_Column_DefaultFromSequence, value,
                    EKind::Absolute, ERole::Dependency, at);
            } else {
                Out.SiblingOf(F::AlterTable_Column_DefaultFromSequence, value,
                    alterTableIndex, at);
            }
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup>) {
        const auto& alter = Tx.GetAlterPersQueueGroup();
        if (alter.HasPathId()) {
            Out.ById(F::AlterPersQueueGroup_PathId, 0, alter.GetPathId());
        } else {
            Out.Add(F::AlterPersQueueGroup_Name, alter.GetName());
        }
        // Incremental-backup destinations are absolute.
        const auto& offload = alter.GetPQTabletConfig().GetOffloadConfig();
        if (offload.HasIncrementalBackup()) {
            Out.Add(F::AlterPersQueueGroup_IncrementalBackup_DstPath,
                offload.GetIncrementalBackup().GetDstPath());
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpModifyACL>) {
        Out.Add(F::ModifyACL_Name, Tx.GetModifyACL().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpRmDir>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions>) {
        const auto& info = Tx.GetSplitMergeTablePartitions();
        if (info.HasTableLocalId()) {
            Out.ById(F::SplitMergeTablePartitions_TableLocalId,
                info.GetTableOwnerId(), info.GetTableLocalId());
        } else {
            // TablePath is resolved without WorkingDir.
            Out.Add(F::SplitMergeTablePartitions_TablePath, info.GetTablePath());
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpBackup>) {
        Out.Add(F::Backup_TableName, Tx.GetBackup().GetTableName());
        Out.Implicit(F::Implicit_Backup_TableChildren, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateSubDomain>) {
        Out.Add(F::SubDomain_Name, Tx.GetSubDomain().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropSubDomain>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateRtmrVolume>) {
        Out.Add(F::CreateRtmrVolume_Name, Tx.GetCreateRtmrVolume().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume>) {
        Out.Add(F::CreateBlockStoreVolume_Name, Tx.GetCreateBlockStoreVolume().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume>) {
        const auto& alter = Tx.GetAlterBlockStoreVolume();
        if (alter.HasPathId()) {
            Out.ById(F::AlterBlockStoreVolume_PathId, 0, alter.GetPathId());
        } else {
            Out.Add(F::AlterBlockStoreVolume_Name, alter.GetName());
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAssignBlockStoreVolume>) {
        Out.Add(F::AssignBlockStoreVolume_Name, Tx.GetAssignBlockStoreVolume().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropBlockStoreVolume>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateKesus>) {
        Out.Add(F::Kesus_Name, Tx.GetKesus().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropKesus>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpForceDropSubDomain>) {
        GenericDrop();
        Out.Implicit(F::Implicit_ForceDropSubDomain_Subtree, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateSolomonVolume>) {
        Out.Add(F::CreateSolomonVolume_Name, Tx.GetCreateSolomonVolume().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropSolomonVolume>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterKesus>) {
        Out.Add(F::Kesus_Name, Tx.GetKesus().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterSubDomain>) {
        Out.Add(F::SubDomain_Name, Tx.GetSubDomain().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterUserAttributes>) {
        Out.Add(F::AlterUserAttributes_PathName, Tx.GetAlterUserAttributes().GetPathName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpForceDropUnsafe>) {
        GenericDrop();
        Out.Implicit(F::Implicit_ForceDropUnsafe_Subtree, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateIndexedTable>) {
        const auto& cfg = Tx.GetCreateIndexedTable();
        const TString& base = cfg.GetTableDescription().GetName();
        Out.Add(F::CreateIndexedTable_TableDescription_Name, base);
        const int baseIndex = Out.Last();
        for (size_t i = 0; i < cfg.IndexDescriptionSize(); ++i) {
            Out.Sibling(F::CreateIndexedTable_IndexDescription_Name,
                cfg.GetIndexDescription(i).GetName(), base, TAt{.Index = ui32(i)});
        }
        for (size_t i = 0; i < cfg.SequenceDescriptionSize(); ++i) {
            Out.Sibling(F::CreateIndexedTable_SequenceDescription_Name,
                cfg.GetSequenceDescription(i).GetName(), base, TAt{.Index = ui32(i)});
        }
        Out.Implicit(F::Implicit_CreateIndexedTable_IndexImplTables, baseIndex);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateTableIndex>) {
        Out.Add(F::CreateTableIndex_Name, Tx.GetCreateTableIndex().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables>) {
        const auto& cfg = Tx.GetCreateConsistentCopyTables();
        for (size_t i = 0; i < cfg.CopyTableDescriptionsSize(); ++i) {
            const auto& item = cfg.GetCopyTableDescriptions(i);
            const TAt at{.Index = ui32(i)};
            Out.Add(F::CopyTables_Item_SrcPath, item.GetSrcPath(), at);
            const int srcIndex = Out.Last();
            Out.Add(F::CopyTables_Item_DstPath, item.GetDstPath(), at);
            if (item.HasCreateSrcCdcStream()) {
                Out.Sibling(F::CopyTables_Item_CreateSrcCdc_StreamName,
                    item.GetCreateSrcCdcStream().GetStreamDescription().GetName(),
                    item.GetSrcPath(), at);
            }
            if (item.HasDropSrcCdcStream()) {
                const auto& drop = item.GetDropSrcCdcStream();
                for (size_t j = 0; j < drop.StreamNameSize(); ++j) {
                    Out.Sibling(F::CopyTables_Item_DropSrcCdc_StreamName,
                        drop.GetStreamName(j), item.GetSrcPath(),
                        TAt{.Index = ui32(i), .SubIndex = ui32(j)});
                }
            }
            for (const auto* kv : SortedByKey(item.GetIndexImplTableCdcStreams())) {
                Out.Sibling(F::CopyTables_Item_IndexImplCdc_StreamName,
                    kv->second.GetStreamDescription().GetName(),
                    Out.Own(JoinPath({item.GetSrcPath(), kv->first})),
                    TAt{.Index = ui32(i), .Key = kv->first});
            }
            for (const auto* kv : SortedByKey(item.GetIndexImplTableDropCdcStreams())) {
                const TStringBuf base = Out.Own(JoinPath({item.GetSrcPath(), kv->first}));
                for (size_t j = 0; j < kv->second.StreamNameSize(); ++j) {
                    Out.Sibling(F::CopyTables_Item_IndexImplDropCdc_StreamName,
                        kv->second.GetStreamName(j), base,
                        TAt{.Index = ui32(i), .SubIndex = ui32(j), .Key = kv->first});
                }
            }
            Out.Implicit(F::Implicit_CopyTables_Item_Children, srcIndex, at);
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropTableIndex>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateExtSubDomain>) {
        Out.Add(F::SubDomain_Name, Tx.GetSubDomain().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterExtSubDomain>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateExtSubDomain>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterExtSubDomainCreateHive>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateExtSubDomain>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain>) {
        GenericDrop();
        Out.Implicit(F::Implicit_ForceDropExtSubDomain_Subtree, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpUpgradeSubDomain>) {
        Out.Add(F::UpgradeSubDomain_Name, Tx.GetUpgradeSubDomain().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpUpgradeSubDomainDecision>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpUpgradeSubDomain>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateIndexBuild>) {
        const auto& cfg = Tx.GetInitiateIndexBuild();
        Out.Add(F::InitiateIndexBuild_Table, cfg.GetTable());
        Out.Sibling(F::InitiateIndexBuild_Index_Name, cfg.GetIndex().GetName(), cfg.GetTable());
        Out.Implicit(F::Implicit_InitiateIndexBuild_IndexImplTables, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpInitiateBuildIndexMainTable>) {
        Out.Add(F::InitiateBuildIndexMainTable_TableName,
            Tx.GetInitiateBuildIndexMainTable().GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpPrepareIndexValidation>) {
        Out.Add(F::PrepareIndexValidation_TableName,
            Tx.GetPrepareIndexValidation().GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateLock>) {
        Out.Add(F::LockConfig_Name, Tx.GetLockConfig().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropLock>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateLock>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpApplyIndexBuild>) {
        const auto& cfg = Tx.GetApplyIndexBuild();
        Out.Add(F::ApplyIndexBuild_TablePath, cfg.GetTablePath());
        Out.Sibling(F::ApplyIndexBuild_IndexName, cfg.GetIndexName(), cfg.GetTablePath());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexMainTable>) {
        Out.Add(F::FinalizeBuildIndexMainTable_TableName,
            Tx.GetFinalizeBuildIndexMainTable().GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterTableIndex>) {
        Out.Add(F::AlterTableIndex_Name, Tx.GetAlterTableIndex().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterSolomonVolume>) {
        Out.Add(F::AlterSolomonVolume_Name, Tx.GetAlterSolomonVolume().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexImplTable>) {
        Out.Add(F::AlterTable_Name, Tx.GetAlterTable().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable>) {
        Out.Add(F::CreateTable_Name, Tx.GetCreateTable().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropIndex>) {
        const auto& cfg = Tx.GetDropIndex();
        Out.Add(F::DropIndex_TableName, cfg.GetTableName());
        Out.Sibling(F::DropIndex_IndexName, cfg.GetIndexName(), cfg.GetTableName());
        Out.Implicit(F::Implicit_DropIndex_IndexImplTables, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropTableIndexAtMainTable>) {
        const auto& cfg = Tx.GetDropIndex();
        // The table is a target resolved as one child of WorkingDir.
        Out.AddAs(F::DropIndex_TableName, cfg.GetTableName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
        Out.Sibling(F::DropIndex_IndexName, cfg.GetIndexName(), cfg.GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCancelIndexBuild>) {
        const auto& cfg = Tx.GetCancelIndexBuild();
        Out.Add(F::CancelIndexBuild_TablePath, cfg.GetTablePath());
        Out.Sibling(F::CancelIndexBuild_IndexName, cfg.GetIndexName(), cfg.GetTablePath());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateFileStore>) {
        Out.Add(F::CreateFileStore_Name, Tx.GetCreateFileStore().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterFileStore>) {
        Out.Add(F::AlterFileStore_Name, Tx.GetAlterFileStore().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropFileStore>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpRestore>) {
        Out.Add(F::Restore_TableName, Tx.GetRestore().GetTableName());
        Out.Implicit(F::Implicit_Restore_TableChildren, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateColumnStore>) {
        Out.Add(F::CreateColumnStore_Name, Tx.GetCreateColumnStore().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterColumnStore>) {
        Out.Add(F::AlterColumnStore_Name, Tx.GetAlterColumnStore().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropColumnStore>) {
        GenericDrop();
        Out.Implicit(F::Implicit_DropColumnStore_ColumnTables, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateColumnTable>) {
        Out.Add(F::CreateColumnTable_Name, Tx.GetCreateColumnTable().GetName());
        // Column-table copy sources are resolved absolutely.
        if (Tx.GetCreateColumnTable().HasCopyFromTable()) {
            Out.Add(F::CreateColumnTable_CopyFromTable,
                Tx.GetCreateColumnTable().GetCopyFromTable());
        }
        EmitTierStorages(F::CreateColumnTable_TierStorage,
            Tx.GetCreateColumnTable().GetTtlSettings());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterColumnTable>) {
        // Fall back to AlterTable.Name when AlterColumnTable is absent.
        if (Tx.HasAlterColumnTable()) {
            Out.Add(F::AlterColumnTable_Name, Tx.GetAlterColumnTable().GetName());
            const int alterColumnTableIndex = Out.Last();
            EmitTierStorages(F::AlterColumnTable_TierStorage,
                Tx.GetAlterColumnTable().GetAlterTtlSettings());
            Out.Implicit(F::Implicit_AlterColumnTable_DroppedTiers, alterColumnTableIndex);
        } else {
            Out.Add(F::AlterTable_Name, Tx.GetAlterTable().GetName());
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropColumnTable>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterLogin>) {
        // Removing a user or group scans the audience subtree for ownership and ACL references.
        if (Tx.GetAlterLogin().HasRemoveUser() || Tx.GetAlterLogin().HasRemoveGroup()) {
            Out.Add(F::WorkingDirItself, {});
            Out.Implicit(F::Implicit_AlterLogin_AclScan, Out.Last());
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateCdcStream>) {
        const auto& op = Tx.GetCreateCdcStream();
        Out.Add(F::CreateCdcStream_TableName, op.GetTableName());
        Out.Sibling(F::CreateCdcStream_StreamDescription_Name,
            op.GetStreamDescription().GetName(), op.GetTableName());
        Out.Implicit(F::Implicit_CreateCdcStream_PqGroupUnderStream, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateCdcStreamImpl>) {
        Out.AddAs(F::CreateCdcStream_StreamDescription_Name,
            Tx.GetCreateCdcStream().GetStreamDescription().GetName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateCdcStreamAtTable>) {
        // The table is the target; resolving the stream supplies its path ID.
        const auto& op = Tx.GetCreateCdcStream();
        // Create resolves TableName as one child, without TSplitChildTag.
        Out.AddAs(F::CreateCdcStream_TableName, op.GetTableName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
        Out.Sibling(F::CreateCdcStream_StreamDescription_Name,
            op.GetStreamDescription().GetName(), op.GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterCdcStream>) {
        const auto& op = Tx.GetAlterCdcStream();
        Out.Add(F::AlterCdcStream_TableName, op.GetTableName());
        Out.Sibling(F::AlterCdcStream_StreamName, op.GetStreamName(), op.GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterCdcStreamImpl>) {
        Out.AddAs(F::AlterCdcStream_StreamName, Tx.GetAlterCdcStream().GetStreamName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterCdcStreamAtTable>) {
        // TableName is split into segments under WorkingDir; StreamName is one child.
        const auto& op = Tx.GetAlterCdcStream();
        Out.AddAs(F::AlterCdcStream_TableName, op.GetTableName(),
            EKind::PathUnderWorkingDirSplit, ERole::Target);
        Out.Sibling(F::AlterCdcStream_StreamName, op.GetStreamName(), op.GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropCdcStream>) {
        const auto& op = Tx.GetDropCdcStream();
        Out.Add(F::DropCdcStream_TableName, op.GetTableName());
        for (size_t i = 0; i < op.StreamNameSize(); ++i) {
            Out.Sibling(F::DropCdcStream_StreamName, op.GetStreamName(i), op.GetTableName(),
                TAt{.Index = ui32(i)});
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropCdcStreamImpl>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropCdcStreamAtTable>) {
        // TableName and each StreamName are resolved as single children.
        const auto& op = Tx.GetDropCdcStream();
        Out.AddAs(F::DropCdcStream_TableName, op.GetTableName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
        for (size_t i = 0; i < op.StreamNameSize(); ++i) {
            Out.Sibling(F::DropCdcStream_StreamName, op.GetStreamName(i), op.GetTableName(),
                TAt{.Index = ui32(i)});
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpRotateCdcStream>) {
        const auto& op = Tx.GetRotateCdcStream();
        Out.Add(F::RotateCdcStream_TableName, op.GetTableName());
        Out.Sibling(F::RotateCdcStream_OldStreamName, op.GetOldStreamName(), op.GetTableName());
        Out.Sibling(F::RotateCdcStream_NewStream_Name,
            op.GetNewStream().GetStreamDescription().GetName(), op.GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpRotateCdcStreamImpl>) {
        const auto& op = Tx.GetRotateCdcStream();
        Out.AddAs(F::RotateCdcStream_OldStreamName, op.GetOldStreamName(),
            EKind::LeafUnderWorkingDir, ERole::Source);
        Out.AddAs(F::RotateCdcStream_NewStream_Name,
            op.GetNewStream().GetStreamDescription().GetName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpRotateCdcStreamAtTable>) {
        // TableName is split under WorkingDir; both stream names are single children.
        const auto& op = Tx.GetRotateCdcStream();
        Out.AddAs(F::RotateCdcStream_TableName, op.GetTableName(),
            EKind::PathUnderWorkingDirSplit, ERole::Target);
        Out.Sibling(F::RotateCdcStream_OldStreamName, op.GetOldStreamName(), op.GetTableName());
        Out.Sibling(F::RotateCdcStream_NewStream_Name,
            op.GetNewStream().GetStreamDescription().GetName(), op.GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpMoveTable>) {
        Out.Add(F::MoveTable_SrcPath, Tx.GetMoveTable().GetSrcPath());
        const int moveSrcIndex = Out.Last();
        Out.Add(F::MoveTable_DstPath, Tx.GetMoveTable().GetDstPath());
        // Cascade children belong to the source.
        Out.Implicit(F::Implicit_MoveTable_Children, moveSrcIndex);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpMoveTableIndex>) {
        Out.Add(F::MoveTableIndex_SrcPath, Tx.GetMoveTableIndex().GetSrcPath());
        const int moveTableIndexSrcIndex = Out.Last();
        Out.Add(F::MoveTableIndex_DstPath, Tx.GetMoveTableIndex().GetDstPath());
        // Implementation tables and sequences belong to the source.
        Out.Implicit(F::Implicit_MoveTableIndex_Children, moveTableIndexSrcIndex);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpMoveSequence>) {
        Out.Add(F::MoveSequence_SrcPath, Tx.GetMoveSequence().GetSrcPath());
        Out.Add(F::MoveSequence_DstPath, Tx.GetMoveSequence().GetDstPath());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateSequence>) {
        Out.Add(F::Sequence_Name, Tx.GetSequence().GetName());
        // Sequence copy sources are resolved absolutely.
        if (Tx.HasCopySequence()) {
            Out.Add(F::CopySequence_CopyFrom, Tx.GetCopySequence().GetCopyFrom());
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterSequence>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateSequence>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropSequence>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateReplication>) {
        Out.Add(F::Replication_Name, Tx.GetReplication().GetName());
        ReplicationPaths(F::Replication_TransferTarget_DstPath,
            F::Replication_TransferTarget_DirectoryPath,
            F::Replication_SpecificTarget_DstPath,
            F::Replication_AlterTransfer_DirectoryPath,
            Tx.GetReplication());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateTransfer>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateReplication>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterReplication>) {
        const auto& op = Tx.GetAlterReplication();
        if (op.HasPathId()) {
            const auto pathId = TPathId::FromProto(op.GetPathId());
            Out.ById(F::AlterReplication_PathId, pathId.OwnerId, pathId.LocalPathId);
        } else {
            Out.Add(F::AlterReplication_Name, op.GetName());
        }
        ReplicationPaths(F::AlterReplication_TransferTarget_DstPath,
            F::AlterReplication_TransferTarget_DirectoryPath,
            F::AlterReplication_SpecificTarget_DstPath,
            F::AlterReplication_AlterTransfer_DirectoryPath,
            op);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterTransfer>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterReplication>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropReplication>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropReplicationCascade>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpDropReplication>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropTransfer>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpDropReplication>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropTransferCascade>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpDropReplication>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateBlobDepot>) {
        Out.Add(F::BlobDepot_Name, Tx.GetBlobDepot().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpMoveIndex>) {
        const auto& op = Tx.GetMoveIndex();
        Out.Add(F::MoveIndex_TablePath, op.GetTablePath());
        Out.Sibling(F::MoveIndex_SrcPath, op.GetSrcPath(), op.GetTablePath());
        const int moveIndexSrcIndex = Out.Last();
        Out.Sibling(F::MoveIndex_DstPath, op.GetDstPath(), op.GetTablePath());
        Out.Implicit(F::Implicit_MoveIndex_IndexImplTables, moveIndexSrcIndex);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateExternalTable>) {
        Out.Add(F::CreateExternalTable_Name, Tx.GetCreateExternalTable().GetName());
        if (Tx.GetCreateExternalTable().HasDataSourcePath()) {
            Out.Add(F::CreateExternalTable_DataSourcePath,
                Tx.GetCreateExternalTable().GetDataSourcePath());
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropExternalTable>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterExternalTable>) {
        Out.Add(F::CreateExternalTable_Name, Tx.GetCreateExternalTable().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateExternalDataSource>) {
        Out.Add(F::CreateExternalDataSource_Name, Tx.GetCreateExternalDataSource().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropExternalDataSource>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterExternalDataSource>) {
        Out.Add(F::CreateExternalDataSource_Name, Tx.GetCreateExternalDataSource().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateColumnBuild>) {
        Out.Add(F::InitiateColumnBuild_Table, Tx.GetInitiateColumnBuild().GetTable());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropColumnBuild>) {
        Out.Add(F::DropColumnBuild_Settings_Table,
            Tx.GetDropColumnBuild().GetSettings().GetTable());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateView>) {
        Out.Add(F::CreateView_Name, Tx.GetCreateView().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropView>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateContinuousBackup>) {
        const auto& op = Tx.GetCreateContinuousBackup();
        Out.Add(F::CreateContinuousBackup_TableName, op.GetTableName());
        const int cbTableIndex = Out.Last();
        // Absent stream names are generated during execution.
        if (op.GetContinuousBackupDescription().HasStreamName()) {
            Out.SiblingOf(F::CreateContinuousBackup_StreamName,
                op.GetContinuousBackupDescription().GetStreamName(), cbTableIndex);
        }
        Out.Implicit(F::Implicit_CreateContinuousBackup_CdcStream, cbTableIndex);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterContinuousBackup>) {
        const auto& op = Tx.GetAlterContinuousBackup();
        // TSplitChildTag keeps even leading-slash paths under WorkingDir.
        Out.Add(F::AlterContinuousBackup_TableName, op.GetTableName());
        const int cbTableIndex = Out.Last();
        if (op.HasTakeIncrementalBackup()) {
            const auto& take = op.GetTakeIncrementalBackup();
            Out.Add(F::AlterContinuousBackup_TakeIncrementalBackup_DstPath, take.GetDstPath());
            if (take.HasDstStreamPath()) {
                // Absent stream names are generated during execution.
                Out.SiblingOf(F::AlterContinuousBackup_TakeIncrementalBackup_DstStreamPath,
                    take.GetDstStreamPath(), cbTableIndex);
            }
        }
        Out.Implicit(F::Implicit_AlterContinuousBackup_IncrementalBackupTable, cbTableIndex);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropContinuousBackup>) {
        Out.Add(F::DropContinuousBackup_TableName, Tx.GetDropContinuousBackup().GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateResourcePool>) {
        Out.Add(F::CreateResourcePool_Name, Tx.GetCreateResourcePool().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterResourcePool>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateResourcePool>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropResourcePool>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups>) {
        // Retired; the factory rejects these operations.
        const auto& op = Tx.GetRestoreMultipleIncrementalBackups();
        for (size_t i = 0; i < op.SrcTablePathsSize(); ++i) {
            Out.Add(F::RestoreMultipleIncrementalBackups_SrcTablePaths,
                op.GetSrcTablePaths(i), TAt{.Index = ui32(i)});
        }
        Out.Add(F::RestoreMultipleIncrementalBackups_DstTablePath, op.GetDstTablePath());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackupAtTable>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateBackupCollection>) {
        const auto& op = Tx.GetCreateBackupCollection();
        Out.Add(F::CreateBackupCollection_Name, op.GetName());
        // Backup collection entries are resolved absolutely.
        const auto& entryList = op.GetExplicitEntryList();
        for (size_t i = 0; i < entryList.EntriesSize(); ++i) {
            Out.Add(F::CreateBackupCollection_Entry_Path, entryList.GetEntries(i).GetPath(),
                TAt{.Index = ui32(i)});
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterBackupCollection>) {
        Out.Add(F::AlterBackupCollection_Name, Tx.GetAlterBackupCollection().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropBackupCollection>) {
        Out.Add(F::DropBackupCollection_Name, Tx.GetDropBackupCollection().GetName());
        Out.Implicit(F::Implicit_DropBackupCollection_Entries, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpBackupBackupCollection>) {
        Out.Add(F::BackupBackupCollection_Name, Tx.GetBackupBackupCollection().GetName());
        Out.Implicit(F::Implicit_BackupBackupCollection_Entries, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection>) {
        Out.Add(F::BackupIncrementalBackupCollection_Name,
            Tx.GetBackupIncrementalBackupCollection().GetName());
        Out.Implicit(F::Implicit_BackupIncrementalBackupCollection_Entries, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateLongIncrementalBackupOp>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateFullBackupOp>) {
        // WorkingDir already points at the backup collection; no name field.
        Out.Add(F::WorkingDirItself, {});
        Out.Implicit(F::Implicit_CreateFullBackupOp_Entries, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpRestoreBackupCollection>) {
        Out.Add(F::RestoreBackupCollection_Name, Tx.GetRestoreBackupCollection().GetName());
        Out.Implicit(F::Implicit_RestoreBackupCollection_Entries, Out.Last());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateLongIncrementalRestoreOp>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpRestoreBackupCollection>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateSysView>) {
        Out.Add(F::CreateSysView_Name, Tx.GetCreateSysView().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropSysView>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpChangePathState>) {
        Out.Add(F::ChangePathState_Path, Tx.GetChangePathState().GetPath());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpIncrementalRestoreLockTargets>) {
        const auto& op = Tx.GetIncrementalRestoreLockTargets();
        for (size_t i = 0; i < op.DstPathsSize(); ++i) {
            Out.Add(F::IncrementalRestoreLockTargets_DstPaths, op.GetDstPaths(i),
                TAt{.Index = ui32(i)});
        }
        for (size_t i = 0; i < op.SrcPathsSize(); ++i) {
            Out.Add(F::IncrementalRestoreLockTargets_SrcPaths, op.GetSrcPaths(i),
                TAt{.Index = ui32(i)});
        }
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpIncrementalRestoreUnlockTargets>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpIncrementalRestoreLockTargets>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpIncrementalRestoreFinalize>) {
        Out.Implicit(F::Implicit_IncrementalRestoreFinalize_PersistedState, -1);
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateSecret>) {
        Out.Add(F::CreateSecret_Name, Tx.GetCreateSecret().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterSecret>) {
        Out.Add(F::AlterSecret_Name, Tx.GetAlterSecret().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropSecret>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateStreamingQuery>) {
        Out.Add(F::CreateStreamingQuery_Name, Tx.GetCreateStreamingQuery().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpAlterStreamingQuery>) {
        (*this)(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateStreamingQuery>{});
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropStreamingQuery>) {
        GenericDrop();
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpTruncateTable>) {
        Out.Add(F::TruncateTable_TableName, Tx.GetTruncateTable().GetTableName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpCreateTestShardSet>) {
        Out.Add(F::CreateTestShardSet_Name, Tx.GetCreateTestShardSet().GetName());
    }

    void operator()(TOperationTag<NKikimrSchemeOp::ESchemeOpDropTestShardSet>) {
        GenericDrop();
    }

private:
    void GenericDrop() {
        const auto& drop = Tx.GetDrop();
        if (drop.HasId()) {
            // An ID takes precedence over Name.
            Out.ById(F::Drop_Id, 0, drop.GetId());
        } else {
            Out.Add(F::Drop_Name, drop.GetName());
        }
    }

    // TTL Storage fields name external data sources by absolute path.
    void EmitTierStorages(F field, const NKikimrSchemeOp::TColumnDataLifeCycle& ttl) {
        if (!ttl.HasEnabled()) {
            return;
        }
        const auto& tiers = ttl.GetEnabled().GetTiers();
        for (int i = 0; i < tiers.size(); ++i) {
            if (tiers[i].HasEvictToExternalStorage()) {
                Out.Add(field, tiers[i].GetEvictToExternalStorage().GetStorage(),
                    TAt{.Index = ui32(i)});
            }
        }
    }

    // SrcPath belongs to the remote cluster; extract only local paths.
    void ReplicationPaths(F transferDstPath, F transferDirectoryPath,
            F specificTargetDstPath, F alterTransferDirectoryPath,
            const NKikimrSchemeOp::TReplicationDescription& desc) {
        const auto& config = desc.GetConfig();
        if (config.HasTransferSpecific()) {
            const auto& target = config.GetTransferSpecific().GetTarget();
            if (target.HasDstPath()) {
                Out.Add(transferDstPath, target.GetDstPath());
            }
            if (target.HasDirectoryPath()) {
                Out.Add(transferDirectoryPath, target.GetDirectoryPath());
            }
        }
        // The replication controller creates these absolute destinations later.
        const auto& specific = config.GetSpecific();
        for (size_t i = 0; i < specific.TargetsSize(); ++i) {
            const auto& target = specific.GetTargets(i);
            if (target.HasDstPath()) {
                Out.Add(specificTargetDstPath, target.GetDstPath(), TAt{.Index = ui32(i)});
            }
        }
        if (desc.HasAlterTransfer() && desc.GetAlterTransfer().HasDirectoryPath()) {
            Out.Add(alterTransferDirectoryPath, desc.GetAlterTransfer().GetDirectoryPath());
        }
    }

    const NKikimrSchemeOp::TModifyScheme& Tx;
    TRefSink& Out;
};

template <NKikimrSchemeOp::EOperationType Type>
void ExtractRegisteredOperation(const NKikimrSchemeOp::TModifyScheme& tx, TRefSink& out) {
    TOperationPathExtractor extractor(tx, out);
    if constexpr (requires { extractor(TOperationTag<Type>{}); }) {
        extractor(TOperationTag<Type>{});
    } else {
        constexpr auto support = GetSchemeOperationSupport(Type);
        static_assert(support == ESchemeOperationSupport::Unsupported
                || support == ESchemeOperationSupport::Stub
                || support == ESchemeOperationSupport::Deprecated,
            "Implemented operations must declare a path extractor");
    }
}

} // namespace

TPathRefs ExtractPathRefs(const NKikimrSchemeOp::TModifyScheme& tx) {
    using F = EPathField;
    using TAt = TRefSink::TAt;

    TPathRefs result;
    TRefSink out(result);

    DispatchSchemeOperation(tx.GetOperationType(), [&](auto tag) {
        ExtractRegisteredOperation<decltype(tag)::value>(tx, out);
    });

    // Every operation resolves ApplyIf IDs on this SchemeShard before proposing.
    for (size_t i = 0; i < size_t(tx.ApplyIfSize()); ++i) {
        const auto& item = tx.GetApplyIf(i);
        if (item.HasPathId()) {
            out.ById(F::ApplyIf_PathId, 0, item.GetPathId(), TAt{.Index = ui32(i)});
        }
    }

    return result;
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
