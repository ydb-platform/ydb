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

TPathRefs ExtractPathRefs(const NKikimrSchemeOp::TModifyScheme& tx) {
    using F = EPathField;
    using TAt = TRefSink::TAt;

    TPathRefs result;
    TRefSink out(result);

    const auto genericDrop = [&]() {
        const auto& drop = tx.GetDrop();
        if (drop.HasId()) {
            // An ID takes precedence over Name.
            out.ById(F::Drop_Id, 0, drop.GetId());
        } else {
            out.Add(F::Drop_Name, drop.GetName());
        }
    };

    // TTL Storage fields name external data sources by absolute path.
    const auto emitTierStorages = [&](F field, const NKikimrSchemeOp::TColumnDataLifeCycle& ttl) {
        if (!ttl.HasEnabled()) {
            return;
        }
        const auto& tiers = ttl.GetEnabled().GetTiers();
        for (int i = 0; i < tiers.size(); ++i) {
            if (tiers[i].HasEvictToExternalStorage()) {
                out.Add(field, tiers[i].GetEvictToExternalStorage().GetStorage(),
                    TAt{.Index = ui32(i)});
            }
        }
    };

    // SrcPath belongs to the remote cluster; extract only local paths.
    const auto replicationPaths = [&](F transferDstPath, F transferDirectoryPath,
            F specificTargetDstPath, F alterTransferDirectoryPath,
            const NKikimrSchemeOp::TReplicationDescription& desc) {
        const auto& config = desc.GetConfig();
        if (config.HasTransferSpecific()) {
            const auto& target = config.GetTransferSpecific().GetTarget();
            if (target.HasDstPath()) {
                out.Add(transferDstPath, target.GetDstPath());
            }
            if (target.HasDirectoryPath()) {
                out.Add(transferDirectoryPath, target.GetDirectoryPath());
            }
        }
        // The replication controller creates these absolute destinations later.
        const auto& specific = config.GetSpecific();
        for (size_t i = 0; i < specific.TargetsSize(); ++i) {
            const auto& target = specific.GetTargets(i);
            if (target.HasDstPath()) {
                out.Add(specificTargetDstPath, target.GetDstPath(), TAt{.Index = ui32(i)});
            }
        }
        if (desc.HasAlterTransfer() && desc.GetAlterTransfer().HasDirectoryPath()) {
            out.Add(alterTransferDirectoryPath, desc.GetAlterTransfer().GetDirectoryPath());
        }
    };

    switch (tx.GetOperationType()) {
    case NKikimrSchemeOp::ESchemeOpMkDir:
        out.Add(F::MkDir_Name, tx.GetMkDir().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateTable:
        out.Add(F::CreateTable_Name, tx.GetCreateTable().GetName());
        // Copy sources are resolved absolutely.
        if (tx.GetCreateTable().HasCopyFromTable()) {
            out.Add(F::CreateTable_CopyFromTable, tx.GetCreateTable().GetCopyFromTable());
        }
        break;
    case NKikimrSchemeOp::ESchemeOpCreatePersQueueGroup:
        out.Add(F::CreatePersQueueGroup_Name, tx.GetCreatePersQueueGroup().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropTable:
        genericDrop();
        out.Implicit(F::Implicit_DropTable_Children, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpDropPersQueueGroup:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpAlterTable: {
        const auto& alter = tx.GetAlterTable();
        if (alter.HasPathId()) {
            const auto pathId = TPathId::FromProto(alter.GetPathId());
            out.ById(F::AlterTable_PathId, pathId.OwnerId, pathId.LocalPathId);
        } else if (alter.HasId_Deprecated()) {
            out.ById(F::AlterTable_Id_Deprecated, 0, alter.GetId_Deprecated());
        } else {
            out.Add(F::AlterTable_Name, alter.GetName());
        }
        const int alterTableIndex = out.Last();
        // Relative sequence names use the table ref as their base, even for ID-addressed tables.
        for (size_t i = 0; i < alter.ColumnsSize(); ++i) {
            const auto& column = alter.GetColumns(i);
            if (!column.HasDefaultFromSequence()) {
                continue;
            }
            const TAt at{.Index = ui32(i)};
            const TString& value = column.GetDefaultFromSequence();
            if (value.StartsWith('/')) {
                out.AddAs(F::AlterTable_Column_DefaultFromSequence, value,
                    EKind::Absolute, ERole::Dependency, at);
            } else {
                out.SiblingOf(F::AlterTable_Column_DefaultFromSequence, value,
                    alterTableIndex, at);
            }
        }
        break;
    }
    case NKikimrSchemeOp::ESchemeOpAlterPersQueueGroup: {
        const auto& alter = tx.GetAlterPersQueueGroup();
        if (alter.HasPathId()) {
            out.ById(F::AlterPersQueueGroup_PathId, 0, alter.GetPathId());
        } else {
            out.Add(F::AlterPersQueueGroup_Name, alter.GetName());
        }
        // Incremental-backup destinations are absolute.
        const auto& offload = alter.GetPQTabletConfig().GetOffloadConfig();
        if (offload.HasIncrementalBackup()) {
            out.Add(F::AlterPersQueueGroup_IncrementalBackup_DstPath,
                offload.GetIncrementalBackup().GetDstPath());
        }
        break;
    }
    case NKikimrSchemeOp::ESchemeOpModifyACL:
        out.Add(F::ModifyACL_Name, tx.GetModifyACL().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpRmDir:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions: {
        const auto& info = tx.GetSplitMergeTablePartitions();
        if (info.HasTableLocalId()) {
            out.ById(F::SplitMergeTablePartitions_TableLocalId,
                info.GetTableOwnerId(), info.GetTableLocalId());
        } else {
            // TablePath is resolved without WorkingDir.
            out.Add(F::SplitMergeTablePartitions_TablePath, info.GetTablePath());
        }
        break;
    }
    case NKikimrSchemeOp::ESchemeOpBackup:
        out.Add(F::Backup_TableName, tx.GetBackup().GetTableName());
        out.Implicit(F::Implicit_Backup_TableChildren, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateSubDomain:
        out.Add(F::SubDomain_Name, tx.GetSubDomain().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropSubDomain:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateRtmrVolume:
        out.Add(F::CreateRtmrVolume_Name, tx.GetCreateRtmrVolume().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume:
        out.Add(F::CreateBlockStoreVolume_Name, tx.GetCreateBlockStoreVolume().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterBlockStoreVolume: {
        const auto& alter = tx.GetAlterBlockStoreVolume();
        if (alter.HasPathId()) {
            out.ById(F::AlterBlockStoreVolume_PathId, 0, alter.GetPathId());
        } else {
            out.Add(F::AlterBlockStoreVolume_Name, alter.GetName());
        }
        break;
    }
    case NKikimrSchemeOp::ESchemeOpAssignBlockStoreVolume:
        out.Add(F::AssignBlockStoreVolume_Name, tx.GetAssignBlockStoreVolume().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropBlockStoreVolume:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateKesus:
        out.Add(F::Kesus_Name, tx.GetKesus().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropKesus:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpForceDropSubDomain:
        genericDrop();
        out.Implicit(F::Implicit_ForceDropSubDomain_Subtree, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateSolomonVolume:
        out.Add(F::CreateSolomonVolume_Name, tx.GetCreateSolomonVolume().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropSolomonVolume:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpAlterKesus:
        out.Add(F::Kesus_Name, tx.GetKesus().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterSubDomain:
        out.Add(F::SubDomain_Name, tx.GetSubDomain().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterUserAttributes:
        out.Add(F::AlterUserAttributes_PathName, tx.GetAlterUserAttributes().GetPathName());
        break;
    case NKikimrSchemeOp::ESchemeOpForceDropUnsafe:
        genericDrop();
        out.Implicit(F::Implicit_ForceDropUnsafe_Subtree, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateIndexedTable: {
        const auto& cfg = tx.GetCreateIndexedTable();
        const TString& base = cfg.GetTableDescription().GetName();
        out.Add(F::CreateIndexedTable_TableDescription_Name, base);
        const int baseIndex = out.Last();
        for (size_t i = 0; i < cfg.IndexDescriptionSize(); ++i) {
            out.Sibling(F::CreateIndexedTable_IndexDescription_Name,
                cfg.GetIndexDescription(i).GetName(), base, TAt{.Index = ui32(i)});
        }
        for (size_t i = 0; i < cfg.SequenceDescriptionSize(); ++i) {
            out.Sibling(F::CreateIndexedTable_SequenceDescription_Name,
                cfg.GetSequenceDescription(i).GetName(), base, TAt{.Index = ui32(i)});
        }
        out.Implicit(F::Implicit_CreateIndexedTable_IndexImplTables, baseIndex);
        break;
    }
    case NKikimrSchemeOp::ESchemeOpCreateTableIndex:
        out.Add(F::CreateTableIndex_Name, tx.GetCreateTableIndex().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateConsistentCopyTables: {
        const auto& cfg = tx.GetCreateConsistentCopyTables();
        for (size_t i = 0; i < cfg.CopyTableDescriptionsSize(); ++i) {
            const auto& item = cfg.GetCopyTableDescriptions(i);
            const TAt at{.Index = ui32(i)};
            out.Add(F::CopyTables_Item_SrcPath, item.GetSrcPath(), at);
            const int srcIndex = out.Last();
            out.Add(F::CopyTables_Item_DstPath, item.GetDstPath(), at);
            if (item.HasCreateSrcCdcStream()) {
                out.Sibling(F::CopyTables_Item_CreateSrcCdc_StreamName,
                    item.GetCreateSrcCdcStream().GetStreamDescription().GetName(),
                    item.GetSrcPath(), at);
            }
            if (item.HasDropSrcCdcStream()) {
                const auto& drop = item.GetDropSrcCdcStream();
                for (size_t j = 0; j < drop.StreamNameSize(); ++j) {
                    out.Sibling(F::CopyTables_Item_DropSrcCdc_StreamName,
                        drop.GetStreamName(j), item.GetSrcPath(),
                        TAt{.Index = ui32(i), .SubIndex = ui32(j)});
                }
            }
            for (const auto* kv : SortedByKey(item.GetIndexImplTableCdcStreams())) {
                out.Sibling(F::CopyTables_Item_IndexImplCdc_StreamName,
                    kv->second.GetStreamDescription().GetName(),
                    out.Own(JoinPath({item.GetSrcPath(), kv->first})),
                    TAt{.Index = ui32(i), .Key = kv->first});
            }
            for (const auto* kv : SortedByKey(item.GetIndexImplTableDropCdcStreams())) {
                const TStringBuf base = out.Own(JoinPath({item.GetSrcPath(), kv->first}));
                for (size_t j = 0; j < kv->second.StreamNameSize(); ++j) {
                    out.Sibling(F::CopyTables_Item_IndexImplDropCdc_StreamName,
                        kv->second.GetStreamName(j), base,
                        TAt{.Index = ui32(i), .SubIndex = ui32(j), .Key = kv->first});
                }
            }
            out.Implicit(F::Implicit_CopyTables_Item_Children, srcIndex, at);
        }
        break;
    }
    case NKikimrSchemeOp::ESchemeOpDropTableIndex:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateExtSubDomain:
    case NKikimrSchemeOp::ESchemeOpAlterExtSubDomain:
    case NKikimrSchemeOp::ESchemeOpAlterExtSubDomainCreateHive:
        out.Add(F::SubDomain_Name, tx.GetSubDomain().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpForceDropExtSubDomain:
        genericDrop();
        out.Implicit(F::Implicit_ForceDropExtSubDomain_Subtree, out.Last());
        break;
    case NKikimrSchemeOp::EOperationType::ESchemeOp_DEPRECATED_35:
        break;
    case NKikimrSchemeOp::ESchemeOpUpgradeSubDomain:
    case NKikimrSchemeOp::ESchemeOpUpgradeSubDomainDecision:
        out.Add(F::UpgradeSubDomain_Name, tx.GetUpgradeSubDomain().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateIndexBuild: {
        const auto& cfg = tx.GetInitiateIndexBuild();
        out.Add(F::InitiateIndexBuild_Table, cfg.GetTable());
        out.Sibling(F::InitiateIndexBuild_Index_Name, cfg.GetIndex().GetName(), cfg.GetTable());
        out.Implicit(F::Implicit_InitiateIndexBuild_IndexImplTables, out.Last());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpInitiateBuildIndexMainTable:
        out.Add(F::InitiateBuildIndexMainTable_TableName,
            tx.GetInitiateBuildIndexMainTable().GetTableName());
        break;
    case NKikimrSchemeOp::ESchemeOpPrepareIndexValidation:
        out.Add(F::PrepareIndexValidation_TableName,
            tx.GetPrepareIndexValidation().GetTableName());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateLock:
    case NKikimrSchemeOp::ESchemeOpDropLock:
        out.Add(F::LockConfig_Name, tx.GetLockConfig().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpApplyIndexBuild: {
        const auto& cfg = tx.GetApplyIndexBuild();
        out.Add(F::ApplyIndexBuild_TablePath, cfg.GetTablePath());
        out.Sibling(F::ApplyIndexBuild_IndexName, cfg.GetIndexName(), cfg.GetTablePath());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexMainTable:
        out.Add(F::FinalizeBuildIndexMainTable_TableName,
            tx.GetFinalizeBuildIndexMainTable().GetTableName());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterTableIndex:
        out.Add(F::AlterTableIndex_Name, tx.GetAlterTableIndex().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterSolomonVolume:
        out.Add(F::AlterSolomonVolume_Name, tx.GetAlterSolomonVolume().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpFinalizeBuildIndexImplTable:
        out.Add(F::AlterTable_Name, tx.GetAlterTable().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable:
        out.Add(F::CreateTable_Name, tx.GetCreateTable().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropIndex: {
        const auto& cfg = tx.GetDropIndex();
        out.Add(F::DropIndex_TableName, cfg.GetTableName());
        out.Sibling(F::DropIndex_IndexName, cfg.GetIndexName(), cfg.GetTableName());
        out.Implicit(F::Implicit_DropIndex_IndexImplTables, out.Last());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpDropTableIndexAtMainTable: {
        const auto& cfg = tx.GetDropIndex();
        // The table is a target resolved as one child of WorkingDir.
        out.AddAs(F::DropIndex_TableName, cfg.GetTableName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
        out.Sibling(F::DropIndex_IndexName, cfg.GetIndexName(), cfg.GetTableName());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpCancelIndexBuild: {
        const auto& cfg = tx.GetCancelIndexBuild();
        out.Add(F::CancelIndexBuild_TablePath, cfg.GetTablePath());
        out.Sibling(F::CancelIndexBuild_IndexName, cfg.GetIndexName(), cfg.GetTablePath());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpCreateFileStore:
        out.Add(F::CreateFileStore_Name, tx.GetCreateFileStore().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterFileStore:
        out.Add(F::AlterFileStore_Name, tx.GetAlterFileStore().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropFileStore:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpRestore:
        out.Add(F::Restore_TableName, tx.GetRestore().GetTableName());
        out.Implicit(F::Implicit_Restore_TableChildren, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateColumnStore:
        out.Add(F::CreateColumnStore_Name, tx.GetCreateColumnStore().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterColumnStore:
        out.Add(F::AlterColumnStore_Name, tx.GetAlterColumnStore().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropColumnStore:
        genericDrop();
        out.Implicit(F::Implicit_DropColumnStore_ColumnTables, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateColumnTable:
        out.Add(F::CreateColumnTable_Name, tx.GetCreateColumnTable().GetName());
        // Column-table copy sources are resolved absolutely.
        if (tx.GetCreateColumnTable().HasCopyFromTable()) {
            out.Add(F::CreateColumnTable_CopyFromTable,
                tx.GetCreateColumnTable().GetCopyFromTable());
        }
        emitTierStorages(F::CreateColumnTable_TierStorage,
            tx.GetCreateColumnTable().GetTtlSettings());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterColumnTable:
        // Fall back to AlterTable.Name when AlterColumnTable is absent.
        if (tx.HasAlterColumnTable()) {
            out.Add(F::AlterColumnTable_Name, tx.GetAlterColumnTable().GetName());
            const int alterColumnTableIndex = out.Last();
            emitTierStorages(F::AlterColumnTable_TierStorage,
                tx.GetAlterColumnTable().GetAlterTtlSettings());
            out.Implicit(F::Implicit_AlterColumnTable_DroppedTiers, alterColumnTableIndex);
        } else {
            out.Add(F::AlterTable_Name, tx.GetAlterTable().GetName());
        }
        break;
    case NKikimrSchemeOp::ESchemeOpDropColumnTable:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpAlterLogin:
        // Removing a user or group scans the audience subtree for ownership and ACL references.
        if (tx.GetAlterLogin().HasRemoveUser() || tx.GetAlterLogin().HasRemoveGroup()) {
            out.Add(F::WorkingDirItself, {});
            out.Implicit(F::Implicit_AlterLogin_AclScan, out.Last());
        }
        break;
    case NKikimrSchemeOp::ESchemeOpCreateCdcStream: {
        const auto& op = tx.GetCreateCdcStream();
        out.Add(F::CreateCdcStream_TableName, op.GetTableName());
        out.Sibling(F::CreateCdcStream_StreamDescription_Name,
            op.GetStreamDescription().GetName(), op.GetTableName());
        out.Implicit(F::Implicit_CreateCdcStream_PqGroupUnderStream, out.Last());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpCreateCdcStreamImpl:
        out.AddAs(F::CreateCdcStream_StreamDescription_Name,
            tx.GetCreateCdcStream().GetStreamDescription().GetName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
        break;
    case NKikimrSchemeOp::ESchemeOpCreateCdcStreamAtTable: {
        // The table is the target; resolving the stream supplies its path ID.
        const auto& op = tx.GetCreateCdcStream();
        // Create resolves TableName as one child, without TSplitChildTag.
        out.AddAs(F::CreateCdcStream_TableName, op.GetTableName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
        out.Sibling(F::CreateCdcStream_StreamDescription_Name,
            op.GetStreamDescription().GetName(), op.GetTableName());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpAlterCdcStream: {
        const auto& op = tx.GetAlterCdcStream();
        out.Add(F::AlterCdcStream_TableName, op.GetTableName());
        out.Sibling(F::AlterCdcStream_StreamName, op.GetStreamName(), op.GetTableName());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpAlterCdcStreamImpl:
        out.AddAs(F::AlterCdcStream_StreamName, tx.GetAlterCdcStream().GetStreamName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
        break;
    case NKikimrSchemeOp::ESchemeOpAlterCdcStreamAtTable: {
        // TableName is split into segments under WorkingDir; StreamName is one child.
        const auto& op = tx.GetAlterCdcStream();
        out.AddAs(F::AlterCdcStream_TableName, op.GetTableName(),
            EKind::PathUnderWorkingDirSplit, ERole::Target);
        out.Sibling(F::AlterCdcStream_StreamName, op.GetStreamName(), op.GetTableName());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpDropCdcStream: {
        const auto& op = tx.GetDropCdcStream();
        out.Add(F::DropCdcStream_TableName, op.GetTableName());
        for (size_t i = 0; i < op.StreamNameSize(); ++i) {
            out.Sibling(F::DropCdcStream_StreamName, op.GetStreamName(i), op.GetTableName(),
                TAt{.Index = ui32(i)});
        }
        break;
    }
    case NKikimrSchemeOp::ESchemeOpDropCdcStreamImpl:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpDropCdcStreamAtTable: {
        // TableName and each StreamName are resolved as single children.
        const auto& op = tx.GetDropCdcStream();
        out.AddAs(F::DropCdcStream_TableName, op.GetTableName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
        for (size_t i = 0; i < op.StreamNameSize(); ++i) {
            out.Sibling(F::DropCdcStream_StreamName, op.GetStreamName(i), op.GetTableName(),
                TAt{.Index = ui32(i)});
        }
        break;
    }
    case NKikimrSchemeOp::ESchemeOpRotateCdcStream: {
        const auto& op = tx.GetRotateCdcStream();
        out.Add(F::RotateCdcStream_TableName, op.GetTableName());
        out.Sibling(F::RotateCdcStream_OldStreamName, op.GetOldStreamName(), op.GetTableName());
        out.Sibling(F::RotateCdcStream_NewStream_Name,
            op.GetNewStream().GetStreamDescription().GetName(), op.GetTableName());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpRotateCdcStreamImpl: {
        const auto& op = tx.GetRotateCdcStream();
        out.AddAs(F::RotateCdcStream_OldStreamName, op.GetOldStreamName(),
            EKind::LeafUnderWorkingDir, ERole::Source);
        out.AddAs(F::RotateCdcStream_NewStream_Name,
            op.GetNewStream().GetStreamDescription().GetName(),
            EKind::LeafUnderWorkingDir, ERole::Target);
        break;
    }
    case NKikimrSchemeOp::ESchemeOpRotateCdcStreamAtTable: {
        // TableName is split under WorkingDir; both stream names are single children.
        const auto& op = tx.GetRotateCdcStream();
        out.AddAs(F::RotateCdcStream_TableName, op.GetTableName(),
            EKind::PathUnderWorkingDirSplit, ERole::Target);
        out.Sibling(F::RotateCdcStream_OldStreamName, op.GetOldStreamName(), op.GetTableName());
        out.Sibling(F::RotateCdcStream_NewStream_Name,
            op.GetNewStream().GetStreamDescription().GetName(), op.GetTableName());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpMoveTable: {
        out.Add(F::MoveTable_SrcPath, tx.GetMoveTable().GetSrcPath());
        const int moveSrcIndex = out.Last();
        out.Add(F::MoveTable_DstPath, tx.GetMoveTable().GetDstPath());
        // Cascade children belong to the source.
        out.Implicit(F::Implicit_MoveTable_Children, moveSrcIndex);
        break;
    }
    case NKikimrSchemeOp::ESchemeOpMoveTableIndex: {
        out.Add(F::MoveTableIndex_SrcPath, tx.GetMoveTableIndex().GetSrcPath());
        const int moveTableIndexSrcIndex = out.Last();
        out.Add(F::MoveTableIndex_DstPath, tx.GetMoveTableIndex().GetDstPath());
        // Implementation tables and sequences belong to the source.
        out.Implicit(F::Implicit_MoveTableIndex_Children, moveTableIndexSrcIndex);
        break;
    }
    case NKikimrSchemeOp::ESchemeOpMoveSequence:
        out.Add(F::MoveSequence_SrcPath, tx.GetMoveSequence().GetSrcPath());
        out.Add(F::MoveSequence_DstPath, tx.GetMoveSequence().GetDstPath());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateSequence:
    case NKikimrSchemeOp::ESchemeOpAlterSequence:
        out.Add(F::Sequence_Name, tx.GetSequence().GetName());
        // Sequence copy sources are resolved absolutely.
        if (tx.HasCopySequence()) {
            out.Add(F::CopySequence_CopyFrom, tx.GetCopySequence().GetCopyFrom());
        }
        break;
    case NKikimrSchemeOp::ESchemeOpDropSequence:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateReplication:
    case NKikimrSchemeOp::ESchemeOpCreateTransfer:
        out.Add(F::Replication_Name, tx.GetReplication().GetName());
        replicationPaths(F::Replication_TransferTarget_DstPath,
            F::Replication_TransferTarget_DirectoryPath,
            F::Replication_SpecificTarget_DstPath,
            F::Replication_AlterTransfer_DirectoryPath,
            tx.GetReplication());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterReplication:
    case NKikimrSchemeOp::ESchemeOpAlterTransfer: {
        const auto& op = tx.GetAlterReplication();
        if (op.HasPathId()) {
            const auto pathId = TPathId::FromProto(op.GetPathId());
            out.ById(F::AlterReplication_PathId, pathId.OwnerId, pathId.LocalPathId);
        } else {
            out.Add(F::AlterReplication_Name, op.GetName());
        }
        replicationPaths(F::AlterReplication_TransferTarget_DstPath,
            F::AlterReplication_TransferTarget_DirectoryPath,
            F::AlterReplication_SpecificTarget_DstPath,
            F::AlterReplication_AlterTransfer_DirectoryPath,
            op);
        break;
    }
    case NKikimrSchemeOp::ESchemeOpDropReplication:
    case NKikimrSchemeOp::ESchemeOpDropReplicationCascade:
    case NKikimrSchemeOp::ESchemeOpDropTransfer:
    case NKikimrSchemeOp::ESchemeOpDropTransferCascade:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateBlobDepot:
        out.Add(F::BlobDepot_Name, tx.GetBlobDepot().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterBlobDepot:
    case NKikimrSchemeOp::ESchemeOpDropBlobDepot:
        // These operations are stubs and touch no paths.
        break;
    case NKikimrSchemeOp::ESchemeOpMoveIndex: {
        const auto& op = tx.GetMoveIndex();
        out.Add(F::MoveIndex_TablePath, op.GetTablePath());
        out.Sibling(F::MoveIndex_SrcPath, op.GetSrcPath(), op.GetTablePath());
        const int moveIndexSrcIndex = out.Last();
        out.Sibling(F::MoveIndex_DstPath, op.GetDstPath(), op.GetTablePath());
        out.Implicit(F::Implicit_MoveIndex_IndexImplTables, moveIndexSrcIndex);
        break;
    }
    case NKikimrSchemeOp::ESchemeOpCreateExternalTable:
        out.Add(F::CreateExternalTable_Name, tx.GetCreateExternalTable().GetName());
        if (tx.GetCreateExternalTable().HasDataSourcePath()) {
            out.Add(F::CreateExternalTable_DataSourcePath,
                tx.GetCreateExternalTable().GetDataSourcePath());
        }
        break;
    case NKikimrSchemeOp::ESchemeOpDropExternalTable:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpAlterExternalTable:
        out.Add(F::CreateExternalTable_Name, tx.GetCreateExternalTable().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateExternalDataSource:
        out.Add(F::CreateExternalDataSource_Name, tx.GetCreateExternalDataSource().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropExternalDataSource:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpAlterExternalDataSource:
        out.Add(F::CreateExternalDataSource_Name, tx.GetCreateExternalDataSource().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateColumnBuild:
        out.Add(F::InitiateColumnBuild_Table, tx.GetInitiateColumnBuild().GetTable());
        break;
    case NKikimrSchemeOp::ESchemeOpDropColumnBuild:
        out.Add(F::DropColumnBuild_Settings_Table,
            tx.GetDropColumnBuild().GetSettings().GetTable());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateView:
        out.Add(F::CreateView_Name, tx.GetCreateView().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropView:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpAlterView:
        // Unimplemented; no paths are resolved.
        break;
    case NKikimrSchemeOp::ESchemeOpCreateContinuousBackup: {
        const auto& op = tx.GetCreateContinuousBackup();
        out.Add(F::CreateContinuousBackup_TableName, op.GetTableName());
        const int cbTableIndex = out.Last();
        // Absent stream names are generated during execution.
        if (op.GetContinuousBackupDescription().HasStreamName()) {
            out.SiblingOf(F::CreateContinuousBackup_StreamName,
                op.GetContinuousBackupDescription().GetStreamName(), cbTableIndex);
        }
        out.Implicit(F::Implicit_CreateContinuousBackup_CdcStream, cbTableIndex);
        break;
    }
    case NKikimrSchemeOp::ESchemeOpAlterContinuousBackup: {
        const auto& op = tx.GetAlterContinuousBackup();
        // TSplitChildTag keeps even leading-slash paths under WorkingDir.
        out.Add(F::AlterContinuousBackup_TableName, op.GetTableName());
        const int cbTableIndex = out.Last();
        if (op.HasTakeIncrementalBackup()) {
            const auto& take = op.GetTakeIncrementalBackup();
            out.Add(F::AlterContinuousBackup_TakeIncrementalBackup_DstPath, take.GetDstPath());
            if (take.HasDstStreamPath()) {
                // Absent stream names are generated during execution.
                out.SiblingOf(F::AlterContinuousBackup_TakeIncrementalBackup_DstStreamPath,
                    take.GetDstStreamPath(), cbTableIndex);
            }
        }
        out.Implicit(F::Implicit_AlterContinuousBackup_IncrementalBackupTable, cbTableIndex);
        break;
    }
    case NKikimrSchemeOp::ESchemeOpDropContinuousBackup:
        out.Add(F::DropContinuousBackup_TableName, tx.GetDropContinuousBackup().GetTableName());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateResourcePool:
    case NKikimrSchemeOp::ESchemeOpAlterResourcePool:
        out.Add(F::CreateResourcePool_Name, tx.GetCreateResourcePool().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropResourcePool:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpRestoreMultipleIncrementalBackups:
    case NKikimrSchemeOp::ESchemeOpRestoreIncrementalBackupAtTable: {
        // Retired; the factory rejects these operations.
        const auto& op = tx.GetRestoreMultipleIncrementalBackups();
        for (size_t i = 0; i < op.SrcTablePathsSize(); ++i) {
            out.Add(F::RestoreMultipleIncrementalBackups_SrcTablePaths,
                op.GetSrcTablePaths(i), TAt{.Index = ui32(i)});
        }
        out.Add(F::RestoreMultipleIncrementalBackups_DstTablePath, op.GetDstTablePath());
        break;
    }
    case NKikimrSchemeOp::ESchemeOpCreateBackupCollection: {
        const auto& op = tx.GetCreateBackupCollection();
        out.Add(F::CreateBackupCollection_Name, op.GetName());
        // Backup collection entries are resolved absolutely.
        const auto& entryList = op.GetExplicitEntryList();
        for (size_t i = 0; i < entryList.EntriesSize(); ++i) {
            out.Add(F::CreateBackupCollection_Entry_Path, entryList.GetEntries(i).GetPath(),
                TAt{.Index = ui32(i)});
        }
        break;
    }
    case NKikimrSchemeOp::ESchemeOpAlterBackupCollection:
        out.Add(F::AlterBackupCollection_Name, tx.GetAlterBackupCollection().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropBackupCollection:
        out.Add(F::DropBackupCollection_Name, tx.GetDropBackupCollection().GetName());
        out.Implicit(F::Implicit_DropBackupCollection_Entries, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpBackupBackupCollection:
        out.Add(F::BackupBackupCollection_Name, tx.GetBackupBackupCollection().GetName());
        out.Implicit(F::Implicit_BackupBackupCollection_Entries, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpBackupIncrementalBackupCollection:
    case NKikimrSchemeOp::ESchemeOpCreateLongIncrementalBackupOp:
        out.Add(F::BackupIncrementalBackupCollection_Name,
            tx.GetBackupIncrementalBackupCollection().GetName());
        out.Implicit(F::Implicit_BackupIncrementalBackupCollection_Entries, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateFullBackupOp:
        // WorkingDir already points at the backup collection; no name field.
        out.Add(F::WorkingDirItself, {});
        out.Implicit(F::Implicit_CreateFullBackupOp_Entries, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpRestoreBackupCollection:
    case NKikimrSchemeOp::ESchemeOpCreateLongIncrementalRestoreOp:
        out.Add(F::RestoreBackupCollection_Name, tx.GetRestoreBackupCollection().GetName());
        out.Implicit(F::Implicit_RestoreBackupCollection_Entries, out.Last());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateSysView:
        out.Add(F::CreateSysView_Name, tx.GetCreateSysView().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropSysView:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpChangePathState:
        out.Add(F::ChangePathState_Path, tx.GetChangePathState().GetPath());
        break;
    case NKikimrSchemeOp::ESchemeOpIncrementalRestoreLockTargets:
    case NKikimrSchemeOp::ESchemeOpIncrementalRestoreUnlockTargets: {
        const auto& op = tx.GetIncrementalRestoreLockTargets();
        for (size_t i = 0; i < op.DstPathsSize(); ++i) {
            out.Add(F::IncrementalRestoreLockTargets_DstPaths, op.GetDstPaths(i),
                TAt{.Index = ui32(i)});
        }
        for (size_t i = 0; i < op.SrcPathsSize(); ++i) {
            out.Add(F::IncrementalRestoreLockTargets_SrcPaths, op.GetSrcPaths(i),
                TAt{.Index = ui32(i)});
        }
        break;
    }
    case NKikimrSchemeOp::ESchemeOpIncrementalRestoreFinalize:
        out.Implicit(F::Implicit_IncrementalRestoreFinalize_PersistedState, -1);
        break;
    case NKikimrSchemeOp::ESchemeOpCreateSecret:
        out.Add(F::CreateSecret_Name, tx.GetCreateSecret().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpAlterSecret:
        out.Add(F::AlterSecret_Name, tx.GetAlterSecret().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropSecret:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpCreateStreamingQuery:
    case NKikimrSchemeOp::ESchemeOpAlterStreamingQuery:
        out.Add(F::CreateStreamingQuery_Name, tx.GetCreateStreamingQuery().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropStreamingQuery:
        genericDrop();
        break;
    case NKikimrSchemeOp::ESchemeOpTruncateTable:
        out.Add(F::TruncateTable_TableName, tx.GetTruncateTable().GetTableName());
        break;
    case NKikimrSchemeOp::ESchemeOpCreateTestShardSet:
        out.Add(F::CreateTestShardSet_Name, tx.GetCreateTestShardSet().GetName());
        break;
    case NKikimrSchemeOp::ESchemeOpDropTestShardSet:
        genericDrop();
        break;
    }

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
