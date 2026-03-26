#pragma once

#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap {

inline THashMap<ui32, TString> BuildColumnIdToNameMap(const NKikimrSchemeOp::TColumnTableSchema& schema) {
    THashMap<ui32, TString> result;
    for (const auto& col : schema.GetColumns()) {
        result[col.GetId()] = col.GetName();
    }
    return result;
}

inline void CreateLocalIndexSchemeObjects(
    TTxId opTxId,
    const NKikimrSchemeOp::TColumnTableSchema& schema,
    const THashSet<TString>& skipNames,
    NSchemeShard::TPath& tablePath,
    TOperationContext& context,
    NIceDb::TNiceDb& db)
{
    if (!schema.IndexesSize()) {
        return;
    }

    auto columnIdToName = BuildColumnIdToNameMap(schema);

    for (const auto& indexProto : schema.GetIndexes()) {
        const TString& indexName = indexProto.GetName();
        if (skipNames.contains(indexName)) {
            continue;
        }

        NKikimrSchemeOp::EIndexType indexType;
        TVector<TString> keyColumns;

        if (indexProto.HasBloomFilter()) {
            indexType = NKikimrSchemeOp::EIndexTypeLocalBloomFilter;
            for (ui32 colId : indexProto.GetBloomFilter().GetColumnIds()) {
                if (auto it = columnIdToName.find(colId); it != columnIdToName.end()) {
                    keyColumns.push_back(it->second);
                }
            }
        } else if (indexProto.HasBloomNGrammFilter()) {
            indexType = NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter;
            if (auto it = columnIdToName.find(indexProto.GetBloomNGrammFilter().GetColumnId()); it != columnIdToName.end()) {
                keyColumns.push_back(it->second);
            }
        } else {
            continue;
        }

        TPathId indexPathId = context.SS->AllocatePathId();
        TPathId domainId = tablePath.Base()->IsDomainRoot()
            ? tablePath.Base()->PathId : tablePath.Base()->DomainPathId;
        TPathElement::TPtr indexPathEl = new TPathElement(
            indexPathId, tablePath.Base()->PathId, domainId, indexName, tablePath.Base()->Owner);
        context.SS->AttachChild(indexPathEl);
        tablePath.Base()->DbRefCount++;
        tablePath.Base()->AllChildrenCount++;
        tablePath.Base()->IncAliveChildrenPrivate();
        context.SS->PathsById[indexPathId] = indexPathEl;

        indexPathEl->CreateTxId = opTxId;
        indexPathEl->LastTxId = opTxId;
        indexPathEl->PathState = NKikimrSchemeOp::EPathStateCreate;
        indexPathEl->PathType = TPathElement::EPathType::EPathTypeTableIndex;

        TTableIndexInfo::TPtr indexInfo = TTableIndexInfo::NotExistedYet(indexType);
        TTableIndexInfo::TPtr alterData = indexInfo->CreateNextVersion();
        alterData->IndexKeys = keyColumns;
        alterData->State = NKikimrSchemeOp::EIndexStateReady;
        if (indexType == NKikimrSchemeOp::EIndexTypeLocalBloomFilter) {
            alterData->SpecializedIndexDescription = indexProto.GetBloomFilter();
        } else {
            alterData->SpecializedIndexDescription = indexProto.GetBloomNGrammFilter();
        }

        context.SS->Indexes[indexPathId] = indexInfo;
        context.SS->IncrementPathDbRefCount(indexPathId);

        context.SS->PersistPath(db, indexPathId);
        context.SS->PersistTableIndexAlterData(db, indexPathId);

        tablePath.DomainInfo()->IncPathsInside(context.SS);
    }

    context.SS->PersistUpdateNextPathId(db);
}

inline void UpdateLocalIndexSchemeObjects(
    const NKikimrSchemeOp::TColumnTableSchema& schema,
    const THashSet<TString>& existingIndexNames,
    TPathElement::TPtr tablePath,
    TOperationContext& context,
    NIceDb::TNiceDb& db)
{
    if (!schema.IndexesSize() || existingIndexNames.empty()) {
        return;
    }

    auto columnIdToName = BuildColumnIdToNameMap(schema);

    for (const auto& indexProto : schema.GetIndexes()) {
        const TString& indexName = indexProto.GetName();
        if (!existingIndexNames.contains(indexName)) {
            continue;
        }

        auto childIt = tablePath->GetChildren().find(indexName);
        if (childIt == tablePath->GetChildren().end()) {
            continue;
        }
        TPathId indexPathId = childIt->second;

        auto indexIt = context.SS->Indexes.find(indexPathId);
        if (indexIt == context.SS->Indexes.end() || indexIt->second->AlterData) {
            continue;
        }

        NKikimrSchemeOp::EIndexType indexType;
        TVector<TString> keyColumns;

        if (indexProto.HasBloomFilter()) {
            indexType = NKikimrSchemeOp::EIndexTypeLocalBloomFilter;
            for (ui32 colId : indexProto.GetBloomFilter().GetColumnIds()) {
                if (auto it = columnIdToName.find(colId); it != columnIdToName.end()) {
                    keyColumns.push_back(it->second);
                }
            }
        } else if (indexProto.HasBloomNGrammFilter()) {
            indexType = NKikimrSchemeOp::EIndexTypeLocalBloomNgramFilter;
            if (auto it = columnIdToName.find(indexProto.GetBloomNGrammFilter().GetColumnId()); it != columnIdToName.end()) {
                keyColumns.push_back(it->second);
            }
        } else {
            continue;
        }

        TTableIndexInfo::TPtr alterData = indexIt->second->CreateNextVersion();
        alterData->IndexKeys = keyColumns;
        alterData->State = NKikimrSchemeOp::EIndexStateReady;
        if (indexType == NKikimrSchemeOp::EIndexTypeLocalBloomFilter) {
            alterData->SpecializedIndexDescription = indexProto.GetBloomFilter();
        } else {
            alterData->SpecializedIndexDescription = indexProto.GetBloomNGrammFilter();
        }

        context.SS->PersistTableIndexAlterData(db, indexPathId);
    }
}

inline void FinalizeNewLocalIndexPaths(
    TStepId step,
    TPathElement::TPtr tablePath,
    TOperationContext& context,
    NIceDb::TNiceDb& db)
{
    for (const auto& [_, childPathId] : tablePath->GetChildren()) {
        auto it = context.SS->PathsById.find(childPathId);
        if (it == context.SS->PathsById.end() || !it->second->IsTableIndex()) {
            continue;
        }
        auto childEl = it->second;

        auto indexIt = context.SS->Indexes.find(childPathId);
        if (indexIt == context.SS->Indexes.end() || !indexIt->second->AlterData) {
            continue;
        }

        if (childEl->PathState == NKikimrSchemeOp::EPathStateCreate) {
            childEl->StepCreated = step;
            context.SS->PersistCreateStep(db, childPathId, step);
            childEl->PathState = TPathElement::EPathState::EPathStateNoChanges;
            context.SS->PersistPath(db, childPathId);
        }

        TTableIndexInfo::TPtr indexData = indexIt->second;
        context.SS->PersistTableIndex(db, childPathId);
        context.SS->Indexes[childPathId] = indexData->AlterData;
    }
}

} // namespace NKikimr::NSchemeShard::NOlap
