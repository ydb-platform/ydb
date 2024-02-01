#pragma once
#include "update.h"

namespace NKikimr::NSchemeShard {

class TOlapSchema;

class TOlapIndexSchema {
private:
    using TBase = TOlapIndexUpsert;
    YDB_READONLY(ui32, Id, Max<ui32>());
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(NBackgroundTasks::TInterfaceProtoContainer<NOlap::NIndexes::IIndexMeta>, IndexMeta);
public:
    TOlapIndexSchema() = default;

    TOlapIndexSchema(const ui32 id, const TString& name, const std::shared_ptr<NOlap::NIndexes::IIndexMeta>& meta)
        : Id(id)
        , Name(name)
        , IndexMeta(meta)
    {

    }

    bool ApplyUpdate(const TOlapSchema& currentSchema, const TOlapIndexUpsert& upsert, IErrorCollector& errors);

    void SerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& indexSchema) const;
    void DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& indexSchema);
};

class TOlapIndexesDescription {
public:
    using TIndex = TOlapIndexSchema;
    using TIndexes = THashMap<ui32, TOlapIndexSchema>;
    using TIndexesByName = THashMap<TString, ui32>;

private:
    YDB_READONLY_DEF(TIndexes, Indexes);
    YDB_READONLY_DEF(TIndexesByName, IndexesByName);
public:
    const TOlapIndexSchema* GetByName(const TString& name) const noexcept {
        auto it = IndexesByName.find(name);
        if (it != IndexesByName.end()) {
            return GetByIdVerified(it->second);
        }
        return nullptr;
    }

    TOlapIndexSchema* MutableByName(const TString& name) noexcept {
        auto it = IndexesByName.find(name);
        if (it != IndexesByName.end()) {
            return MutableByIdVerified(it->second);
        }
        return nullptr;
    }

    const TOlapIndexSchema* GetById(const ui32 id) const noexcept {
        auto it = Indexes.find(id);
        if (it != Indexes.end()) {
            return &it->second;
        }
        return nullptr;
    }

    TOlapIndexSchema* MutableById(const ui32 id) noexcept {
        auto it = Indexes.find(id);
        if (it != Indexes.end()) {
            return &it->second;
        }
        return nullptr;
    }

    const TOlapIndexSchema* GetByIdVerified(const ui32 id) const noexcept;
    TOlapIndexSchema* MutableByIdVerified(const ui32 id) noexcept;

    bool ApplyUpdate(const TOlapSchema& currentSchema, const TOlapIndexesUpdate& schemaUpdate, IErrorCollector& errors, ui32& nextEntityId);

    void Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema);
    void Serialize(NKikimrSchemeOp::TColumnTableSchema& tableSchema) const;
    bool Validate(const NKikimrSchemeOp::TColumnTableSchema& opSchema, IErrorCollector& errors) const;
};
}
