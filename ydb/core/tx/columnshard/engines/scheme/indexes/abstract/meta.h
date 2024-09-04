#pragma once
#include "checker.h"
#include "program.h"

#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/library/conclusion/status.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NYql::NNodes {
class TExprBase;
}

namespace NKikimr::NOlap {
struct TIndexInfo;
class TProgramContainer;
class TIndexChunk;
}

namespace NKikimr::NSchemeShard {
class TOlapSchema;
}

namespace NKikimr::NOlap::NIndexes {

class IIndexMeta {
private:
    YDB_READONLY_DEF(TString, IndexName);
    YDB_READONLY(ui32, IndexId, 0);
    YDB_READONLY(TString, StorageId, IStoragesManager::DefaultStorageId);
protected:
    virtual std::shared_ptr<IPortionDataChunk> DoBuildIndex(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const = 0;
    virtual void DoFillIndexCheckers(const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const = 0;
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const = 0;
    virtual NJson::TJsonValue DoSerializeDataToJson(const TString& /*data*/, const TIndexInfo& /*indexInfo*/) const {
        return "NO_IMPLEMENTED";
    }

public:
    using TFactory = NObjectFactory::TObjectFactory<IIndexMeta, TString>;
    using TProto = NKikimrSchemeOp::TOlapIndexDescription;

    IIndexMeta() = default;
    IIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId)
        : IndexName(indexName)
        , IndexId(indexId)
        , StorageId(storageId)
    {

    }

    NJson::TJsonValue SerializeDataToJson(const TIndexChunk& iChunk, const TIndexInfo& indexInfo) const;

    TConclusionStatus CheckModificationCompatibility(const std::shared_ptr<IIndexMeta>& newMeta) const {
        if (!newMeta) {
            return TConclusionStatus::Fail("new meta cannot be absent");
        }
        if (newMeta->GetClassName() != GetClassName()) {
            return TConclusionStatus::Fail("new meta have to be same index class (" + GetClassName() + "), but new class name: " + newMeta->GetClassName());
        }
        return DoCheckModificationCompatibility(*newMeta);
    }

    virtual ~IIndexMeta() = default;

    std::shared_ptr<IPortionDataChunk> BuildIndex(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const {
        return DoBuildIndex(data, indexInfo);
    }

    void FillIndexCheckers(const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const {
        return DoFillIndexCheckers(info, schema);
    }

    bool DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto);
    void SerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const;

    virtual TString GetClassName() const = 0;
};

class TIndexMetaContainer: public NBackgroundTasks::TInterfaceProtoContainer<IIndexMeta> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IIndexMeta>;
public:
    TIndexMetaContainer() = default;
    TIndexMetaContainer(const std::shared_ptr<IIndexMeta>& object)
        : TBase(object)
    {
        AFL_VERIFY(Object);
    }
};

}   // namespace NKikimr::NOlap::NIndexes