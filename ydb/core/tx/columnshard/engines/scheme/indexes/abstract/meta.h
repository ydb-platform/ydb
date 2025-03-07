#pragma once
#include "abstract.h"
#include "checker.h"
#include "collection.h"
#include "coverage.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/columnshard/splitter/chunks.h>

#include <ydb/library/conclusion/status.h>
#include <ydb/services/bg_tasks/abstract/interface.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NYql::NNodes {
class TExprBase;
}

namespace NKikimr::NOlap {
struct TIndexInfo;
class TIndexChunk;
namespace NReader::NCommon {
class IKernelFetchLogic;
}
}   // namespace NKikimr::NOlap

namespace NKikimr::NSchemeShard {
class TOlapSchema;
}

namespace NKikimr::NOlap::NIndexes {

class IIndexMeta {
private:
    YDB_READONLY_DEF(TString, IndexName);
    YDB_READONLY(ui32, IndexId, 0);
    YDB_READONLY(TString, StorageId, IStoragesManager::DefaultStorageId);

    virtual std::shared_ptr<NReader::NCommon::IKernelFetchLogic> DoBuildFetchTask(const TIndexDataAddress& dataAddress,
        const std::vector<TChunkOriginalData>& chunks, const TIndexesCollection& collection, const std::shared_ptr<IIndexMeta>& selfPtr,
        const std::shared_ptr<IStoragesManager>& storagesManager) const;

    virtual TConclusion<std::shared_ptr<IIndexHeader>> DoBuildHeader(const TChunkOriginalData& data) const {
        return std::make_shared<TDefaultHeader>(data.GetSize());
    }

protected:
    virtual TConclusion<std::shared_ptr<IPortionDataChunk>> DoBuildIndexOptional(
        const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const ui32 recordsCount,
        const TIndexInfo& indexInfo) const = 0;
    virtual void DoFillIndexCheckers(
        const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const = 0;
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const = 0;
    virtual NJson::TJsonValue DoSerializeDataToJson(const TString& /*data*/, const TIndexInfo& /*indexInfo*/) const {
        return "NO_IMPLEMENTED";
    }

public:
    using TFactory = NObjectFactory::TObjectFactory<IIndexMeta, TString>;
    using TProto = NKikimrSchemeOp::TOlapIndexDescription;

    TConclusion<std::shared_ptr<IIndexHeader>> BuildHeader(const TChunkOriginalData& data) const {
        return DoBuildHeader(data);
    }

    std::shared_ptr<NReader::NCommon::IKernelFetchLogic> BuildFetchTask(const TIndexDataAddress& dataAddress,
        const std::vector<TChunkOriginalData>& chunks, const TIndexesCollection& collection, const std::shared_ptr<IIndexMeta>& meta,
        const std::shared_ptr<IStoragesManager>& storagesManager) const {
        return DoBuildFetchTask(dataAddress, chunks, collection, meta, storagesManager);
    }

    bool IsInplaceData() const {
        return StorageId == NBlobOperations::TGlobal::LocalMetadataStorageId;
    }

    IIndexMeta() = default;
    IIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId)
        : IndexName(indexName)
        , IndexId(indexId)
        , StorageId(storageId) {
    }

    NJson::TJsonValue SerializeDataToJson(const TIndexChunk& iChunk, const TIndexInfo& indexInfo) const;

    TConclusionStatus CheckModificationCompatibility(const std::shared_ptr<IIndexMeta>& newMeta) const {
        if (!newMeta) {
            return TConclusionStatus::Fail("new meta cannot be absent");
        }
        if (newMeta->GetClassName() != GetClassName()) {
            return TConclusionStatus::Fail(
                "new meta have to be same index class (" + GetClassName() + "), but new class name: " + newMeta->GetClassName());
        }
        return DoCheckModificationCompatibility(*newMeta);
    }

    virtual ~IIndexMeta() = default;

    TConclusion<std::shared_ptr<IPortionDataChunk>> BuildIndexOptional(
        const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const ui32 recordsCount,
        const TIndexInfo& indexInfo) const {
        return DoBuildIndexOptional(data, recordsCount, indexInfo);
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
        : TBase(object) {
        AFL_VERIFY(Object);
    }
};

}   // namespace NKikimr::NOlap::NIndexes
