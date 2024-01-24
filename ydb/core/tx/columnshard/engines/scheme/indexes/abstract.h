#pragma once

#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <library/cpp/object_factory/object_factory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <util/generic/string.h>

#include <memory>
#include <vector>

namespace NKikimr::NOlap {
struct TIndexInfo;
}

namespace NKikimr::NSchemeShard {
class TOlapSchema;
class IErrorCollector;
}

namespace NKikimr::NOlap::NIndexes {

class IIndexChecker {
protected:
    virtual bool DoCheck(std::vector<TString>&& blobs) const = 0;
public:
    virtual ~IIndexChecker() = default;
    bool Check(std::vector<TString>&& blobs) const {
        return DoCheck(std::move(blobs));
    }
};

class TIndexCheckerContainer {
private:
    YDB_READONLY(ui32, IndexId, 0);
    YDB_READONLY_DEF(std::shared_ptr<IIndexChecker>, Object);
public:
    TIndexCheckerContainer(const ui32 indexId, const std::shared_ptr<IIndexChecker>& object)
        : IndexId(indexId)
        , Object(object) {
        AFL_VERIFY(IndexId);
        AFL_VERIFY(Object);
    }

    const IIndexChecker* operator->() const {
        return Object.get();
    }
};

class IIndexMeta {
protected:
    virtual std::shared_ptr<IPortionDataChunk> DoBuildIndex(const ui32 indexId, std::map<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const = 0;
    virtual std::shared_ptr<IIndexChecker> DoBuildIndexChecker(const TProgramContainer& program) const = 0;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const = 0;

public:
    using TFactory = NObjectFactory::TObjectFactory<IIndexMeta, TString>;
    using TProto = NKikimrSchemeOp::TOlapIndexDescription;

    virtual ~IIndexMeta() = default;

    std::shared_ptr<IPortionDataChunk> BuildIndex(const ui32 indexId, std::map<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, const TIndexInfo& indexInfo) const {
        return DoBuildIndex(indexId, data, indexInfo);
    }

    std::shared_ptr<IIndexChecker> BuildIndexChecker(const TProgramContainer& program) const {
        return DoBuildIndexChecker(program);
    }

    bool DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) {
        return DoDeserializeFromProto(proto);
    }

    void SerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const {
        return DoSerializeToProto(proto);
    }

    virtual TString GetClassName() const = 0;
};

class IIndexMetaConstructor {
protected:
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) = 0;
    virtual std::shared_ptr<IIndexMeta> DoCreateIndexMeta(const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const = 0;
public:
    using TFactory = NObjectFactory::TObjectFactory<IIndexMetaConstructor, TString>;
    using TProto = NKikimrSchemeOp::TOlapIndexRequested;

    virtual ~IIndexMetaConstructor() = default;

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonInfo) {
        return DoDeserializeFromJson(jsonInfo);
    }

    std::shared_ptr<IIndexMeta> CreateIndexMeta(const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
        return DoCreateIndexMeta(currentSchema, errors);
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
        return DoDeserializeFromProto(proto);
    }

    void SerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
        return DoSerializeToProto(proto);
    }

    virtual TString GetClassName() const = 0;
};

class TIndexMetaContainer: public NBackgroundTasks::TInterfaceProtoContainer<IIndexMeta> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IIndexMeta>;
    YDB_READONLY(ui32, IndexId, 0);
public:
    TIndexMetaContainer() = default;
    TIndexMetaContainer(const ui32 indexId, const std::shared_ptr<IIndexMeta>& object)
        : TBase(object)
        , IndexId(indexId)
    {
        AFL_VERIFY(IndexId);
        AFL_VERIFY(Object);
    }

    bool DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) {
        if (!TBase::DeserializeFromProto(proto)) {
            return false;
        }
        IndexId = proto.GetId();
        return true;
    }

    std::optional<TIndexCheckerContainer> BuildIndexChecker(const TProgramContainer& program) const {
        auto checker = GetObjectPtr()->BuildIndexChecker(program);
        if (!checker) {
            return {};
        }
        return TIndexCheckerContainer(IndexId, checker);
    }
};

}   // namespace NKikimr::NOlap::NIndexes