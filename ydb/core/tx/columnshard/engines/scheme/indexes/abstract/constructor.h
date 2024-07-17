#pragma once
#include "meta.h"

#include <ydb/library/conclusion/status.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NSchemeShard {
class TOlapSchema;
}

namespace NKikimr::NOlap::NIndexes {

class IIndexMetaConstructor {
private:
    YDB_READONLY_DEF(std::optional<TString>, StorageId);

protected:
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) = 0;
    virtual std::shared_ptr<IIndexMeta> DoCreateIndexMeta(const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const = 0;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) = 0;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const = 0;
public:
    using TFactory = NObjectFactory::TObjectFactory<IIndexMetaConstructor, TString>;
    using TProto = NKikimrSchemeOp::TOlapIndexRequested;

    virtual ~IIndexMetaConstructor() = default;

    TConclusionStatus DeserializeFromJson(const NJson::TJsonValue& jsonInfo);

    std::shared_ptr<IIndexMeta> CreateIndexMeta(const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const {
        return DoCreateIndexMeta(indexId, indexName, currentSchema, errors);
    }

    TConclusionStatus DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) {
        if (proto.HasStorageId()) {
            StorageId = proto.GetStorageId();
        }
        return DoDeserializeFromProto(proto);
    }

    void SerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const {
        if (StorageId) {
            proto.SetStorageId(*StorageId);
        }
        return DoSerializeToProto(proto);
    }

    virtual TString GetClassName() const = 0;
};


}   // namespace NKikimr::NOlap::NIndexes