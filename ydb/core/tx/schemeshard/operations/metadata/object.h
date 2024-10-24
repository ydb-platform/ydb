#pragma once
#include "properties.h"

#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/operations/abstract/object.h>

namespace NKikimr::NSchemeShard::NOperations {

class TMetadataEntity final: public ISSEntity {
private:
    YDB_ACCESSOR_DEF(TMetadataObjectInfo::TPtr, ObjectInfo);

private:
    TConclusionStatus DoInitialize(const TEntityInitializationContext& context) override;
    TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context) const override;
    TConclusion<std::shared_ptr<ISSEntityUpdate>> DoRestoreUpdate(const TUpdateRestoreContext& context) const override;

    TString GetClassName() const override {
        return "METADATA_OBJECT";
    }

public:
    TMetadataEntity(const TPathId& pathId)
        : ISSEntity(pathId) {
    }
};

}   // namespace NKikimr::NSchemeShard::NOperations
