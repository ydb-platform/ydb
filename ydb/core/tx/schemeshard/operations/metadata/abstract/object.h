#pragma once
#include <ydb/core/tx/schemeshard/operations/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>

namespace NKikimr::NSchemeShard::NOperations {

class TMetadataEntity: public ISSEntity {
private:
    using TBase = ISSEntity;

protected:
    TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context) const override;
    TConclusion<std::shared_ptr<ISSEntityUpdate>> DoRestoreUpdate(const TUpdateRestoreContext& context) const override;

public:
    TMetadataEntity(const TPathId& pathId)
        : TBase(pathId) {
    }
};

}