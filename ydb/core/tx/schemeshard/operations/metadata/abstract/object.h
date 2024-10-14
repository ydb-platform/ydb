#pragma once
#include <ydb/core/tx/schemeshard/operations/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/table/table.h>

namespace NKikimr::NSchemeShard::NOperations {
class TMetadataUpdateDrop;
}

namespace NKikimr::NSchemeShard::NOperations {

class TMetadataEntity: public ISSEntity {
public:
    using TFactory = NObjectFactory::TObjectFactory<TMetadataEntity, NKikimrSchemeOp::EPathType, const TPathId&>;

private:
    using TBase = ISSEntity;

protected:
    TConclusion<std::shared_ptr<ISSEntityUpdate>> DoCreateUpdate(const TUpdateInitializationContext& context) const override;
    TConclusion<std::shared_ptr<ISSEntityUpdate>> DoRestoreUpdate(const TUpdateRestoreContext& context) const override;

    virtual std::shared_ptr<TMetadataUpdateDrop> GetDropUpdate() const = 0;

public:
    TMetadataEntity(const TPathId& pathId)
        : TBase(pathId) {
    }

    TConclusion<std::shared_ptr<ISSEntityUpdate>> RestoreDropUpdate(const TUpdateRestoreContext& context) const;

    static TConclusion<std::shared_ptr<TMetadataEntity>> GetEntity(TOperationContext& context, const TPath& path);
};

}   // namespace NKikimr::NSchemeShard::NOperations
