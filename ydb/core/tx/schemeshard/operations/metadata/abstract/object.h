#pragma once
#include "info.h"

#include <ydb/core/tx/schemeshard/olap/table/table.h>
#include <ydb/core/tx/schemeshard/operations/abstract/object.h>

namespace NKikimr::NSchemeShard::NOperations {
class TMetadataUpdateDrop;
}

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

    // virtual std::shared_ptr<TMetadataUpdateDrop> GetDropUpdate() const = 0;

public:
    // TConclusion<std::shared_ptr<ISSEntityUpdate>> RestoreDropUpdate(const TUpdateRestoreContext& context) const;

    TMetadataEntity(const TPathId& pathId)
        : ISSEntity(pathId) {
    }
};

// template <class TInfo, NKikimrSchemeOp::EPathType PathType, class TDropUpdate>
// class TMetadataEntityImpl: public TMetadataEntity {
// private:
//     using TSelf = TMetadataEntityImpl<TInfo, PathType, TDropUpdate>;
//     using TSSInfo = TMetadataObjectInfo<TInfo>;

//     static inline TFactory::TRegistrator<TSelf> Registrator = PathType;
//     TMetadataObjectInfo::TPtr ObjectInfo;

// protected:
//     TMetadataEntityImpl(const TPathId& pathId, const TSSInfo::TPtr& objectInfo)
//         : TMetadataEntity(pathId)
//         , ObjectInfo(objectInfo) {
//     }

//     TSSInfo& GetObjectInfo() {
//         AFL_VERIFY(IsInitialized())("type", GetClassName());
//         TSSInfo* converted = dynamic_cast<TSSInfo>(ObjectInfo.Get());
//         AFL_VERIFY(converted)("type", GetClassName());
//         return *converted;
//     }

// public:
//     TString GetClassName() const override {
//         return TInfo::GetTypeId();
//     }

//     std::shared_ptr<TMetadataUpdateDrop> GetDropUpdate() const override {
//         return std::make_shared<TDropUpdate>();
//     }

//     TMetadataEntityImpl(const TPathId& pathId)
//         : TMetadataEntity(pathId) {
//     }
// };

}   // namespace NKikimr::NSchemeShard::NOperations
