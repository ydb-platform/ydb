#pragma once
#include "manager.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class TManagerConstructionContext {
private:
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);

public:
    TManagerConstructionContext(const NActors::TActorId& tabletActorId)
        : TabletActorId(tabletActorId) {
    }
};

class IManagerConstructor {
private:
    virtual std::shared_ptr<IMetadataMemoryManager> DoBuild(const TManagerConstructionContext& context) const = 0;

public:
    using TFactory = NObjectFactory::TObjectFactory<IManagerConstructor, TString>;
    using TProto = NKikimrSchemeOp::TMetadataManagerConstructorContainer;

    virtual TString GetClassName() const = 0;

    std::shared_ptr<IMetadataMemoryManager> Build(const TManagerConstructionContext& context) {
        return DoBuild(context);
    }
};

class TMetadataManagerConstructorConatiner: public NBackgroundTasks::TInterfaceProtoContainer<IManagerConstructor> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IManagerConstructor>;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
