#pragma once
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/initializer/common.h>

namespace NKikimr::NColumnShard::NTiers {

class TTiersInitializer: public NMetadata::IInitializationBehaviour {
protected:
    TVector<NMetadataInitializer::ITableModifier::TPtr> BuildModifiers() const;
    virtual void DoPrepare(NMetadataInitializer::IInitializerInput::TPtr controller) const override;
public:
};

}
