#include "initializer.h"
#include "behaviour.h"

namespace NKikimr::NKqp {

TVector<NKikimr::NMetadata::NInitializer::ITableModifier::TPtr> TTableStoreInitializer::BuildModifiers() const {
    TVector<NMetadata::NInitializer::ITableModifier::TPtr> result;
    return result;
}

void TTableStoreInitializer::DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const {
    controller->OnPreparationFinished(BuildModifiers());
}

}
