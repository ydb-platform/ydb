#pragma once

#include <ydb/services/metadata/abstract/initialization.h>

namespace NKikimr::NKqp {

class TStreamingQueryInitializer : public NMetadata::NInitializer::IInitializationBehaviour {
protected:
    void DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const override;
};

}  // namespace NKikimr::NKqp
