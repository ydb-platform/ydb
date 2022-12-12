#pragma once
#include <ydb/services/metadata/manager/table_record.h>
#include <ydb/services/metadata/initializer/common.h>

namespace NKikimr::NMetadata::NInitializer {

class IInitializationBehaviour {
protected:
    virtual void DoPrepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const = 0;
public:
    using TPtr = std::shared_ptr<IInitializationBehaviour>;
    virtual ~IInitializationBehaviour() = default;
    void Prepare(NMetadata::NInitializer::IInitializerInput::TPtr controller) const {
        return DoPrepare(controller);
    }
};

}
