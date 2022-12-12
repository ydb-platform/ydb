#pragma once
#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NMetadata {

namespace NInitializer {
class IInitializationBehaviour;
}

namespace NModifications {
class IOperationsManager;
}

class IClassBehaviour {
public:
    using TFactory = NObjectFactory::TObjectFactory<IClassBehaviour, TString>;
    using TPtr = std::shared_ptr<IClassBehaviour>;
private:
    mutable std::shared_ptr<NInitializer::IInitializationBehaviour> Initializer;
    mutable std::shared_ptr<NModifications::IOperationsManager> OperationsManager;
protected:
    virtual TString GetInternalStorageTablePath() const = 0;
    virtual TString GetInternalStorageHistoryTablePath() const;
    virtual std::shared_ptr<NInitializer::IInitializationBehaviour> ConstructInitializer() const = 0;
    virtual std::shared_ptr<NModifications::IOperationsManager> ConstructOperationsManager() const = 0;
public:
    virtual ~IClassBehaviour() = default;
    TString GetStorageTablePath() const;
    TString GetStorageHistoryTablePath() const;
    std::shared_ptr<NInitializer::IInitializationBehaviour> GetInitializer() const;
    std::shared_ptr<NModifications::IOperationsManager> GetOperationsManager() const;

    virtual TString GetTypeId() const = 0;
};
}
