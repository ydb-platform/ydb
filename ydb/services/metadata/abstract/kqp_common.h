#pragma once
#include <library/cpp/object_factory/object_factory.h>
#include <ydb/services/metadata/manager/object.h>

namespace NKikimr::NMetadata {

namespace NInitializer {
class IInitializationBehaviour;
}

namespace NModifications {
class IOperationsManager;
}

// TODO: Think how to make this class less garbage
class IClassBehaviour {
public:
    using TFactory = NObjectFactory::TObjectFactory<IClassBehaviour, TString>;
    using TPtr = std::shared_ptr<IClassBehaviour>;
private:
    mutable std::shared_ptr<NInitializer::IInitializationBehaviour> Initializer;
protected:
    virtual std::shared_ptr<NInitializer::IInitializationBehaviour> ConstructInitializer() const = 0;
    virtual TString GetInternalStorageTablePath() const = 0;
    virtual TString GetInternalStorageHistoryTablePath() const;
public:
    virtual ~IClassBehaviour() = default;
    TString GetLocalStorageDirectory() const;
    TString GetStorageTablePath() const;
    TString GetStorageTableDirectory() const;
    TString GetStorageHistoryTablePath() const;
    std::shared_ptr<NInitializer::IInitializationBehaviour> GetInitializer() const;
    virtual std::shared_ptr<NModifications::IOperationsManager> GetOperationsManager() const = 0;
    virtual std::shared_ptr<NModifications::IObjectManager> GetObjectManager() const = 0;

    virtual TString GetTypeId() const = 0;
};

template <class TObject>
class TClassBehaviour: public IClassBehaviour {
private:
protected:
    virtual std::shared_ptr<NModifications::IOperationsManager> ConstructOperationsManager() const = 0;
public:
    virtual std::shared_ptr<NModifications::IOperationsManager> GetOperationsManager() const override final {
        static std::shared_ptr<NModifications::IOperationsManager> manager = ConstructOperationsManager();
        return manager;
    }
    virtual std::shared_ptr<NModifications::IObjectManager> GetObjectManager() const override final {
        if constexpr (NModifications::RecordSerializableObject<TObject>) {
            return std::make_shared<NModifications::TObjectManager<TObject>>();
        }
        return nullptr;
    }
};

}
