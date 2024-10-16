#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorInventory : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorInventory(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorInventory> Registrar;
};

}
