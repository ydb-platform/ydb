#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorStoreSales : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorStoreSales(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorStoreSales> Registrar;
};

}
