#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorShipMode : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorShipMode(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorShipMode> Registrar;
};

}
