#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorTimeDim : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorTimeDim(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorTimeDim> Registrar;
};

}
