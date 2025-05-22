#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorHouseholdDemographics : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorHouseholdDemographics(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs, TGuard<TAdaptiveLock>&& g) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorHouseholdDemographics> Registrar;
};

}
