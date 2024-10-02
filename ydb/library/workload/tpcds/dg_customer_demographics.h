#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorCustomerDemographics : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorCustomerDemographics(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorCustomerDemographics> Registrar;
};

}
