#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorCustomerAddress : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorCustomerAddress(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorCustomerAddress> Registrar;
};

}
