#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorCustomer : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorCustomer(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorCustomer> Registrar;
};

}
