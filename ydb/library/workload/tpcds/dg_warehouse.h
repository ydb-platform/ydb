#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorWarehouse : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorWarehouse(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorWarehouse> Registrar;
};

}
