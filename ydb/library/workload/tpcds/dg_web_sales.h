#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorWebSales : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorWebSales(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorWebSales> Registrar;
};

}
