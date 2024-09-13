#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorCatalogSales : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorCatalogSales(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorCatalogSales> Registrar;
};

}
