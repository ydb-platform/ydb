#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorCatalogPage : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorCatalogPage(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorCatalogPage> Registrar;
};

}
