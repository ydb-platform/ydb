#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorWebSite : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorWebSite(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorWebSite> Registrar;
};

}
