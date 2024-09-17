#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorWebPage : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorWebPage(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorWebPage> Registrar;
};

}
