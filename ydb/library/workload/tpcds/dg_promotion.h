#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorPromotion : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorPromotion(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorPromotion> Registrar;
};

}
