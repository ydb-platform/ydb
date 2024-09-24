#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorIncomeBand : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorIncomeBand(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorIncomeBand> Registrar;
};

}
