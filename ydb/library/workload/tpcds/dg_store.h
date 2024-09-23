#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorStore : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorStore(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorStore> Registrar;
};

}
