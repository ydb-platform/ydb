#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorCallCenter : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorCallCenter(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorCallCenter> Registrar;
};

}
