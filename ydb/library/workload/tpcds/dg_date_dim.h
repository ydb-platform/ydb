#include "data_generator.h"

namespace NYdbWorkload {

class TTpcDSGeneratorDateDim : public TTpcdsWorkloadDataInitializerGenerator::TBulkDataGenerator {
public:
    explicit TTpcDSGeneratorDateDim(const TTpcdsWorkloadDataInitializerGenerator& owner);

protected:
    virtual void GenerateRows(TContexts& ctxs) override;
    static const TFactory::TRegistrator<TTpcDSGeneratorDateDim> Registrar;
};

}
