#include "abstract.h"

namespace NKikimr::NKqp {

class TResetOptionsOperation: public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "RESET_OPTIONS";
    }

    static inline const auto Registrator = TFactory::TRegistrator<TResetOptionsOperation>(GetTypeName());

    using EOption = NKikimrSchemeOp::TColumnTableRequestedOptions::EOption;

private:
    std::set<EOption> Options;
public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

    void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& schemaData) const override;
};

}
