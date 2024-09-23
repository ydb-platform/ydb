#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/scheme/statistics/abstract/constructor.h>

namespace NKikimr::NKqp {

class TAlterShardingOperation: public ITableStoreOperation {
private:
    static TString GetTypeName() {
        return "ALTER_SHARDING";
    }

    static inline const auto Registrator = TFactory::TRegistrator<TAlterShardingOperation>(GetTypeName());
private:
    std::optional<bool> Increase;
    virtual void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& /*scheme*/) const override {
        AFL_VERIFY(false);
    }
    virtual void DoSerializeScheme(NKikimrSchemeOp::TModifyScheme& scheme, const bool isStandalone) const override;

public:
    TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) override;

};

}

