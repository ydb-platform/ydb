#include <library/cpp/object_factory/object_factory.h>
#include <ydb/services/metadata/manager/abstract.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
namespace NKikimr::NKqp {

class ITableStoreOperation {
public:
    using TFactory = NObjectFactory::TObjectFactory<ITableStoreOperation, TString>;
    using TPtr = std::shared_ptr<ITableStoreOperation>;
private:
    TString PresetName = "default";
    TString WorkingDir;
    YDB_READONLY_DEF(TString, StoreName);
public:
    virtual ~ITableStoreOperation() {};

    TConclusionStatus Deserialize(const NYql::TObjectSettingsImpl& settings);

    void SerializeScheme(NKikimrSchemeOp::TModifyScheme& scheme, const bool isStandalone) const;
private:
    virtual TConclusionStatus DoDeserialize(NYql::TObjectSettingsImpl::TFeaturesExtractor& features) = 0;
    virtual void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchema& scheme) const = 0;
    virtual void DoSerializeScheme(NKikimrSchemeOp::TModifyScheme& scheme, const bool isStandalone) const;
};

}

