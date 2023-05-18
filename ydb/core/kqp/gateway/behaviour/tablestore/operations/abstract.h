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
    TString StoreName;
public:
    virtual ~ITableStoreOperation() {};

    NMetadata::NModifications::TObjectOperatorResult Deserialize(const NYql::TObjectSettingsImpl& settings);

    void SerializeScheme(NKikimrSchemeOp::TModifyScheme& scheme) const;
private:
    virtual NMetadata::NModifications::TObjectOperatorResult DoDeserialize(const NYql::TObjectSettingsImpl::TFeatures& features) = 0;
    virtual void DoSerializeScheme(NKikimrSchemeOp::TAlterColumnTableSchemaPreset& scheme) const = 0;
};

}

