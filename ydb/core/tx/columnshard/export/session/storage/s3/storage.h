#pragma once
#include <ydb/core/tx/columnshard/export/session/storage/abstract/storage.h>

namespace NKikimr::NOlap::NExport {

class TS3StorageInitializer: public IStorageInitializer {
public:
    static TString GetClassNameStatic() {
        return "S3";
    }
private:
    TString StorageName;
    NKikimrSchemeOp::TS3Settings S3Settings;
    static inline const TFactory::TRegistrator<TS3StorageInitializer> Registrator = TFactory::TRegistrator<TS3StorageInitializer>(GetClassNameStatic());
protected:
    virtual TConclusion<std::shared_ptr<IBlobsStorageOperator>> DoInitializeOperator(const std::shared_ptr<IStoragesManager>& storages) const override;
public:
    TS3StorageInitializer() = default;
    TS3StorageInitializer(const TString& storageName, const NKikimrSchemeOp::TS3Settings& s3Settings)
        : StorageName(storageName)
        , S3Settings(s3Settings)
    {

    }

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrColumnShardExportProto::TStorageInitializerContainer& proto) override {
        if (!proto.HasExternalS3()) {
            return TConclusionStatus::Fail("has not s3 configuration");
        }
        S3Settings = proto.GetExternalS3().GetSettings();
        StorageName = proto.GetExternalS3().GetStorageName();
        return TConclusionStatus::Success();
    }

    virtual void DoSerializeToProto(NKikimrColumnShardExportProto::TStorageInitializerContainer& proto) const override {
        *proto.MutableExternalS3()->MutableSettings() = S3Settings;
        proto.MutableExternalS3()->SetStorageName(StorageName);
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};
}