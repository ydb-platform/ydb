#pragma once

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/formats/arrow/protos/accessor.pb.h>
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/services/metadata/abstract/request_features.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_base.h>
#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TDataBuilder;

class IDataAdapter {
public:
    using TProto = NKikimrArrowAccessorProto::TDataExtractor;
    using TFactory = NObjectFactory::TObjectFactory<IDataAdapter, TString>;

private:
    virtual TConclusionStatus DoAddDataToBuilders(
        const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept = 0;
    virtual bool DoDeserializeFromProto(const TProto& proto) = 0;
    virtual void DoSerializeToProto(TProto& proto) const = 0;
    virtual NJson::TJsonValue DoDebugJson() const {
        return NJson::JSON_MAP;
    }
    virtual TConclusionStatus DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) = 0;

public:
    virtual TString GetClassName() const = 0;
    bool DeserializeFromProto(const TProto& proto) {
        return DoDeserializeFromProto(proto);
    }

    TConclusionStatus DeserializeFromRequest(NYql::TFeaturesExtractor& features) {
        return DoDeserializeFromRequest(features);
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("class_name", GetClassName());
        result.InsertValue("details", DoDebugJson());
        return result;
    }
    void SerializeToProto(TProto& proto) const {
        DoSerializeToProto(proto);
    }

    virtual ~IDataAdapter() = default;

    [[nodiscard]] TConclusionStatus AddDataToBuilders(
        const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept;
};

class TFirstLevelSchemaData: public IDataAdapter {
public:
    static TString GetClassNameStatic() {
        return "JSON_SCANNER";
    }

private:
    bool FirstLevelOnly = false;
    virtual TConclusionStatus DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) override {
        if (auto scanFlag = features.Extract<bool>("SCAN_FIRST_LEVEL_ONLY")) {
            FirstLevelOnly = *scanFlag;
        }
        return TConclusionStatus::Success();
    }

    virtual TConclusionStatus DoAddDataToBuilders(
        const std::shared_ptr<arrow::Array>& sourceArray, TDataBuilder& dataBuilder) const noexcept override;
    virtual bool DoDeserializeFromProto(const TProto& proto) override {
        if (!proto.HasJsonScanner()) {
            return true;
        }
        FirstLevelOnly = proto.GetJsonScanner().GetFirstLevelOnly();
        return true;
    }
    virtual void DoSerializeToProto(TProto& proto) const override {
        proto.MutableJsonScanner()->SetFirstLevelOnly(FirstLevelOnly);
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    static const inline auto Registrator = TFactory::TRegistrator<TFirstLevelSchemaData>(GetClassNameStatic());

public:
    TFirstLevelSchemaData() = default;
    TFirstLevelSchemaData(const bool firstLevelOnly)
        : FirstLevelOnly(firstLevelOnly) {
    }
};

class TDataAdapterContainer: public NBackgroundTasks::TInterfaceProtoContainer<IDataAdapter> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IDataAdapter>;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
