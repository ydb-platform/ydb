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
    virtual bool HasInternalConversion() const = 0;
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
        return "BINARY_JSON_SCANNER";
    }

private:
    virtual bool HasInternalConversion() const override {
        return false;
    }

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
        if (!proto.HasBinaryJsonScanner()) {
            return true;
        }
        FirstLevelOnly = proto.GetBinaryJsonScanner().GetFirstLevelOnly();
        return true;
    }
    virtual void DoSerializeToProto(TProto& proto) const override {
        proto.MutableBinaryJsonScanner()->SetFirstLevelOnly(FirstLevelOnly);
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    static const inline auto Registrator = TFactory::TRegistrator<TFirstLevelSchemaData>(GetClassNameStatic());
    static const inline auto Registrator1 = TFactory::TRegistrator<TFirstLevelSchemaData>("JSON_SCANNER");

public:
    TFirstLevelSchemaData() = default;
    TFirstLevelSchemaData(const bool firstLevelOnly)
        : FirstLevelOnly(firstLevelOnly) {
    }
};

class TSIMDJsonExtractor: public IDataAdapter {
public:
    static TString GetClassNameStatic() {
        return "SIMD_JSON_SCANNER";
    }

private:
    virtual bool HasInternalConversion() const override {
        return true;
    }

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
        if (!proto.HasSIMDJsonScanner()) {
            return true;
        }
        FirstLevelOnly = proto.GetSIMDJsonScanner().GetFirstLevelOnly();
        return true;
    }
    virtual void DoSerializeToProto(TProto& proto) const override {
        proto.MutableSIMDJsonScanner()->SetFirstLevelOnly(FirstLevelOnly);
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    static const inline auto Registrator = TFactory::TRegistrator<TSIMDJsonExtractor>(GetClassNameStatic());

public:
    TSIMDJsonExtractor() = default;
    TSIMDJsonExtractor(const bool firstLevelOnly)
        : FirstLevelOnly(firstLevelOnly) {
    }
};

class TDataAdapterContainer: public NBackgroundTasks::TInterfaceProtoContainer<IDataAdapter> {
private:
    using TBase = NBackgroundTasks::TInterfaceProtoContainer<IDataAdapter>;

public:
    static TDataAdapterContainer GetDefault();

    using TBase::TBase;
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
