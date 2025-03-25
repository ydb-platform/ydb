#pragma once
#include "abstract.h"
#include "collection.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NArrow::NSSA {

class IKernelLogic {
private:
    virtual TConclusion<bool> DoExecute(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output,
        const std::shared_ptr<TAccessorsCollection>& resources) const = 0;

public:
    virtual ~IKernelLogic() = default;

    using TFactory = NObjectFactory::TObjectFactory<IKernelLogic, TString>;

    virtual TString GetClassName() const = 0;

    virtual std::optional<TFetchingInfo> BuildFetchTask(const ui32 columnId, const NAccessor::IChunkedArray::EType arrType,
        const std::vector<TColumnChainInfo>& input, const std::shared_ptr<TAccessorsCollection>& resources) const = 0;

    TConclusion<bool> Execute(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output,
        const std::shared_ptr<TAccessorsCollection>& resources) const {
        if (!resources) {
            return TConclusionStatus::Fail("resources in incorrect (nullptr)");
        }
        return DoExecute(input, output, resources);
    }

    virtual bool IsBoolInResult() const = 0;
};

class TGetJsonPath: public IKernelLogic {
public:
    static TString GetClassNameStatic() {
        return "JsonValue";
    }

private:
    virtual bool IsBoolInResult() const override {
        return false;
    }
    class TDescription {
    private:
        std::shared_ptr<NAccessor::IChunkedArray> InputAccessor;
        std::string_view JsonPath;

    public:
        TDescription(const std::shared_ptr<NAccessor::IChunkedArray>& inputAccessor, const std::string_view jsonPath)
            : InputAccessor(inputAccessor)
            , JsonPath(jsonPath) {
        }

        const std::shared_ptr<NAccessor::IChunkedArray>& GetInputAccessor() const {
            return InputAccessor;
        }
        std::string_view GetJsonPath() const {
            return JsonPath;
        }
    };

    TConclusion<TDescription> BuildDescription(
        const std::vector<TColumnChainInfo>& input, const std::shared_ptr<TAccessorsCollection>& resources) const {
        if (input.size() != 2) {
            return TConclusionStatus::Fail("incorrect parameters count (2 expected) for json path extraction");
        }
        auto jsonPathScalar = resources->GetConstantScalarOptional(input[1].GetColumnId());
        if (!jsonPathScalar) {
            return TConclusionStatus::Fail("no data for json path (cannot find parameter)");
        }
        if (jsonPathScalar->type->id() != arrow::utf8()->id()) {
            return TConclusionStatus::Fail("incorrect json path (have to be utf8)");
        }
        const auto buffer = std::static_pointer_cast<arrow::StringScalar>(jsonPathScalar)->value;
        std::string_view svPath((const char*)buffer->data(), buffer->size());
        if (!svPath.starts_with("$.") || svPath.size() == 2) {
            return TConclusionStatus::Fail("incorrect path format: have to be as '$.**...**'");
        }
        svPath = svPath.substr(2);
        if (svPath.starts_with("\"") && svPath.ends_with("\"") && svPath.size() > 2) {
            svPath = svPath.substr(1, svPath.size() - 2);
        }

        return TDescription(resources->GetAccessorOptional(input.front().GetColumnId()), svPath);
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    static const inline TFactory::TRegistrator<TGetJsonPath> Registrator = TFactory::TRegistrator<TGetJsonPath>(GetClassNameStatic());

    virtual std::optional<TFetchingInfo> BuildFetchTask(const ui32 columnId, const NAccessor::IChunkedArray::EType arrType,
        const std::vector<TColumnChainInfo>& input, const std::shared_ptr<TAccessorsCollection>& resources) const override;

    virtual TConclusion<bool> DoExecute(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output,
        const std::shared_ptr<TAccessorsCollection>& resources) const override;

protected:
    virtual NAccessor::TCompositeChunkedArray::TBuilder MakeCompositeBuilder() const;
    virtual std::shared_ptr<IChunkedArray> ExtractArray(const std::shared_ptr<IChunkedArray>& jsonAcc, const std::string_view svPath) const;

public:
};

class TExistsJsonPath: public TGetJsonPath {
private:
    using TBase = TGetJsonPath;

public:
    static TString GetClassNameStatic() {
        return "JsonExists";
    }

private:
    virtual bool IsBoolInResult() const override {
        return true;
    }
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    static const inline TFactory::TRegistrator<TExistsJsonPath> Registrator = TFactory::TRegistrator<TExistsJsonPath>(GetClassNameStatic());
    virtual std::shared_ptr<IChunkedArray> ExtractArray(
        const std::shared_ptr<IChunkedArray>& jsonAcc, const std::string_view svPath) const override;
    virtual NAccessor::TCompositeChunkedArray::TBuilder MakeCompositeBuilder() const override;

public:
};
}   // namespace NKikimr::NArrow::NSSA
