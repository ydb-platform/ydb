#pragma once
#include "abstract.h"
#include "collection.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>

#include <library/cpp/object_factory/object_factory.h>

namespace NKikimr::NArrow::NSSA {

enum class ECalculationHardness {
    JustAccessorUsage = 1,
    NotSpecified = 3,
    Equals = 5,
    StringMatching = 10,
    Unknown = 8
};

class IKernelLogic {
private:
    virtual TConclusion<bool> DoExecute(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output,
        const std::shared_ptr<TAccessorsCollection>& resources) const = 0;

    virtual std::optional<TIndexCheckOperation> DoGetIndexCheckerOperation() const = 0;
    YDB_READONLY_DEF(std::optional<ui32>, YqlOperationId);
    virtual NJson::TJsonValue DoDebugJson() const {
        return NJson::JSON_NULL;
    }
public:
    NJson::TJsonValue DebugJson() const;

    IKernelLogic() = default;

    IKernelLogic(const ui32 yqlOperationId)
        : YqlOperationId(yqlOperationId) {
    }

    virtual ~IKernelLogic() = default;

    virtual TString SignalDescription() const {
        return GetClassName();
    }
    virtual ECalculationHardness GetWeight() const = 0;

    using TFactory = NObjectFactory::TObjectFactory<IKernelLogic, TString>;

    virtual TString GetClassName() const = 0;

    TConclusion<bool> Execute(const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output,
        const std::shared_ptr<TAccessorsCollection>& resources) const {
        if (!resources) {
            return TConclusionStatus::Fail("resources in incorrect (nullptr)");
        }
        return DoExecute(input, output, resources);
    }

    virtual bool IsBoolInResult() const = 0;
    std::optional<TIndexCheckOperation> GetIndexCheckerOperation() const {
        return DoGetIndexCheckerOperation();
    }
};

class TSimpleKernelLogic: public IKernelLogic {
private:
    using TBase = IKernelLogic;

    virtual TConclusion<bool> DoExecute(const std::vector<TColumnChainInfo>& /*input*/, const std::vector<TColumnChainInfo>& /*output*/,
        const std::shared_ptr<TAccessorsCollection>& /*resources*/) const override {
        return false;
    }

    virtual NJson::TJsonValue DoDebugJson() const override;
    virtual std::optional<TIndexCheckOperation> DoGetIndexCheckerOperation() const override {
        return std::nullopt;
    }

public:
    TSimpleKernelLogic() = default;
    TSimpleKernelLogic(const ui32 yqlOperationId)
        : TBase(yqlOperationId) {
    }

    virtual TString SignalDescription() const override;

    virtual ECalculationHardness GetWeight() const override {
        if (!GetYqlOperationId()) {
            return ECalculationHardness::Unknown;
        }
        return ECalculationHardness::NotSpecified;
    }

    virtual TString GetClassName() const override {
        return "SIMPLE";
    }

    virtual bool IsBoolInResult() const override;
};

class TLogicMatchString: public IKernelLogic {
private:
    using TBase = IKernelLogic;
    virtual TConclusion<bool> DoExecute(const std::vector<TColumnChainInfo>& /*input*/, const std::vector<TColumnChainInfo>& /*output*/,
        const std::shared_ptr<TAccessorsCollection>& /*resources*/) const override {
        return false;
    }
    virtual std::optional<TIndexCheckOperation> DoGetIndexCheckerOperation() const override {
        return TIndexCheckOperation(Operation, CaseSensitive);
    }
    virtual ECalculationHardness GetWeight() const override {
        return ECalculationHardness::StringMatching;
    }

    const TIndexCheckOperation::EOperation Operation;
    const bool CaseSensitive;
    const bool IsSimpleFunction;

    virtual NJson::TJsonValue DoDebugJson() const override {
        return ::ToString(Operation) + "::" + ::ToString(CaseSensitive) + "::" + ::ToString(IsSimpleFunction);
    }

public:
    TLogicMatchString(const TIndexCheckOperation::EOperation operation, const bool caseSensitive, const bool isSimpleFunction)
        : Operation(operation)
        , CaseSensitive(caseSensitive)
        , IsSimpleFunction(isSimpleFunction) {
    }

    virtual TString SignalDescription() const override {
        return "MATCH_STRING::" + ::ToString(Operation) + "::" + ::ToString(CaseSensitive);
    }

    virtual TString GetClassName() const override {
        return "MATCH_STRING";
    }

    virtual bool IsBoolInResult() const override {
        return !IsSimpleFunction;
    }
};

class TLogicMatchAsciiEqualsIgnoreCase: public TLogicMatchString {
private:
    static TString GetClassNameStatic() {
        return "String._yql_AsciiEqualsIgnoreCase";
    }
public:
    TLogicMatchAsciiEqualsIgnoreCase() : TLogicMatchString(TIndexCheckOperation::EOperation::Contains, false, false) {

    }
    static const inline auto Registrator = TFactory::TRegistrator<TLogicMatchAsciiEqualsIgnoreCase>(GetClassNameStatic());
};

class TLogicMatchAsciiContainsIgnoreCase: public TLogicMatchString {
private:
    static TString GetClassNameStatic() {
        return "String._yql_AsciiContainsIgnoreCase";
    }
public:
    TLogicMatchAsciiContainsIgnoreCase() : TLogicMatchString(TIndexCheckOperation::EOperation::Contains, false, false) {

    }
    static const inline auto Registrator = TFactory::TRegistrator<TLogicMatchAsciiContainsIgnoreCase>(GetClassNameStatic());
};

class TLogicMatchAsciiStartsWithIgnoreCase: public TLogicMatchString {
private:
    static TString GetClassNameStatic() {
        return "String._yql_AsciiStartsWithIgnoreCase";
    }
public:
    TLogicMatchAsciiStartsWithIgnoreCase() : TLogicMatchString(TIndexCheckOperation::EOperation::StartsWith, false, false) {

    }
    static const inline auto Registrator = TFactory::TRegistrator<TLogicMatchAsciiStartsWithIgnoreCase>(GetClassNameStatic());
};

class TLogicMatchAsciiEndsWithIgnoreCase: public TLogicMatchString {
private:
    static TString GetClassNameStatic() {
        return "String._yql_AsciiEndsWithIgnoreCase";
    }
public:
    TLogicMatchAsciiEndsWithIgnoreCase() : TLogicMatchString(TIndexCheckOperation::EOperation::EndsWith, false, false) {

    }
    static const inline auto Registrator = TFactory::TRegistrator<TLogicMatchAsciiEndsWithIgnoreCase>(GetClassNameStatic());
};

class TLogicEquals: public IKernelLogic {
private:
    using TBase = IKernelLogic;
    virtual TConclusion<bool> DoExecute(const std::vector<TColumnChainInfo>& /*input*/, const std::vector<TColumnChainInfo>& /*output*/,
        const std::shared_ptr<TAccessorsCollection>& /*resources*/) const override {
        return false;
    }
    virtual std::optional<TIndexCheckOperation> DoGetIndexCheckerOperation() const override {
        return TIndexCheckOperation(TIndexCheckOperation::EOperation::Equals, true);
    }
    const bool IsSimpleFunction;

    virtual ECalculationHardness GetWeight() const override {
        return ECalculationHardness::Equals;
    }

public:
    TLogicEquals(const bool isSimpleFunction)
        : IsSimpleFunction(isSimpleFunction) {
    }

    virtual TString GetClassName() const override {
        return "EQUALS";
    }

    virtual bool IsBoolInResult() const override {
        return !IsSimpleFunction;
    }
};

class TGetJsonPath: public IKernelLogic {
public:
    static TString GetClassNameStatic() {
        return "JsonValue";
    }
    virtual std::optional<TIndexCheckOperation> DoGetIndexCheckerOperation() const override {
        return std::nullopt;
    }

    virtual ECalculationHardness GetWeight() const override {
        return ECalculationHardness::JustAccessorUsage;
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

        return TDescription(resources->GetAccessorOptional(input.front().GetColumnId()), svPath);
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    static const inline TFactory::TRegistrator<TGetJsonPath> Registrator = TFactory::TRegistrator<TGetJsonPath>(GetClassNameStatic());

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
