#pragma once
#include "abstract.h"
#include "collection.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/json_value_path.h>

#include <library/cpp/object_factory/object_factory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/function.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/kernel.h>
#include <yql/essentials/core/arrow_kernels/request/request.h>

namespace NKikimr::NArrow::NSSA {

enum class ECalculationHardness {
    JustAccessorUsage = 1,
    NotSpecified = 3,
    Compare = 5,
    StringMatching = 10,
    Unknown = 8
};

class IKernelLogic {
private:
    virtual TConclusion<bool> DoExecute(
        const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output, TAccessorsCollection& resources) const = 0;

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

    // Returns an input whose index representation is preserved by this calculation.
    virtual std::optional<ui32> GetOriginalAddressFromInput() const {
        return std::nullopt;
    }

    TConclusion<bool> Execute(
        const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output, TAccessorsCollection& resources) const {
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
        TAccessorsCollection& /*resources*/) const override {
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

class TToStringKernel: public TSimpleKernelLogic {
private:
    // We are conservative in this field computation for correctness sake and set it only for known good cases.
    // More cases may be expanded to true as necessary.
    YDB_READONLY(bool, PreservesOriginalAddress, false);

    static bool DoesPreserveOriginalAddress(const arrow::compute::ScalarKernel& kernel) {
        const auto& signature = *kernel.signature;
        const auto& inputTypes = signature.in_types();
        return !signature.is_varargs() && inputTypes.size() == 1 &&
               inputTypes.front().kind() == arrow::compute::InputType::EXACT_TYPE &&
               (inputTypes.front().type()->Equals(*arrow::utf8()) || inputTypes.front().type()->Equals(*arrow::binary())) &&
               signature.out_type().kind() == arrow::compute::OutputType::FIXED &&
               signature.out_type().type()->Equals(*arrow::binary());
    }

public:
    TToStringKernel() = default;

    explicit TToStringKernel(const arrow::compute::ScalarKernel& kernel)
        : PreservesOriginalAddress(DoesPreserveOriginalAddress(kernel)) {
    }

    static std::shared_ptr<TToStringKernel> Resolve(const arrow::compute::ScalarFunction& function) {
        const auto kernels = function.kernels();
        // kernels.size() == 0 would probably be a valid setup for no-op (like ToString from type to itself) passed by upper levels.
        // but for now no such case exists
        if (kernels.size() != 1) {
            return std::make_shared<TToStringKernel>();
        }
        return std::make_shared<TToStringKernel>(*kernels.front());
    }

    static TString GetClassNameStatic() {
        return ToString(NYql::TKernelRequestBuilder::EUnaryOp::ToString);
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    virtual std::optional<ui32> GetOriginalAddressFromInput() const override {
        return PreservesOriginalAddress ? std::optional<ui32>(0) : std::nullopt;
    }

    static const inline auto Registrator = TFactory::TRegistrator<TToStringKernel>(GetClassNameStatic());
};

class TLogicMatchString: public IKernelLogic {
private:
    using TBase = IKernelLogic;
    virtual TConclusion<bool> DoExecute(const std::vector<TColumnChainInfo>& /*input*/, const std::vector<TColumnChainInfo>& /*output*/,
        TAccessorsCollection& /*resources*/) const override {
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
    TLogicMatchAsciiEqualsIgnoreCase()
        : TLogicMatchString(TIndexCheckOperation::EOperation::Contains, false, false) {
    }
    static const inline auto Registrator = TFactory::TRegistrator<TLogicMatchAsciiEqualsIgnoreCase>(GetClassNameStatic());
};

class TLogicMatchAsciiContainsIgnoreCase: public TLogicMatchString {
private:
    static TString GetClassNameStatic() {
        return "String._yql_AsciiContainsIgnoreCase";
    }

public:
    TLogicMatchAsciiContainsIgnoreCase()
        : TLogicMatchString(TIndexCheckOperation::EOperation::Contains, false, false) {
    }
    static const inline auto Registrator = TFactory::TRegistrator<TLogicMatchAsciiContainsIgnoreCase>(GetClassNameStatic());
};

class TLogicMatchOlapKernelsAsciiContainsIgnoreCase: public TLogicMatchString {
private:
    static TString GetClassNameStatic() {
        return "OlapKernels._yql_AsciiContainsIgnoreCase";
    }

public:
    TLogicMatchOlapKernelsAsciiContainsIgnoreCase()
        : TLogicMatchString(TIndexCheckOperation::EOperation::Contains, false, false) {
    }
    static const inline auto Registrator = TFactory::TRegistrator<TLogicMatchOlapKernelsAsciiContainsIgnoreCase>(GetClassNameStatic());
};

class TLogicMatchAsciiStartsWithIgnoreCase: public TLogicMatchString {
private:
    static TString GetClassNameStatic() {
        return "String._yql_AsciiStartsWithIgnoreCase";
    }

public:
    TLogicMatchAsciiStartsWithIgnoreCase()
        : TLogicMatchString(TIndexCheckOperation::EOperation::StartsWith, false, false) {
    }
    static const inline auto Registrator = TFactory::TRegistrator<TLogicMatchAsciiStartsWithIgnoreCase>(GetClassNameStatic());
};

class TLogicMatchAsciiEndsWithIgnoreCase: public TLogicMatchString {
private:
    static TString GetClassNameStatic() {
        return "String._yql_AsciiEndsWithIgnoreCase";
    }

public:
    TLogicMatchAsciiEndsWithIgnoreCase()
        : TLogicMatchString(TIndexCheckOperation::EOperation::EndsWith, false, false) {
    }
    static const inline auto Registrator = TFactory::TRegistrator<TLogicMatchAsciiEndsWithIgnoreCase>(GetClassNameStatic());
};

class TCompareKernel: public IKernelLogic {
private:
    using TBase = IKernelLogic;
    virtual TConclusion<bool> DoExecute(const std::vector<TColumnChainInfo>& /*input*/, const std::vector<TColumnChainInfo>& /*output*/,
        TAccessorsCollection& /*resources*/) const override {
        return false;
    }
    virtual std::optional<TIndexCheckOperation> DoGetIndexCheckerOperation() const override {
        return TIndexCheckOperation(Op, true);
    }
    const bool IsSimpleFunction;
    const TIndexCheckOperation::EOperation Op;

    virtual ECalculationHardness GetWeight() const override {
        return ECalculationHardness::Compare;
    }

public:
    TCompareKernel(const bool isSimpleFunction, TIndexCheckOperation::EOperation op)
        : IKernelLogic([&] {
            {
                using enum TIndexCheckOperation::EOperation;

                AFL_VERIFY(op == Less || op == LessOrEqual ||
                    op == Greater || op == GreaterOrEqual || op == Equals
                );
            }

            switch(op) {
                case TIndexCheckOperation::EOperation::Equals:
                    return (ui32)NYql::TKernelRequestBuilder::EBinaryOp::Equals;
                case TIndexCheckOperation::EOperation::Less:
                    return (ui32)NYql::TKernelRequestBuilder::EBinaryOp::Less;
                case TIndexCheckOperation::EOperation::Greater:
                    return (ui32)NYql::TKernelRequestBuilder::EBinaryOp::Greater;
                case TIndexCheckOperation::EOperation::LessOrEqual:
                    return (ui32)NYql::TKernelRequestBuilder::EBinaryOp::LessOrEqual;
                case TIndexCheckOperation::EOperation::GreaterOrEqual:
                    return (ui32)NYql::TKernelRequestBuilder::EBinaryOp::GreaterOrEqual;
                default:
                    AFL_VERIFY(false);
            }
        }()),
          IsSimpleFunction(isSimpleFunction)
        , Op(op)
    {}

    virtual TString GetClassName() const override {
        return TStringBuilder() << "TCompareKernel{" << Op << "}";
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

    TConclusion<TDescription> BuildDescription(const std::vector<TColumnChainInfo>& input, const TAccessorsCollection& resources) const {
        if (input.size() != 2) {
            return TConclusionStatus::Fail("incorrect parameters count (2 expected) for json path extraction");
        }
        auto jsonPathScalar = resources.GetConstantScalarOptional(input[1].GetColumnId());
        if (!jsonPathScalar) {
            return TConclusionStatus::Fail("no data for json path (cannot find parameter)");
        }
        if (jsonPathScalar->type->id() != arrow::utf8()->id()) {
            return TConclusionStatus::Fail("incorrect json path (have to be utf8)");
        }
        const auto buffer = std::static_pointer_cast<arrow::StringScalar>(jsonPathScalar)->value;
        std::string_view svPath((const char*)buffer->data(), buffer->size());
        if (const auto pathValidation = NAccessor::NSubColumns::ValidateJsonPath(svPath); pathValidation.IsFail()) {
            return pathValidation;
        }

        return TDescription(resources.GetAccessorOptional(input.front().GetColumnId()), svPath);
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    static const inline TFactory::TRegistrator<TGetJsonPath> Registrator = TFactory::TRegistrator<TGetJsonPath>(GetClassNameStatic());

    virtual TConclusion<bool> DoExecute(
        const std::vector<TColumnChainInfo>& input, const std::vector<TColumnChainInfo>& output, TAccessorsCollection& resources) const override;

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
