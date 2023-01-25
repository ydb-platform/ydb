#pragma once
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>

#include "defs.h"
#include "util.h"
#include "args_dechunker.h"

#include <arrow/array/array_base.h>
#include <arrow/array/util.h>
#include <arrow/c/bridge.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/exec_internal.h>

namespace NYql {
namespace NUdf {

using TExec = arrow::Status(*)(arrow::compute::KernelContext*, const arrow::compute::ExecBatch&, arrow::Datum*);

class TSimpleArrowUdfImpl : public TBoxedValue {
public:
    TSimpleArrowUdfImpl(const TVector<std::shared_ptr<arrow::DataType>>& argTypes, bool onlyScalars, IArrowType::TPtr&& returnType,
        TExec exec, IFunctionTypeInfoBuilder& builder, const TString& name)
        : ArgTypes_(argTypes)
        , OnlyScalars_(onlyScalars)
        , ReturnType_(std::move(returnType))
        , Exec_(exec)
        , Pos_(GetSourcePosition(builder))
        , Name_(name)
        , KernelContext_(&ExecContext_)
    {
        Kernel_.null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
        Kernel_.exec = Exec_;
        std::vector<arrow::compute::InputType> inTypes;
        for (const auto& t : ArgTypes_) {
            inTypes.emplace_back(t);
            ArgsValuesDescr_.emplace_back(t);
        }

        ArrowSchema s;
        ReturnType_->Export(&s);
        arrow::compute::OutputType outType = ARROW_RESULT(arrow::ImportType(&s));

        Kernel_.signature = arrow::compute::KernelSignature::Make(std::move(inTypes), std::move(outType));
    }

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        try {
            TVector<arrow::Datum> argDatums(ArgTypes_.size());
            for (ui32 i = 0; i < ArgTypes_.size(); ++i) {
                bool isScalar;
                ui64 length;
                ui32 chunkCount = valueBuilder->GetArrowBlockChunks(args[i], isScalar, length);
                if (isScalar) {
                    ArrowArray a;
                    valueBuilder->ExportArrowBlock(args[i], 0, &a);
                    auto arr = ARROW_RESULT(arrow::ImportArray(&a, ArgTypes_[i]));
                    auto scalar = ARROW_RESULT(arr->GetScalar(0));
                    argDatums[i] = scalar;
                } else {
                    TVector<std::shared_ptr<arrow::Array>> imported(chunkCount);
                    for (ui32 i = 0; i < chunkCount; ++i) {
                        ArrowArray a;
                        valueBuilder->ExportArrowBlock(args[i], i, &a);
                        auto arr = ARROW_RESULT(arrow::ImportArray(&a, ArgTypes_[i]));
                        imported[i] = arr;
                    }

                    if (chunkCount == 1) {
                        argDatums[i] = imported.front();
                    } else {
                        argDatums[i] = ARROW_RESULT(arrow::ChunkedArray::Make(std::move(imported), ArgTypes_[i]));
                    }
                }
            }

            auto executor = arrow::compute::detail::KernelExecutor::MakeScalar();
            ARROW_OK(executor->Init(&KernelContext_, { &Kernel_, ArgsValuesDescr_, nullptr }));

            arrow::Datum res;
            if (OnlyScalars_) {
                auto listener = std::make_shared<arrow::compute::detail::DatumAccumulator>();
                ARROW_OK(executor->Execute(argDatums, listener.get()));
                res = executor->WrapResults(argDatums, listener->values());
            } else {
                TArgsDechunker dechunker(std::move(argDatums));
                std::vector<arrow::Datum> chunk;
                TVector<std::shared_ptr<arrow::ArrayData>> arrays;

                while (dechunker.Next(chunk)) {
                    arrow::compute::detail::DatumAccumulator listener;
                    ARROW_OK(executor->Execute(chunk, &listener));
                    auto output = executor->WrapResults(chunk, listener.values());

                    ForEachArrayData(output, [&](const auto& arr) { arrays.push_back(arr); });
                }

                res = MakeArray(arrays);
            }

            if (OnlyScalars_) {
                auto arr = ARROW_RESULT(arrow::MakeArrayFromScalar(*res.scalar(), 1));
                ArrowArray a;
                ARROW_OK(arrow::ExportArray(*arr, &a));
                return valueBuilder->ImportArrowBlock(&a, 1, true, *ReturnType_);
            } else {
                TVector<ArrowArray> a;
                if (res.is_array()) {
                    a.resize(1);
                    ARROW_OK(arrow::ExportArray(*res.make_array(), &a[0]));
                } else {
                    Y_ENSURE(res.is_arraylike());
                    a.resize(res.chunks().size());
                    for (ui32 i = 0; i < res.chunks().size(); ++i) {
                        ARROW_OK(arrow::ExportArray(*res.chunks()[i], &a[i]));
                    }
                }

                return valueBuilder->ImportArrowBlock(a.data(), a.size(), false, *ReturnType_);
            }
        } catch (const std::exception&) {
            TStringBuilder sb;
            sb << Pos_ << " ";
            sb << CurrentExceptionMessage();
            sb << Endl << "[" << Name_ << "]";
            UdfTerminate(sb.c_str());
        }
    }

private:
    const TVector<std::shared_ptr<arrow::DataType>> ArgTypes_;
    const bool OnlyScalars_;
    IArrowType::TPtr ReturnType_;
    const TExec Exec_;
    TSourcePosition Pos_;
    const TString Name_;

    arrow::compute::ExecContext ExecContext_;
    mutable arrow::compute::KernelContext KernelContext_;
    arrow::compute::ScalarKernel Kernel_;
    std::vector<arrow::ValueDescr> ArgsValuesDescr_;
};

inline void PrepareSimpleArrowUdf(IFunctionTypeInfoBuilder& builder, TType* signature, TType* userType, TExec exec, bool typesOnly,
    const TString& name) {
    auto typeInfoHelper = builder.TypeInfoHelper();
    TCallableTypeInspector callableInspector(*typeInfoHelper, signature);
    Y_ENSURE(callableInspector);
    Y_ENSURE(callableInspector.GetArgsCount() > 0);
    TTupleTypeInspector userTypeInspector(*typeInfoHelper, userType);
    Y_ENSURE(userTypeInspector);
    Y_ENSURE(userTypeInspector.GetElementsCount() == 3);
    TTupleTypeInspector argsInspector(*typeInfoHelper, userTypeInspector.GetElementType(0));
    Y_ENSURE(argsInspector);
    Y_ENSURE(argsInspector.GetElementsCount() == callableInspector.GetArgsCount());

    bool hasBlocks = false;
    bool onlyScalars = true;
    for (ui32 i = 0; i < argsInspector.GetElementsCount(); ++i) {
        TBlockTypeInspector blockInspector(*typeInfoHelper, argsInspector.GetElementType(i));
        if (blockInspector) {
            if (i == 0) {
                hasBlocks = true;
            } else {
                Y_ENSURE(hasBlocks);
            }

            onlyScalars = onlyScalars && blockInspector.IsScalar();
        }
    }

    builder.SupportsBlocks();
    builder.UserType(userType);
    Y_ENSURE(hasBlocks);

    TVector<std::shared_ptr<arrow::DataType>> argTypes;
    auto argsBuilder = builder.Args(callableInspector.GetArgsCount());
    for (ui32 i = 0; i < argsInspector.GetElementsCount(); ++i) {
        TBlockTypeInspector blockInspector(*typeInfoHelper, argsInspector.GetElementType(i));
        auto initalType = callableInspector.GetArgType(i);
        argsBuilder->Add(builder.Block(blockInspector.IsScalar())->Item(initalType).Build());
        if (callableInspector.GetArgumentName(i).Size() > 0) {
            argsBuilder->Name(callableInspector.GetArgumentName(i));
        }

        if (callableInspector.GetArgumentFlags(i) != 0) {
            argsBuilder->Flags(callableInspector.GetArgumentFlags(i));
        }

        auto arrowTypeHandle = typeInfoHelper->MakeArrowType(initalType);
        Y_ENSURE(arrowTypeHandle);
        ArrowSchema s;
        arrowTypeHandle->Export(&s);
        auto type = ARROW_RESULT(arrow::ImportType(&s));
        argTypes.emplace_back(type);
    }

    builder.Returns(builder.Block(onlyScalars)->Item(callableInspector.GetReturnType()).Build());
    if (callableInspector.GetOptionalArgsCount() > 0) {
        builder.OptionalArgs(callableInspector.GetOptionalArgsCount());
    }

    if (callableInspector.GetPayload().Size() > 0) {
        builder.PayloadImpl(callableInspector.GetPayload());
    }

    if (!typesOnly) {
        auto returnType = typeInfoHelper->MakeArrowType(callableInspector.GetReturnType());
        Y_ENSURE(returnType);
        builder.Implementation(new TSimpleArrowUdfImpl(argTypes, onlyScalars, std::move(returnType), exec, builder, name));
    }
}

}
}

#define BEGIN_ARROW_UDF(udfNameBlocks, signatureFunc) \
    class udfNameBlocks { \
    public: \
        typedef bool TTypeAwareMarker; \
        static const ::NYql::NUdf::TStringRef& Name() { \
            static auto name = ::NYql::NUdf::TStringRef::Of(#udfNameBlocks).Substring(1, 256); \
            return name; \
        } \
        static ::NYql::NUdf::TType* GetSignatureType(::NYql::NUdf::IFunctionTypeInfoBuilder& builder) { \
            return builder.SimpleSignatureType<signatureFunc>(); \
        } \
        static bool DeclareSignature(\
            const ::NYql::NUdf::TStringRef& name, \
            ::NYql::NUdf::TType* userType, \
            ::NYql::NUdf::IFunctionTypeInfoBuilder& builder, \
            bool typesOnly); \
    };

#define BEGIN_SIMPLE_ARROW_UDF(udfName, signatureFunc) \
    BEGIN_ARROW_UDF(udfName##_BlocksImpl, signatureFunc) \
    UDF(udfName, builder.SimpleSignature<signatureFunc>().SupportsBlocks();)

#define END_ARROW_UDF(udfNameBlocks, exec) \
    inline bool udfNameBlocks::DeclareSignature(\
        const ::NYql::NUdf::TStringRef& name, \
        ::NYql::NUdf::TType* userType, \
        ::NYql::NUdf::IFunctionTypeInfoBuilder& builder, \
        bool typesOnly) { \
            if (Name() == name) { \
                PrepareSimpleArrowUdf(builder, GetSignatureType(builder), userType, exec, typesOnly, TString(name)); \
                return true; \
            } \
            return false; \
    }

#define END_SIMPLE_ARROW_UDF(udfName, exec) \
    END_ARROW_UDF(udfName##_BlocksImpl, exec) \
    template<> \
    struct ::NYql::NUdf::TUdfTraits<udfName> { \
        static constexpr bool SupportsBlocks = true; \
        using TBlockUdf = udfName##_BlocksImpl; \
    };
