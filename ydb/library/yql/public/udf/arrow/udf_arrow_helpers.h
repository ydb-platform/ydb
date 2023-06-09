#pragma once
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>

#include "defs.h"
#include "util.h"
#include "args_dechunker.h"
#include "block_reader.h"
#include "block_builder.h"

#include <arrow/array/array_base.h>
#include <arrow/array/util.h>
#include <arrow/c/bridge.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/exec_internal.h>

namespace NYql {
namespace NUdf {

using TExec = arrow::Status(*)(arrow::compute::KernelContext*, const arrow::compute::ExecBatch&, arrow::Datum*);

class TUdfKernelState : public arrow::compute::KernelState {
public:
    TUdfKernelState(const TVector<const TType*>& argTypes, const TType* outputType, bool onlyScalars, const ITypeInfoHelper* typeInfoHelper, const IPgBuilder& pgBuilder)
        : ArgTypes_(argTypes)
        , OutputType_(outputType)
        , OnlyScalars_(onlyScalars)
        , TypeInfoHelper_(typeInfoHelper)
        , PgBuilder_(pgBuilder)
    {
        Readers_.resize(ArgTypes_.size());
    }

    IBlockReader& GetReader(ui32 index) {
        if (!Readers_[index]) {
            Readers_[index] = MakeBlockReader(*TypeInfoHelper_, ArgTypes_[index]);
        }

        return *Readers_[index];
    }

    IArrayBuilder& GetArrayBuilder() {
        Y_ENSURE(!OnlyScalars_);
        if (!ArrayBuilder_) {
            ArrayBuilder_ = MakeArrayBuilder(*TypeInfoHelper_, OutputType_, *arrow::default_memory_pool(), TypeInfoHelper_->GetMaxBlockLength(OutputType_), &PgBuilder_);
        }

        return *ArrayBuilder_;
    }

    IScalarBuilder& GetScalarBuilder() {
        Y_ENSURE(OnlyScalars_);
        if (!ScalarBuilder_) {
            ScalarBuilder_ = MakeScalarBuilder(*TypeInfoHelper_, OutputType_);
        }

        return *ScalarBuilder_;
    }

private:
    const TVector<const TType*> ArgTypes_;
    const TType* OutputType_;
    const bool OnlyScalars_;
    const ITypeInfoHelper* TypeInfoHelper_;
    const IPgBuilder& PgBuilder_;  
    TVector<std::unique_ptr<IBlockReader>> Readers_;
    std::unique_ptr<IArrayBuilder> ArrayBuilder_;
    std::unique_ptr<IScalarBuilder> ScalarBuilder_;
};

class TSimpleArrowUdfImpl : public TBoxedValue {
public:
    TSimpleArrowUdfImpl(const TVector<const TType*> argBlockTypes, const TType* outputType, bool onlyScalars,
        TExec exec, IFunctionTypeInfoBuilder& builder, const TString& name,
        arrow::compute::NullHandling::type nullHandling)
        : OnlyScalars_(onlyScalars)
        , Exec_(exec)
        , Pos_(GetSourcePosition(builder))
        , Name_(name)
        , OutputType_(outputType)
    {
        TypeInfoHelper_ = builder.TypeInfoHelper();
        Kernel_.null_handling = nullHandling;
        Kernel_.exec = Exec_;
        std::vector<arrow::compute::InputType> inTypes;
        for (const auto& blockType : argBlockTypes) {
            TBlockTypeInspector blockInspector(*TypeInfoHelper_, blockType);
            Y_ENSURE(blockInspector);
            ArgTypes_.push_back(blockInspector.GetItemType());

            auto arrowTypeHandle = TypeInfoHelper_->MakeArrowType(blockInspector.GetItemType());
            Y_ENSURE(arrowTypeHandle);
            ArrowSchema s;
            arrowTypeHandle->Export(&s);
            auto type = ARROW_RESULT(arrow::ImportType(&s));
            ArgArrowTypes_.emplace_back(type);

            auto shape = blockInspector.IsScalar() ? arrow::ValueDescr::SCALAR : arrow::ValueDescr::ARRAY;

            inTypes.emplace_back(arrow::compute::InputType(type, shape));
            ArgsValuesDescr_.emplace_back(arrow::ValueDescr(type, shape));
        }

        ReturnArrowTypeHandle_ = TypeInfoHelper_->MakeArrowType(outputType);
        Y_ENSURE(ReturnArrowTypeHandle_);

        ArrowSchema s;
        ReturnArrowTypeHandle_->Export(&s);
        auto outputShape = onlyScalars ? arrow::ValueDescr::SCALAR : arrow::ValueDescr::ARRAY;
        arrow::compute::OutputType outType(arrow::ValueDescr(ARROW_RESULT(arrow::ImportType(&s)), outputShape));

        Kernel_.signature = arrow::compute::KernelSignature::Make(std::move(inTypes), std::move(outType));
    }

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        try {
            TVector<arrow::Datum> argDatums(ArgArrowTypes_.size());
            for (ui32 i = 0; i < ArgArrowTypes_.size(); ++i) {
                bool isScalar;
                ui64 length;
                ui32 chunkCount = valueBuilder->GetArrowBlockChunks(args[i], isScalar, length);
                if (isScalar) {
                    ArrowArray a;
                    valueBuilder->ExportArrowBlock(args[i], 0, &a);
                    auto arr = ARROW_RESULT(arrow::ImportArray(&a, ArgArrowTypes_[i]));
                    auto scalar = ARROW_RESULT(arr->GetScalar(0));
                    argDatums[i] = scalar;
                } else {
                    TVector<std::shared_ptr<arrow::Array>> imported(chunkCount);
                    for (ui32 i = 0; i < chunkCount; ++i) {
                        ArrowArray a;
                        valueBuilder->ExportArrowBlock(args[i], i, &a);
                        auto arr = ARROW_RESULT(arrow::ImportArray(&a, ArgArrowTypes_[i]));
                        imported[i] = arr;
                    }

                    if (chunkCount == 1) {
                        argDatums[i] = imported.front();
                    } else {
                        argDatums[i] = ARROW_RESULT(arrow::ChunkedArray::Make(std::move(imported), ArgArrowTypes_[i]));
                    }
                }
            }

            TUdfKernelState kernelState(ArgTypes_, OutputType_, OnlyScalars_, TypeInfoHelper_.Get(), valueBuilder->GetPgBuilder());
            arrow::compute::ExecContext execContext;
            arrow::compute::KernelContext kernelContext(&execContext);
            kernelContext.SetState(&kernelState);

            auto executor = arrow::compute::detail::KernelExecutor::MakeScalar();
            ARROW_OK(executor->Init(&kernelContext, { &Kernel_, ArgsValuesDescr_, nullptr }));

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
                return valueBuilder->ImportArrowBlock(&a, 1, true, *ReturnArrowTypeHandle_);
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

                return valueBuilder->ImportArrowBlock(a.data(), a.size(), false, *ReturnArrowTypeHandle_);
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
    const bool OnlyScalars_;
    const TExec Exec_;
    TSourcePosition Pos_;
    const TString Name_;
    const TType* OutputType_;
    ITypeInfoHelper::TPtr TypeInfoHelper_;

    TVector<std::shared_ptr<arrow::DataType>> ArgArrowTypes_;
    IArrowType::TPtr ReturnArrowTypeHandle_;

    arrow::compute::ScalarKernel Kernel_;
    std::vector<arrow::ValueDescr> ArgsValuesDescr_;
    TVector<const TType*> ArgTypes_;
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

    TVector<const TType*> argBlockTypes;
    auto argsBuilder = builder.Args(callableInspector.GetArgsCount());
    for (ui32 i = 0; i < argsInspector.GetElementsCount(); ++i) {
        TBlockTypeInspector blockInspector(*typeInfoHelper, argsInspector.GetElementType(i));
        auto type = callableInspector.GetArgType(i);
        auto argBlockType = builder.Block(blockInspector.IsScalar())->Item(type).Build();
        argsBuilder->Add(argBlockType);
        if (callableInspector.GetArgumentName(i).Size() > 0) {
            argsBuilder->Name(callableInspector.GetArgumentName(i));
        }

        if (callableInspector.GetArgumentFlags(i) != 0) {
            argsBuilder->Flags(callableInspector.GetArgumentFlags(i));
        }

        argBlockTypes.emplace_back(argBlockType);
    }

    builder.Returns(builder.Block(onlyScalars)->Item(callableInspector.GetReturnType()).Build());
    if (callableInspector.GetOptionalArgsCount() > 0) {
        builder.OptionalArgs(callableInspector.GetOptionalArgsCount());
    }

    if (callableInspector.GetPayload().Size() > 0) {
        builder.PayloadImpl(callableInspector.GetPayload());
    }

    if (!typesOnly) {
        builder.Implementation(new TSimpleArrowUdfImpl(argBlockTypes, callableInspector.GetReturnType(),
            onlyScalars, exec, builder, name, arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE));
    }
}

template <typename TDerived>
struct TUnaryKernelExec {
    static arrow::Status Do(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        auto& state = dynamic_cast<TUdfKernelState&>(*ctx->state());
        auto& reader = state.GetReader(0);
        const auto& arg = batch.values[0];
        if (arg.is_scalar()) {
            auto& builder = state.GetScalarBuilder();
            auto item = reader.GetScalarItem(*arg.scalar());
            TDerived::Process(item, [&](TBlockItem out) {
                *res = builder.Build(out);
            });
        }
        else {
            auto& array = *arg.array();
            auto& builder = state.GetArrayBuilder();
            size_t maxBlockLength = builder.MaxLength();
            Y_ENSURE(maxBlockLength > 0);
            TVector<std::shared_ptr<arrow::ArrayData>> outputArrays;
            for (int64_t i = 0; i < array.length;) {
                for (size_t j = 0; j < maxBlockLength && i < array.length; ++j, ++i) {
                    auto item = reader.GetItem(array, i);
                    TDerived::Process(item, [&](TBlockItem out) {
                        builder.Add(out);
                    });
                }
                auto outputDatum = builder.Build(false);
                ForEachArrayData(outputDatum, [&](const auto& arr) { outputArrays.push_back(arr); });
            }

            *res = MakeArray(outputArrays);
        }

        return arrow::Status::OK();
    }
};

template <typename TDerived>
struct TBinaryKernelExec {
    static arrow::Status Do(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        auto& state = dynamic_cast<TUdfKernelState&>(*ctx->state());
        auto& reader1 = state.GetReader(0);
        auto& reader2 = state.GetReader(1);
        const auto& arg1 = batch.values[0];
        const auto& arg2 = batch.values[1];
        if (arg1.is_scalar() && arg2.is_scalar()) {
            auto& builder = state.GetScalarBuilder();
            auto item1 = reader1.GetScalarItem(*arg1.scalar());
            auto item2 = reader2.GetScalarItem(*arg2.scalar());
            TDerived::Process(item1, item2, [&](TBlockItem out) {
                *res = builder.Build(out);
            });
        }
        else if (arg1.is_scalar() && arg2.is_array()) {
            auto item1 = reader1.GetScalarItem(*arg1.scalar());
            auto& array2 = *arg2.array();
            auto& builder = state.GetArrayBuilder();
            size_t maxBlockLength = builder.MaxLength();
            Y_ENSURE(maxBlockLength > 0);
            TVector<std::shared_ptr<arrow::ArrayData>> outputArrays;
            for (int64_t i = 0; i < array2.length;) {
                for (size_t j = 0; j < maxBlockLength && i < array2.length; ++j, ++i) {
                    auto item2 = reader2.GetItem(array2, i);
                    TDerived::Process(item1, item2, [&](TBlockItem out) {
                        builder.Add(out);
                    });
                }
                auto outputDatum = builder.Build(false);
                ForEachArrayData(outputDatum, [&](const auto& arr) { outputArrays.push_back(arr); });
            }

            *res = MakeArray(outputArrays);
        } else if (arg1.is_array() && arg2.is_scalar()) {
            auto& array1 = *arg1.array();            
            auto item2 = reader2.GetScalarItem(*arg2.scalar());
            auto& builder = state.GetArrayBuilder();
            size_t maxBlockLength = builder.MaxLength();
            Y_ENSURE(maxBlockLength > 0);
            TVector<std::shared_ptr<arrow::ArrayData>> outputArrays;
            for (int64_t i = 0; i < array1.length;) {
                for (size_t j = 0; j < maxBlockLength && i < array1.length; ++j, ++i) {
                    auto item1 = reader1.GetItem(array1, i);
                    TDerived::Process(item1, item2, [&](TBlockItem out) {
                        builder.Add(out);
                    });
                }
                auto outputDatum = builder.Build(false);
                ForEachArrayData(outputDatum, [&](const auto& arr) { outputArrays.push_back(arr); });
            }

            *res = MakeArray(outputArrays);
        } else {
            Y_ENSURE(arg1.is_array() && arg2.is_array());
            auto& array1 = *arg1.array();
            auto& array2 = *arg2.array();
            auto& builder = state.GetArrayBuilder();
            size_t maxBlockLength = builder.MaxLength();
            Y_ENSURE(maxBlockLength > 0);
            TVector<std::shared_ptr<arrow::ArrayData>> outputArrays;
            Y_ENSURE(array1.length == array2.length);
            for (int64_t i = 0; i < array1.length;) {
                for (size_t j = 0; j < maxBlockLength && i < array1.length; ++j, ++i) {
                    auto item1 = reader1.GetItem(array1, i);
                    auto item2 = reader2.GetItem(array2, i);
                    TDerived::Process(item1, item2, [&](TBlockItem out) {
                        builder.Add(out);
                    });
                }
                auto outputDatum = builder.Build(false);
                ForEachArrayData(outputDatum, [&](const auto& arr) { outputArrays.push_back(arr); });
            }

            *res = MakeArray(outputArrays);
        }

        return arrow::Status::OK();
    }
};

template <typename TInput, typename TOutput, TOutput(*Core)(TInput)>
arrow::Status UnaryPreallocatedExecImpl(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
    Y_UNUSED(ctx);
    auto& inArray = batch.values[0].array();
    auto& outArray = res->array();
    const TInput* inValues = inArray->GetValues<TInput>(1);
    TOutput* outValues = outArray->GetMutableValues<TOutput>(1);
    auto length = inArray->length;
    for (int64_t i = 0; i < length; ++i) {
        outValues[i] = Core(inValues[i]);
    }

    return arrow::Status::OK();
}

template <typename TInput, typename TOutput, TOutput(*Core)(TInput)>
class TUnaryOverOptionalImpl : public TBoxedValue {
public:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        Y_UNUSED(valueBuilder);
        if (!args[0]) {
            return {};
        }

        return TUnboxedValuePod(Core(args[0].Get<TInput>()));
    }
};

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
    UDF_IMPL(udfName, builder.SimpleSignature<signatureFunc>().SupportsBlocks();, ;, ;, "", "", udfName##_BlocksImpl)

#define BEGIN_SIMPLE_STRICT_ARROW_UDF(udfName, signatureFunc) \
    BEGIN_ARROW_UDF(udfName##_BlocksImpl, signatureFunc) \
    UDF_IMPL(udfName, builder.SimpleSignature<signatureFunc>().SupportsBlocks().IsStrict();, ;, ;, "", "", udfName##_BlocksImpl)

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
    END_ARROW_UDF(udfName##_BlocksImpl, exec)
