#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>

#include <arrow/array/array_base.h>
#include <arrow/array/util.h>
#include <arrow/c/bridge.h>
#include <arrow/chunked_array.h>
#include <arrow/compute/kernel.h>

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
    {}

    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const final {
        try {
            TVector<arrow::Datum> datums(ArgTypes_.size());
            for (ui32 i = 0; i < ArgTypes_.size(); ++i) {
                bool isScalar;
                ui64 length;
                ui32 chunkCount = valueBuilder->GetArrowBlockChunks(args[i], isScalar, length);
                if (isScalar) {
                    ArrowArray a;
                    valueBuilder->ExportArrowBlock(args[i], 0, &a);
                    auto res = arrow::ImportArray(&a, ArgTypes_[i]);
                    if (!res.status().ok()) {
                        throw yexception() << res.status().ToString();
                    }

                    auto arr = std::move(res).ValueOrDie();
                    auto scalarRes = arr->GetScalar(0);
                    if (!scalarRes.status().ok()) {
                        throw yexception() << scalarRes.status().ToString();
                    }

                    auto scalar = std::move(scalarRes).ValueOrDie();
                    datums[i] = scalar;
                } else {
                    TVector<std::shared_ptr<arrow::Array>> imported(chunkCount);
                    for (ui32 i = 0; i < chunkCount; ++i) {
                        ArrowArray a;
                        valueBuilder->ExportArrowBlock(args[i], i, &a);
                        auto arrRes = arrow::ImportArray(&a, ArgTypes_[i]);
                        if (!arrRes.status().ok()) {
                            UdfTerminate(arrRes.status().ToString().c_str());
                        }

                        imported[i] = std::move(arrRes).ValueOrDie();
                    }

                    if (chunkCount == 1) {
                        datums[i] = imported.front();
                    } else {
                        datums[i] = arrow::ChunkedArray::Make(std::move(imported), ArgTypes_[i]).ValueOrDie();
                    }
                }
            }

            // TODO dechunking, scalar executor
            Y_ENSURE(false);
            Y_UNUSED(Exec_);
            Y_UNUSED(KernelContext_);
            arrow::Datum res;
            if (OnlyScalars_) {
                auto arrRes = arrow::MakeArrayFromScalar(*res.scalar(), 1);
                if (!arrRes.status().ok()) {
                    throw yexception() << arrRes.status().ToString();
                }

                auto arr = std::move(arrRes).ValueOrDie();
                ArrowArray a;
                auto status = arrow::ExportArray(*arr, &a);
                if (!status.ok()) {
                    throw yexception() << status.ToString();
                }

                return valueBuilder->ImportArrowBlock(&a, 1, true, *ReturnType_);
            } else {
                TVector<ArrowArray> a;
                if (res.is_array()) {
                    a.resize(1);
                    auto status = arrow::ExportArray(*res.make_array(), &a[0]);
                    if (!status.ok()) {
                        throw yexception() << status.ToString();
                    }
                } else {
                    Y_ENSURE(res.is_arraylike());
                    a.resize(res.chunks().size());
                    for (ui32 i = 0; i < res.chunks().size(); ++i) {
                        auto status = arrow::ExportArray(*res.chunks()[i], &a[i]);
                        if (!status.ok()) {
                            throw yexception() << status.ToString();
                        }
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

        // TODO fill argTypes
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
