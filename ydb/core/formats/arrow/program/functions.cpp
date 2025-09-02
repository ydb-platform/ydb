#include "functions.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/kernels/aggregate_basic_internal.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/kernels/codegen_internal.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/registry_internal.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

namespace NKikimr::NArrow::NSSA {

namespace internal {

// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_boolean<I>> {
  using Type = arrow::UInt64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_signed_integer<I>> {
  using Type = arrow::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_unsigned_integer<I>> {
  using Type = arrow::UInt64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow::enable_if_floating_point<I>> {
  using Type = arrow::DoubleType;
};

template <>
struct FindAccumulatorType<arrow::FloatType, void> {
  using Type = arrow::FloatType;
};

template <typename ArrowType, arrow::compute::SimdLevel::type SimdLevel>
struct SumImpl : public arrow::compute::ScalarAggregator {
  using ThisType = SumImpl<ArrowType, SimdLevel>;
  using CType = typename ArrowType::c_type;
  using SumType = typename FindAccumulatorType<ArrowType>::Type;
  using OutputType = typename arrow::TypeTraits<SumType>::ScalarType;

  arrow::Status Consume(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch) override {
    if (batch[0].is_array()) {
      const auto& data = batch[0].array();
      this->Count += data->length - data->GetNullCount();
      if (arrow::is_boolean_type<ArrowType>::value) {
        this->Sum +=
            static_cast<typename SumType::c_type>(arrow::BooleanArray(data).true_count());
      } else {
        this->Sum +=
            arrow::compute::detail::SumArray<CType, typename SumType::c_type, SimdLevel>(
                *data);
      }
    } else {
      const auto& data = *batch[0].scalar();
      this->Count += data.is_valid * batch.length;
      if (data.is_valid) {
        this->Sum += arrow::compute::internal::UnboxScalar<ArrowType>::Unbox(data) * batch.length;
      }
    }
    return arrow::Status::OK();
  }

  arrow::Status MergeFrom(arrow::compute::KernelContext*, arrow::compute::KernelState&& src) override {
    const auto& other = arrow::checked_cast<const ThisType&>(src);
    this->Count += other.Count;
    this->Sum += other.Sum;
    return arrow::Status::OK();
  }

  arrow::Status Finalize(arrow::compute::KernelContext*, arrow::Datum* out) override {
    if (this->Count < Options.min_count) {
      out->value = std::make_shared<OutputType>();
    } else {
      out->value = arrow::MakeScalar(this->Sum);
    }
    return arrow::Status::OK();
  }

  size_t Count = 0;
  typename SumType::c_type Sum = 0;
  arrow::compute::ScalarAggregateOptions Options;
};

template <typename ArrowType>
struct SumImplDefault : public SumImpl<ArrowType, arrow::compute::SimdLevel::NONE> {
  explicit SumImplDefault(const arrow::compute::ScalarAggregateOptions& options) {
    this->Options = options;
  }
};

void AddScalarAggKernels(arrow::compute::KernelInit init,
                         const std::vector<std::shared_ptr<arrow::DataType>>& types,
                         std::shared_ptr<arrow::DataType> out_ty,
                         arrow::compute::ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    // scalar[InT] -> scalar[OutT]
    auto sig = arrow::compute::KernelSignature::Make({arrow::compute::InputType::Scalar(ty)}, arrow::ValueDescr::Scalar(out_ty));
    AddAggKernel(std::move(sig), init, func, arrow::compute::SimdLevel::NONE);
  }
}

void AddArrayScalarAggKernels(arrow::compute::KernelInit init,
                              const std::vector<std::shared_ptr<arrow::DataType>>& types,
                              std::shared_ptr<arrow::DataType> out_ty,
                              arrow::compute::ScalarAggregateFunction* func,
                              arrow::compute::SimdLevel::type simd_level = arrow::compute::SimdLevel::NONE) {
  arrow::compute::aggregate::AddBasicAggKernels(init, types, out_ty, func, simd_level);
  AddScalarAggKernels(init, types, out_ty, func);
}

arrow::Result<std::unique_ptr<arrow::compute::KernelState>> SumInit(arrow::compute::KernelContext* ctx,
                                             const arrow::compute::KernelInitArgs& args) {
  arrow::compute::aggregate::SumLikeInit<SumImplDefault> visitor(
      ctx, *args.inputs[0].type,
      static_cast<const arrow::compute::ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

static std::unique_ptr<arrow::compute::FunctionRegistry> CreateCustomRegistry() {
  arrow::compute::FunctionRegistry* defaultRegistry = arrow::compute::GetFunctionRegistry();
  auto registry = arrow::compute::FunctionRegistry::Make();
  for (const auto& func : defaultRegistry->GetFunctionNames()) {
    if (func == "sum") {
        auto aggregateFunc = dynamic_cast<arrow::compute::ScalarAggregateFunction*>(defaultRegistry->GetFunction(func)->get());
        if (!aggregateFunc) {
            DCHECK_OK(registry->AddFunction(*defaultRegistry->GetFunction(func)));
            continue;
        }
        arrow::compute::ScalarAggregateFunction newFunc(func, aggregateFunc->arity(), &aggregateFunc->doc(), aggregateFunc->default_options());
        for (const arrow::compute::ScalarAggregateKernel* kernel : aggregateFunc->kernels()) {
            auto shouldReplaceKernel = [](const arrow::compute::ScalarAggregateKernel& kernel) {
                const auto& params = kernel.signature->in_types();
                if (params.empty()) {
                    return false;
                }

                if (params[0].kind() == arrow::compute::InputType::Kind::EXACT_TYPE) {
                    auto type = params[0].type();
                    return type->id() == arrow::Type::FLOAT;
                }

                return false;
            };

            if (shouldReplaceKernel(*kernel)) {
                AddArrayScalarAggKernels(SumInit, {arrow::float32()}, arrow::float32(), &newFunc);
            } else {
                DCHECK_OK(newFunc.AddKernel(*kernel));
            }
        }
        DCHECK_OK(registry->AddFunction(std::make_shared<arrow::compute::ScalarAggregateFunction>(std::move(newFunc))));
    } else {
        DCHECK_OK(registry->AddFunction(*defaultRegistry->GetFunction(func)));
    }
  }

  return registry;
}
arrow::compute::FunctionRegistry* GetCustomFunctionRegistry() {
  static auto registry = internal::CreateCustomRegistry();
  return registry.get();
}

}  // namespace internal

TConclusion<arrow::Datum> TInternalFunction::Call(
    const TExecFunctionContext& context, const TAccessorsCollection& resources) const {
    auto funcNames = GetRegistryFunctionNames();

    auto argumentsReader = resources.GetArguments(TColumnChainInfo::ExtractColumnIds(context.GetColumns()), NeedConcatenation);
    TAccessorsCollection::TChunksMerger merger;
    while (auto arguments = argumentsReader.ReadNext()) {
        arrow::Result<arrow::Datum> result = arrow::Status::UnknownError<std::string>("unknown function");
        for (const auto& funcName : funcNames) {
            if (GetContext() && GetContext()->func_registry()->GetFunction(funcName).ok()) {
                result = arrow::compute::CallFunction(funcName, *arguments, FunctionOptions.get(), GetContext());
            } else {
                arrow::compute::ExecContext defaultContext(arrow::default_memory_pool(), nullptr, internal::GetCustomFunctionRegistry());
                result = arrow::compute::CallFunction(funcName, *arguments, FunctionOptions.get(), &defaultContext);
            }

            if (result.ok() && funcName == "count"sv) {
                result = result->scalar()->CastTo(std::make_shared<arrow::UInt64Type>());
            }
            if (result.ok()) {
                auto prepareStatus = PrepareResult(std::move(*result));
                if (prepareStatus.IsFail()) {
                    return prepareStatus;
                }
                result = prepareStatus.DetachResult();
                break;
            }
        }
        if (result.ok()) {
            merger.AddChunk(*result);
        } else {
            return TConclusionStatus::Fail(result.status().message());
        }
    }
    return merger.Execute();
}

}   // namespace NKikimr::NArrow::NSSA
