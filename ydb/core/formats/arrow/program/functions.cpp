#include "functions.h"

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/kernels/aggregate_basic_internal.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/kernels/codegen_internal.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/registry_internal.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/table.h>

namespace NKikimr::NArrow::NSSA {

namespace internal {

// Find the largest compatible primitive type for a primitive type.
template <typename I, typename Enable = void>
struct FindAccumulatorType {};

template <typename I>
struct FindAccumulatorType<I, arrow20::enable_if_boolean<I>> {
  using Type = arrow20::UInt64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow20::enable_if_signed_integer<I>> {
  using Type = arrow20::Int64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow20::enable_if_unsigned_integer<I>> {
  using Type = arrow20::UInt64Type;
};

template <typename I>
struct FindAccumulatorType<I, arrow20::enable_if_floating_point<I>> {
  using Type = arrow20::DoubleType;
};

template <>
struct FindAccumulatorType<arrow20::FloatType, void> {
  using Type = arrow20::FloatType;
};

template <typename ArrowType, arrow20::compute::SimdLevel::type SimdLevel>
struct SumImpl : public arrow20::compute::ScalarAggregator {
  using ThisType = SumImpl<ArrowType, SimdLevel>;
  using CType = typename ArrowType::c_type;
  using SumType = typename FindAccumulatorType<ArrowType>::Type;
  using OutputType = typename arrow20::TypeTraits<SumType>::ScalarType;

  arrow20::Status Consume(arrow20::compute::KernelContext*, const arrow20::compute::ExecBatch& batch) override {
    if (batch[0].is_array()) {
      const auto& data = batch[0].array();
      this->Count += data->length - data->GetNullCount();
      if (arrow20::is_boolean_type<ArrowType>::value) {
        this->Sum +=
            static_cast<typename SumType::c_type>(arrow20::BooleanArray(data).true_count());
      } else {
        this->Sum +=
            arrow20::compute::detail::SumArray<CType, typename SumType::c_type, SimdLevel>(
                *data);
      }
    } else {
      const auto& data = *batch[0].scalar();
      this->Count += data.is_valid * batch.length;
      if (data.is_valid) {
        this->Sum += arrow20::compute::internal::UnboxScalar<ArrowType>::Unbox(data) * batch.length;
      }
    }
    return arrow20::Status::OK();
  }

  arrow20::Status MergeFrom(arrow20::compute::KernelContext*, arrow20::compute::KernelState&& src) override {
    const auto& other = arrow20::checked_cast<const ThisType&>(src);
    this->Count += other.Count;
    this->Sum += other.Sum;
    return arrow20::Status::OK();
  }

  arrow20::Status Finalize(arrow20::compute::KernelContext*, arrow20::Datum* out) override {
    if (this->Count < Options.min_count) {
      out->value = std::make_shared<OutputType>();
    } else {
      out->value = arrow20::MakeScalar(this->Sum);
    }
    return arrow20::Status::OK();
  }

  size_t Count = 0;
  typename SumType::c_type Sum = 0;
  arrow20::compute::ScalarAggregateOptions Options;
};

template <typename ArrowType>
struct SumImplDefault : public SumImpl<ArrowType, arrow20::compute::SimdLevel::NONE> {
  explicit SumImplDefault(const arrow20::compute::ScalarAggregateOptions& options) {
    this->Options = options;
  }
};

void AddScalarAggKernels(arrow20::compute::KernelInit init,
                         const std::vector<std::shared_ptr<arrow20::DataType>>& types,
                         std::shared_ptr<arrow20::DataType> out_ty,
                         arrow20::compute::ScalarAggregateFunction* func) {
  for (const auto& ty : types) {
    // scalar[InT] -> scalar[OutT]
    auto sig = arrow20::compute::KernelSignature::Make({arrow20::compute::InputType(ty)},
                                                         arrow20::compute::OutputType(out_ty));
    AddAggKernel(std::move(sig), init, func, arrow20::compute::SimdLevel::NONE);
  }
}

void AddArrayScalarAggKernels(arrow20::compute::KernelInit init,
                              const std::vector<std::shared_ptr<arrow20::DataType>>& types,
                              std::shared_ptr<arrow20::DataType> out_ty,
                              arrow20::compute::ScalarAggregateFunction* func,
                              arrow20::compute::SimdLevel::type simd_level = arrow20::compute::SimdLevel::NONE) {
  arrow20::compute::aggregate::AddBasicAggKernels(init, types, out_ty, func, simd_level);
  AddScalarAggKernels(init, types, out_ty, func);
}

arrow20::Result<std::unique_ptr<arrow20::compute::KernelState>> SumInit(arrow20::compute::KernelContext* ctx,
                                             const arrow20::compute::KernelInitArgs& args) {
  arrow20::compute::aggregate::SumLikeInit<SumImplDefault> visitor(
      ctx, *args.inputs[0].type,
      static_cast<const arrow20::compute::ScalarAggregateOptions&>(*args.options));
  return visitor.Create();
}

static std::unique_ptr<arrow20::compute::FunctionRegistry> CreateCustomRegistry() {
  arrow20::compute::FunctionRegistry* defaultRegistry = arrow20::compute::GetFunctionRegistry();
  auto registry = arrow20::compute::FunctionRegistry::Make();
  for (const auto& func : defaultRegistry->GetFunctionNames()) {
    if (func == "sum") {
        auto aggregateFunc = dynamic_cast<arrow20::compute::ScalarAggregateFunction*>(defaultRegistry->GetFunction(func)->get());
        if (!aggregateFunc) {
            DCHECK_OK(registry->AddFunction(*defaultRegistry->GetFunction(func)));
            continue;
        }
        arrow20::compute::ScalarAggregateFunction newFunc(func, aggregateFunc->arity(), &aggregateFunc->doc(), aggregateFunc->default_options());
        for (const arrow20::compute::ScalarAggregateKernel* kernel : aggregateFunc->kernels()) {
            auto shouldReplaceKernel = [](const arrow20::compute::ScalarAggregateKernel& kernel) {
                const auto& params = kernel.signature->in_types();
                if (params.empty()) {
                    return false;
                }

                if (params[0].kind() == arrow20::compute::InputType::Kind::EXACT_TYPE) {
                    auto type = params[0].type();
                    return type->id() == arrow20::Type::FLOAT;
                }

                return false;
            };

            if (shouldReplaceKernel(*kernel)) {
                AddArrayScalarAggKernels(SumInit, {arrow20::float32()}, arrow20::float32(), &newFunc);
            } else {
                DCHECK_OK(newFunc.AddKernel(*kernel));
            }
        }
        DCHECK_OK(registry->AddFunction(std::make_shared<arrow20::compute::ScalarAggregateFunction>(std::move(newFunc))));
    } else {
        DCHECK_OK(registry->AddFunction(*defaultRegistry->GetFunction(func)));
    }
  }

  return registry;
}
arrow20::compute::FunctionRegistry* GetCustomFunctionRegistry() {
  static auto registry = internal::CreateCustomRegistry();
  return registry.get();
}

}  // namespace internal

TConclusion<arrow20::Datum> TInternalFunction::Call(
    const TExecFunctionContext& context, const TAccessorsCollection& resources) const {
    auto funcNames = GetRegistryFunctionNames();

    auto argumentsReader = resources.GetArguments(TColumnChainInfo::ExtractColumnIds(context.GetColumns()), NeedConcatenation);
    TAccessorsCollection::TChunksMerger merger;
    while (auto arguments = argumentsReader.ReadNext()) {
        arrow20::Result<arrow20::Datum> result = arrow20::Status::UnknownError<std::string>("unknown function");
        for (const auto& funcName : funcNames) {
            if (GetContext() && GetContext()->func_registry()->GetFunction(funcName).ok()) {
                result = arrow20::compute::CallFunction(funcName, *arguments, FunctionOptions.get(), GetContext());
            } else {
                arrow20::compute::ExecContext defaultContext(arrow20::default_memory_pool(), nullptr, internal::GetCustomFunctionRegistry());
                result = arrow20::compute::CallFunction(funcName, *arguments, FunctionOptions.get(), &defaultContext);
            }

            if (result.ok() && funcName == "count"sv) {
                result = result->scalar()->CastTo(std::make_shared<arrow20::UInt64Type>());
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
