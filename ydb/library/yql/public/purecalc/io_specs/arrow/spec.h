#pragma once

#include <ydb/library/yql/public/purecalc/common/interface.h>
#include <arrow/compute/kernel.h>

namespace NYql {
namespace NPureCalc {

/**
 * Processing mode for working with Apache Arrow batches inputs.
 *
 * In this mode purecalc accept pointers to abstract Arrow ExecBatches and
 * processes them. All Datums in batches should respect the given YT schema
 * (the one you pass to the constructor of the input spec).
 *
 * All working modes are implemented. In pull list and pull stream modes a
 * program would accept a pointer to a single stream object or vector of
 * pointers of stream objects of Arrow ExecBatch pointers. In push mode, a
 * program will return a consumer of pointers to Arrow ExecBatch.
 *
 * The program synopsis follows:
 *
 * @code
 * ... TPullListProgram::Apply(IStream<arrow::compute::ExecBatch*>*);
 * ... TPullListProgram::Apply(TVector<IStream<arrow::compute::ExecBatch*>*>);
 * ... TPullStreamProgram::Apply(IStream<arrow::compute::ExecBatch*>*);
 * ... TPullStreamProgram::Apply(TVector<IStream<arrow::compute::ExecBatch*>*>);
 * TConsumer<arrow::compute::ExecBatch*> TPushStreamProgram::Apply(...);
 * @endcode
 */

class TArrowInputSpec: public TInputSpecBase {
private:
    const TVector<NYT::TNode> Schemas_;

public:
    explicit TArrowInputSpec(const TVector<NYT::TNode>& schemas);
    const TVector<NYT::TNode>& GetSchemas() const override;
    const NYT::TNode& GetSchema(ui32 index) const;
    bool ProvidesBlocks() const override { return true; }
};

/**
 * Processing mode for working with Apache Arrow batches outputs.
 *
 * In this mode purecalc yields pointers to abstract Arrow ExecBatches. All
 * Datums in generated batches respects the given YT schema.
 *
 * Note that one should not expect that the returned pointer will be valid
 * forever; in can (and will) become outdated once a new output is
 * requested/pushed.
 *
 * All working modes are implemented. In pull stream and pull list modes a
 * program will return a pointer to a stream of pointers to Arrow ExecBatches.
 * In push mode, it will accept a single consumer of pointers to Arrow ExecBatch.
 *
 * The program synopsis follows:
 *
 * @code
 * IStream<arrow::compute::ExecBatch*> TPullStreamProgram::Apply(...);
 * IStream<arrow::compute::ExecBatch*> TPullListProgram::Apply(...);
 * ... TPushStreamProgram::Apply(TConsumer<arrow::compute::ExecBatch*>);
 * @endcode
 */

class TArrowOutputSpec: public TOutputSpecBase {
private:
    const NYT::TNode Schema_;

public:
    explicit TArrowOutputSpec(const NYT::TNode& schema);
    const NYT::TNode& GetSchema() const override;
    bool AcceptsBlocks() const override { return true; }
};

template <>
struct TInputSpecTraits<TArrowInputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullListMode = true;
    static const constexpr bool SupportPullStreamMode = true;
    static const constexpr bool SupportPushStreamMode = true;

    using TInputItemType = arrow::compute::ExecBatch*;
    using IInputStream = IStream<TInputItemType>;
    using TConsumerType = THolder<IConsumer<TInputItemType>>;

    static void PreparePullListWorker(const TArrowInputSpec&, IPullListWorker*,
        IInputStream*);
    static void PreparePullListWorker(const TArrowInputSpec&, IPullListWorker*,
        THolder<IInputStream>);
    static void PreparePullListWorker(const TArrowInputSpec&, IPullListWorker*,
        const TVector<IInputStream*>&);
    static void PreparePullListWorker(const TArrowInputSpec&, IPullListWorker*,
        TVector<THolder<IInputStream>>&&);

    static void PreparePullStreamWorker(const TArrowInputSpec&, IPullStreamWorker*,
        IInputStream*);
    static void PreparePullStreamWorker(const TArrowInputSpec&, IPullStreamWorker*,
        THolder<IInputStream>);
    static void PreparePullStreamWorker(const TArrowInputSpec&, IPullStreamWorker*,
        const TVector<IInputStream*>&);
    static void PreparePullStreamWorker(const TArrowInputSpec&, IPullStreamWorker*,
        TVector<THolder<IInputStream>>&&);

    static TConsumerType MakeConsumer(const TArrowInputSpec&, TWorkerHolder<IPushStreamWorker>);
};

template <>
struct TOutputSpecTraits<TArrowOutputSpec> {
    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullListMode = true;
    static const constexpr bool SupportPullStreamMode = true;
    static const constexpr bool SupportPushStreamMode = true;

    using TOutputItemType = arrow::compute::ExecBatch*;
    using IOutputStream = IStream<TOutputItemType>;
    using TPullListReturnType = THolder<IOutputStream>;
    using TPullStreamReturnType = THolder<IOutputStream>;

    static const constexpr TOutputItemType StreamSentinel = nullptr;

    static TPullListReturnType ConvertPullListWorkerToOutputType(const TArrowOutputSpec&, TWorkerHolder<IPullListWorker>);
    static TPullStreamReturnType ConvertPullStreamWorkerToOutputType(const TArrowOutputSpec&, TWorkerHolder<IPullStreamWorker>);
    static void SetConsumerToWorker(const TArrowOutputSpec&, IPushStreamWorker*, THolder<IConsumer<TOutputItemType>>);
};

} // namespace NPureCalc
} // namespace NYql
