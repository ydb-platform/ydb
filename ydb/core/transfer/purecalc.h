#pragma once

#include <yql/essentials/public/purecalc/common/interface.h>

namespace NYdb::NTopic::NPurecalc {

using namespace NYql::NPureCalc;

struct TMessage {
    TString Data;
    TString MessageGroupId;
    ui64 Offset = 0;
    ui32 Partition = 0;
    TString ProducerId;
    ui64 SeqNo = 0;
};

class TMessageInputSpec: public TInputSpecBase {
public:
    /**
     * Build input spec and associate the given message descriptor.
     */
    explicit TMessageInputSpec() =  default;

public:
    const TVector<NYT::TNode>& GetSchemas() const override;
    bool ProvidesBlocks() const override { return false; }
};

}

namespace NYql::NPureCalc {

template<>
struct TInputSpecTraits<NYdb::NTopic::NPurecalc::TMessageInputSpec> {

    static const constexpr bool IsPartial = false;

    static const constexpr bool SupportPullStreamMode = true;
    static const constexpr bool SupportPullListMode = true;
    static const constexpr bool SupportPushStreamMode = true;

    using TInput = NYdb::NTopic::NPurecalc::TMessage;
    using TInputSpecType = NYdb::NTopic::NPurecalc::TMessageInputSpec;
    using TConsumerType = THolder<IConsumer<TInput*>>;

    static void PreparePullStreamWorker(const TInputSpecType&, IPullStreamWorker*, THolder<IStream<TInput*>>);
    static void PreparePullListWorker(const TInputSpecType&, IPullListWorker*, THolder<IStream<TInput*>>);
    static TConsumerType MakeConsumer(const TInputSpecType&, TWorkerHolder<IPushStreamWorker>);
};

}
