#pragma once

#include "dq_output.h"

#include <ydb/library/yql/minikql/mkql_alloc.h>

namespace NKikimr::NMiniKQL {
class TTypeEnvironment;
} // namespace NKikimr::NMiniKQL

namespace NYql::NDq {

class IDqOutputConsumer : public TSimpleRefCount<IDqOutputConsumer>, public NKikimr::NMiniKQL::TWithMiniKQLAlloc {
public:
    using TPtr = TIntrusivePtr<IDqOutputConsumer>;

public:
    virtual ~IDqOutputConsumer() = default;

    virtual bool IsFull() const = 0;
    virtual void Consume(NKikimr::NUdf::TUnboxedValue&& value) = 0;
    virtual void Finish() = 0;
};

IDqOutputConsumer::TPtr CreateOutputMultiConsumer(TVector<IDqOutputConsumer::TPtr>&& consumers);

IDqOutputConsumer::TPtr CreateOutputMapConsumer(IDqOutput::TPtr output);

IDqOutputConsumer::TPtr CreateOutputHashPartitionConsumer(
    TVector<IDqOutput::TPtr>&& outputs,
    TVector<NUdf::TDataTypeId>&& keyColumnTypes, TVector<ui32>&& keyColumnIndices,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv);

IDqOutputConsumer::TPtr CreateOutputBroadcastConsumer(TVector<IDqOutput::TPtr>&& outputs);


} // namespace NYql::NDq
