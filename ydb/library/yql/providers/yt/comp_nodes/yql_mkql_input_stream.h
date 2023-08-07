#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <util/generic/ptr.h>

namespace NYql {

struct IInputState {
    virtual ~IInputState() = default;

    virtual bool IsValid() const = 0;
    virtual NUdf::TUnboxedValue GetCurrent() = 0;
    virtual void Next() = 0;
};

class TInputStreamValue
    : public NKikimr::NMiniKQL::TComputationValue<TInputStreamValue>
{
public:
    TInputStreamValue(NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo, IInputState* state);

private:
    virtual NUdf::IBoxedValuePtr ToIndexDictImpl(const NUdf::IValueBuilder& builder) const override;
    virtual NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override;

    bool AtStart_ = true;
    IInputState* State_;
};

class THoldingInputStreamValue : private THolder<IInputState>, public TInputStreamValue {
public:
    inline THoldingInputStreamValue(NKikimr::NMiniKQL::TMemoryUsageInfo* memInfo, IInputState* state)
        : THolder<IInputState>(state)
        , TInputStreamValue(memInfo, this->Get())
    {
    }
};

} // NYql
