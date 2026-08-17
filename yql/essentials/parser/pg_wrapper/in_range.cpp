#include "pg_include.h"

#include <yql/essentials/parser/pg_wrapper/interface/in_range.h>

#include <yql/essentials/parser/pg_wrapper/pg_ops.h>

#include <yql/essentials/minikql/mkql_alloc.h>

namespace NYql {

namespace {

using TPgRangeResolvedCall = TPgResolvedCall<false>;

TPgRangeResolvedCall CreateInRangeCallNoCast(ui32 procId, const TVector<ui32>& argTypes) {
    return TPgRangeResolvedCall("in_range", procId, argTypes, NPg::LookupType("bool").TypeId);
}

class TCallStateImpl: public TPgInRange::TCallState {
public:
    explicit TCallStateImpl(const TPgRangeResolvedCall& call)
        : Call_(call)
        ,
        CallState_(call.CreateState())
    {
    }

    ~TCallStateImpl() override = default;

    NUdf::TUnboxedValue Call(NUdf::TUnboxedValue tailval, NUdf::TUnboxedValue currval, NUdf::TUnboxedValue offset, bool sub, bool less) const override {
        std::array<NUdf::TUnboxedValue, 5> values = {tailval, currval, offset, NUdf::TUnboxedValuePod(sub), NUdf::TUnboxedValuePod(less)};
        return Call_.CallFunction(*CallState_, values);
    }

private:
    const TPgRangeResolvedCall& Call_;
    THolder<TPgResolvedCallState> CallState_;
};

} // namespace

class TPgInRange::TImpl {
public:
    TImpl(ui32 procId, NKikimr::NMiniKQL::TPgType* columnType, NKikimr::NMiniKQL::TPgType* offsetType)
        : Call_(CreateInRangeCallNoCast(procId, {columnType->GetTypeId(),
                                                 columnType->GetTypeId(),
                                                 offsetType->GetTypeId(),
                                                 NPg::LookupType("bool").TypeId,
                                                 NPg::LookupType("bool").TypeId}))
    {
        MKQL_ENSURE(Call_.GetReturnTypeId() == NPg::LookupType("bool").TypeId, "Result type mismatch");
    }

    ~TImpl() = default;

    THolder<TCallStateImpl> MakeCallState() const {
        return MakeHolder<TCallStateImpl>(Call_);
    }

private:
    TPgRangeResolvedCall Call_;
};

TPgInRange::TPgInRange(ui32 procId, NKikimr::NMiniKQL::TPgType* columnType, NKikimr::NMiniKQL::TPgType* offsetType)
    : Impl_(new TImpl(procId, columnType, offsetType))
{
}

TPgInRange::TPgInRange(TPgInRange&&) noexcept = default;

TPgInRange& TPgInRange::operator=(TPgInRange&&) noexcept = default;

TPgInRange::~TPgInRange() = default;

THolder<TPgInRange::TCallState> TPgInRange::MakeCallState() const {
    return Impl_->MakeCallState();
}

} // namespace NYql
