#pragma once

#include "comp_factory_utils.h"

#include <memory>

namespace NYql {

class TPgOpBase {
public:
    TPgOpBase() = default;
    ~TPgOpBase() = default;

private:
    const NKikimr::NMiniKQL::TThrowingBindTerminator BindTerminator;
};

// Base class for sign computation
class TPgSign: public TPgOpBase {
public:
    class TCallState {
    public:
        virtual TMaybe<i32> GetSign(const NUdf::TUnboxedValue& value) = 0;
        virtual ~TCallState() = default;
    };

    virtual ~TPgSign() = default;

    virtual std::unique_ptr<TCallState> MakeCallState() const = 0;

    static std::unique_ptr<TPgSign> Create(ui32 inputTypeOid);
};

// Concrete comparison operator: operName is "<" or ">"
class TPgCompareOp: public TPgOpBase {
public:
    TPgCompareOp(ui32 lhsTypeOid, ui32 rhsTypeOid, std::string_view operName);

    class TCallState {
    public:
        explicit TCallState(const TPgResolvedCallWithCast& call);

        TMaybe<bool> Compare(const NUdf::TUnboxedValue& lhs, const NUdf::TUnboxedValue& rhs);

    private:
        TPgResolvedCallWithCastState State;
        const TPgResolvedCallWithCast& Call;
    };

    TCallState MakeCallState() const;

private:
    TPgResolvedCallWithCast Call;
};

} // namespace NYql
