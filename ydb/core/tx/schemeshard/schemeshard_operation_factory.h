#pragma once

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NKikimrSchemeOp {
    class TModifyScheme;
}

namespace NKikimr::NSchemeShard {

class ISubOperation;
struct TOperation;
struct TOperationContext;

class IOperationFactory {
protected:
    using TTxTransaction = NKikimrSchemeOp::TModifyScheme;

public:
    virtual ~IOperationFactory() = default;

    virtual TVector<TIntrusivePtr<ISubOperation>> MakeOperationParts(
        const TOperation& op,
        const TTxTransaction& tx,
        TOperationContext& ctx) const = 0;
};

IOperationFactory* DefaultOperationFactory();

}
