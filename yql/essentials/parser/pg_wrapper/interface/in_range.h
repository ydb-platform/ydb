#pragma once

#include <yql/essentials/minikql/mkql_node.h>

namespace NYql {

class TPgInRange {
public:
    class TCallState {
    public:
        virtual ~TCallState() = default;
        virtual NUdf::TUnboxedValue Call(NUdf::TUnboxedValue tailval, NUdf::TUnboxedValue currval, NUdf::TUnboxedValue offset, bool sub, bool less) const = 0;
    };

    TPgInRange(ui32 procId, NKikimr::NMiniKQL::TPgType* columnType, NKikimr::NMiniKQL::TPgType* offsetType);

    TPgInRange(TPgInRange&&) noexcept;

    TPgInRange& operator=(TPgInRange&&) noexcept;

    ~TPgInRange();

    THolder<TCallState> MakeCallState() const;

private:
    class TImpl;
    THolder<const TImpl> Impl_;
};

} // namespace NYql
