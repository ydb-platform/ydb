#pragma once

#include "scheme_type_id.h"
#include "scheme_types_defs.h"


namespace NKikimr {
namespace NScheme {

template <typename TRawType>
struct TTypeAuto;

template <typename TRawType>
struct TTypeIsAuto {
    enum {
        Value = false
    };
};

#define DEFINE_TYPE_AUTO(TRawType, TImplType) \
template<> \
struct TTypeAuto<TRawType> { \
    static const TTypeId TypeId = TImplType::TypeId; \
    using TInstance = typename TImplType::TInstance; \
    using TType = TImplType; \
}; \
template <> \
struct TTypeIsAuto<TRawType> { \
    enum { \
        Value = true \
    }; \
};

DEFINE_TYPE_AUTO(i32, TInt32);
DEFINE_TYPE_AUTO(ui32, TUint32);
DEFINE_TYPE_AUTO(i64, TInt64);
DEFINE_TYPE_AUTO(ui64, TUint64);
DEFINE_TYPE_AUTO(ui8, TUint8);
DEFINE_TYPE_AUTO(bool, TBool);
DEFINE_TYPE_AUTO(float, TFloat);
DEFINE_TYPE_AUTO(double, TDouble);

DEFINE_TYPE_AUTO(TString, TString);
DEFINE_TYPE_AUTO(const char*, TString);

using TPairUi64Ui64Type = std::pair<ui64, ui64>;
DEFINE_TYPE_AUTO(TPairUi64Ui64Type, TPairUi64Ui64);

#undef DEFINE_TYPE_AUTO

} // namespace NScheme
} // namespace NKikimr

