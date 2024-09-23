#include "scheme_type_registry.h"
#include "scheme_types.h"
#include "scheme_types_defs.h"

#include <util/digest/murmur.h>
#include <util/generic/algorithm.h>


#define REGISTER_TYPE(name, size, ...) RegisterType<T##name>();

namespace NKikimr {
namespace NScheme {

TTypeRegistry::TTypeRegistry()
{
    // move to 'init defaults?'
    RegisterType<TInt8>();
    RegisterType<TUint8>();
    RegisterType<TInt16>();
    RegisterType<TUint16>();
    RegisterType<TInt32>();
    RegisterType<TUint32>();
    RegisterType<TInt64>();
    RegisterType<TUint64>();
    RegisterType<TDouble>();
    RegisterType<TFloat>();
    RegisterType<TBool>();
    RegisterType<TPairUi64Ui64>();
    RegisterType<TString>();
    RegisterType<TSmallBoundedString>();
    RegisterType<TLargeBoundedString>();
    RegisterType<TUtf8>();
    RegisterType<TYson>();
    RegisterType<TJson>();
    RegisterType<TJsonDocument>();
    RegisterType<TDecimal>();
    RegisterType<TDate>();
    RegisterType<TDatetime>();
    RegisterType<TTimestamp>();
    RegisterType<TInterval>();
    RegisterType<TDyNumber>();
    RegisterType<TUuid>();
    RegisterType<TDate32>();
    RegisterType<TDatetime64>();
    RegisterType<TTimestamp64>();
    RegisterType<TInterval64>();
}

void TTypeRegistry::CalculateMetadataEtag() {
}

} // namespace NScheme
} // namespace NKikimr
