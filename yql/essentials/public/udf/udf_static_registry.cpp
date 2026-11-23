#include "udf_static_registry.h"

#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NYql::NUdf {

void AddToStaticUdfRegistry(const TYqlUdfStaticLibrary* library) {
    Y_ENSURE(library && library->StructSize >= sizeof(TYqlUdfStaticLibrary),
             "Invalid static UDF descriptor");
    Y_ENSURE(library->Register, "A static UDF must provide a registration entry point");
    Singleton<TVector<const TYqlUdfStaticLibrary*>>()->push_back(library);
}

const TVector<const TYqlUdfStaticLibrary*>& GetStaticUdfLibraryList() {
    return *Singleton<TVector<const TYqlUdfStaticLibrary*>>();
}

} // namespace NYql::NUdf
