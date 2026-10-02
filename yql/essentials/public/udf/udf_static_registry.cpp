#include "udf_static_registry.h"

#include <util/generic/singleton.h>
#include <util/generic/vector.h>

namespace NYql::NUdf {

TUdfModuleWrapperList* StaticUdfModuleWrapperList() {
    return Singleton<TUdfModuleWrapperList>();
}

const TUdfModuleWrapperList& GetStaticUdfModuleWrapperList() {
    return *StaticUdfModuleWrapperList();
}

void AddToStaticUdfRegistry(TUdfModuleWrapper&& wrapper) {
    StaticUdfModuleWrapperList()->emplace_back(wrapper);
};

} // namespace NYql::NUdf
