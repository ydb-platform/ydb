#include "constructor.h"
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>

namespace NKikimr::NArrow::NAccessor {

TConstructorContainer TConstructorContainer::GetDefaultConstructor() {
    static std::shared_ptr<IConstructor> result = std::make_shared<NPlain::TConstructor>();
    return result;
}

}
