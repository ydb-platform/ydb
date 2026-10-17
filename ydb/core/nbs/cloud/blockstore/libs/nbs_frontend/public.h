#pragma once

#include <memory>

namespace NYdb::NBS::NBlockStore {

struct INbsBlockStoreFacade;
using INbsBlockStoreFacadePtr = std::shared_ptr<INbsBlockStoreFacade>;

}   // namespace NYdb::NBS::NBlockStore
