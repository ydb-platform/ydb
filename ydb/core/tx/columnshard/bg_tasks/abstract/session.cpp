#include "session.h"
#include "adapter.h"

namespace NKikimr::NOlap::NBackground {

NKikimr::NOlap::TTabletId TStartContext::GetTabletId() const {
    return Adapter->GetTabletId();
}

}