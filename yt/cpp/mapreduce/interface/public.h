#pragma once

#include <memory>

namespace NYT::NAuth {

struct IServiceTicketAuthPtrWrapper;
using IServiceTicketAuthPtrWrapperPtr = std::shared_ptr<IServiceTicketAuthPtrWrapper>;

} // namespace NYT::NAuth
