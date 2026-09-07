#pragma once

#include <memory>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct ICertificateProvider;
using ICertificateProviderPtr = std::shared_ptr<ICertificateProvider>;

}   // namespace NYdb::NBS
