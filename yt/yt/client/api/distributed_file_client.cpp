#include "distributed_file_client.h"

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi {

///////////////////////////////////////////////////////////////////////////////

void TDistributedWriteFileSessionWithCookies::Register(TRegistrar registrar)
{
    registrar.Parameter("session", &TThis::Session);
    registrar.Parameter("cookies", &TThis::Cookies);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
