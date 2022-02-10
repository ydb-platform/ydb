#pragma once

// Do not build with ya.make using EXTERNAL_BUILD
// That will cause conflicts with protos and grpc_client_low

#ifndef EXTERNAL_BUILD
 #include <util/generic/string.h>
#else
 #include <string>
#endif

namespace NYdb {

#ifndef EXTERNAL_BUILD
    using TStringType = TString;
#else
    using TStringType = std::string;
    using ToString = std::to_string;
#endif

} // namespace NYdb