#pragma once
#include <util/generic/fwd.h>

namespace NGrpc {
    // Validates gRPC metadata header key and value. Returns 'true' if validations fails, otherwise returns 'false' and sets 'error' value.
    // For more information see 'Custom-Metadata' section in gRPC over HTTP2 (https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md).
    // Note that in case of authentication data validation 'error' may contain sensitive information.
    bool ValidateHeaderIsLegal(const TString& key, const TString& value, TString& error);

    // Creates hexadecimal representation of 's' in format '{0x01, 0x02, 0x03}'
    TString ToHexString(const TString& s);
}
