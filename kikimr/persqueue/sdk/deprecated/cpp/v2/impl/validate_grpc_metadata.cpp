#include "validate_grpc_metadata.h"
#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <util/generic/string.h>
#include <util/string/escape.h>
#include <util/string/builder.h>

namespace NGrpc {
    bool ValidateHeaderIsLegal(const TString& key, const TString& value, TString& error) {
        error.clear();
        grpc_slice keySlice = grpc_slice_from_static_buffer(key.c_str(), key.size());
        int ok = grpc_header_key_is_legal(keySlice);
        if (!ok) {
            error = TStringBuilder() << "gRPC metadata header key is illegal: \"" << EscapeC(key) << "\"";
            return false;
        }
        grpc_slice valueSlice = grpc_slice_from_static_buffer(value.c_str(), value.size());
        ok = grpc_is_binary_header(keySlice) || grpc_header_nonbin_value_is_legal(valueSlice);
        if (!ok) {
            error = TStringBuilder() << "gRPC metadata header value with key \"" << key << "\" is illegal: \"" << EscapeC(value) << "\"";
            return false;
        }
        return true;
    }
}
