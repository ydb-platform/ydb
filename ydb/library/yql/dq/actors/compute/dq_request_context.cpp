#include "dq_request_context.h"

namespace NYql::NDq {
    
    TRequestContext::TRequestContext(const google::protobuf::Map<TString, TString>& map) {
        for (auto& [key, value] : map) {
            Map.insert({key, value});
        }
    }

    void TRequestContext::Out(IOutputStream& o) const {
        o << "{";
        for (const auto& [key, value] : Map) {
            o << " " << key << " : " << value << ". ";
        }
        o << "}";
    }
}
