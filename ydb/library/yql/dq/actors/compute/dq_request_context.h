#pragma once

#include <util/stream/output.h>
#include <util/generic/fwd.h>
#include <contrib/libs/protobuf/src/google/protobuf/map.h>
#include <util/generic/hash.h>

namespace NYql::NDq {
    
    struct TRequestContext : public TAtomicRefCount<TRequestContext> {
        THashMap<TString, TString> Map;

        TRequestContext() = default;
        TRequestContext(const THashMap<TString, TString>& map) : Map(map) {};

        explicit TRequestContext(const google::protobuf::Map<TProtoStringType, TProtoStringType>& map);

        void Out(IOutputStream& o) const;
    };
}

template<>
inline void Out<NYql::NDq::TRequestContext>(IOutputStream& o, const NYql::NDq::TRequestContext &x) {
    return x.Out(o);
}
