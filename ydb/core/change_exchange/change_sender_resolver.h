#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <util/generic/vector.h>

namespace NKikimr::NChangeExchange {

class IChangeSenderResolver {
public:
    virtual ~IChangeSenderResolver() = default;

    virtual void Resolve() = 0;
    virtual bool IsResolving() const = 0;
    virtual bool IsResolved() const = 0;
};

} // NKikimr::NChangeExchange
