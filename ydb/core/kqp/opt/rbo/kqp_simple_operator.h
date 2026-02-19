#pragma once

#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

class ISimpleOperator: public TSimpleRefCount<ISimpleOperator> {
public:
    virtual ~ISimpleOperator() = default;
    ISimpleOperator() = default;
};

} // namespace NKqp
} // namespace NKikimr
