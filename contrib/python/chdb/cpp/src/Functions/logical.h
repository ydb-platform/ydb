#pragma once
#include <memory>

namespace DB_CHDB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

FunctionOverloadResolverPtr createInternalFunctionOrOverloadResolver();
FunctionOverloadResolverPtr createInternalFunctionAndOverloadResolver();
FunctionOverloadResolverPtr createInternalFunctionXorOverloadResolver();
FunctionOverloadResolverPtr createInternalFunctionNotOverloadResolver();

}
