#include "arrayPop.h"
#include <Functions/FunctionFactory.h>


namespace DB_CHDB
{

class FunctionArrayPopBack : public FunctionArrayPop
{
public:
    static constexpr auto name = "arrayPopBack";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPopBack>(); }
    FunctionArrayPopBack() : FunctionArrayPop(false, name) {}
};

REGISTER_FUNCTION(ArrayPopBack)
{
    factory.registerFunction<FunctionArrayPopBack>();
}

}
