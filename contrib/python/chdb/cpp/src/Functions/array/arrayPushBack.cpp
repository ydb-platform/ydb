#include "arrayPush.h"
#include <Functions/FunctionFactory.h>


namespace DB_CHDB
{

class FunctionArrayPushBack : public FunctionArrayPush
{
public:
    static constexpr auto name = "arrayPushBack";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayPushBack>(); }
    FunctionArrayPushBack() : FunctionArrayPush(false, name) {}
};

REGISTER_FUNCTION(ArrayPushBack)
{
    factory.registerFunction<FunctionArrayPushBack>();
}

}
