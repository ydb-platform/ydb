#pragma once

#include <string_view>
#include <unordered_map>

namespace DB_CHDB
{

class FunctionFactory;

using FunctionRegisterFunctionPtr = void (*)(::DB_CHDB::FunctionFactory &);

struct FunctionRegisterMap : public std::unordered_map<std::string_view, FunctionRegisterFunctionPtr>
{
    static FunctionRegisterMap & instance();
};

struct FunctionRegister
{
    FunctionRegister(std::string_view name, FunctionRegisterFunctionPtr func_ptr)
    {
        FunctionRegisterMap::instance().emplace(std::move(name), func_ptr);
    }
};

}

#define REGISTER_FUNCTION_IMPL(fn, func_name, register_name) \
    void func_name(::DB_CHDB::FunctionFactory & factory); \
    static ::DB_CHDB::FunctionRegister register_name(#fn, func_name); \
    void func_name(::DB_CHDB::FunctionFactory & factory)

#define REGISTER_FUNCTION(fn) REGISTER_FUNCTION_IMPL(fn, registerFunction##fn, REGISTER_FUNCTION_##fn)
