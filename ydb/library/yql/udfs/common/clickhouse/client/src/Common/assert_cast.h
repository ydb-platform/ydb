#pragma once

#include <type_traits>
#include <typeinfo>
#include <typeindex>
#include <string>

#include <Common/Exception.h>
#include <common/demangle.h>


namespace NDB
{
    namespace ErrorCodes
    {
        extern const int LOGICAL_ERROR;
    }
}


/** Perform static_cast in release build.
  * Checks type by comparing typeid and throw an exception in debug build.
  * The exact match of the type is checked. That is, cast to the ancestor will be unsuccessful.
  */
template <typename To, typename From>
To assert_cast(From && from)
{
#ifndef NDEBUG
    try
    {
        if constexpr (std::is_pointer_v<To>)
        {
            if (typeid(*from) == typeid(std::remove_pointer_t<To>))
                return static_cast<To>(from);
        }
        else
        {
            if (typeid(from) == typeid(To))
                return static_cast<To>(from);
        }
    }
    catch (const std::exception & e)
    {
        throw NDB::Exception(e.what(), NDB::ErrorCodes::LOGICAL_ERROR);
    }

    throw NDB::Exception("Bad cast from type " + demangle(typeid(from).name()) + " to " + demangle(typeid(To).name()),
                        NDB::ErrorCodes::LOGICAL_ERROR);
#else
    return static_cast<To>(from);
#endif
}
