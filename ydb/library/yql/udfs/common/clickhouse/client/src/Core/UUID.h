#pragma once

#include <Core/Types.h>


namespace NDB
{

namespace UUIDHelpers
{
    /// Generate random UUID.
    UUID generateV4();

    const UUID Nil{};
}

}
