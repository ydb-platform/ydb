#pragma once

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TYsonStructWriteSchemaOptions
{
    // Add demangled cpp type names to schema.
    bool AddCppTypeNames = false;

    // Add default value to every type in scheama.
    // Notice that the same field of child struct can have different value in child struct and in containing parent struct.
    bool AddDefaultValues = false;

    // Add source location to schema.
    bool AddSourceLocation = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
