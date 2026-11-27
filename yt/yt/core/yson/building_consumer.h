#pragma once

#include "consumer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! An interface for building an object by parsing a YSON stream.
template <class T>
struct IBuildingYsonConsumer
    : public virtual IYsonConsumer
{
    //! Finalizes the parsing process and transfers ownership of the object built by the YSON stream.
    virtual T Finish() && = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

