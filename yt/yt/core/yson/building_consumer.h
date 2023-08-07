#pragma once

#include "consumer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! An interface for building an object by parsing a YSON stream.
template <class T>
class IBuildingYsonConsumer
    : public virtual IYsonConsumer
{
public:
    //! Finalizes the parsing process and returns the object built by the processed YSON stream.
    virtual T Finish() = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

