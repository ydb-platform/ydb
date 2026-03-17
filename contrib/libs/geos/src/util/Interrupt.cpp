/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2012 Sandro Santilli <strk@kbt.io>
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#include <geos/util/Interrupt.h>
#include <geos/util/GEOSException.h> // for inheritance

namespace {
/* Could these be portably stored in thread-specific space ? */
bool requested = false;

geos::util::Interrupt::Callback* callback = nullptr;
}

namespace geos {
namespace util { // geos::util

class GEOS_DLL InterruptedException: public GEOSException {
public:
    InterruptedException() :
        GEOSException("InterruptedException", "Interrupted!") {}
};

void
Interrupt::request()
{
    requested = true;
}

void
Interrupt::cancel()
{
    requested = false;
}

bool
Interrupt::check()
{
    return requested;
}

Interrupt::Callback*
Interrupt::registerCallback(Interrupt::Callback* cb)
{
    Callback* prev = callback;
    callback = cb;
    return prev;
}

void
Interrupt::process()
{
    if(callback) {
        (*callback)();
    }
    if(requested) {
        requested = false;
        interrupt();
    }
}


void
Interrupt::interrupt()
{
    requested = false;
    throw InterruptedException();
}


} // namespace geos::util
} // namespace geos

