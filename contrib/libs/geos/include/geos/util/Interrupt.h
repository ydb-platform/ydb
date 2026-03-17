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

#ifndef GEOS_UTIL_INTERRUPT_H
#define GEOS_UTIL_INTERRUPT_H

#include <geos/export.h>

namespace geos {
namespace util { // geos::util

#define GEOS_CHECK_FOR_INTERRUPTS() geos::util::Interrupt::process()

/** \brief Used to manage interruption requests and callbacks. */
class GEOS_DLL Interrupt {

public:

    typedef void (Callback)(void);

    /**
     * Request interruption of operations
     *
     * Operations will be terminated by a GEOSInterrupt
     * exception at first occasion.
     */
    static void request();

    /** Cancel a pending interruption request */
    static void cancel();

    /** Check if an interruption request is pending */
    static bool check();

    /** \brief
     * Register a callback that will be invoked
     * before checking for interruption requests.
     *
     * NOTE that interruption request checking may happen
     * frequently so any callback would better be quick.
     *
     * The callback can be used to call Interrupt::request()
     *
     */
    static Callback* registerCallback(Callback* cb);

    /**
     * Invoke the callback, if any. Process pending interruption, if any.
     *
     */
    static void process();

    /* Perform the actual interruption (simply throw an exception) */
    static void interrupt();

};


} // namespace geos::util
} // namespace geos


#endif // GEOS_UTIL_INTERRUPT_H
