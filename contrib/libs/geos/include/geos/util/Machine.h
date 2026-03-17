/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2001-2009 Vivid Solutions Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/
#ifndef GEOS_UTIL_MACHINE_H_INCLUDED
#define GEOS_UTIL_MACHINE_H_INCLUDED

/**
 * Check endianness of current machine.
 * @return 0 for big_endian | xdr; 1 == little_endian | ndr
 */
inline int
getMachineByteOrder()
{
    static int endian_check = 1; // don't modify !!
    return *((char*)&endian_check);
}

#endif
