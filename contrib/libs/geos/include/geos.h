/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2005 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************/

#ifndef GEOS_H
#define GEOS_H

/*
 * \file geos.h
 * \brief
 * This file is intended as an include wrapper for client application.
 * It includes commonly needed GEOS headers.
 */

#include <geos/version.h>
#include <geos/geom.h>
#include <geos/util.h>
#include <geos/io/ByteOrderDataInStream.h>
#include <geos/io/ByteOrderValues.h>
#include <geos/io/ParseException.h>
#include <geos/io/WKBConstants.h>
#include <geos/io/WKBReader.h>
#include <geos/io/WKBWriter.h>
#include <geos/io/WKTReader.h>
#include <geos/io/WKTWriter.h>
#include <geos/io/CLocalizer.h>
#include <geos/unload.h>

/// Basic namespace for all GEOS functionalities.
namespace geos {
}

#endif
