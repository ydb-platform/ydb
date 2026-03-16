#ifndef _PYGEOS_H
#define _PYGEOS_H

#include "geos.h"

char PyGEOSEqualsIdentical(GEOSContextHandle_t ctx, const GEOSGeometry* geom1, const GEOSGeometry* geom2);

#endif  // _PYGEOS_H
