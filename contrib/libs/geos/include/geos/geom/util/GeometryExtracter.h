/**********************************************************************
 *
 * GEOS - Geometry Engine Open Source
 * http://geos.osgeo.org
 *
 * Copyright (C) 2011 Sandro Santilli <strk@kbt.io>
 * Copyright (C) 2001-2002 Vivid Solutions Inc.
 * Copyright (C) 2006 Refractions Research Inc.
 *
 * This is free software; you can redistribute and/or modify it under
 * the terms of the GNU Lesser General Public Licence as published
 * by the Free Software Foundation.
 * See the COPYING file for more information.
 *
 **********************************************************************
 *
 * Last port: geom/util/GeometryExtracter.java r320 (JTS-1.12)
 *
 **********************************************************************/

#ifndef GEOS_GEOM_UTIL_GEOMETRYEXTRACTER_H
#define GEOS_GEOM_UTIL_GEOMETRYEXTRACTER_H

#include <geos/export.h>
#include <geos/geom/GeometryFilter.h>
#include <geos/geom/GeometryCollection.h>
#include <vector>

namespace geos {
namespace geom { // geos.geom
namespace util { // geos.geom.util

/**
 * Extracts the components of a given type from a {@link Geometry}.
 */
class GEOS_DLL GeometryExtracter {

public:

    /**
     * Extracts the components of type <tt>clz</tt> from a {@link Geometry}
     * and adds them to the provided container.
     *
     * @param geom the geometry from which to extract
     * @param lst the list to add the extracted elements to
     */
    template <class ComponentType, class TargetContainer>
    static void
    extract(const Geometry& geom, TargetContainer& lst)
    {
        if(const ComponentType* p_c = dynamic_cast<const ComponentType*>(&geom)) {
            lst.push_back(p_c);
        }
        else if(const GeometryCollection* p_c1 =
                    dynamic_cast<const GeometryCollection*>(&geom)) {
            GeometryExtracter::Extracter<ComponentType, TargetContainer> extracter(lst);
            p_c1->apply_ro(&extracter);
        }
    }

private:

    template <class ComponentType, class TargetContainer>
    struct Extracter: public GeometryFilter {

        /**
         * Constructs a filter with a list in which to store the elements found.
         *
         * @param comps the container to extract into (will push_back to it)
         */
        Extracter(TargetContainer& comps) : comps_(comps) {}

        TargetContainer& comps_;

        void
        filter_ro(const Geometry* geom) override
        {
            if(const ComponentType* c = dynamic_cast<const ComponentType*>(geom)) {
                comps_.push_back(c);
            }
        }

        // Declare type as noncopyable
        Extracter(const Extracter& other);
        Extracter& operator=(const Extracter& rhs);
    };

    // Declare type as noncopyable
    GeometryExtracter(const GeometryExtracter& other) = delete;
    GeometryExtracter& operator=(const GeometryExtracter& rhs) = delete;
};

} // namespace geos.geom.util
} // namespace geos.geom
} // namespace geos

#endif
