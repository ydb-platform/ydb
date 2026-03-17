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
 ***********************************************************************
 *
 * Last port: precision/GeometryPrecisionReducer.cpp rev. 1.10 (JTS-1.7)
 *
 **********************************************************************/

#ifndef GEOS_PRECISION_GEOMETRYPRECISIONREDUCER_H
#define GEOS_PRECISION_GEOMETRYPRECISIONREDUCER_H

#include <geos/export.h>
#include <geos/geom/GeometryFactory.h> // for GeometryFactory::Ptr
#include <memory> // for unique_ptr

// Forward declarations
namespace geos {
namespace geom {
class PrecisionModel;
class GeometryFactory;
class Geometry;
}
}

namespace geos {
namespace precision { // geos.precision

/** \brief
 * Reduces the precision of a {@link geom::Geometry}
 * according to the supplied {@link geom::PrecisionModel},
 * ensuring that the result is valid (unless specified otherwise).
 *
 * By default the reduced result is topologically valid
 * To ensure this a polygonal geometry is reduced in a topologically valid fashion
 * (technically, by using snap-rounding).
 * It can be forced to be reduced pointwise by using setPointwise(boolean).
 * Note that in this case the result geometry may be invalid.
 * Linear and point geometry is always reduced pointwise (i.e. without further change to
 * its topology or stucture), since this does not change validity.
 *
 * By default the geometry precision model is not changed.
 * This can be overridden by usingsetChangePrecisionModel(boolean).
 *
 * Normally collapsed components (e.g. lines collapsing to a point)
 * are not included in the result.
 * This behavior can be changed by using setRemoveCollapsedComponents(boolean).
 */
class GEOS_DLL GeometryPrecisionReducer {

private:

    // Externally owned
    const geom::GeometryFactory* newFactory;

    const geom::PrecisionModel& targetPM;

    bool removeCollapsed;
    bool changePrecisionModel;
    bool useAreaReducer;
    bool isPointwise;

    std::unique_ptr<geom::Geometry> reducePointwise(const geom::Geometry& geom);

    std::unique_ptr<geom::Geometry> fixPolygonalTopology(const geom::Geometry& geom);

    geom::GeometryFactory::Ptr createFactory(
        const geom::GeometryFactory& oldGF,
        const geom::PrecisionModel& newPM);

    GeometryPrecisionReducer(GeometryPrecisionReducer const&); /*= delete*/
    GeometryPrecisionReducer& operator=(GeometryPrecisionReducer const&); /*= delete*/

public:

    /**
     * Convenience method for doing precision reduction
     * on a single geometry,
     * with collapses removed
     * and keeping the geometry precision model the same,
     * and preserving polygonal topology.
     *
     * @param g the geometry to reduce
     * @param precModel the precision model to use
     * @return the reduced geometry
     */
    static std::unique_ptr<geom::Geometry>
    reduce(
        const geom::Geometry& g,
        const geom::PrecisionModel& precModel)
    {
        GeometryPrecisionReducer reducer(precModel);
        return reducer.reduce(g);
    }

    /**
     * Convenience method for doing precision reduction
     * on a single geometry,
     * with collapses removed
     * and keeping the geometry precision model the same,
     * but NOT preserving valid polygonal topology.
     *
     * @param g the geometry to reduce
     * @param precModel the precision model to use
     * @return the reduced geometry
     */
    static std::unique_ptr<geom::Geometry>
    reducePointwise(
        const geom::Geometry& g,
        const geom::PrecisionModel& precModel)
    {
        GeometryPrecisionReducer reducer(precModel);
        reducer.setPointwise(true);
        return reducer.reduce(g);
    }

    GeometryPrecisionReducer(const geom::PrecisionModel& pm)
        : newFactory(nullptr)
        , targetPM(pm)
        , removeCollapsed(true)
        , changePrecisionModel(false)
        , useAreaReducer(false)
        , isPointwise(false)
    {}

    /**
     * \brief
     * Create a reducer that will change the precision model of the
     * new reduced Geometry
     *
     * @param changeFactory the factory for the created Geometry.
     *           Its PrecisionModel will be used for the reduction.
     *           NOTE: ownership left to caller must be kept alive for
     *           the whole lifetime of the returned Geometry.
     */
    GeometryPrecisionReducer(const geom::GeometryFactory& changeFactory)
        : newFactory(&changeFactory)
        , targetPM(*(changeFactory.getPrecisionModel()))
        , removeCollapsed(true)
        , changePrecisionModel(false)
        , useAreaReducer(false)
        , isPointwise(false)
    {}

    /**
     * Sets whether the reduction will result in collapsed components
     * being removed completely, or simply being collapsed to an (invalid)
     * Geometry of the same type.
     *
     * @param remove if true collapsed components will be removed
     */
    void
    setRemoveCollapsedComponents(bool remove)
    {
        removeCollapsed = remove;
    }

    /** \brief
    * Sets whether the {@link geom::PrecisionModel} of the new reduced Geometry
    * will be changed to be the {@link geom::PrecisionModel} supplied to
    * specify the precision reduction.
    * The default is to not change the precision model
    *
    * @param change if true the precision
    * model of the created Geometry will be the
    * the precisionModel supplied in the constructor.
    */
    void
    setChangePrecisionModel(bool change)
    {
        changePrecisionModel = change;
    }

    void
    setUseAreaReducer(bool useAR)
    {
        useAreaReducer = useAR;
    }

    /** \brief
     * Sets whether the precision reduction will be done
     * in pointwise fashion only.
     *
     * Pointwise precision reduction reduces the precision
     * of the individual coordinates only, but does
     * not attempt to recreate valid topology.
     * This is only relevant for geometries containing polygonal components.
     *
     * @param pointwise if reduction should be done pointwise only
     */
    void
    setPointwise(bool pointwise)
    {
        isPointwise = pointwise;
    }

    std::unique_ptr<geom::Geometry> reduce(const geom::Geometry& geom);

};

} // namespace geos.precision
} // namespace geos

#endif // GEOS_PRECISION_GEOMETRYPRECISIONREDUCER_H
