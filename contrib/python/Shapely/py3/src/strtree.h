#ifndef _RTREE_H
#define _RTREE_H

#include <Python.h>

#include "geos.h"
#include "pygeom.h"

/* A resizable vector with addresses of geometries within tree geometries array */
typedef struct {
  size_t n, m;
  GeometryObject*** a;
} tree_geom_vec_t;

/* A struct to hold pairs of GeometryObject** and distance for use in
 * STRtree::query_nearest
 */
typedef struct {
  GeometryObject** geom;
  double distance;
} tree_geom_dist_vec_item_t;

/* A resizeable vector with pairs of GeometryObject** and distance for use in
 * STRtree::query_nearest */
typedef struct {
  size_t n, m;
  tree_geom_dist_vec_item_t* a;
} tree_geom_dist_vec_t;

/* A resizeable vector with distances to nearest tree items */
typedef struct {
  size_t n, m;
  double* a;
} tree_dist_vec_t;

/* A struct to hold userdata argument data for distance_callback used by
 * GEOSSTRtree_nearest_generic_r */
typedef struct {
  GEOSContextHandle_t ctx;
  tree_geom_dist_vec_t* dist_pairs;
  double min_distance;
  int exclusive;    // if 1, only non-equal tree geometries will be returned
  int all_matches;  // if 0, only first nearest tree geometry will be returned
} tree_nearest_userdata_t;

typedef struct {
  PyObject_HEAD void* ptr;
  npy_intp count;           // count of geometries added to the tree
  size_t _geoms_size;       // size of _geoms array (same as original size of input array)
  GeometryObject** _geoms;  // array of input geometries
} STRtreeObject;

extern PyTypeObject STRtreeType;

extern int init_strtree_type(PyObject* m);

#endif
