#define PY_SSIZE_T_CLEAN

#include "pygeom.h"

#include <Python.h>
#include <structmember.h>

#include "geos.h"
#include "pygeos.h"

/* This initializes a global geometry type registry */
PyObject* geom_registry[1] = {NULL};

/* Initializes a new geometry object */
PyObject* GeometryObject_FromGEOS(GEOSGeometry* ptr, GEOSContextHandle_t ctx) {
  if (ptr == NULL) {
    Py_INCREF(Py_None);
    return Py_None;
  }

  int type_id = GEOSGeomTypeId_r(ctx, ptr);

  if (type_id == -1) {
    return NULL;
  }

  // Nonlinear types (CircularString, CompoundCurve, MultiCurve, CurvePolygon,
  // MultiSurface are not currently supported
  // TODO: this can be removed once these types are added to the type registry
  if (type_id >= 8) {
    PyErr_Format(PyExc_NotImplementedError,
                 "Nonlinear geometry types are not currently supported");
    return NULL;
  }

  PyObject* type_obj = PyList_GET_ITEM(geom_registry[0], type_id);
  if (type_obj == NULL) {
    return NULL;
  }
  if (!PyType_Check(type_obj)) {
    PyErr_Format(PyExc_RuntimeError, "Invalid registry value");
    return NULL;
  }
  PyTypeObject* type = (PyTypeObject*)type_obj;
  GeometryObject* self = (GeometryObject*)type->tp_alloc(type, 0);
  if (self == NULL) {
    return NULL;
  } else {
    self->ptr = ptr;
    self->ptr_prepared = NULL;
    self->weakreflist = (PyObject*)NULL;
    return (PyObject*)self;
  }
}

static void GeometryObject_dealloc(GeometryObject* self) {
  if (self->weakreflist != NULL) {
    PyObject_ClearWeakRefs((PyObject*)self);
  }
  if (self->ptr != NULL) {
    // not using GEOS_INIT, but using global context instead
    GEOSContextHandle_t ctx = geos_context[0];
    GEOSGeom_destroy_r(ctx, self->ptr);
    if (self->ptr_prepared != NULL) {
      GEOSPreparedGeom_destroy_r(ctx, self->ptr_prepared);
    }
  }
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyMemberDef GeometryObject_members[] = {
    {"_geom", T_PYSSIZET, offsetof(GeometryObject, ptr), READONLY,
     "pointer to GEOSGeometry"},
    {"_geom_prepared", T_PYSSIZET, offsetof(GeometryObject, ptr_prepared), READONLY,
     "pointer to PreparedGEOSGeometry"},
    {NULL} /* Sentinel */
};

static PyObject* GeometryObject_ToWKT(GeometryObject* obj) {
  char* wkt;
  PyObject* result;
  GEOSGeometry* geom = obj->ptr;
  char trim = 1;
  int precision = 3;
#if GEOS_SINCE_3_12_0
  int dimension = 4;
#else
  int dimension = 3;
#endif
  // int use_old_3d = 0;  // default is false

  if (geom == NULL) {
    Py_INCREF(Py_None);
    return Py_None;
  }

  GEOS_INIT;
#if !GEOS_SINCE_3_13_0
  if (trim) {
    errstate = check_to_wkt_trim_compatible(ctx, geom, dimension);
  }
  if (errstate != PGERR_SUCCESS) {
    goto finish;
  }
#endif  // !GEOS_SINCE_3_13_0

#if !GEOS_SINCE_3_12_0
  // Since GEOS 3.9.0 and before 3.12.0 further handling required
  errstate = wkt_empty_3d_geometry(ctx, geom, &wkt);
  if (errstate != PGERR_SUCCESS) {
    goto finish;
  }
  if (wkt != NULL) {
    result = PyUnicode_FromString(wkt);
    goto finish;
  }
#endif // !GEOS_SINCE_3_12_0

  GEOSWKTWriter* writer = GEOSWKTWriter_create_r(ctx);
  if (writer == NULL) {
    errstate = PGERR_GEOS_EXCEPTION;
    goto finish;
  }

  GEOSWKTWriter_setRoundingPrecision_r(ctx, writer, precision);
#if !GEOS_SINCE_3_12_0
  // Override defaults only for older versions
  // See https://github.com/libgeos/geos/pull/915
  GEOSWKTWriter_setTrim_r(ctx, writer, trim);
  GEOSWKTWriter_setOutputDimension_r(ctx, writer, dimension);
  // GEOSWKTWriter_setOld3D_r(ctx, writer, use_old_3d);
#endif  // !GEOS_SINCE_3_12_0

  // Check if the above functions caused a GEOS exception
  if (last_error[0] != 0) {
    errstate = PGERR_GEOS_EXCEPTION;
    goto finish;
  }

  wkt = GEOSWKTWriter_write_r(ctx, writer, geom);
  result = PyUnicode_FromString(wkt);
  GEOSFree_r(ctx, wkt);
  GEOSWKTWriter_destroy_r(ctx, writer);

finish:
  GEOS_FINISH;
  if (errstate == PGERR_SUCCESS) {
    return result;
  } else {
    return NULL;
  }
}

static PyObject* GeometryObject_ToWKB(GeometryObject* obj) {
  unsigned char* wkb = NULL;
  char has_empty = 0;
  size_t size;
  PyObject* result = NULL;
  GEOSGeometry* geom = NULL;
  GEOSWKBWriter* writer = NULL;
  if (obj->ptr == NULL) {
    Py_INCREF(Py_None);
    return Py_None;
  }

  GEOS_INIT;
  geom = obj->ptr;

  /* Create the WKB writer */
  writer = GEOSWKBWriter_create_r(ctx);
  if (writer == NULL) {
    errstate = PGERR_GEOS_EXCEPTION;
    goto finish;
  }
#if !GEOS_SINCE_3_12_0
  // Allow 3D output for GEOS<3.12 (it is default 4 afterwards)
  // See https://github.com/libgeos/geos/pull/908
  GEOSWKBWriter_setOutputDimension_r(ctx, writer, 3);
#endif  // !GEOS_SINCE_3_12_0
  // include SRID
  GEOSWKBWriter_setIncludeSRID_r(ctx, writer, 1);
  // Check if the above functions caused a GEOS exception
  if (last_error[0] != 0) {
    errstate = PGERR_GEOS_EXCEPTION;
    goto finish;
  }

  wkb = GEOSWKBWriter_write_r(ctx, writer, geom, &size);
  if (wkb == NULL) {
    errstate = PGERR_GEOS_EXCEPTION;
    goto finish;
  }

  result = PyBytes_FromStringAndSize((char*)wkb, size);

finish:
  // Destroy the geom if it was patched (POINT EMPTY patch)
  if (has_empty && (geom != NULL)) {
    GEOSGeom_destroy_r(ctx, geom);
  }
  if (writer != NULL) {
    GEOSWKBWriter_destroy_r(ctx, writer);
  }
  if (wkb != NULL) {
    GEOSFree_r(ctx, wkb);
  }

  GEOS_FINISH;

  return result;
}

static PyObject* GeometryObject_repr(GeometryObject* self) {
  PyObject *result, *wkt, *truncated;

  wkt = GeometryObject_ToWKT(self);
  // we never want a repr() to fail; that can be very confusing
  if (wkt == NULL) {
    PyErr_Clear();
    return PyUnicode_FromString("<shapely.Geometry Exception in WKT writer>");
  }
  // the total length is limited to 80 characters
  if (PyUnicode_GET_LENGTH(wkt) > 62) {
    truncated = PyUnicode_Substring(wkt, 0, 59);
    result = PyUnicode_FromFormat("<shapely.Geometry %U...>", truncated);
    Py_XDECREF(truncated);
  } else {
    result = PyUnicode_FromFormat("<shapely.Geometry %U>", wkt);
  }
  Py_XDECREF(wkt);
  return result;
}

static PyObject* GeometryObject_str(GeometryObject* self) {
  return GeometryObject_ToWKT(self);
}

/* For lookups in sets / dicts.
 * Python should be told how to generate a hash from the Geometry object. */
static Py_hash_t GeometryObject_hash(GeometryObject* self) {
  PyObject* wkb = NULL;
  Py_hash_t x;

  if (self->ptr == NULL) {
    return -1;
  }

  // Transform to a WKB (PyBytes object)
  wkb = GeometryObject_ToWKB(self);
  if (wkb == NULL) {
    return -1;
  }

  // Use the python built-in method to hash the PyBytes object
  x = wkb->ob_type->tp_hash(wkb);
  if (x == -1) {
    x = -2;
  } else {
    x ^= 374761393UL;  // to make the result distinct from the actual WKB hash //
  }

  Py_DECREF(wkb);

  return x;
}

static PyObject* GeometryObject_richcompare(GeometryObject* self, PyObject* other,
                                            int op) {
  PyObject* result = NULL;
  GEOS_INIT;
  if (Py_TYPE(self)->tp_richcompare != Py_TYPE(other)->tp_richcompare) {
    result = Py_NotImplemented;
  } else {
    GeometryObject* other_geom = (GeometryObject*)other;
    switch (op) {
      case Py_LT:
        result = Py_NotImplemented;
        break;
      case Py_LE:
        result = Py_NotImplemented;
        break;
      case Py_EQ:
        result =
            PyGEOSEqualsIdentical(ctx, self->ptr, other_geom->ptr) ? Py_True : Py_False;
        break;
      case Py_NE:
        result =
            PyGEOSEqualsIdentical(ctx, self->ptr, other_geom->ptr) ? Py_False : Py_True;
        break;
      case Py_GT:
        result = Py_NotImplemented;
        break;
      case Py_GE:
        result = Py_NotImplemented;
        break;
    }
  }
  GEOS_FINISH;
  Py_XINCREF(result);
  return result;
}


static PyObject* GeometryObject_SetState(PyObject* self, PyObject* value) {
  unsigned char* wkb = NULL;
  Py_ssize_t size;
  GEOSGeometry* geom = NULL;
  GEOSWKBReader* reader = NULL;

  PyErr_WarnFormat(PyExc_UserWarning, 0,
                   "Unpickling a shapely <2.0 geometry object. "
                   "Please save the pickle again as this compatibility may be "
                   "removed in a future version of shapely.");

  /* Cast the PyObject bytes to char */
  if (!PyBytes_Check(value)) {
    PyErr_Format(PyExc_TypeError, "Expected bytes, found %s", value->ob_type->tp_name);
    return NULL;
  }
  size = PyBytes_Size(value);
  wkb = (unsigned char*)PyBytes_AsString(value);
  if (wkb == NULL) {
    return NULL;
  }

  PyObject* linearring_type_obj = PyList_GET_ITEM(geom_registry[0], 2);
  if (linearring_type_obj == NULL) {
    return NULL;
  }
  if (!PyType_Check(linearring_type_obj)) {
    PyErr_Format(PyExc_RuntimeError, "Invalid registry value");
    return NULL;
  }
  PyTypeObject* linearring_type = (PyTypeObject*)linearring_type_obj;

  GEOS_INIT;

  reader = GEOSWKBReader_create_r(ctx);
  if (reader == NULL) {
    errstate = PGERR_GEOS_EXCEPTION;
    goto finish;
  }
  geom = GEOSWKBReader_read_r(ctx, reader, wkb, size);
  if (geom == NULL) {
    errstate = PGERR_GEOS_EXCEPTION;
    goto finish;
  }
  if (Py_TYPE(self) == linearring_type) {
    const GEOSCoordSequence* coord_seq = GEOSGeom_getCoordSeq_r(ctx, geom);
    if (coord_seq == NULL) {
      errstate = PGERR_GEOS_EXCEPTION;
      goto finish;
    }
    geom = GEOSGeom_createLinearRing_r(ctx, (GEOSCoordSequence*)coord_seq);
    if (geom == NULL) {
      errstate = PGERR_GEOS_EXCEPTION;
      goto finish;
    }
  }

  if (((GeometryObject*)self)->ptr != NULL) {
    GEOSGeom_destroy_r(ctx, ((GeometryObject*)self)->ptr);
  }
  ((GeometryObject*)self)->ptr = geom;

finish:

  if (reader != NULL) {
    GEOSWKBReader_destroy_r(ctx, reader);
  }

  GEOS_FINISH;

  if (errstate == PGERR_SUCCESS) {
    Py_INCREF(Py_None);
    return Py_None;
  }
  return NULL;
}


static PyMethodDef GeometryObject_methods[] = {
    {"__setstate__", (PyCFunction)GeometryObject_SetState, METH_O,
     "For unpickling pre-shapely 2.0 pickles"},
    {NULL} /* Sentinel */
};

PyTypeObject GeometryType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "shapely.lib.Geometry",
    .tp_doc = "Geometry type",
    .tp_basicsize = sizeof(GeometryObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_dealloc = (destructor)GeometryObject_dealloc,
    .tp_members = GeometryObject_members,
    .tp_methods = GeometryObject_methods,
    .tp_repr = (reprfunc)GeometryObject_repr,
    .tp_hash = (hashfunc)GeometryObject_hash,
    .tp_richcompare = (richcmpfunc)GeometryObject_richcompare,
    .tp_weaklistoffset = offsetof(GeometryObject, weakreflist),
    .tp_str = (reprfunc)GeometryObject_str,
};

/* Check if type `a` is a subclass of type `b`
(copied from cython generated code) */
int __Pyx_InBases(PyTypeObject* a, PyTypeObject* b) {
  while (a) {
    a = a->tp_base;
    if (a == b) return 1;
  }
  return b == &PyBaseObject_Type;
}

/* Get a GEOSGeometry pointer from a GeometryObject, or NULL if the input is
Py_None. Returns 0 on error, 1 on success. */
char get_geom(GeometryObject* obj, GEOSGeometry** out) {
  // Numpy treats NULL the same as Py_None
  if ((obj == NULL) || ((PyObject*)obj == Py_None)) {
    *out = NULL;
    return 1;
  }
  PyTypeObject* type = ((PyObject*)obj)->ob_type;
  if ((type != &GeometryType) && !(__Pyx_InBases(type, &GeometryType))) {
    return 0;
  } else {
    *out = obj->ptr;
    return 1;
  }
}

/* Get a GEOSGeometry AND GEOSPreparedGeometry pointer from a GeometryObject,
or NULL if the input is Py_None. Returns 0 on error, 1 on success. */
char get_geom_with_prepared(GeometryObject* obj, GEOSGeometry** out,
                            GEOSPreparedGeometry** prep) {
  if (!get_geom(obj, out)) {
    // It is not a GeometryObject / None: Error
    return 0;
  }
  if (*out != NULL) {
    // Only if it is not None, fill the prepared geometry
    *prep = obj->ptr_prepared;
  } else {
    *prep = NULL;
  }
  return 1;
}

int init_geom_type(PyObject* m) {
  Py_ssize_t i;
  PyObject* type;
  if (PyType_Ready(&GeometryType) < 0) {
    return -1;
  }

  type = (PyObject*)&GeometryType;
  Py_INCREF(type);
  PyModule_AddObject(m, "Geometry", type);

  geom_registry[0] = PyList_New(8);
  for (i = 0; i < 8; i++) {
    Py_INCREF(type);
    PyList_SET_ITEM(geom_registry[0], i, type);
  }
  PyModule_AddObject(m, "registry", geom_registry[0]);
  return 0;
}
