/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#include "grib_iterator_factory.h"
#include "accessor/grib_accessor_class_iterator.h"

#if GRIB_PTHREADS
static pthread_once_t once   = PTHREAD_ONCE_INIT;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void init_mutex()
{
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex, &attr);
    pthread_mutexattr_destroy(&attr);
}
#elif GRIB_OMP_THREADS
static int once = 0;
static omp_nest_lock_t mutex;

static void init_mutex()
{
    GRIB_OMP_CRITICAL(lock_grib_iterator_factory_c)
    {
        if (once == 0) {
            omp_init_nest_lock(&mutex);
            once = 1;
        }
    }
}
#endif

struct table_entry
{
    const char* type;
    eccodes::geo_iterator::Iterator** iterator;
};

static const struct table_entry table[] = {
    { "gaussian", &grib_iterator_gaussian, },
    { "gaussian_reduced", &grib_iterator_gaussian_reduced, },
    { "healpix", &grib_iterator_healpix, },
    { "lambert_azimuthal_equal_area", &grib_iterator_lambert_azimuthal_equal_area, },
    { "lambert_conformal", &grib_iterator_lambert_conformal, },
    { "latlon", &grib_iterator_latlon, },
    { "latlon_reduced", &grib_iterator_latlon_reduced, },
    { "mercator", &grib_iterator_mercator, },
    { "polar_stereographic", &grib_iterator_polar_stereographic, },
    { "regular", &grib_iterator_regular, },
    { "space_view", &grib_iterator_space_view, },
    { "unstructured", &grib_iterator_unstructured, },
};

eccodes::geo_iterator::Iterator* grib_iterator_factory(grib_handle* h, grib_arguments* args, unsigned long flags, int* error)
{
    size_t i = 0, num_table_entries = 0;
    const char* type = (char*)args->get_name(h, 0);
    *error = GRIB_NOT_IMPLEMENTED;

    num_table_entries = sizeof(table) / sizeof(table[0]);
    for (i = 0; i < num_table_entries; i++) {
        if (strcmp(type, table[i].type) == 0) {
            eccodes::geo_iterator::Iterator* builder = *(table[i].iterator);
            eccodes::geo_iterator::Iterator* it = builder->create();
            it->flags_              = flags;

            GRIB_MUTEX_INIT_ONCE(&once, &init_mutex);
            GRIB_MUTEX_LOCK(&mutex);
            *error = it->init(h, args);
            GRIB_MUTEX_UNLOCK(&mutex);

            if (*error == GRIB_SUCCESS)
                return it;
            grib_context_log(h->context, GRIB_LOG_ERROR, "Geoiterator factory: Error instantiating iterator %s (%s)",
                             table[i].type, grib_get_error_message(*error));
            gribIteratorDelete(it);
            return NULL;
        }
    }

    grib_context_log(h->context, GRIB_LOG_ERROR, "Geoiterator factory: Unknown type: %s", type);

    return NULL;
}

int grib_get_data(const grib_handle* h, double* lats, double* lons, double* values)
{
    int err             = 0;
    eccodes::geo_iterator::Iterator* iter = NULL;
    double *lat, *lon, *val;

    iter = eccodes::geo_iterator::gribIteratorNew(h, 0, &err);
    if (!iter || err != GRIB_SUCCESS)
        return err;

    lat = lats;
    lon = lons;
    val = values;
    while (iter->next(lat++, lon++, val++)) {}

    gribIteratorDelete(iter);

    return err;
}

/*
 * Return pointer to data at (i,j) (Fortran convention)
 */
static double* pointer_to_data(unsigned int i, unsigned int j,
                               long iScansNegatively, long jScansPositively,
                               long jPointsAreConsecutive, long alternativeRowScanning,
                               unsigned int nx, unsigned int ny, double* data)
{
    /* Regular grid */
    if (nx > 0 && ny > 0) {
        if (i >= nx || j >= ny)
            return NULL;
        j = (jScansPositively) ? j : ny - 1 - j;
        i = ((alternativeRowScanning) && (j % 2 == 1)) ? nx - 1 - i : i;
        i = (iScansNegatively) ? nx - 1 - i : i;

        return (jPointsAreConsecutive) ? data + j + i * ny : data + i + nx * j;
    }

    /* Reduced or other data not on a grid */
    return NULL;
}

/* Apply the scanning mode flags which may require data array to be transformed
 * to standard west-to-east (+i) south-to-north (+j) mode.
 * The data array passed in should have 'numPoints' elements.
*/
int transform_iterator_data(grib_context* context, double* data,
                            long iScansNegatively, long jScansPositively,
                            long jPointsAreConsecutive, long alternativeRowScanning,
                            size_t numPoints, long nx, long ny)
{
    double* data2;
    double *pData0, *pData1, *pData2;
    long ix, iy;

    if (!iScansNegatively && jScansPositively && !jPointsAreConsecutive && !alternativeRowScanning) {
        /* Already +i and +j. No need to change */
        return GRIB_SUCCESS;
    }
    if (!data) return GRIB_SUCCESS;

    if (!context) context = grib_context_get_default();

    if (!iScansNegatively && !jScansPositively && !jPointsAreConsecutive && !alternativeRowScanning &&
        nx > 0 && ny > 0) {
        /* Regular grid +i -j: convert from we:ns to we:sn */
        size_t row_size = ((size_t)nx) * sizeof(double);
        data2           = (double*)grib_context_malloc(context, row_size);
        if (!data2) {
            grib_context_log(context, GRIB_LOG_ERROR, "Geoiterator data: Error allocating %ld bytes", row_size);
            return GRIB_OUT_OF_MEMORY;
        }
        for (iy = 0; iy < ny / 2; iy++) {
            memcpy(data2, data + ((size_t)iy) * nx, row_size);
            memcpy(data + iy * nx, data + (ny - 1 - iy) * ((size_t)nx), row_size);
            memcpy(data + (ny - 1 - iy) * ((size_t)nx), data2, row_size);
        }
        grib_context_free(context, data2);
        return GRIB_SUCCESS;
    }

    if (nx < 1 || ny < 1) {
        grib_context_log(context, GRIB_LOG_ERROR, "Geoiterator data: Invalid values for Nx and/or Ny");
        return GRIB_GEOCALCULUS_PROBLEM;
    }
    data2 = (double*)grib_context_malloc(context, numPoints * sizeof(double));
    if (!data2) {
        grib_context_log(context, GRIB_LOG_ERROR, "Geoiterator data: Error allocating %ld bytes", numPoints * sizeof(double));
        return GRIB_OUT_OF_MEMORY;
    }
    pData0 = data2;
    for (iy = 0; iy < ny; iy++) {
        long deltaX = 0;
        pData1 = pointer_to_data(0, iy, iScansNegatively, jScansPositively, jPointsAreConsecutive, alternativeRowScanning, nx, ny, data);
        if (!pData1) {
            grib_context_free(context, data2);
            return GRIB_GEOCALCULUS_PROBLEM;
        }
        pData2 = pointer_to_data(1, iy, iScansNegatively, jScansPositively, jPointsAreConsecutive, alternativeRowScanning, nx, ny, data);
        if (!pData2) {
            grib_context_free(context, data2);
            return GRIB_GEOCALCULUS_PROBLEM;
        }
        deltaX = pData2 - pData1;
        for (ix = 0; ix < nx; ix++) {
            *pData0++ = *pData1;
            pData1 += deltaX;
        }
    }
    memcpy(data, data2, ((size_t)numPoints) * sizeof(double));
    grib_context_free(context, data2);

    return GRIB_SUCCESS;
}
