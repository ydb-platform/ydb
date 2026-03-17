# PROJ.4 API Defnition
cdef extern from "proj.h":
    cdef enum:
        PROJ_VERSION_MAJOR
        PROJ_VERSION_MINOR
        PROJ_VERSION_PATCH
    void proj_context_set_search_paths(PJ_CONTEXT *ctx, int count_paths, const char* const* paths);

    cdef struct PJ_INFO:
        int         major
        int         minor
        int         patch
        const char  *release
        const char  *version
        const char  *searchpath
        const char * const *paths
        size_t path_count

    PJ_INFO proj_info();

    # projCtx has been replaced by PJ_CONTEXT *.
    # projPJ  has been replaced by PJ *
    ctypedef struct PJ
    ctypedef struct PJ_CONTEXT
    PJ_CONTEXT *proj_context_create ()
    PJ_CONTEXT *proj_context_destroy (PJ_CONTEXT *ctx)

    ctypedef enum PJ_LOG_LEVEL:
        PJ_LOG_NONE  = 0
        PJ_LOG_ERROR = 1
        PJ_LOG_DEBUG = 2
        PJ_LOG_TRACE = 3
        PJ_LOG_TELL  = 4
    ctypedef void (*PJ_LOG_FUNCTION)(void *, int, const char *)
    void proj_log_func (PJ_CONTEXT *ctx, void *app_data, PJ_LOG_FUNCTION logf)

    int  proj_errno (const PJ *P)
    int proj_context_errno (PJ_CONTEXT *ctx)
    const char * proj_errno_string (int err)
    PJ *proj_create (PJ_CONTEXT *ctx, const char *definition)
    PJ *proj_normalize_for_visualization(PJ_CONTEXT *ctx, const PJ* obj)

    cdef struct PJ_PROJ_INFO:
        const char  *id
        const char  *description
        const char  *definition
        int         has_inverse #1 if an inverse mapping exists, 0 otherwise              */
        double      accuracy

    PJ_PROJ_INFO proj_pj_info(PJ *P)

    ctypedef struct PJ_XYZT:
        double   x,   y,  z, t
    ctypedef struct PJ_UVWT:
        double   u,   v,  w, t
    ctypedef struct PJ_LPZT:
        double lam, phi,  z, t
    ctypedef struct PJ_OPK:
        double o, p, k
    ctypedef struct PJ_ENU:
        double e, n, u
    ctypedef struct PJ_GEOD:
        double s, a1, a2

    ctypedef struct PJ_UV:
        double   u,   v
    ctypedef struct PJ_XY:
        double   x,   y
    ctypedef struct PJ_LP:
        double lam, phi

    ctypedef struct PJ_XYZ:
        double   x,   y,  z
    ctypedef struct PJ_UVW:
        double   u,   v,  w
    ctypedef struct PJ_LPZ:
        double lam, phi,  z


    cdef union PJ_COORD:
        double v[4];
        PJ_XYZT xyzt;
        PJ_UVWT uvwt;
        PJ_LPZT lpzt;
        PJ_GEOD geod;
        PJ_OPK opk;
        PJ_ENU enu;
        PJ_XYZ xyz;
        PJ_UVW uvw;
        PJ_LPZ lpz;
        PJ_XY xy;
        PJ_UV uv;
        PJ_LP lp;

    PJ_COORD proj_coord (double x, double y, double z, double t)

    cdef enum PJ_DIRECTION:
        PJ_FWD   =  1 # Forward
        PJ_IDENT =  0 # Do nothing
        PJ_INV   = -1 # Inverse

    int proj_angular_input (PJ *P, PJ_DIRECTION dir)
    int proj_angular_output (PJ *P, PJ_DIRECTION dir)
    PJ_COORD proj_trans (PJ *P, PJ_DIRECTION direction, PJ_COORD coord)
    size_t proj_trans_generic (
        PJ *P,
        PJ_DIRECTION direction,
        double *x, size_t sx, size_t nx,
        double *y, size_t sy, size_t ny,
        double *z, size_t sz, size_t nz,
        double *t, size_t st, size_t nt
    );
    ctypedef struct PJ_AREA
    PJ *proj_create_crs_to_crs(PJ_CONTEXT *ctx, const char *source_crs, const char *target_crs, PJ_AREA *area);

    cdef enum PJ_COMPARISON_CRITERION:
        PJ_COMP_STRICT
        PJ_COMP_EQUIVALENT
        PJ_COMP_EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS

    void proj_destroy(PJ *obj)
    int proj_is_equivalent_to(const PJ *obj, const PJ* other,
                              PJ_COMPARISON_CRITERION criterion)

    const char* proj_get_id_auth_name(const PJ *obj, int index)
    const char* proj_get_id_code(const PJ *obj, int index)
    int proj_get_area_of_use(PJ_CONTEXT *ctx,
                             const PJ *obj,
                             double* out_west_lon_degree,
                             double* out_south_lat_degree,
                             double* out_east_lon_degree,
                             double* out_north_lat_degree,
                             const char **out_area_name)

    ctypedef enum PJ_WKT_TYPE:
        PJ_WKT2_2015
        PJ_WKT2_2015_SIMPLIFIED
        PJ_WKT2_2018
        PJ_WKT2_2018_SIMPLIFIED
        PJ_WKT1_GDAL
        PJ_WKT1_ESRI

    const char* proj_as_wkt(PJ_CONTEXT *ctx,
                            const PJ *obj, PJ_WKT_TYPE type,
                            const char* const *options)

    ctypedef enum PJ_PROJ_STRING_TYPE:
        PJ_PROJ_5
        PJ_PROJ_4

    const char* proj_as_proj_string(PJ_CONTEXT *ctx,
                                    const PJ *obj,
                                    PJ_PROJ_STRING_TYPE type,
                                    const char* const *options)
    PJ *proj_crs_get_geodetic_crs(PJ_CONTEXT *ctx, const PJ *crs)

    ctypedef enum PJ_TYPE:
        PJ_TYPE_UNKNOWN
        PJ_TYPE_ELLIPSOID
        PJ_TYPE_PRIME_MERIDIAN
        PJ_TYPE_GEODETIC_REFERENCE_FRAME
        PJ_TYPE_DYNAMIC_GEODETIC_REFERENCE_FRAME
        PJ_TYPE_VERTICAL_REFERENCE_FRAME
        PJ_TYPE_DYNAMIC_VERTICAL_REFERENCE_FRAME
        PJ_TYPE_DATUM_ENSEMBLE
        PJ_TYPE_CRS
        PJ_TYPE_GEODETIC_CRS
        PJ_TYPE_GEOCENTRIC_CRS
        PJ_TYPE_GEOGRAPHIC_CRS
        PJ_TYPE_GEOGRAPHIC_2D_CRS
        PJ_TYPE_GEOGRAPHIC_3D_CRS
        PJ_TYPE_VERTICAL_CRS
        PJ_TYPE_PROJECTED_CRS
        PJ_TYPE_COMPOUND_CRS
        PJ_TYPE_TEMPORAL_CRS
        PJ_TYPE_ENGINEERING_CRS
        PJ_TYPE_BOUND_CRS
        PJ_TYPE_OTHER_CRS
        PJ_TYPE_CONVERSION
        PJ_TYPE_TRANSFORMATION
        PJ_TYPE_CONCATENATED_OPERATION
        PJ_TYPE_OTHER_COORDINATE_OPERATION

    PJ_TYPE proj_get_type(const PJ *obj)
    const char * proj_get_name(const PJ *obj)

    int proj_is_crs(const PJ *obj)
    PJ *proj_crs_get_datum(PJ_CONTEXT *ctx, const PJ *crs)
    PJ *proj_crs_get_horizontal_datum(PJ_CONTEXT *ctx, const PJ *crs)

    ctypedef enum PJ_COORDINATE_SYSTEM_TYPE:
        PJ_CS_TYPE_UNKNOWN
        PJ_CS_TYPE_CARTESIAN
        PJ_CS_TYPE_ELLIPSOIDAL
        PJ_CS_TYPE_VERTICAL
        PJ_CS_TYPE_SPHERICAL
        PJ_CS_TYPE_ORDINAL
        PJ_CS_TYPE_PARAMETRIC
        PJ_CS_TYPE_DATETIMETEMPORAL
        PJ_CS_TYPE_TEMPORALCOUNT
        PJ_CS_TYPE_TEMPORALMEASURE

    PJ *proj_crs_get_coordinate_system(PJ_CONTEXT *ctx, const PJ *crs)
    PJ_COORDINATE_SYSTEM_TYPE proj_cs_get_type(PJ_CONTEXT *ctx,
                                               const PJ *cs)
    int proj_cs_get_axis_count(PJ_CONTEXT *ctx,
                               const PJ *cs)
    int proj_cs_get_axis_info(PJ_CONTEXT *ctx,
                              const PJ *cs, int index,
                              const char **out_name,
                              const char **out_abbrev,
                              const char **out_direction,
                              double *out_unit_conv_factor,
                              const char **out_unit_name,
                              const char **out_unit_auth_name,
                              const char **out_unit_code)

    PJ *proj_get_ellipsoid(PJ_CONTEXT *ctx, const PJ *obj)
    int proj_ellipsoid_get_parameters(PJ_CONTEXT *ctx,
                                      const PJ *ellipsoid,
                                      double *out_semi_major_metre,
                                      double *out_semi_minor_metre,
                                      int    *out_is_semi_minor_computed,
                                      double *out_inv_flattening);
    PJ *proj_get_prime_meridian(PJ_CONTEXT *ctx, const PJ *obj)
    int proj_prime_meridian_get_parameters(PJ_CONTEXT *ctx,
                                           const PJ *prime_meridian,
                                           double *out_longitude,
                                           double *out_unit_conv_factor,
                                           const char **out_unit_name)
    PJ *proj_crs_get_sub_crs(PJ_CONTEXT *ctx, const PJ *crs, int index)
    PJ *proj_get_source_crs(PJ_CONTEXT *ctx, const PJ *obj)
    PJ *proj_get_target_crs(PJ_CONTEXT *ctx, const PJ *obj)

    ctypedef struct PJ_OBJ_LIST
    PJ_OBJ_LIST *proj_identify(PJ_CONTEXT *ctx,
                               const PJ* obj,
                               const char *auth_name,
                               const char* const *options,
                               int **out_confidence)
    PJ *proj_list_get(PJ_CONTEXT *ctx,
                      const PJ_OBJ_LIST *result,
                      int index)
    int proj_list_get_count(const PJ_OBJ_LIST *result);
    void proj_list_destroy(PJ_OBJ_LIST *result)
    void proj_int_list_destroy(int* list);
    void proj_context_use_proj4_init_rules(PJ_CONTEXT *ctx, int enable)
    ctypedef enum PJ_GUESSED_WKT_DIALECT:
        PJ_GUESSED_WKT2_2018
        PJ_GUESSED_WKT2_2015
        PJ_GUESSED_WKT1_GDAL
        PJ_GUESSED_WKT1_ESRI
        PJ_GUESSED_NOT_WKT

    PJ_GUESSED_WKT_DIALECT proj_context_guess_wkt_dialect(PJ_CONTEXT *ctx,
                                                          const char *wkt)

    ctypedef struct PJ_OPERATIONS:
        const char  *id
        PJ          *(*proj)(PJ *)
        const char  * const *descr

    const PJ_OPERATIONS *proj_list_operations()

    ctypedef struct PJ_UNITS:
        const char  *id
        const char  *to_meter
        const char  *name
        double      factor
    const PJ_UNITS *proj_list_units()
    const PJ_UNITS *proj_list_angular_units()

    ctypedef struct PJ_ELLPS:
        const char  *id   # ellipse keyword name
        const char  *major  # a= value
        const char  *ell  # elliptical parameter
        const char  *name  # comments
    const PJ_ELLPS *proj_list_ellps()

    ctypedef struct PJ_PRIME_MERIDIANS:
        const char  *id
        const char  *defn
    const PJ_PRIME_MERIDIANS *proj_list_prime_meridians()

    PJ *proj_crs_get_coordoperation(PJ_CONTEXT *ctx,
                                    const PJ *crs);

    int proj_coordoperation_get_method_info(PJ_CONTEXT *ctx,
                                            const PJ *coordoperation,
                                            const char **out_method_name,
                                            const char **out_method_auth_name,
                                            const char **out_method_code);

    int proj_coordoperation_is_instantiable(PJ_CONTEXT *ctx,
                                            const PJ *coordoperation);

    int proj_coordoperation_has_ballpark_transformation(PJ_CONTEXT *ctx,
                                                        const PJ *coordoperation);

    int proj_coordoperation_get_param_count(PJ_CONTEXT *ctx,
                                            const PJ *coordoperation);

    int proj_coordoperation_get_param_index(PJ_CONTEXT *ctx,
                                            const PJ *coordoperation,
                                            const char *name);

    int proj_coordoperation_get_param(PJ_CONTEXT *ctx,
                                      const PJ *coordoperation,
                                      int index,
                                      const char **out_name,
                                      const char **out_auth_name,
                                      const char **out_code,
                                      double *out_value,
                                      const char **out_value_string,
                                      double *out_unit_conv_factor,
                                      const char **out_unit_name,
                                      const char **out_unit_auth_name,
                                      const char **out_unit_code,
                                      const char **out_unit_category);

    int proj_coordoperation_get_grid_used_count(PJ_CONTEXT *ctx,
                                                        const PJ *coordoperation);

    int proj_coordoperation_get_grid_used(PJ_CONTEXT *ctx,
                                          const PJ *coordoperation,
                                          int index,
                                          const char **out_short_name,
                                          const char **out_full_name,
                                          const char **out_package_name,
                                          const char **out_url,
                                          int *out_direct_download,
                                          int *out_open_license,
                                          int *out_available);

    double proj_coordoperation_get_accuracy(PJ_CONTEXT *ctx,
                                            const PJ *obj);

    int proj_coordoperation_get_towgs84_values(PJ_CONTEXT *ctx,
                                               const PJ *coordoperation,
                                               double *out_values,
                                               int value_count,
                                               int emit_error_if_incompatible)

    ctypedef enum PJ_CATEGORY:
        PJ_CATEGORY_ELLIPSOID,
        PJ_CATEGORY_PRIME_MERIDIAN,
        PJ_CATEGORY_DATUM,
        PJ_CATEGORY_CRS,
        PJ_CATEGORY_COORDINATE_OPERATION
    PJ *proj_create_from_database(PJ_CONTEXT *ctx,
                                  const char *auth_name,
                                  const char *code,
                                  PJ_CATEGORY category,
                                  int usePROJAlternativeGridNames,
                                  const char* const *options)
