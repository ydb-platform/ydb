# PROJ API Definition

cdef extern from "proj.h" nogil:
    cdef int PROJ_VERSION_MAJOR
    cdef int PROJ_VERSION_MINOR
    cdef int PROJ_VERSION_PATCH
    void proj_context_set_search_paths(
        PJ_CONTEXT *ctx, int count_paths, const char* const* paths)
    int proj_context_set_database_path(PJ_CONTEXT *ctx,
                                       const char *dbPath,
                                       const char *const *auxDbPaths,
                                       const char* const *options)
    void proj_context_set_ca_bundle_path(PJ_CONTEXT *ctx, const char *path)
    const char *proj_context_get_database_metadata(PJ_CONTEXT* ctx,
                                                   const char* key)
    ctypedef struct PJ
    ctypedef struct PJ_CONTEXT
    PJ_CONTEXT *proj_context_create ()
    PJ_CONTEXT *proj_context_clone (PJ_CONTEXT *ctx)
    PJ_CONTEXT *proj_context_destroy (PJ_CONTEXT *ctx)
    void proj_assign_context(PJ* pj, PJ_CONTEXT* ctx)

    ctypedef enum PJ_LOG_LEVEL:
        PJ_LOG_NONE
        PJ_LOG_ERROR
        PJ_LOG_DEBUG
        PJ_LOG_TRACE
        PJ_LOG_TELL
    ctypedef void (*PJ_LOG_FUNCTION)(void *, int, const char *)
    void proj_log_func (PJ_CONTEXT *ctx, void *app_data, PJ_LOG_FUNCTION logf)

    int proj_errno (const PJ *P)
    const char * proj_context_errno_string(PJ_CONTEXT* ctx, int err)
    int  proj_errno_reset (const PJ *P)
    PJ *proj_create (PJ_CONTEXT *ctx, const char *definition)
    PJ *proj_normalize_for_visualization(PJ_CONTEXT *ctx, const PJ* obj)

    ctypedef struct PJ_INFO:
        int major               # Major release number
        int minor               # Minor release number
        int patch               # Patch level
        const char *release     # Release info. Version + date
        const char *version     # Full version number
        const char *searchpath  # Paths where init and grid files are
                                # looked for. Paths are separated by
                                # semi-colons on Windows, and colons
                                # on non-Windows platforms.
        const char *const *paths
        size_t path_count
    PJ_INFO proj_info()

    ctypedef struct PJ_PROJ_INFO:
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


    ctypedef union PJ_COORD:
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

    ctypedef enum PJ_DIRECTION:
        PJ_FWD        # Forward
        PJ_IDENT      # Do nothing
        PJ_INV        # Inverse

    int proj_angular_input (PJ *P, PJ_DIRECTION dir)
    int proj_angular_output (PJ *P, PJ_DIRECTION dir)
    int proj_degree_input (PJ *P, PJ_DIRECTION dir)
    int proj_degree_output (PJ *P, PJ_DIRECTION dir)

    PJ_COORD proj_trans (PJ *P, PJ_DIRECTION direction, PJ_COORD coord)
    size_t proj_trans_generic (
        PJ *P,
        PJ_DIRECTION direction,
        double *x, size_t sx, size_t nx,
        double *y, size_t sy, size_t ny,
        double *z, size_t sz, size_t nz,
        double *t, size_t st, size_t nt
    )
    int proj_trans_bounds(
        PJ_CONTEXT* context,
        PJ *P,
        PJ_DIRECTION direction,
        const double xmin,
        const double ymin,
        const double xmax,
        const double ymax,
        double* out_xmin,
        double* out_ymin,
        double* out_xmax,
        double* out_ymax,
        int densify_pts
    )
    PJ* proj_trans_get_last_used_operation(PJ *P)
    ctypedef struct PJ_AREA
    PJ *proj_create_crs_to_crs_from_pj(
        PJ_CONTEXT *ctx,
        PJ *source_crs,
        PJ *target_crs,
        PJ_AREA *area,
        const char* const *options,
    )

    ctypedef enum PJ_COMPARISON_CRITERION:
        PJ_COMP_STRICT
        PJ_COMP_EQUIVALENT
        PJ_COMP_EQUIVALENT_EXCEPT_AXIS_ORDER_GEOGCRS

    void proj_destroy(PJ *obj)
    int proj_is_equivalent_to_with_ctx(PJ_CONTEXT *ctx,
                                       const PJ *obj, const PJ *other,
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
    PJ_AREA *proj_area_create()
    void proj_area_set_bbox(PJ_AREA *area,
                            double west_lon_degree,
                            double south_lat_degree,
                            double east_lon_degree,
                            double north_lat_degree)
    void proj_area_destroy(PJ_AREA* area)

    ctypedef enum PJ_WKT_TYPE:
        PJ_WKT2_2015
        PJ_WKT2_2015_SIMPLIFIED
        PJ_WKT2_2019
        PJ_WKT2_2019_SIMPLIFIED
        PJ_WKT1_GDAL
        PJ_WKT1_ESRI

    const char* proj_as_wkt(PJ_CONTEXT *ctx,
                            const PJ *obj,
                            PJ_WKT_TYPE type,
                            const char* const *options)

    ctypedef enum PJ_PROJ_STRING_TYPE:
        PJ_PROJ_5
        PJ_PROJ_4

    const char* proj_as_proj_string(PJ_CONTEXT *ctx,
                                    const PJ *obj,
                                    PJ_PROJ_STRING_TYPE type,
                                    const char* const *options)
    const char* proj_as_projjson(PJ_CONTEXT *ctx,
                                 const PJ *obj,
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
        PJ_TYPE_TEMPORAL_DATUM
        PJ_TYPE_ENGINEERING_DATUM
        PJ_TYPE_PARAMETRIC_DATUM
        PJ_TYPE_DERIVED_PROJECTED_CRS

    PJ_TYPE proj_get_type(const PJ *obj)
    const char* proj_get_name(const PJ *obj)
    const char* proj_get_remarks(const PJ *obj)
    const char* proj_get_scope(const PJ *obj)

    int proj_is_crs(const PJ *obj)
    int proj_is_derived_crs(PJ_CONTEXT *ctx, const PJ* crs)
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
                                      double *out_inv_flattening)
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
    int proj_list_get_count(const PJ_OBJ_LIST *result)
    void proj_list_destroy(PJ_OBJ_LIST *result)
    void proj_int_list_destroy(int* list)
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
    ctypedef char **PROJ_STRING_LIST
    void proj_string_list_destroy(PROJ_STRING_LIST list)
    PROJ_STRING_LIST proj_get_authorities_from_database(PJ_CONTEXT *ctx)
    PROJ_STRING_LIST proj_get_codes_from_database(PJ_CONTEXT *ctx,
                                                  const char *auth_name,
                                                  PJ_TYPE type,
                                                  int allow_deprecated)

    ctypedef struct PROJ_CRS_INFO:
        char* auth_name
        char* code
        char* name
        PJ_TYPE type
        int deprecated
        int bbox_valid
        double west_lon_degree
        double south_lat_degree
        double east_lon_degree
        double north_lat_degree
        char* area_name
        char* projection_method_name

    ctypedef struct PROJ_CRS_LIST_PARAMETERS:
        const PJ_TYPE* types
        size_t typesCount
        int crs_area_of_use_contains_bbox
        int bbox_valid
        double west_lon_degree
        double south_lat_degree
        double east_lon_degree
        double north_lat_degree
        int allow_deprecated

    PROJ_CRS_LIST_PARAMETERS *proj_get_crs_list_parameters_create()

    void proj_get_crs_list_parameters_destroy(PROJ_CRS_LIST_PARAMETERS* params)

    PROJ_CRS_INFO **proj_get_crs_info_list_from_database(
                                        PJ_CONTEXT *ctx,
                                        const char *auth_name,
                                        const PROJ_CRS_LIST_PARAMETERS* params,
                                        int *out_result_count)

    void proj_crs_info_list_destroy(PROJ_CRS_INFO** list)

    PJ *proj_crs_get_coordoperation(PJ_CONTEXT *ctx,
                                    const PJ *crs)

    int proj_coordoperation_get_method_info(PJ_CONTEXT *ctx,
                                            const PJ *coordoperation,
                                            const char **out_method_name,
                                            const char **out_method_auth_name,
                                            const char **out_method_code)

    int proj_coordoperation_is_instantiable(PJ_CONTEXT *ctx,
                                            const PJ *coordoperation)

    int proj_coordoperation_has_ballpark_transformation(PJ_CONTEXT *ctx,
                                                        const PJ *coordoperation)

    int proj_coordoperation_get_param_count(PJ_CONTEXT *ctx,
                                            const PJ *coordoperation)

    int proj_coordoperation_get_param_index(PJ_CONTEXT *ctx,
                                            const PJ *coordoperation,
                                            const char *name)

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
                                      const char **out_unit_category)

    int proj_coordoperation_get_grid_used_count(PJ_CONTEXT *ctx,
                                                        const PJ *coordoperation)

    int proj_coordoperation_get_grid_used(PJ_CONTEXT *ctx,
                                          const PJ *coordoperation,
                                          int index,
                                          const char **out_short_name,
                                          const char **out_full_name,
                                          const char **out_package_name,
                                          const char **out_url,
                                          int *out_direct_download,
                                          int *out_open_license,
                                          int *out_available)

    double proj_coordoperation_get_accuracy(PJ_CONTEXT *ctx,
                                            const PJ *obj)

    int proj_coordoperation_get_towgs84_values(PJ_CONTEXT *ctx,
                                               const PJ *coordoperation,
                                               double *out_values,
                                               int value_count,
                                               int emit_error_if_incompatible)
    int proj_concatoperation_get_step_count(PJ_CONTEXT *ctx,
                                            const PJ *concatoperation)
    PJ *proj_concatoperation_get_step(PJ_CONTEXT *ctx,
                                      const PJ *concatoperation,
                                      int i_step)
    ctypedef enum PJ_CATEGORY:
        PJ_CATEGORY_ELLIPSOID
        PJ_CATEGORY_PRIME_MERIDIAN
        PJ_CATEGORY_DATUM
        PJ_CATEGORY_CRS
        PJ_CATEGORY_COORDINATE_OPERATION
        PJ_CATEGORY_DATUM_ENSEMBLE
    PJ *proj_create_from_database(PJ_CONTEXT *ctx,
                                  const char *auth_name,
                                  const char *code,
                                  PJ_CATEGORY category,
                                  int usePROJAlternativeGridNames,
                                  const char* const *options)
    PJ_OBJ_LIST *proj_create_from_name(PJ_CONTEXT *ctx,
                                       const char *auth_name,
                                       const char *searchedName,
                                       const PJ_TYPE* types,
                                       size_t typesCount,
                                       int approximateMatch,
                                       size_t limitResultCount,
                                       const char* const *options)

    ctypedef struct PJ_OPERATION_FACTORY_CONTEXT

    PJ_OPERATION_FACTORY_CONTEXT *proj_create_operation_factory_context(
        PJ_CONTEXT *ctx,
        const char *authority
    )
    void proj_operation_factory_context_destroy(
        PJ_OPERATION_FACTORY_CONTEXT *ctx
    )
    PJ_OBJ_LIST *proj_create_operations(
        PJ_CONTEXT *ctx,
        const PJ *source_crs,
        const PJ *target_crs,
        const PJ_OPERATION_FACTORY_CONTEXT *operationContext
    )
    void proj_operation_factory_context_set_grid_availability_use(
        PJ_CONTEXT *ctx,
        PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
        PROJ_GRID_AVAILABILITY_USE use
    )
    void proj_operation_factory_context_set_spatial_criterion(
        PJ_CONTEXT *ctx,
        PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
        PROJ_SPATIAL_CRITERION criterion
    )
    void proj_operation_factory_context_set_area_of_interest(
        PJ_CONTEXT *ctx,
        PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
        double west_lon_degree,
        double south_lat_degree,
        double east_lon_degree,
        double north_lat_degree
    )
    void proj_operation_factory_context_set_allow_ballpark_transformations(
        PJ_CONTEXT *ctx,
        PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
        int allow
    )
    void proj_operation_factory_context_set_discard_superseded(
        PJ_CONTEXT *ctx,
        PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
        int discard
    )
    void proj_operation_factory_context_set_desired_accuracy(
        PJ_CONTEXT *ctx,
        PJ_OPERATION_FACTORY_CONTEXT *factory_ctx,
        double accuracy
    )
    ctypedef enum PROJ_SPATIAL_CRITERION:
        PROJ_SPATIAL_CRITERION_STRICT_CONTAINMENT
        PROJ_SPATIAL_CRITERION_PARTIAL_INTERSECTION

    ctypedef enum PROJ_GRID_AVAILABILITY_USE:
        PROJ_GRID_AVAILABILITY_USED_FOR_SORTING
        PROJ_GRID_AVAILABILITY_DISCARD_OPERATION_IF_MISSING_GRID
        PROJ_GRID_AVAILABILITY_IGNORED
        PROJ_GRID_AVAILABILITY_KNOWN_AVAILABLE

    ctypedef struct PJ_FACTORS:
        double meridional_scale
        double parallel_scale
        double areal_scale
        double angular_distortion
        double meridian_parallel_angle
        double meridian_convergence
        double tissot_semimajor
        double tissot_semiminor
        double dx_dlam
        double dx_dphi
        double dy_dlam
        double dy_dphi

    PJ_FACTORS proj_factors(PJ *P, PJ_COORD lp)
    # neworking related
    const char *proj_context_get_user_writable_directory(PJ_CONTEXT *ctx, int create)
    int proj_context_set_enable_network(PJ_CONTEXT* ctx, int enabled)
    int proj_context_is_network_enabled(PJ_CONTEXT* ctx)
    # units
    ctypedef struct PROJ_UNIT_INFO:
        # Authority name.
        char* auth_name
        # Object code.
        char* code
        # Object name. For example "metre", "US survey foot", etc. */
        char* name
        # Category of the unit: one of "linear", "linear_per_time", "angular",
        # "angular_per_time", "scale", "scale_per_time" or "time" */
        char* category
        # Conversion factor to apply to transform from that unit to the
        # corresponding SI unit (metre for "linear", radian for "angular", etc.).
        # It might be 0 in some cases to indicate no known conversion factor.
        double conv_factor
        # PROJ short name, like "m", "ft", "us-ft", etc... Might be NULL */
        char* proj_short_name
        # Whether the object is deprecated
        int deprecated

    PROJ_UNIT_INFO **proj_get_units_from_database(
        PJ_CONTEXT *ctx,
        const char *auth_name,
        const char *category,
        int allow_deprecated,
        int *out_result_count,
    )
    void proj_unit_list_destroy(PROJ_UNIT_INFO** list)
    const char *proj_context_get_url_endpoint(PJ_CONTEXT* ctx)

    int proj_is_deprecated(const PJ *obj)
    PJ_OBJ_LIST *proj_get_non_deprecated(PJ_CONTEXT *ctx, const PJ *obj)
