/******************************************************************************
 *
 * Project:  PROJ
 * Purpose:  Constants
 * Author:   Even Rouault <even dot rouault at spatialys dot com>
 *
 ******************************************************************************
 * Copyright (c) 2018, Even Rouault <even dot rouault at spatialys dot com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 ****************************************************************************/

#ifndef PROJ_CONSTANTS_INCLUDED
#define PROJ_CONSTANTS_INCLUDED

/* Projection methods */

#define EPSG_NAME_METHOD_TRANSVERSE_MERCATOR "Transverse Mercator"
#define EPSG_CODE_METHOD_TRANSVERSE_MERCATOR 9807

#define EPSG_NAME_METHOD_TRANSVERSE_MERCATOR_3D "Transverse Mercator (3D)"
#define EPSG_CODE_METHOD_TRANSVERSE_MERCATOR_3D 1111

#define EPSG_NAME_METHOD_TRANSVERSE_MERCATOR_SOUTH_ORIENTATED                  \
    "Transverse Mercator (South Orientated)"
#define EPSG_CODE_METHOD_TRANSVERSE_MERCATOR_SOUTH_ORIENTATED 9808

#define PROJ_WKT2_NAME_METHOD_TWO_POINT_EQUIDISTANT "Two Point Equidistant"

#define EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_1SP                           \
    "Lambert Conic Conformal (1SP)"
#define EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP 9801

#define EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_1SP_VARIANT_B                 \
    "Lambert Conic Conformal (1SP variant B)"
#define EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_1SP_VARIANT_B 1102

#define EPSG_NAME_METHOD_NZMG "New Zealand Map Grid"
#define EPSG_CODE_METHOD_NZMG 9811

/* Deprecated because of wrong name. Use EPSG_xxx_METHOD_TUNISIA_MINING_GRID
 * instead */
#define EPSG_NAME_METHOD_TUNISIA_MAPPING_GRID "Tunisia Mapping Grid"
#define EPSG_CODE_METHOD_TUNISIA_MAPPING_GRID 9816

#define EPSG_NAME_METHOD_TUNISIA_MINING_GRID "Tunisia Mining Grid"
#define EPSG_CODE_METHOD_TUNISIA_MINING_GRID 9816

#define EPSG_NAME_METHOD_ALBERS_EQUAL_AREA "Albers Equal Area"
#define EPSG_CODE_METHOD_ALBERS_EQUAL_AREA 9822

#define EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_2SP                           \
    "Lambert Conic Conformal (2SP)"
#define EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP 9802

#define EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_2SP_BELGIUM                   \
    "Lambert Conic Conformal (2SP Belgium)"
#define EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP_BELGIUM 9803

#define EPSG_NAME_METHOD_LAMBERT_CONIC_CONFORMAL_2SP_MICHIGAN                  \
    "Lambert Conic Conformal (2SP Michigan)"
#define EPSG_CODE_METHOD_LAMBERT_CONIC_CONFORMAL_2SP_MICHIGAN 1051

#define EPSG_NAME_METHOD_AZIMUTHAL_EQUIDISTANT "Azimuthal Equidistant"
#define EPSG_CODE_METHOD_AZIMUTHAL_EQUIDISTANT 1125

#define EPSG_NAME_METHOD_MODIFIED_AZIMUTHAL_EQUIDISTANT                        \
    "Modified Azimuthal Equidistant"
#define EPSG_CODE_METHOD_MODIFIED_AZIMUTHAL_EQUIDISTANT 9832

#define EPSG_NAME_METHOD_GUAM_PROJECTION "Guam Projection"
#define EPSG_CODE_METHOD_GUAM_PROJECTION 9831

#define EPSG_NAME_METHOD_BONNE "Bonne"
#define EPSG_CODE_METHOD_BONNE 9827

#define PROJ_WKT2_NAME_METHOD_COMPACT_MILLER "Compact Miller"

#define EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL              \
    "Lambert Cylindrical Equal Area (Spherical)"
#define EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA_SPHERICAL 9834

#define EPSG_NAME_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA                        \
    "Lambert Cylindrical Equal Area"
#define EPSG_CODE_METHOD_LAMBERT_CYLINDRICAL_EQUAL_AREA 9835

#define EPSG_NAME_METHOD_CASSINI_SOLDNER "Cassini-Soldner"
#define EPSG_CODE_METHOD_CASSINI_SOLDNER 9806

#define EPSG_NAME_METHOD_HYPERBOLIC_CASSINI_SOLDNER "Hyperbolic Cassini-Soldner"
#define EPSG_CODE_METHOD_HYPERBOLIC_CASSINI_SOLDNER 9833

#define PROJ_WKT2_NAME_METHOD_EQUIDISTANT_CONIC "Equidistant Conic"

#define EPSG_NAME_METHOD_EQUIDISTANT_CONIC "Equidistant Conic"
#define EPSG_CODE_METHOD_EQUIDISTANT_CONIC 1119

#define PROJ_WKT2_NAME_METHOD_ECKERT_I "Eckert I"

#define PROJ_WKT2_NAME_METHOD_ECKERT_II "Eckert II"

#define PROJ_WKT2_NAME_METHOD_ECKERT_III "Eckert III"

#define PROJ_WKT2_NAME_METHOD_ECKERT_IV "Eckert IV"

#define PROJ_WKT2_NAME_METHOD_ECKERT_V "Eckert V"

#define PROJ_WKT2_NAME_METHOD_ECKERT_VI "Eckert VI"

#define EPSG_NAME_METHOD_EQUIDISTANT_CYLINDRICAL "Equidistant Cylindrical"
#define EPSG_CODE_METHOD_EQUIDISTANT_CYLINDRICAL 1028

#define EPSG_NAME_METHOD_EQUIDISTANT_CYLINDRICAL_SPHERICAL                     \
    "Equidistant Cylindrical (Spherical)"
#define EPSG_CODE_METHOD_EQUIDISTANT_CYLINDRICAL_SPHERICAL 1029

#define PROJ_WKT2_NAME_METHOD_FLAT_POLAR_QUARTIC "Flat Polar Quartic"

#define PROJ_WKT2_NAME_METHOD_GALL_STEREOGRAPHIC "Gall Stereographic"

#define PROJ_WKT2_NAME_METHOD_GOODE_HOMOLOSINE "Goode Homolosine"

#define PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE                     \
    "Interrupted Goode Homolosine"

#define PROJ_WKT2_NAME_METHOD_INTERRUPTED_GOODE_HOMOLOSINE_OCEAN               \
    "Interrupted Goode Homolosine Ocean"

#define PROJ_WKT2_NAME_METHOD_GEOSTATIONARY_SATELLITE_SWEEP_X                  \
    "Geostationary Satellite (Sweep X)"

#define PROJ_WKT2_NAME_METHOD_GEOSTATIONARY_SATELLITE_SWEEP_Y                  \
    "Geostationary Satellite (Sweep Y)"

#define PROJ_WKT2_NAME_METHOD_GAUSS_SCHREIBER_TRANSVERSE_MERCATOR              \
    "Gauss Schreiber Transverse Mercator"

#define PROJ_WKT2_NAME_METHOD_GNOMONIC "Gnomonic"

#define EPSG_NAME_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_A                     \
    "Hotine Oblique Mercator (variant A)"
#define EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_A 9812

#define EPSG_NAME_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_B                     \
    "Hotine Oblique Mercator (variant B)"
#define EPSG_CODE_METHOD_HOTINE_OBLIQUE_MERCATOR_VARIANT_B 9815

#define PROJ_WKT2_NAME_METHOD_HOTINE_OBLIQUE_MERCATOR_TWO_POINT_NATURAL_ORIGIN \
    "Hotine Oblique Mercator Two Point Natural Origin"

#define PROJ_WKT2_NAME_INTERNATIONAL_MAP_WORLD_POLYCONIC                       \
    "International Map of the World Polyconic"

#define EPSG_NAME_METHOD_KROVAK_NORTH_ORIENTED "Krovak (North Orientated)"
#define EPSG_CODE_METHOD_KROVAK_NORTH_ORIENTED 1041

#define EPSG_NAME_METHOD_KROVAK "Krovak"
#define EPSG_CODE_METHOD_KROVAK 9819

#define EPSG_NAME_METHOD_KROVAK_MODIFIED "Krovak Modified"
#define EPSG_CODE_METHOD_KROVAK_MODIFIED 1042

#define EPSG_NAME_METHOD_KROVAK_MODIFIED_NORTH_ORIENTED                        \
    "Krovak Modified (North Orientated)"
#define EPSG_CODE_METHOD_KROVAK_MODIFIED_NORTH_ORIENTED 1043

#define EPSG_NAME_METHOD_LAMBERT_AZIMUTHAL_EQUAL_AREA                          \
    "Lambert Azimuthal Equal Area"
#define EPSG_CODE_METHOD_LAMBERT_AZIMUTHAL_EQUAL_AREA 9820

#define EPSG_NAME_METHOD_LAMBERT_AZIMUTHAL_EQUAL_AREA_SPHERICAL                \
    "Lambert Azimuthal Equal Area (Spherical)"
#define EPSG_CODE_METHOD_LAMBERT_AZIMUTHAL_EQUAL_AREA_SPHERICAL 1027

#define PROJ_WKT2_NAME_METHOD_MILLER_CYLINDRICAL "Miller Cylindrical"

#define EPSG_CODE_METHOD_MERCATOR_VARIANT_A 9804
#define EPSG_NAME_METHOD_MERCATOR_VARIANT_A "Mercator (variant A)"

#define EPSG_CODE_METHOD_MERCATOR_VARIANT_B 9805
#define EPSG_NAME_METHOD_MERCATOR_VARIANT_B "Mercator (variant B)"

#define EPSG_NAME_METHOD_POPULAR_VISUALISATION_PSEUDO_MERCATOR                 \
    "Popular Visualisation Pseudo Mercator"
#define EPSG_CODE_METHOD_POPULAR_VISUALISATION_PSEUDO_MERCATOR 1024

#define EPSG_NAME_METHOD_MERCATOR_SPHERICAL "Mercator (Spherical)"
#define EPSG_CODE_METHOD_MERCATOR_SPHERICAL 1026

#define PROJ_WKT2_NAME_METHOD_MOLLWEIDE "Mollweide"

#define PROJ_WKT2_NAME_METHOD_NATURAL_EARTH "Natural Earth"
#define PROJ_WKT2_NAME_METHOD_NATURAL_EARTH_II "Natural Earth II"

#define EPSG_NAME_METHOD_OBLIQUE_STEREOGRAPHIC "Oblique Stereographic"
#define EPSG_CODE_METHOD_OBLIQUE_STEREOGRAPHIC 9809

#define EPSG_NAME_METHOD_ORTHOGRAPHIC "Orthographic"
#define EPSG_CODE_METHOD_ORTHOGRAPHIC 9840

#define EPSG_NAME_METHOD_LOCAL_ORTHOGRAPHIC "Local Orthographic"
#define EPSG_CODE_METHOD_LOCAL_ORTHOGRAPHIC 1130

#define PROJ_WKT2_NAME_ORTHOGRAPHIC_SPHERICAL "Orthographic (Spherical)"

#define PROJ_WKT2_NAME_METHOD_PATTERSON "Patterson"

#define EPSG_NAME_METHOD_AMERICAN_POLYCONIC "American Polyconic"
#define EPSG_CODE_METHOD_AMERICAN_POLYCONIC 9818

#define EPSG_NAME_METHOD_POLAR_STEREOGRAPHIC_VARIANT_A                         \
    "Polar Stereographic (variant A)"
#define EPSG_CODE_METHOD_POLAR_STEREOGRAPHIC_VARIANT_A 9810

#define EPSG_NAME_METHOD_POLAR_STEREOGRAPHIC_VARIANT_B                         \
    "Polar Stereographic (variant B)"
#define EPSG_CODE_METHOD_POLAR_STEREOGRAPHIC_VARIANT_B 9829

#define PROJ_WKT2_NAME_METHOD_ROBINSON "Robinson"

#define PROJ_WKT2_NAME_METHOD_SINUSOIDAL "Sinusoidal"

#define PROJ_WKT2_NAME_METHOD_STEREOGRAPHIC "Stereographic"

#define PROJ_WKT2_NAME_METHOD_TIMES "Times"

#define PROJ_WKT2_NAME_METHOD_VAN_DER_GRINTEN "Van Der Grinten"

#define PROJ_WKT2_NAME_METHOD_WAGNER_I "Wagner I"
#define PROJ_WKT2_NAME_METHOD_WAGNER_II "Wagner II"
#define PROJ_WKT2_NAME_METHOD_WAGNER_III "Wagner III"
#define PROJ_WKT2_NAME_METHOD_WAGNER_IV "Wagner IV"
#define PROJ_WKT2_NAME_METHOD_WAGNER_V "Wagner V"
#define PROJ_WKT2_NAME_METHOD_WAGNER_VI "Wagner VI"
#define PROJ_WKT2_NAME_METHOD_WAGNER_VII "Wagner VII"

#define PROJ_WKT2_NAME_METHOD_QUADRILATERALIZED_SPHERICAL_CUBE                 \
    "Quadrilateralized Spherical Cube"

#define PROJ_WKT2_NAME_METHOD_S2 "S2"

#define PROJ_WKT2_NAME_METHOD_SPHERICAL_CROSS_TRACK_HEIGHT                     \
    "Spherical Cross-Track Height"

#define EPSG_NAME_METHOD_EQUAL_EARTH "Equal Earth"
#define EPSG_CODE_METHOD_EQUAL_EARTH 1078

#define EPSG_NAME_METHOD_LABORDE_OBLIQUE_MERCATOR "Laborde Oblique Mercator"
#define EPSG_CODE_METHOD_LABORDE_OBLIQUE_MERCATOR 9813

#define EPSG_NAME_METHOD_VERTICAL_PERSPECTIVE "Vertical Perspective"
#define EPSG_CODE_METHOD_VERTICAL_PERSPECTIVE 9838

#define PROJ_WKT2_NAME_METHOD_POLE_ROTATION_GRIB_CONVENTION                    \
    "Pole rotation (GRIB convention)"

#define PROJ_WKT2_NAME_METHOD_POLE_ROTATION_NETCDF_CF_CONVENTION               \
    "Pole rotation (netCDF CF convention)"

#define EPSG_CODE_METHOD_COLOMBIA_URBAN 1052
#define EPSG_NAME_METHOD_COLOMBIA_URBAN "Colombia Urban"

#define PROJ_WKT2_NAME_METHOD_PEIRCE_QUINCUNCIAL_SQUARE                        \
    "Peirce Quincuncial (Square)"
#define PROJ_WKT2_NAME_METHOD_PEIRCE_QUINCUNCIAL_DIAMOND                       \
    "Peirce Quincuncial (Diamond)"

/* ------------------------------------------------------------------------ */

/* Projection parameters */

#define EPSG_NAME_PARAMETER_COLATITUDE_CONE_AXIS "Co-latitude of cone axis"
#define EPSG_CODE_PARAMETER_COLATITUDE_CONE_AXIS 1036

#define EPSG_NAME_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN                         \
    "Latitude of natural origin"
#define EPSG_CODE_PARAMETER_LATITUDE_OF_NATURAL_ORIGIN 8801

#define EPSG_NAME_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN                        \
    "Longitude of natural origin"
#define EPSG_CODE_PARAMETER_LONGITUDE_OF_NATURAL_ORIGIN 8802

#define EPSG_NAME_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN                     \
    "Scale factor at natural origin"
#define EPSG_CODE_PARAMETER_SCALE_FACTOR_AT_NATURAL_ORIGIN 8805

#define EPSG_NAME_PARAMETER_FALSE_EASTING "False easting"
#define EPSG_CODE_PARAMETER_FALSE_EASTING 8806

#define EPSG_NAME_PARAMETER_FALSE_NORTHING "False northing"
#define EPSG_CODE_PARAMETER_FALSE_NORTHING 8807

#define EPSG_NAME_PARAMETER_LATITUDE_PROJECTION_CENTRE                         \
    "Latitude of projection centre"
#define EPSG_CODE_PARAMETER_LATITUDE_PROJECTION_CENTRE 8811

#define EPSG_NAME_PARAMETER_LONGITUDE_PROJECTION_CENTRE                        \
    "Longitude of projection centre"
#define EPSG_CODE_PARAMETER_LONGITUDE_PROJECTION_CENTRE 8812

// Before EPSG 11.015
#define EPSG_NAME_PARAMETER_AZIMUTH_INITIAL_LINE "Azimuth of initial line"
#define EPSG_CODE_PARAMETER_AZIMUTH_INITIAL_LINE 8813

// Since EPSG 11.015
#define EPSG_NAME_PARAMETER_AZIMUTH_PROJECTION_CENTRE                          \
    "Azimuth at projection centre"
#define EPSG_CODE_PARAMETER_AZIMUTH_PROJECTION_CENTRE 8813

#define EPSG_NAME_PARAMETER_ANGLE_RECTIFIED_TO_SKEW_GRID                       \
    "Angle from Rectified to Skew Grid"
#define EPSG_CODE_PARAMETER_ANGLE_RECTIFIED_TO_SKEW_GRID 8814

// Before EPSG 11.015
#define EPSG_NAME_PARAMETER_SCALE_FACTOR_INITIAL_LINE                          \
    "Scale factor on initial line"
#define EPSG_CODE_PARAMETER_SCALE_FACTOR_INITIAL_LINE 8815

// Since EPSG 11.015
#define EPSG_NAME_PARAMETER_SCALE_FACTOR_PROJECTION_CENTRE                     \
    "Scale factor at projection centre"
#define EPSG_CODE_PARAMETER_SCALE_FACTOR_PROJECTION_CENTRE 8815

#define EPSG_NAME_PARAMETER_EASTING_PROJECTION_CENTRE                          \
    "Easting at projection centre"
#define EPSG_CODE_PARAMETER_EASTING_PROJECTION_CENTRE 8816

#define EPSG_NAME_PARAMETER_NORTHING_PROJECTION_CENTRE                         \
    "Northing at projection centre"
#define EPSG_CODE_PARAMETER_NORTHING_PROJECTION_CENTRE 8817

#define EPSG_NAME_PARAMETER_LATITUDE_PSEUDO_STANDARD_PARALLEL                  \
    "Latitude of pseudo standard parallel"
#define EPSG_CODE_PARAMETER_LATITUDE_PSEUDO_STANDARD_PARALLEL 8818

#define EPSG_NAME_PARAMETER_SCALE_FACTOR_PSEUDO_STANDARD_PARALLEL              \
    "Scale factor on pseudo standard parallel"
#define EPSG_CODE_PARAMETER_SCALE_FACTOR_PSEUDO_STANDARD_PARALLEL 8819

#define EPSG_NAME_PARAMETER_LATITUDE_FALSE_ORIGIN "Latitude of false origin"
#define EPSG_CODE_PARAMETER_LATITUDE_FALSE_ORIGIN 8821

#define EPSG_NAME_PARAMETER_LONGITUDE_FALSE_ORIGIN "Longitude of false origin"
#define EPSG_CODE_PARAMETER_LONGITUDE_FALSE_ORIGIN 8822

#define EPSG_NAME_PARAMETER_LATITUDE_1ST_STD_PARALLEL                          \
    "Latitude of 1st standard parallel"
#define EPSG_CODE_PARAMETER_LATITUDE_1ST_STD_PARALLEL 8823

#define EPSG_NAME_PARAMETER_LATITUDE_2ND_STD_PARALLEL                          \
    "Latitude of 2nd standard parallel"
#define EPSG_CODE_PARAMETER_LATITUDE_2ND_STD_PARALLEL 8824

#define EPSG_NAME_PARAMETER_EASTING_FALSE_ORIGIN "Easting at false origin"
#define EPSG_CODE_PARAMETER_EASTING_FALSE_ORIGIN 8826

#define EPSG_NAME_PARAMETER_NORTHING_FALSE_ORIGIN "Northing at false origin"
#define EPSG_CODE_PARAMETER_NORTHING_FALSE_ORIGIN 8827

#define EPSG_NAME_PARAMETER_LATITUDE_STD_PARALLEL                              \
    "Latitude of standard parallel"
#define EPSG_CODE_PARAMETER_LATITUDE_STD_PARALLEL 8832

#define EPSG_NAME_PARAMETER_LONGITUDE_OF_ORIGIN "Longitude of origin"
#define EPSG_CODE_PARAMETER_LONGITUDE_OF_ORIGIN 8833

#define EPSG_NAME_PARAMETER_ELLIPSOID_SCALE_FACTOR "Ellipsoid scaling factor"
#define EPSG_CODE_PARAMETER_ELLIPSOID_SCALE_FACTOR 1038

#define EPSG_NAME_PARAMETER_LATITUDE_TOPOGRAPHIC_ORIGIN                        \
    "Latitude of topocentric origin"
#define EPSG_CODE_PARAMETER_LATITUDE_TOPOGRAPHIC_ORIGIN 8834

#define EPSG_NAME_PARAMETER_LONGITUDE_TOPOGRAPHIC_ORIGIN                       \
    "Longitude of topocentric origin"
#define EPSG_CODE_PARAMETER_LONGITUDE_TOPOGRAPHIC_ORIGIN 8835

#define EPSG_NAME_PARAMETER_ELLIPSOIDAL_HEIGHT_TOPOCENTRIC_ORIGIN              \
    "Ellipsoidal height of topocentric origin"
#define EPSG_CODE_PARAMETER_ELLIPSOIDAL_HEIGHT_TOPOCENTRIC_ORIGIN 8836

#define EPSG_NAME_PARAMETER_VIEWPOINT_HEIGHT "Viewpoint height"
#define EPSG_CODE_PARAMETER_VIEWPOINT_HEIGHT 8840

#define EPSG_NAME_PARAMETER_PROJECTION_PLANE_ORIGIN_HEIGHT                     \
    "Projection plane origin height"
#define EPSG_CODE_PARAMETER_PROJECTION_PLANE_ORIGIN_HEIGHT 1039

/* ------------------------------------------------------------------------ */

/* Other conversions and transformations */

#define EPSG_NAME_METHOD_COORDINATE_FRAME_GEOCENTRIC                           \
    "Coordinate Frame rotation (geocentric domain)"
#define EPSG_CODE_METHOD_COORDINATE_FRAME_GEOCENTRIC 1032

#define EPSG_NAME_METHOD_COORDINATE_FRAME_FULL_MATRIX_GEOCENTRIC               \
    "Coordinate Frame rotation full matrix (geocen)"
#define EPSG_CODE_METHOD_COORDINATE_FRAME_FULL_MATRIX_GEOCENTRIC 1132

#define EPSG_NAME_METHOD_COORDINATE_FRAME_GEOGRAPHIC_2D                        \
    "Coordinate Frame rotation (geog2D domain)"
#define EPSG_CODE_METHOD_COORDINATE_FRAME_GEOGRAPHIC_2D 9607

#define EPSG_NAME_METHOD_COORDINATE_FRAME_FULL_MATRIX_GEOGRAPHIC_2D            \
    "Coordinate Frame rotation full matrix (geog2D)"
#define EPSG_CODE_METHOD_COORDINATE_FRAME_FULL_MATRIX_GEOGRAPHIC_2D 1133

#define EPSG_NAME_METHOD_COORDINATE_FRAME_FULL_MATRIX_GEOGRAPHIC_3D            \
    "Coordinate Frame rotation full matrix (geog3D)"
#define EPSG_CODE_METHOD_COORDINATE_FRAME_FULL_MATRIX_GEOGRAPHIC_3D 1140

#define EPSG_NAME_METHOD_COORDINATE_FRAME_GEOGRAPHIC_3D                        \
    "Coordinate Frame rotation (geog3D domain)"
#define EPSG_CODE_METHOD_COORDINATE_FRAME_GEOGRAPHIC_3D 1038

#define EPSG_NAME_METHOD_COORDINATE_FRAME_GEOG3D_TO_COMPOUND                   \
    "Coordinate Frame rotation (geog3D to compound)"
#define EPSG_CODE_METHOD_COORDINATE_FRAME_GEOG3D_TO_COMPOUND 1149

#define EPSG_NAME_METHOD_POSITION_VECTOR_GEOCENTRIC                            \
    "Position Vector transformation (geocentric domain)"
#define EPSG_CODE_METHOD_POSITION_VECTOR_GEOCENTRIC 1033

#define EPSG_NAME_METHOD_POSITION_VECTOR_GEOGRAPHIC_2D                         \
    "Position Vector transformation (geog2D domain)"
#define EPSG_CODE_METHOD_POSITION_VECTOR_GEOGRAPHIC_2D 9606

#define EPSG_NAME_METHOD_POSITION_VECTOR_GEOGRAPHIC_3D                         \
    "Position Vector transformation (geog3D domain)"
#define EPSG_CODE_METHOD_POSITION_VECTOR_GEOGRAPHIC_3D 1037

#define EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATION_GEOCENTRIC                     \
    "Geocentric translations (geocentric domain)"
#define EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_GEOCENTRIC 1031

#define EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATION_GEOGRAPHIC_2D                  \
    "Geocentric translations (geog2D domain)"
#define EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_GEOGRAPHIC_2D 9603

#define EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATION_GEOGRAPHIC_3D                  \
    "Geocentric translations (geog3D domain)"
#define EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_GEOGRAPHIC_3D 1035

#define EPSG_NAME_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOCENTRIC             \
    "Time-dependent Position Vector tfm (geocentric)"
#define EPSG_CODE_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOCENTRIC 1053

#define EPSG_NAME_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_2D          \
    "Time-dependent Position Vector tfm (geog2D)"
#define EPSG_CODE_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_2D 1054

#define EPSG_NAME_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_3D          \
    "Time-dependent Position Vector tfm (geog3D)"
#define EPSG_CODE_METHOD_TIME_DEPENDENT_POSITION_VECTOR_GEOGRAPHIC_3D 1055

#define EPSG_NAME_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOCENTRIC            \
    "Time-dependent Coordinate Frame rotation  geocen)"
#define EPSG_CODE_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOCENTRIC 1056

#define EPSG_NAME_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_2D         \
    "Time-dependent Coordinate Frame rotation (geog2D)"
#define EPSG_CODE_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_2D 1057

#define EPSG_NAME_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_3D         \
    "Time-dependent Coordinate Frame rotation (geog3D)"
#define EPSG_CODE_METHOD_TIME_DEPENDENT_COORDINATE_FRAME_GEOGRAPHIC_3D 1058

#define EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_CF_GEOCENTRIC                      \
    "Molodensky-Badekas (CF geocentric domain)"
#define EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_CF_GEOCENTRIC 1034

#define EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_PV_GEOCENTRIC                      \
    "Molodensky-Badekas (PV geocentric domain)"
#define EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_PV_GEOCENTRIC 1061

#define EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_CF_GEOGRAPHIC_3D                   \
    "Molodensky-Badekas (CF geog3D domain)"
#define EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_CF_GEOGRAPHIC_3D 1039

#define EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_PV_GEOGRAPHIC_3D                   \
    "Molodensky-Badekas (PV geog3D domain)"
#define EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_PV_GEOGRAPHIC_3D 1062

#define EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_CF_GEOGRAPHIC_2D                   \
    "Molodensky-Badekas (CF geog2D domain)"
#define EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_CF_GEOGRAPHIC_2D 9636

#define EPSG_NAME_METHOD_MOLODENSKY_BADEKAS_PV_GEOGRAPHIC_2D                   \
    "Molodensky-Badekas (PV geog2D domain)"
#define EPSG_CODE_METHOD_MOLODENSKY_BADEKAS_PV_GEOGRAPHIC_2D 1063

#define EPSG_CODE_PARAMETER_X_AXIS_TRANSLATION 8605
#define EPSG_CODE_PARAMETER_Y_AXIS_TRANSLATION 8606
#define EPSG_CODE_PARAMETER_Z_AXIS_TRANSLATION 8607
#define EPSG_CODE_PARAMETER_X_AXIS_ROTATION 8608
#define EPSG_CODE_PARAMETER_Y_AXIS_ROTATION 8609
#define EPSG_CODE_PARAMETER_Z_AXIS_ROTATION 8610
#define EPSG_CODE_PARAMETER_SCALE_DIFFERENCE 8611
#define EPSG_CODE_PARAMETER_RATE_X_AXIS_TRANSLATION 1040
#define EPSG_CODE_PARAMETER_RATE_Y_AXIS_TRANSLATION 1041
#define EPSG_CODE_PARAMETER_RATE_Z_AXIS_TRANSLATION 1042
#define EPSG_CODE_PARAMETER_RATE_X_AXIS_ROTATION 1043
#define EPSG_CODE_PARAMETER_RATE_Y_AXIS_ROTATION 1044
#define EPSG_CODE_PARAMETER_RATE_Z_AXIS_ROTATION 1045
#define EPSG_CODE_PARAMETER_RATE_SCALE_DIFFERENCE 1046
#define EPSG_CODE_PARAMETER_REFERENCE_EPOCH 1047
#define EPSG_CODE_PARAMETER_TRANSFORMATION_REFERENCE_EPOCH 1049

#define EPSG_NAME_PARAMETER_X_AXIS_TRANSLATION "X-axis translation"
#define EPSG_NAME_PARAMETER_Y_AXIS_TRANSLATION "Y-axis translation"
#define EPSG_NAME_PARAMETER_Z_AXIS_TRANSLATION "Z-axis translation"
#define EPSG_NAME_PARAMETER_X_AXIS_ROTATION "X-axis rotation"
#define EPSG_NAME_PARAMETER_Y_AXIS_ROTATION "Y-axis rotation"
#define EPSG_NAME_PARAMETER_Z_AXIS_ROTATION "Z-axis rotation"
#define EPSG_NAME_PARAMETER_SCALE_DIFFERENCE "Scale difference"

#define EPSG_NAME_PARAMETER_RATE_X_AXIS_TRANSLATION                            \
    "Rate of change of X-axis translation"
#define EPSG_NAME_PARAMETER_RATE_Y_AXIS_TRANSLATION                            \
    "Rate of change of Y-axis translation"
#define EPSG_NAME_PARAMETER_RATE_Z_AXIS_TRANSLATION                            \
    "Rate of change of Z-axis translation"
#define EPSG_NAME_PARAMETER_RATE_X_AXIS_ROTATION                               \
    "Rate of change of X-axis rotation"
#define EPSG_NAME_PARAMETER_RATE_Y_AXIS_ROTATION                               \
    "Rate of change of Y-axis rotation"
#define EPSG_NAME_PARAMETER_RATE_Z_AXIS_ROTATION                               \
    "Rate of change of Z-axis rotation"
#define EPSG_NAME_PARAMETER_RATE_SCALE_DIFFERENCE                              \
    "Rate of change of Scale difference"
#define EPSG_NAME_PARAMETER_REFERENCE_EPOCH "Parameter reference epoch"

#define EPSG_CODE_PARAMETER_ORDINATE_1_EVAL_POINT 8617
#define EPSG_CODE_PARAMETER_ORDINATE_2_EVAL_POINT 8618
#define EPSG_CODE_PARAMETER_ORDINATE_3_EVAL_POINT 8667

#define EPSG_NAME_PARAMETER_ORDINATE_1_EVAL_POINT                              \
    "Ordinate 1 of evaluation point"
#define EPSG_NAME_PARAMETER_ORDINATE_2_EVAL_POINT                              \
    "Ordinate 2 of evaluation point"
#define EPSG_NAME_PARAMETER_ORDINATE_3_EVAL_POINT                              \
    "Ordinate 3 of evaluation point"

#define EPSG_NAME_PARAMETER_TRANSFORMATION_REFERENCE_EPOCH                     \
    "Transformation reference epoch"

#define EPSG_NAME_METHOD_MOLODENSKY "Molodensky"
#define EPSG_CODE_METHOD_MOLODENSKY 9604

#define EPSG_NAME_METHOD_ABRIDGED_MOLODENSKY "Abridged Molodensky"
#define EPSG_CODE_METHOD_ABRIDGED_MOLODENSKY 9605

#define EPSG_CODE_PARAMETER_SEMI_MAJOR_AXIS_DIFFERENCE 8654
#define EPSG_CODE_PARAMETER_FLATTENING_DIFFERENCE 8655

#define EPSG_NAME_PARAMETER_SEMI_MAJOR_AXIS_DIFFERENCE                         \
    "Semi-major axis length difference"
#define EPSG_NAME_PARAMETER_FLATTENING_DIFFERENCE "Flattening difference"

#define PROJ_WKT2_NAME_PARAMETER_SOUTH_POLE_LATITUDE_GRIB_CONVENTION           \
    "Latitude of the southern pole (GRIB convention)"

#define PROJ_WKT2_NAME_PARAMETER_SOUTH_POLE_LONGITUDE_GRIB_CONVENTION          \
    "Longitude of the southern pole (GRIB convention)"

#define PROJ_WKT2_NAME_PARAMETER_AXIS_ROTATION_GRIB_CONVENTION                 \
    "Axis rotation (GRIB convention)"

#define PROJ_WKT2_NAME_PARAMETER_GRID_NORTH_POLE_LATITUDE_NETCDF_CONVENTION    \
    "Grid north pole latitude (netCDF CF convention)"

#define PROJ_WKT2_NAME_PARAMETER_GRID_NORTH_POLE_LONGITUDE_NETCDF_CONVENTION   \
    "Grid north pole longitude (netCDF CF convention)"

#define PROJ_WKT2_NAME_PARAMETER_NORTH_POLE_GRID_LONGITUDE_NETCDF_CONVENTION   \
    "North pole grid longitude (netCDF CF convention)"

/* ------------------------------------------------------------------------ */

#define EPSG_CODE_METHOD_NTV1 9614
#define EPSG_NAME_METHOD_NTV1 "NTv1"

#define EPSG_CODE_METHOD_NTV2 9615
#define EPSG_NAME_METHOD_NTV2 "NTv2"

#define EPSG_CODE_PARAMETER_LATITUDE_LONGITUDE_DIFFERENCE_FILE 8656
#define EPSG_NAME_PARAMETER_LATITUDE_LONGITUDE_DIFFERENCE_FILE                 \
    "Latitude and longitude difference file"

#define EPSG_NAME_PARAMETER_GEOID_CORRECTION_FILENAME                          \
    "Geoid (height correction) model file"
#define EPSG_CODE_PARAMETER_GEOID_CORRECTION_FILENAME 8666

/* Before EPSG 12.019 */
#define EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATION_BY_GRID_INTERPOLATION_IGN      \
    "Geocentric translation by Grid Interpolation (IGN)"
#define EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATION_BY_GRID_INTERPOLATION_IGN 1087

/* Since EPSG 12.019 */
#define EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATIONS_GEOG2D_DOMAIN_BY_GRID_IGN     \
    "Geocentric translations (geog2D domain) by grid (IGN)"
#define EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATIONS_GEOG2D_DOMAIN_BY_GRID_IGN 1087

#define EPSG_CODE_PARAMETER_GEOCENTRIC_TRANSLATION_FILE 8727
#define EPSG_NAME_PARAMETER_GEOCENTRIC_TRANSLATION_FILE                        \
    "Geocentric translation file"

#define EPSG_NAME_PARAMETER_EPSG_CODE_FOR_INTERPOLATION_CRS                    \
    "EPSG code for Interpolation CRS"
#define EPSG_CODE_PARAMETER_EPSG_CODE_FOR_INTERPOLATION_CRS 1048

/* ------------------------------------------------------------------------ */

#define EPSG_NAME_METHOD_POINT_MOTION_BY_GRID_CANADA_NTV2_VEL                  \
    "Point motion by grid (Canada NTv2_Vel)"
#define EPSG_CODE_METHOD_POINT_MOTION_BY_GRID_CANADA_NTV2_VEL 1070

// Before EPSG 12.019
#define EPSG_NAME_METHOD_POINT_MOTION_BY_GRID_CANADA_NEU_DOMAIN_NTV2_VEL       \
    "Point motion by grid (NEU domain) (NTv2_Vel)"
#define EPSG_CODE_METHOD_POINT_MOTION_BY_GRID_CANADA_NEU_DOMAIN_NTV2_VEL 1141

// Since EPSG 12.019
#define EPSG_NAME_METHOD_POINT_MOTION_GEOG3D_DOMAIN_USING_NEU_VELOCITY_GRID_NTV2_VEL \
    "Point motion (geog3D domain) using NEU velocity grid (NTv2_Vel)"
#define EPSG_CODE_METHOD_POINT_MOTION_GEOG3D_DOMAIN_USING_NEU_VELOCITY_GRID_NTV2_VEL \
    1141

#define EPSG_CODE_PARAMETER_POINT_MOTION_VELOCITY_GRID_FILE 1050
#define EPSG_NAME_PARAMETER_POINT_MOTION_VELOCITY_GRID_FILE                    \
    "Point motion velocity grid file"

#define EPSG_CODE_METHOD_POINT_MOTION_GEOCEN_DOMAIN_USING_NEU_VELOCITY_GRID_GRAVSOFT \
    1139
#define EPSG_NAME_METHOD_POINT_MOTION_GEOCEN_DOMAIN_USING_NEU_VELOCITY_GRID_GRAVSOFT \
    "Point motion (geocen domain) using NEU velocity grid (Gravsoft)"

#define EPSG_CODE_PARAMETER_POINT_MOTION_VELOCITY_NORTH_GRID_FILE 1072
#define EPSG_NAME_PARAMETER_POINT_MOTION_VELOCITY_NORTH_GRID_FILE              \
    "Point motion velocity north grid file"

#define EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATIONS_BY_GRID_GTG_AND_GEOCENTRIC_TRANSLATIONS_NEU_VELOCITIES_GTG \
    1142
#define EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATIONS_BY_GRID_GTG_AND_GEOCENTRIC_TRANSLATIONS_NEU_VELOCITIES_GTG \
    "Geocen translations by grid (gtg) & Geocen translations NEU "                                          \
    "velocities (gtg)"

#define EPSG_CODE_METHOD_POSITION_VECTOR_GEOCENTRIC_AND_GEOCENTRIC_TRANSLATIONS_NEU_VELOCITIES_GTG \
    1143
#define EPSG_NAME_METHOD_POSITION_VECTOR_GEOCENTRIC_AND_GEOCENTRIC_TRANSLATIONS_NEU_VELOCITIES_GTG \
    "Position Vector (geocen) & Geocen translations NEU velocities (gtg)"

#define EPSG_CODE_METHOD_GEOCENTRIC_TRANSLATIONS_USING_NEU_VELOCITY_GRID_GTG   \
    1144
#define EPSG_NAME_METHOD_GEOCENTRIC_TRANSLATIONS_USING_NEU_VELOCITY_GRID_GTG   \
    "Geocentric translations using NEU velocity grid (gtg)"

#define EPSG_CODE_PARAMETER_SOURCE_EPOCH 1068
#define EPSG_NAME_PARAMETER_SOURCE_EPOCH "Source epoch"

#define EPSG_CODE_PARAMETER_TARGET_EPOCH 1069
#define EPSG_NAME_PARAMETER_TARGET_EPOCH "Target epoch"

/* ------------------------------------------------------------------------ */

#define EPSG_NAME_METHOD_NEW_ZEALAND_DEFORMATION_MODEL                         \
    "New Zealand Deformation Model"
#define EPSG_CODE_METHOD_NEW_ZEALAND_DEFORMATION_MODEL 1079

/* ------------------------------------------------------------------------ */

/* Has been renamed to
 * EPSG_NAME_METHOD_GEOGRAPHIC3D_OFFSET_BY_VELOCITY_GRID_NTV2_VEL */
#define EPSG_NAME_METHOD_GEOGRAPHIC3D_OFFSET_BY_VELOCITY_GRID_NRCAN            \
    "Geographic3D Offset by velocity grid (NRCan byn)"
#define EPSG_CODE_METHOD_GEOGRAPHIC3D_OFFSET_BY_VELOCITY_GRID_NRCAN 1114

#define EPSG_NAME_METHOD_GEOGRAPHIC3D_OFFSET_BY_VELOCITY_GRID_NTV2_VEL         \
    "Geographic3D Offset by velocity grid (NTv2_Vel)"
#define EPSG_CODE_METHOD_GEOGRAPHIC3D_OFFSET_BY_VELOCITY_GRID_NTV2_VEL 1114

/* ------------------------------------------------------------------------ */

/* Before EPSG 12.019 */
#define EPSG_NAME_METHOD_VERTICAL_OFFSET_BY_VELOCITY_GRID_NRCAN                \
    "Vertical Offset by velocity grid (NRCan NTv2_Vel)"
#define EPSG_CODE_METHOD_VERTICAL_OFFSET_BY_VELOCITY_GRID_NRCAN 1113

/* Since EPSG 12.019 */

#define EPSG_NAME_METHOD_VERTICAL_OFFSET_USING_NEU_VELOCITY_GRID_NTV2_VEL      \
    "Vertical Offset using NEU velocity grid (NTv2_Vel)"
#define EPSG_CODE_METHOD_VERTICAL_OFFSET_USING_NEU_VELOCITY_GRID_NTV2_VEL 1113

/* ------------------------------------------------------------------------ */

#define EPSG_NAME_METHOD_GEOGRAPHIC3D_TO_GRAVITYRELATEDHEIGHT                  \
    "Geographic3D to GravityRelatedHeight"
#define EPSG_CODE_METHOD_GEOGRAPHIC3D_TO_GRAVITYRELATEDHEIGHT 1136

#define EPSG_NAME_METHOD_GEOGRAPHIC3D_TO_GEOG2D_GRAVITYRELATEDHEIGHT           \
    "Geog3D to Geog2D+GravityRelatedHeight"
#define EPSG_CODE_METHOD_GEOGRAPHIC3D_TO_GEOG2D_GRAVITYRELATEDHEIGHT 1131

/* ------------------------------------------------------------------------ */

#define PROJ_WKT2_NAME_METHOD_HEIGHT_TO_GEOG3D                                 \
    "GravityRelatedHeight to Geographic3D"

#define PROJ_WKT2_NAME_METHOD_CTABLE2 "CTABLE2"

#define PROJ_WKT2_NAME_METHOD_HORIZONTAL_SHIFT_GTIFF "HORIZONTAL_SHIFT_GTIFF"

#define PROJ_WKT2_NAME_METHOD_GENERAL_SHIFT_GTIFF "GENERAL_SHIFT_GTIFF"

#define PROJ_WKT2_PARAMETER_LATITUDE_LONGITUDE_ELLIPOISDAL_HEIGHT_DIFFERENCE_FILE \
    "Latitude, longitude and ellipsoidal height difference file"

/* ------------------------------------------------------------------------ */

#define EPSG_CODE_METHOD_VERTCON 9658
#define EPSG_NAME_METHOD_VERTCON_OLDNAME "VERTCON"
#define EPSG_NAME_METHOD_VERTCON                                               \
    "Vertical Offset by Grid Interpolation (VERTCON)"

#define EPSG_CODE_METHOD_VERTICALGRID_NZLVD 1071
#define EPSG_NAME_METHOD_VERTICALGRID_NZLVD                                    \
    "Vertical Offset by Grid Interpolation (NZLVD)"

#define EPSG_CODE_METHOD_VERTICALGRID_BEV_AT 1080
#define EPSG_NAME_METHOD_VERTICALGRID_BEV_AT                                   \
    "Vertical Offset by Grid Interpolation (BEV AT)"

#define EPSG_CODE_METHOD_VERTICALGRID_GTX 1084
#define EPSG_NAME_METHOD_VERTICALGRID_GTX                                      \
    "Vertical Offset by Grid Interpolation (gtx)"

#define EPSG_CODE_METHOD_VERTICALGRID_ASC 1085
#define EPSG_NAME_METHOD_VERTICALGRID_ASC                                      \
    "Vertical Offset by Grid Interpolation (asc)"

#define EPSG_CODE_METHOD_VERTICALGRID_GTG 1129
#define EPSG_NAME_METHOD_VERTICALGRID_GTG                                      \
    "Vertical Offset by Grid Interpolation (gtg)"

#define EPSG_CODE_METHOD_VERTICALGRID_PL_TXT 1101
#define EPSG_NAME_METHOD_VERTICALGRID_PL_TXT                                   \
    "Vertical Offset by Grid Interpolation (PL txt)"

/* has been deprecated by
 * EPSG_CODE_METHOD_VERTICALCHANGE_BY_GEOID_GRID_DIFFERENCE_NRCAN */
#define EPSG_CODE_METHOD_VERTICALGRID_NRCAN_BYN 1112
#define EPSG_NAME_METHOD_VERTICALGRID_NRCAN_BYN                                \
    "Vertical Offset by Grid Interpolation (NRCan byn)"

#define EPSG_NAME_PARAMETER_VERTICAL_OFFSET_FILE "Vertical offset file"
#define EPSG_CODE_PARAMETER_VERTICAL_OFFSET_FILE 8732

#define EPSG_CODE_METHOD_VERTICALCHANGE_BY_GEOID_GRID_DIFFERENCE_NRCAN 1126
#define EPSG_NAME_METHOD_VERTICALCHANGE_BY_GEOID_GRID_DIFFERENCE_NRCAN         \
    "Vertical change by geoid grid difference (NRCan)"

#define EPSG_NAME_PARAMETER_GEOID_MODEL_DIFFERENCE_FILE                        \
    "Geoid model difference file"
#define EPSG_CODE_PARAMETER_GEOID_MODEL_DIFFERENCE_FILE 1063

/* ------------------------------------------------------------------------ */

#define EPSG_CODE_METHOD_NADCON 9613
#define EPSG_NAME_METHOD_NADCON "NADCON"

#define EPSG_NAME_PARAMETER_LATITUDE_DIFFERENCE_FILE "Latitude difference file"
#define EPSG_CODE_PARAMETER_LATITUDE_DIFFERENCE_FILE 8657

#define EPSG_NAME_PARAMETER_LONGITUDE_DIFFERENCE_FILE                          \
    "Longitude difference file"
#define EPSG_CODE_PARAMETER_LONGITUDE_DIFFERENCE_FILE 8658

#define EPSG_CODE_METHOD_NADCON5_2D 1074
#define EPSG_NAME_METHOD_NADCON5_2D "NADCON5 (2D)"

#define EPSG_NAME_PARAMETER_ELLIPSOIDAL_HEIGHT_DIFFERENCE_FILE                 \
    "Ellipsoidal height difference file"
#define EPSG_CODE_PARAMETER_ELLIPSOIDAL_HEIGHT_DIFFERENCE_FILE 1058

#define EPSG_CODE_METHOD_NADCON5_3D 1075
#define EPSG_NAME_METHOD_NADCON5_3D "NADCON5 (3D)"

/* ------------------------------------------------------------------------ */

/* TIN-based transformations */

#define EPSG_NAME_METHOD_VERTICAL_OFFSET_BY_TIN_INTERPOLATION_JSON             \
    "Vertical Offset by TIN Interpolation (JSON)"
#define EPSG_CODE_METHOD_VERTICAL_OFFSET_BY_TIN_INTERPOLATION_JSON 1137

#define EPSG_NAME_METHOD_CARTESIAN_GRID_OFFSETS_BY_TIN_INTERPOLATION_JSON      \
    "Cartesian Grid Offsets by TIN Interpolation (JSON)"
#define EPSG_CODE_METHOD_CARTESIAN_GRID_OFFSETS_BY_TIN_INTERPOLATION_JSON 1138

#define EPSG_NAME_METHOD_GEOGRAPHIC2D_OFFSETS_BY_TIN_INTERPOLATION_JSON        \
    "Geographic2D Offsets by TIN Interpolation (JSON)"
#define EPSG_CODE_METHOD_GEOGRAPHIC2D_OFFSETS_BY_TIN_INTERPOLATION_JSON 1145

#define EPSG_NAME_PARAMETER_TIN_OFFSET_FILE "TIN offset file"
#define EPSG_CODE_PARAMETER_TIN_OFFSET_FILE 1064

/* ------------------------------------------------------------------------ */

#define EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT 1069
#define EPSG_NAME_METHOD_CHANGE_VERTICAL_UNIT "Change of Vertical Unit"

#define EPSG_CODE_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR 1104
#define EPSG_NAME_METHOD_CHANGE_VERTICAL_UNIT_NO_CONV_FACTOR                   \
    "Change of Vertical Unit"

#define EPSG_NAME_PARAMETER_UNIT_CONVERSION_SCALAR "Unit conversion scalar"
#define EPSG_CODE_PARAMETER_UNIT_CONVERSION_SCALAR 1051

/* ------------------------------------------------------------------------ */

#define EPSG_CODE_METHOD_LONGITUDE_ROTATION 9601
#define EPSG_NAME_METHOD_LONGITUDE_ROTATION "Longitude rotation"

#define EPSG_CODE_METHOD_VERTICAL_OFFSET 9616
#define EPSG_NAME_METHOD_VERTICAL_OFFSET "Vertical Offset"

#define EPSG_CODE_METHOD_VERTICAL_OFFSET_AND_SLOPE 1046
#define EPSG_NAME_METHOD_VERTICAL_OFFSET_AND_SLOPE "Vertical Offset and Slope"

#define EPSG_CODE_METHOD_GEOGRAPHIC2D_OFFSETS 9619
#define EPSG_NAME_METHOD_GEOGRAPHIC2D_OFFSETS "Geographic2D offsets"

#define EPSG_CODE_METHOD_GEOGRAPHIC2D_WITH_HEIGHT_OFFSETS 9618
#define EPSG_NAME_METHOD_GEOGRAPHIC2D_WITH_HEIGHT_OFFSETS                      \
    "Geographic2D with Height Offsets"

#define EPSG_CODE_METHOD_GEOGRAPHIC3D_OFFSETS 9660
#define EPSG_NAME_METHOD_GEOGRAPHIC3D_OFFSETS "Geographic3D offsets"

#define EPSG_CODE_METHOD_GEOGRAPHIC_GEOCENTRIC 9602
#define EPSG_NAME_METHOD_GEOGRAPHIC_GEOCENTRIC                                 \
    "Geographic/geocentric conversions"

#define EPSG_NAME_PARAMETER_LATITUDE_OFFSET "Latitude offset"
#define EPSG_CODE_PARAMETER_LATITUDE_OFFSET 8601

#define EPSG_NAME_PARAMETER_LONGITUDE_OFFSET "Longitude offset"
#define EPSG_CODE_PARAMETER_LONGITUDE_OFFSET 8602

#define EPSG_NAME_PARAMETER_VERTICAL_OFFSET "Vertical Offset"
#define EPSG_CODE_PARAMETER_VERTICAL_OFFSET 8603

#define EPSG_NAME_PARAMETER_GEOID_HEIGHT "Geoid height"
#define EPSG_CODE_PARAMETER_GEOID_HEIGHT 8604

/* Geoid undulation is the name before EPSG v11.023 */
#define EPSG_NAME_PARAMETER_GEOID_UNDULATION "Geoid undulation"
#define EPSG_CODE_PARAMETER_GEOID_UNDULATION 8604

#define EPSG_NAME_PARAMETER_INCLINATION_IN_LATITUDE "Inclination in latitude"
#define EPSG_CODE_PARAMETER_INCLINATION_IN_LATITUDE 8730

#define EPSG_NAME_PARAMETER_INCLINATION_IN_LONGITUDE "Inclination in longitude"
#define EPSG_CODE_PARAMETER_INCLINATION_IN_LONGITUDE 8731

#define EPSG_NAME_PARAMETER_EPSG_CODE_FOR_HORIZONTAL_CRS                       \
    "EPSG code for Horizontal CRS"
#define EPSG_CODE_PARAMETER_EPSG_CODE_FOR_HORIZONTAL_CRS 1037

/* ------------------------------------------------------------------------ */

#define EPSG_CODE_METHOD_AFFINE_PARAMETRIC_TRANSFORMATION 9624
#define EPSG_NAME_METHOD_AFFINE_PARAMETRIC_TRANSFORMATION                      \
    "Affine parametric transformation"

#define EPSG_NAME_PARAMETER_A0 "A0"
#define EPSG_CODE_PARAMETER_A0 8623

#define EPSG_NAME_PARAMETER_A1 "A1"
#define EPSG_CODE_PARAMETER_A1 8624

#define EPSG_NAME_PARAMETER_A2 "A2"
#define EPSG_CODE_PARAMETER_A2 8625

#define EPSG_NAME_PARAMETER_B0 "B0"
#define EPSG_CODE_PARAMETER_B0 8639

#define EPSG_NAME_PARAMETER_B1 "B1"
#define EPSG_CODE_PARAMETER_B1 8640

#define EPSG_NAME_PARAMETER_B2 "B2"
#define EPSG_CODE_PARAMETER_B2 8641

/* ------------------------------------------------------------------------ */

#define EPSG_CODE_METHOD_SIMILARITY_TRANSFORMATION 9621
#define EPSG_NAME_METHOD_SIMILARITY_TRANSFORMATION "Similarity transformation"

#define EPSG_NAME_PARAMETER_ORDINATE_1_EVAL_POINT_TARGET_CRS                   \
    "Ordinate 1 of evaluation point in target CRS"
#define EPSG_CODE_PARAMETER_ORDINATE_1_EVAL_POINT_TARGET_CRS 8621

#define EPSG_NAME_PARAMETER_ORDINATE_2_EVAL_POINT_TARGET_CRS                   \
    "Ordinate 2 of evaluation point in target CRS"
#define EPSG_CODE_PARAMETER_ORDINATE_2_EVAL_POINT_TARGET_CRS 8622

#define EPSG_NAME_PARAMETER_SCALE_FACTOR_FOR_SOURCE_CRS_AXES                   \
    "Scale factor for source CRS axes"
#define EPSG_CODE_PARAMETER_SCALE_FACTOR_FOR_SOURCE_CRS_AXES 1061

#define EPSG_NAME_PARAMETER_ROTATION_ANGLE_OF_SOURCE_CRS_AXES                  \
    "Rotation angle of source CRS axes"
#define EPSG_CODE_PARAMETER_ROTATION_ANGLE_OF_SOURCE_CRS_AXES 8614

/* ------------------------------------------------------------------------ */

#define EPSG_CODE_METHOD_AXIS_ORDER_REVERSAL_2D 9843
#define EPSG_NAME_METHOD_AXIS_ORDER_REVERSAL_2D "Axis Order Reversal (2D)"

#define EPSG_CODE_METHOD_AXIS_ORDER_REVERSAL_3D 9844
#define EPSG_NAME_METHOD_AXIS_ORDER_REVERSAL_3D                                \
    "Axis Order Reversal (Geographic3D horizontal)"

/* ------------------------------------------------------------------------ */

#define EPSG_CODE_METHOD_HEIGHT_DEPTH_REVERSAL 1068
#define EPSG_NAME_METHOD_HEIGHT_DEPTH_REVERSAL "Height Depth Reversal"

/* ------------------------------------------------------------------------ */

#define EPSG_CODE_METHOD_CARTESIAN_GRID_OFFSETS 9656
#define EPSG_NAME_METHOD_CARTESIAN_GRID_OFFSETS "Cartesian Grid Offsets"

#define EPSG_CODE_PARAMETER_EASTING_OFFSET 8728
#define EPSG_NAME_PARAMETER_EASTING_OFFSET "Easting offset"

#define EPSG_CODE_PARAMETER_NORTHING_OFFSET 8729
#define EPSG_NAME_PARAMETER_NORTHING_OFFSET "Northing offset"

/* ------------------------------------------------------------------------ */

#define EPSG_NAME_METHOD_GEOCENTRIC_TOPOCENTRIC                                \
    "Geocentric/topocentric conversions"
#define EPSG_CODE_METHOD_GEOCENTRIC_TOPOCENTRIC 9836

#define EPSG_NAME_PARAMETER_GEOCENTRIC_X_TOPOCENTRIC_ORIGIN                    \
    "Geocentric X of topocentric origin"
#define EPSG_CODE_PARAMETER_GEOCENTRIC_X_TOPOCENTRIC_ORIGIN 8837

#define EPSG_NAME_PARAMETER_GEOCENTRIC_Y_TOPOCENTRIC_ORIGIN                    \
    "Geocentric Y of topocentric origin"
#define EPSG_CODE_PARAMETER_GEOCENTRIC_Y_TOPOCENTRIC_ORIGIN 8838

#define EPSG_NAME_PARAMETER_GEOCENTRIC_Z_TOPOCENTRIC_ORIGIN                    \
    "Geocentric Z of topocentric origin"
#define EPSG_CODE_PARAMETER_GEOCENTRIC_Z_TOPOCENTRIC_ORIGIN 8839

/* ------------------------------------------------------------------------ */

#define EPSG_NAME_METHOD_GEOGRAPHIC_TOPOCENTRIC                                \
    "Geographic/topocentric conversions"
#define EPSG_CODE_METHOD_GEOGRAPHIC_TOPOCENTRIC 9837

/* ------------------------------------------------------------------------ */

#define PROJ_WKT2_NAME_METHOD_GEOGRAPHIC_GEOCENTRIC_LATITUDE                   \
    "Geographic latitude / Geocentric latitude"

#endif /* PROJ_CONSTANTS_INCLUDED */
