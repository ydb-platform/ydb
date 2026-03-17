/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2020 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

#ifndef YY_PJ_WKT2_WKT2_GENERATED_PARSER_H_INCLUDED
# define YY_PJ_WKT2_WKT2_GENERATED_PARSER_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int pj_wkt2_debug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    END = 0,
    T_PROJECTION = 258,
    T_DATUM = 259,
    T_SPHEROID = 260,
    T_PRIMEM = 261,
    T_UNIT = 262,
    T_AXIS = 263,
    T_PARAMETER = 264,
    T_GEODCRS = 265,
    T_LENGTHUNIT = 266,
    T_ANGLEUNIT = 267,
    T_SCALEUNIT = 268,
    T_TIMEUNIT = 269,
    T_ELLIPSOID = 270,
    T_CS = 271,
    T_ID = 272,
    T_PROJCRS = 273,
    T_BASEGEODCRS = 274,
    T_MERIDIAN = 275,
    T_BEARING = 276,
    T_ORDER = 277,
    T_ANCHOR = 278,
    T_ANCHOREPOCH = 279,
    T_CONVERSION = 280,
    T_METHOD = 281,
    T_REMARK = 282,
    T_GEOGCRS = 283,
    T_BASEGEOGCRS = 284,
    T_SCOPE = 285,
    T_AREA = 286,
    T_BBOX = 287,
    T_CITATION = 288,
    T_URI = 289,
    T_VERTCRS = 290,
    T_VDATUM = 291,
    T_GEOIDMODEL = 292,
    T_COMPOUNDCRS = 293,
    T_PARAMETERFILE = 294,
    T_COORDINATEOPERATION = 295,
    T_SOURCECRS = 296,
    T_TARGETCRS = 297,
    T_INTERPOLATIONCRS = 298,
    T_OPERATIONACCURACY = 299,
    T_CONCATENATEDOPERATION = 300,
    T_STEP = 301,
    T_BOUNDCRS = 302,
    T_ABRIDGEDTRANSFORMATION = 303,
    T_DERIVINGCONVERSION = 304,
    T_TDATUM = 305,
    T_CALENDAR = 306,
    T_TIMEORIGIN = 307,
    T_TIMECRS = 308,
    T_VERTICALEXTENT = 309,
    T_TIMEEXTENT = 310,
    T_USAGE = 311,
    T_DYNAMIC = 312,
    T_FRAMEEPOCH = 313,
    T_MODEL = 314,
    T_VELOCITYGRID = 315,
    T_ENSEMBLE = 316,
    T_MEMBER = 317,
    T_ENSEMBLEACCURACY = 318,
    T_DERIVEDPROJCRS = 319,
    T_BASEPROJCRS = 320,
    T_EDATUM = 321,
    T_ENGCRS = 322,
    T_PDATUM = 323,
    T_PARAMETRICCRS = 324,
    T_PARAMETRICUNIT = 325,
    T_BASEVERTCRS = 326,
    T_BASEENGCRS = 327,
    T_BASEPARAMCRS = 328,
    T_BASETIMECRS = 329,
    T_EPOCH = 330,
    T_COORDEPOCH = 331,
    T_COORDINATEMETADATA = 332,
    T_POINTMOTIONOPERATION = 333,
    T_VERSION = 334,
    T_AXISMINVALUE = 335,
    T_AXISMAXVALUE = 336,
    T_RANGEMEANING = 337,
    T_exact = 338,
    T_wraparound = 339,
    T_DEFININGTRANSFORMATION = 340,
    T_GEODETICCRS = 341,
    T_GEODETICDATUM = 342,
    T_PROJECTEDCRS = 343,
    T_PRIMEMERIDIAN = 344,
    T_GEOGRAPHICCRS = 345,
    T_TRF = 346,
    T_VERTICALCRS = 347,
    T_VERTICALDATUM = 348,
    T_VRF = 349,
    T_TIMEDATUM = 350,
    T_TEMPORALQUANTITY = 351,
    T_ENGINEERINGDATUM = 352,
    T_ENGINEERINGCRS = 353,
    T_PARAMETRICDATUM = 354,
    T_AFFINE = 355,
    T_CARTESIAN = 356,
    T_CYLINDRICAL = 357,
    T_ELLIPSOIDAL = 358,
    T_LINEAR = 359,
    T_PARAMETRIC = 360,
    T_POLAR = 361,
    T_SPHERICAL = 362,
    T_VERTICAL = 363,
    T_TEMPORAL = 364,
    T_TEMPORALCOUNT = 365,
    T_TEMPORALMEASURE = 366,
    T_ORDINAL = 367,
    T_TEMPORALDATETIME = 368,
    T_NORTH = 369,
    T_NORTHNORTHEAST = 370,
    T_NORTHEAST = 371,
    T_EASTNORTHEAST = 372,
    T_EAST = 373,
    T_EASTSOUTHEAST = 374,
    T_SOUTHEAST = 375,
    T_SOUTHSOUTHEAST = 376,
    T_SOUTH = 377,
    T_SOUTHSOUTHWEST = 378,
    T_SOUTHWEST = 379,
    T_WESTSOUTHWEST = 380,
    T_WEST = 381,
    T_WESTNORTHWEST = 382,
    T_NORTHWEST = 383,
    T_NORTHNORTHWEST = 384,
    T_UP = 385,
    T_DOWN = 386,
    T_GEOCENTRICX = 387,
    T_GEOCENTRICY = 388,
    T_GEOCENTRICZ = 389,
    T_COLUMNPOSITIVE = 390,
    T_COLUMNNEGATIVE = 391,
    T_ROWPOSITIVE = 392,
    T_ROWNEGATIVE = 393,
    T_DISPLAYRIGHT = 394,
    T_DISPLAYLEFT = 395,
    T_DISPLAYUP = 396,
    T_DISPLAYDOWN = 397,
    T_FORWARD = 398,
    T_AFT = 399,
    T_PORT = 400,
    T_STARBOARD = 401,
    T_CLOCKWISE = 402,
    T_COUNTERCLOCKWISE = 403,
    T_TOWARDS = 404,
    T_AWAYFROM = 405,
    T_FUTURE = 406,
    T_PAST = 407,
    T_UNSPECIFIED = 408,
    T_STRING = 409,
    T_UNSIGNED_INTEGER_DIFFERENT_ONE_TWO_THREE = 410
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef int YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int pj_wkt2_parse (pj_wkt2_parse_context *context);

#endif /* !YY_PJ_WKT2_WKT2_GENERATED_PARSER_H_INCLUDED  */
