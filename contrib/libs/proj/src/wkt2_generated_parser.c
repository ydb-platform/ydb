/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison implementation for Yacc-like parsers in C

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

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
#define YYBISON_VERSION "3.5.1"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1


/* Substitute the variable and function names.  */
#define yyparse         pj_wkt2_parse
#define yylex           pj_wkt2_lex
#define yyerror         pj_wkt2_error
#define yydebug         pj_wkt2_debug
#define yynerrs         pj_wkt2_nerrs

/* First part of user prologue.  */

/******************************************************************************
 * Project:  PROJ
 * Purpose:  WKT2 parser grammar
 * Author:   Even Rouault, <even.rouault at spatialys.com>
 *
 ******************************************************************************
 * Copyright (c) 2018 Even Rouault, <even.rouault at spatialys.com>
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

#include "wkt2_parser.h"



# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 1
#endif

/* Use api.header.include to #include this header
   instead of duplicating it here.  */
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



#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))

/* Stored state numbers (used for stacks). */
typedef yytype_int16 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif

#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(E) ((void) (E))
#else
# define YYUSE(E) /* empty */
#endif

#if defined __GNUC__ && ! defined __ICC && 407 <= __GNUC__ * 100 + __GNUC_MINOR__
/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                            \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  106
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   3517

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  171
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  375
/* YYNRULES -- Number of rules.  */
#define YYNRULES  750
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  1531

#define YYUNDEFTOK  2
#define YYMAXUTOK   410


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
     167,   169,     2,   161,   170,   162,   156,     2,     2,   158,
     159,   160,     2,     2,     2,     2,     2,     2,   163,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   157,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,   164,     2,     2,     2,     2,     2,
     165,   166,     2,   168,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,    83,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,   117,   118,   119,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,   133,   134,
     135,   136,   137,   138,   139,   140,   141,   142,   143,   144,
     145,   146,   147,   148,   149,   150,   151,   152,   153,   154,
     155
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   214,   214,   214,   214,   214,   214,   214,   215,   215,
     215,   216,   219,   219,   220,   220,   220,   221,   223,   223,
     227,   231,   231,   233,   235,   237,   237,   239,   239,   241,
     243,   245,   247,   249,   249,   251,   251,   253,   253,   253,
     253,   255,   255,   259,   261,   265,   266,   267,   269,   269,
     271,   273,   275,   277,   281,   282,   285,   286,   288,   290,
     292,   295,   296,   297,   299,   301,   303,   303,   305,   308,
     309,   311,   311,   316,   316,   318,   318,   320,   322,   324,
     328,   329,   332,   333,   334,   336,   336,   337,   340,   341,
     345,   346,   347,   351,   352,   353,   354,   356,   360,   362,
     365,   367,   370,   371,   372,   373,   374,   375,   376,   377,
     378,   379,   380,   381,   382,   383,   384,   387,   388,   389,
     390,   391,   392,   393,   394,   395,   396,   397,   398,   399,
     400,   401,   405,   407,   409,   413,   418,   420,   422,   424,
     426,   430,   435,   436,   438,   440,   442,   446,   450,   452,
     452,   454,   454,   459,   464,   465,   466,   467,   468,   469,
     470,   472,   474,   476,   476,   478,   478,   480,   482,   484,
     486,   488,   490,   494,   496,   500,   500,   503,   506,   511,
     511,   511,   511,   511,   514,   519,   519,   519,   519,   522,
     526,   527,   529,   545,   549,   550,   552,   552,   554,   554,
     560,   560,   562,   564,   571,   571,   571,   571,   573,   575,
     582,   589,   590,   591,   592,   594,   595,   598,   600,   603,
     604,   605,   606,   607,   609,   610,   611,   612,   614,   621,
     622,   623,   624,   626,   633,   640,   641,   642,   644,   646,
     646,   646,   646,   646,   646,   646,   646,   646,   648,   648,
     650,   650,   652,   652,   652,   654,   659,   665,   670,   673,
     676,   677,   678,   679,   680,   681,   682,   683,   684,   687,
     688,   689,   690,   691,   692,   693,   694,   697,   698,   699,
     700,   701,   702,   703,   704,   707,   708,   711,   712,   713,
     714,   715,   719,   720,   721,   722,   723,   724,   725,   726,
     727,   730,   731,   732,   733,   736,   737,   738,   739,   742,
     743,   746,   747,   748,   753,   754,   757,   758,   759,   760,
     761,   764,   765,   766,   767,   768,   769,   770,   771,   772,
     773,   774,   775,   776,   777,   778,   779,   780,   781,   782,
     783,   784,   785,   786,   787,   788,   789,   790,   791,   792,
     793,   794,   795,   796,   797,   798,   799,   801,   804,   806,
     808,   810,   812,   815,   816,   817,   818,   820,   821,   822,
     823,   824,   826,   827,   829,   830,   832,   833,   834,   834,
     836,   852,   852,   854,   861,   862,   864,   865,   867,   875,
     876,   878,   880,   882,   887,   888,   890,   892,   894,   896,
     898,   900,   902,   907,   911,   913,   916,   919,   920,   921,
     923,   924,   926,   931,   932,   934,   934,   936,   940,   940,
     940,   942,   942,   944,   952,   961,   969,   979,   980,   982,
     984,   984,   986,   986,   989,   990,   994,  1000,  1001,  1002,
    1004,  1004,  1006,  1008,  1010,  1014,  1019,  1019,  1021,  1024,
    1025,  1030,  1031,  1033,  1038,  1038,  1038,  1040,  1042,  1043,
    1044,  1045,  1046,  1047,  1048,  1049,  1051,  1054,  1056,  1058,
    1061,  1063,  1063,  1063,  1067,  1073,  1073,  1077,  1077,  1078,
    1078,  1080,  1085,  1086,  1087,  1088,  1089,  1091,  1097,  1102,
    1108,  1110,  1112,  1114,  1118,  1124,  1125,  1126,  1128,  1130,
    1132,  1136,  1136,  1138,  1140,  1145,  1146,  1147,  1149,  1151,
    1153,  1155,  1159,  1159,  1161,  1167,  1174,  1174,  1177,  1184,
    1185,  1186,  1187,  1188,  1190,  1191,  1192,  1194,  1198,  1200,
    1202,  1202,  1206,  1211,  1211,  1211,  1215,  1220,  1220,  1222,
    1226,  1226,  1228,  1229,  1230,  1231,  1235,  1240,  1242,  1246,
    1246,  1250,  1255,  1257,  1261,  1262,  1263,  1264,  1265,  1267,
    1267,  1269,  1272,  1274,  1274,  1276,  1278,  1280,  1284,  1290,
    1291,  1292,  1293,  1295,  1297,  1301,  1306,  1308,  1311,  1317,
    1318,  1319,  1321,  1325,  1331,  1331,  1331,  1331,  1331,  1331,
    1335,  1340,  1342,  1347,  1347,  1348,  1350,  1350,  1352,  1359,
    1359,  1361,  1368,  1368,  1370,  1377,  1384,  1389,  1390,  1391,
    1393,  1399,  1404,  1412,  1418,  1420,  1422,  1433,  1434,  1435,
    1437,  1439,  1439,  1440,  1440,  1444,  1450,  1450,  1452,  1457,
    1463,  1468,  1474,  1479,  1484,  1490,  1495,  1500,  1506,  1511,
    1516,  1522,  1522,  1523,  1523,  1524,  1524,  1525,  1525,  1526,
    1526,  1527,  1527,  1530,  1530,  1532,  1533,  1534,  1536,  1538,
    1542,  1545,  1545,  1548,  1549,  1550,  1552,  1556,  1557,  1559,
    1561,  1561,  1562,  1562,  1563,  1563,  1563,  1564,  1565,  1565,
    1566,  1566,  1567,  1567,  1569,  1569,  1570,  1570,  1571,  1572,
    1572,  1576,  1580,  1581,  1584,  1589,  1590,  1591,  1592,  1593,
    1594,  1595,  1597,  1599,  1601,  1604,  1606,  1608,  1610,  1612,
    1614,  1616,  1618,  1620,  1622,  1627,  1631,  1632,  1635,  1640,
    1641,  1642,  1643,  1644,  1646,  1651,  1656,  1657,  1660,  1666,
    1666,  1666,  1666,  1668,  1669,  1670,  1671,  1673,  1675,  1680,
    1686,  1688,  1693,  1694,  1697,  1705,  1706,  1707,  1708,  1710,
    1712
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || 1
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of string\"", "error", "$undefined", "\"PROJECTION\"",
  "\"DATUM\"", "\"SPHEROID\"", "\"PRIMEM\"", "\"UNIT\"", "\"AXIS\"",
  "\"PARAMETER\"", "\"GEODCRS\"", "\"LENGTHUNIT\"", "\"ANGLEUNIT\"",
  "\"SCALEUNIT\"", "\"TIMEUNIT\"", "\"ELLIPSOID\"", "\"CS\"", "\"ID\"",
  "\"PROJCRS\"", "\"BASEGEODCRS\"", "\"MERIDIAN\"", "\"BEARING\"",
  "\"ORDER\"", "\"ANCHOR\"", "\"ANCHOREPOCH\"", "\"CONVERSION\"",
  "\"METHOD\"", "\"REMARK\"", "\"GEOGCRS\"", "\"BASEGEOGCRS\"",
  "\"SCOPE\"", "\"AREA\"", "\"BBOX\"", "\"CITATION\"", "\"URI\"",
  "\"VERTCRS\"", "\"VDATUM\"", "\"GEOIDMODEL\"", "\"COMPOUNDCRS\"",
  "\"PARAMETERFILE\"", "\"COORDINATEOPERATION\"", "\"SOURCECRS\"",
  "\"TARGETCRS\"", "\"INTERPOLATIONCRS\"", "\"OPERATIONACCURACY\"",
  "\"CONCATENATEDOPERATION\"", "\"STEP\"", "\"BOUNDCRS\"",
  "\"ABRIDGEDTRANSFORMATION\"", "\"DERIVINGCONVERSION\"", "\"TDATUM\"",
  "\"CALENDAR\"", "\"TIMEORIGIN\"", "\"TIMECRS\"", "\"VERTICALEXTENT\"",
  "\"TIMEEXTENT\"", "\"USAGE\"", "\"DYNAMIC\"", "\"FRAMEEPOCH\"",
  "\"MODEL\"", "\"VELOCITYGRID\"", "\"ENSEMBLE\"", "\"MEMBER\"",
  "\"ENSEMBLEACCURACY\"", "\"DERIVEDPROJCRS\"", "\"BASEPROJCRS\"",
  "\"EDATUM\"", "\"ENGCRS\"", "\"PDATUM\"", "\"PARAMETRICCRS\"",
  "\"PARAMETRICUNIT\"", "\"BASEVERTCRS\"", "\"BASEENGCRS\"",
  "\"BASEPARAMCRS\"", "\"BASETIMECRS\"", "\"EPOCH\"", "\"COORDEPOCH\"",
  "\"COORDINATEMETADATA\"", "\"POINTMOTIONOPERATION\"", "\"VERSION\"",
  "\"AXISMINVALUE\"", "\"AXISMAXVALUE\"", "\"RANGEMEANING\"", "\"exact\"",
  "\"wraparound\"", "\"DEFININGTRANSFORMATION\"", "\"GEODETICCRS\"",
  "\"GEODETICDATUM\"", "\"PROJECTEDCRS\"", "\"PRIMEMERIDIAN\"",
  "\"GEOGRAPHICCRS\"", "\"TRF\"", "\"VERTICALCRS\"", "\"VERTICALDATUM\"",
  "\"VRF\"", "\"TIMEDATUM\"", "\"TEMPORALQUANTITY\"",
  "\"ENGINEERINGDATUM\"", "\"ENGINEERINGCRS\"", "\"PARAMETRICDATUM\"",
  "\"affine\"", "\"Cartesian\"", "\"cylindrical\"", "\"ellipsoidal\"",
  "\"linear\"", "\"parametric\"", "\"polar\"", "\"spherical\"",
  "\"vertical\"", "\"temporal\"", "\"temporalCount\"",
  "\"temporalMeasure\"", "\"ordinal\"", "\"temporalDateTime\"",
  "\"north\"", "\"northNorthEast\"", "\"northEast\"", "\"eastNorthEast\"",
  "\"east\"", "\"eastSouthEast\"", "\"southEast\"", "\"southSouthEast\"",
  "\"south\"", "\"southSouthWest\"", "\"southWest\"", "\"westSouthWest\"",
  "\"west\"", "\"westNorthWest\"", "\"northWest\"", "\"northNorthWest\"",
  "\"up\"", "\"down\"", "\"geocentricX\"", "\"geocentricY\"",
  "\"geocentricZ\"", "\"columnPositive\"", "\"columnNegative\"",
  "\"rowPositive\"", "\"rowNegative\"", "\"displayRight\"",
  "\"displayLeft\"", "\"displayUp\"", "\"displayDown\"", "\"forward\"",
  "\"aft\"", "\"port\"", "\"starboard\"", "\"clockwise\"",
  "\"counterClockwise\"", "\"towards\"", "\"awayFrom\"", "\"future\"",
  "\"part\"", "\"unspecified\"", "\"string\"", "\"unsigned integer\"",
  "'.'", "'E'", "'1'", "'2'", "'3'", "'+'", "'-'", "':'", "'T'", "'Z'",
  "'['", "'('", "']'", "')'", "','", "$accept", "input", "datum", "crs",
  "period", "number", "signed_numeric_literal_with_sign",
  "signed_numeric_literal", "unsigned_numeric_literal", "opt_sign",
  "approximate_numeric_literal", "mantissa", "exponent", "signed_integer",
  "exact_numeric_literal", "opt_period_unsigned_integer",
  "unsigned_integer", "sign", "colon", "hyphen", "datetime",
  "opt_24_hour_clock", "year", "month", "day", "_24_hour_clock",
  "opt_colon_minute_colon_second_time_zone_designator",
  "opt_colon_second_time_zone_designator", "time_designator", "hour",
  "minute", "second_time_zone_designator", "seconds_integer",
  "seconds_fraction", "time_zone_designator", "utc_designator",
  "local_time_zone_designator", "opt_colon_minute", "left_delimiter",
  "right_delimiter", "wkt_separator", "quoted_latin_text",
  "quoted_unicode_text", "opt_separator_scope_extent_identifier_remark",
  "no_opt_separator_scope_extent_identifier_remark",
  "opt_identifier_list_remark",
  "scope_extent_opt_identifier_list_opt_remark",
  "scope_extent_opt_identifier_list_remark",
  "usage_list_opt_identifier_list_remark", "usage", "usage_keyword",
  "scope", "scope_keyword", "scope_text_description", "extent",
  "extent_opt_identifier_list_remark", "area_description",
  "area_description_keyword", "area_text_description",
  "geographic_bounding_box", "geographic_bounding_box_keyword",
  "lower_left_latitude", "lower_left_longitude", "upper_right_latitude",
  "upper_right_longitude", "vertical_extent", "opt_separator_length_unit",
  "vertical_extent_keyword", "vertical_extent_minimum_height",
  "vertical_extent_maximum_height", "temporal_extent",
  "temporal_extent_keyword", "temporal_extent_start",
  "temporal_extent_end", "identifier",
  "opt_version_authority_citation_uri", "identifier_keyword",
  "authority_name", "authority_unique_identifier", "version",
  "authority_citation", "citation_keyword", "citation", "id_uri",
  "uri_keyword", "uri", "remark", "remark_keyword", "unit", "spatial_unit",
  "angle_or_length_or_parametric_or_scale_unit",
  "angle_or_length_or_parametric_or_scale_unit_keyword",
  "angle_or_length_or_scale_unit", "angle_or_length_or_scale_unit_keyword",
  "angle_unit", "opt_separator_identifier_list", "length_unit",
  "time_unit", "opt_separator_conversion_factor_identifier_list",
  "angle_unit_keyword", "length_unit_keyword", "time_unit_keyword",
  "unit_name", "conversion_factor",
  "coordinate_system_scope_extent_identifier_remark",
  "coordinate_system_defining_transformation_scope_extent_identifier_remark",
  "spatial_cs_scope_extent_identifier_remark",
  "spatial_cs_defining_transformation_scope_extent_identifier_remark",
  "opt_separator_spatial_axis_list_opt_separator_cs_unit_scope_extent_identifier_remark",
  "opt_defining_transformation_separator_scope_extent_identifier_remark",
  "defining_transformation", "defining_transformation_name",
  "no_opt_defining_transformation_separator_scope_extent_identifier_remark",
  "opt_separator_spatial_axis_list_opt_defining_transformation_opt_separator_cs_unit_scope_extent_identifier_remark",
  "wkt2015temporal_cs_scope_extent_identifier_remark",
  "opt_separator_cs_unit_scope_extent_identifier_remark",
  "temporalcountmeasure_cs_scope_extent_identifier_remark",
  "ordinaldatetime_cs_scope_extent_identifier_remark",
  "opt_separator_ordinaldatetime_axis_list_scope_extent_identifier_remark",
  "cs_keyword", "spatial_cs_type", "temporalcountmeasure_cs_type",
  "ordinaldatetime_cs_type", "dimension", "spatial_axis",
  "temporalcountmeasure_axis", "ordinaldatetime_axis", "axis_keyword",
  "axis_name_abbrev",
  "axis_direction_opt_axis_order_spatial_unit_identifier_list",
  "north_south_options_spatial_unit",
  "clockwise_counter_clockwise_options_spatial_unit",
  "axis_direction_except_n_s_cw_ccw_opt_axis_spatial_unit_identifier_list",
  "axis_direction_except_n_s_cw_ccw_opt_axis_spatial_unit_identifier_list_options",
  "axis_direction_opt_axis_order_identifier_list", "north_south_options",
  "clockwise_counter_clockwise_options",
  "axis_direction_except_n_s_cw_ccw_opt_axis_identifier_list",
  "axis_direction_except_n_s_cw_ccw_opt_axis_identifier_list_options",
  "opt_separator_axis_time_unit_identifier_list",
  "axis_direction_except_n_s_cw_ccw_opt_axis_time_unit_identifier_list_options",
  "axis_direction_except_n_s_cw_ccw", "meridian", "meridian_keyword",
  "bearing", "bearing_keyword", "axis_order", "axis_order_keyword",
  "axis_range_opt_separator_identifier_list",
  "opt_separator_axis_range_opt_separator_identifier_list",
  "axis_minimum_value", "axis_minimum_value_keyword", "axis_maximum_value",
  "axis_maximum_value_keyword", "axis_range_meaning",
  "axis_range_meaning_keyword", "axis_range_meaning_value", "cs_unit",
  "datum_ensemble", "geodetic_datum_ensemble_without_pm",
  "datum_ensemble_member_list_ellipsoid_accuracy_identifier_list",
  "opt_separator_datum_ensemble_identifier_list",
  "vertical_datum_ensemble",
  "datum_ensemble_member_list_accuracy_identifier_list",
  "datum_ensemble_keyword", "datum_ensemble_name", "datum_ensemble_member",
  "opt_datum_ensemble_member_identifier_list",
  "datum_ensemble_member_keyword", "datum_ensemble_member_name",
  "datum_ensemble_member_identifier", "datum_ensemble_accuracy",
  "datum_ensemble_accuracy_keyword", "accuracy",
  "datum_ensemble_identifier", "dynamic_crs", "dynamic_crs_keyword",
  "frame_reference_epoch", "frame_reference_epoch_keyword",
  "reference_epoch", "opt_separator_deformation_model_id",
  "deformation_model_id", "opt_separator_identifier",
  "deformation_model_id_keyword", "deformation_model_name", "geodetic_crs",
  "geographic_crs", "static_geodetic_crs", "dynamic_geodetic_crs",
  "static_geographic_crs", "dynamic_geographic_crs",
  "opt_prime_meridian_coordinate_system_defining_transformation_scope_extent_identifier_remark",
  "crs_name", "geodetic_crs_keyword", "geographic_crs_keyword",
  "geodetic_reference_frame_or_geodetic_datum_ensemble_without_pm",
  "ellipsoid", "opt_separator_length_unit_identifier_list",
  "ellipsoid_keyword", "ellipsoid_name", "semi_major_axis",
  "inverse_flattening", "prime_meridian", "prime_meridian_keyword",
  "prime_meridian_name", "irm_longitude_opt_separator_identifier_list",
  "geodetic_reference_frame_with_opt_pm",
  "geodetic_reference_frame_without_pm",
  "geodetic_reference_frame_keyword", "datum_name",
  "opt_separator_datum_anchor_anchor_epoch_identifier_list",
  "datum_anchor", "datum_anchor_keyword", "datum_anchor_description",
  "datum_anchor_epoch", "datum_anchor_epoch_keyword", "anchor_epoch",
  "projected_crs", "projected_crs_keyword", "base_geodetic_crs",
  "base_static_geodetic_crs",
  "opt_separator_pm_ellipsoidal_cs_unit_opt_separator_identifier_list",
  "base_dynamic_geodetic_crs", "base_static_geographic_crs",
  "base_dynamic_geographic_crs", "base_geodetic_crs_keyword",
  "base_geographic_crs_keyword", "base_crs_name", "ellipsoidal_cs_unit",
  "map_projection", "opt_separator_parameter_list_identifier_list",
  "map_projection_keyword", "map_projection_name", "map_projection_method",
  "map_projection_method_keyword", "map_projection_method_name",
  "map_projection_parameter", "opt_separator_param_unit_identifier_list",
  "parameter_keyword", "parameter_name", "parameter_value",
  "map_projection_parameter_unit", "vertical_crs", "static_vertical_crs",
  "dynamic_vertical_crs",
  "vertical_reference_frame_or_vertical_datum_ensemble",
  "vertical_cs_opt_geoid_model_id_scope_extent_identifier_remark",
  "opt_separator_cs_unit_opt_geoid_model_id_scope_extent_identifier_remark",
  "opt_geoid_model_id_list_opt_separator_scope_extent_identifier_remark",
  "geoid_model_id", "geoid_model_keyword", "geoid_model_name",
  "vertical_crs_keyword", "vertical_reference_frame",
  "vertical_reference_frame_keyword", "engineering_crs",
  "engineering_crs_keyword", "engineering_datum",
  "engineering_datum_keyword",
  "opt_separator_datum_anchor_identifier_list", "parametric_crs",
  "parametric_crs_keyword", "parametric_datum", "parametric_datum_keyword",
  "temporal_crs", "temporal_crs_keyword", "temporal_datum",
  "opt_separator_temporal_datum_end", "temporal_datum_keyword",
  "temporal_origin", "temporal_origin_keyword",
  "temporal_origin_description", "calendar", "calendar_keyword",
  "calendar_identifier", "deriving_conversion",
  "opt_separator_parameter_or_parameter_file_identifier_list",
  "deriving_conversion_keyword", "deriving_conversion_name",
  "operation_method", "operation_method_keyword", "operation_method_name",
  "operation_parameter", "opt_separator_parameter_unit_identifier_list",
  "parameter_unit", "length_or_angle_or_scale_or_time_or_parametric_unit",
  "length_or_angle_or_scale_or_time_or_parametric_unit_keyword",
  "operation_parameter_file", "parameter_file_keyword",
  "parameter_file_name", "derived_geodetic_crs", "derived_geographic_crs",
  "derived_static_geod_crs",
  "base_static_geod_crs_or_base_static_geog_crs",
  "derived_dynamic_geod_crs",
  "base_dynamic_geod_crs_or_base_dynamic_geog_crs",
  "derived_static_geog_crs", "derived_dynamic_geog_crs",
  "base_static_geod_crs", "opt_separator_pm_opt_separator_identifier_list",
  "base_dynamic_geod_crs", "base_static_geog_crs", "base_dynamic_geog_crs",
  "derived_projected_crs", "derived_projected_crs_keyword",
  "derived_crs_name", "base_projected_crs",
  "base_projected_crs_opt_separator_cs_identifier",
  "base_projected_crs_keyword", "base_geodetic_geographic_crs",
  "derived_vertical_crs", "base_vertical_crs", "base_static_vertical_crs",
  "base_dynamic_vertical_crs", "base_vertical_crs_keyword",
  "derived_engineering_crs", "base_engineering_crs",
  "base_engineering_crs_keyword", "derived_parametric_crs",
  "base_parametric_crs", "base_parametric_crs_keyword",
  "derived_temporal_crs", "base_temporal_crs", "base_temporal_crs_keyword",
  "compound_crs", "single_crs", "single_crs_or_bound_crs",
  "opt_wkt_separator_single_crs_list_opt_separator_scope_extent_identifier_remark",
  "compound_crs_keyword", "compound_crs_name", "metadata_coordinate_epoch",
  "coordinate_epoch_keyword", "coordinate_epoch", "coordinate_metadata",
  "coordinate_metadata_crs", "coordinate_metadata_keyword",
  "static_crs_coordinate_metadata", "dynamic_crs_coordinate_metadata",
  "coordinate_operation", "coordinate_operation_next",
  "coordinate_operation_end",
  "opt_parameter_or_parameter_file_list_opt_interpolation_crs_opt_operation_accuracy_opt_separator_scope_extent_identifier_remark",
  "operation_keyword", "operation_name", "operation_version",
  "operation_version_keyword", "operation_version_text", "source_crs",
  "source_crs_keyword", "target_crs", "target_crs_keyword",
  "interpolation_crs", "interpolation_crs_keyword", "operation_accuracy",
  "operation_accuracy_keyword", "point_motion_operation",
  "point_motion_operation_next", "point_motion_operation_end",
  "opt_parameter_or_parameter_file_list_opt_operation_accuracy_opt_separator_scope_extent_identifier_remark",
  "point_motion_keyword", "concatenated_operation",
  "concatenated_operation_next", "concatenated_operation_end", "step",
  "opt_concatenated_operation_end", "concatenated_operation_keyword",
  "step_keyword", "bound_crs", "bound_crs_keyword",
  "abridged_coordinate_transformation",
  "abridged_coordinate_transformation_next",
  "abridged_coordinate_transformation_end",
  "opt_end_abridged_coordinate_transformation",
  "abridged_transformation_keyword", "abridged_transformation_parameter", YY_NULLPTR
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_int16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
     295,   296,   297,   298,   299,   300,   301,   302,   303,   304,
     305,   306,   307,   308,   309,   310,   311,   312,   313,   314,
     315,   316,   317,   318,   319,   320,   321,   322,   323,   324,
     325,   326,   327,   328,   329,   330,   331,   332,   333,   334,
     335,   336,   337,   338,   339,   340,   341,   342,   343,   344,
     345,   346,   347,   348,   349,   350,   351,   352,   353,   354,
     355,   356,   357,   358,   359,   360,   361,   362,   363,   364,
     365,   366,   367,   368,   369,   370,   371,   372,   373,   374,
     375,   376,   377,   378,   379,   380,   381,   382,   383,   384,
     385,   386,   387,   388,   389,   390,   391,   392,   393,   394,
     395,   396,   397,   398,   399,   400,   401,   402,   403,   404,
     405,   406,   407,   408,   409,   410,    46,    69,    49,    50,
      51,    43,    45,    58,    84,    90,    91,    40,    93,    41,
      44
};
# endif

#define YYPACT_NINF (-1291)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-691)

#define yytable_value_is_error(Yyn) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
    2165, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291,
   -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291,
   -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291,
   -1291, -1291, -1291, -1291, -1291, -1291, -1291,   115, -1291, -1291,
   -1291,   157, -1291, -1291, -1291,   157, -1291, -1291, -1291, -1291,
   -1291, -1291,   157,   157, -1291,   157, -1291,   -25,   157, -1291,
     157, -1291,   157, -1291, -1291, -1291,   157, -1291,   157, -1291,
     157, -1291,   157, -1291,   157, -1291,   157, -1291,   157, -1291,
     157, -1291, -1291, -1291, -1291, -1291, -1291, -1291,   157, -1291,
   -1291, -1291, -1291, -1291, -1291,   157, -1291,   157, -1291,   157,
   -1291,   157, -1291,   157, -1291,   157, -1291, -1291, -1291,     0,
       0,     0,     0,     0, -1291,    78,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,  1039,
       0,     0,     0,   140, -1291, -1291,   -25, -1291,   -25, -1291,
     -25,   -25, -1291,   -25, -1291, -1291, -1291,   157, -1291,   -25,
     -25, -1291,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25, -1291,   -25, -1291,   -25, -1291, -1291, -1291, -1291,   -13,
   -1291, -1291, -1291, -1291, -1291,    66,   141,   179, -1291, -1291,
   -1291, -1291,   341, -1291,   -25, -1291,   -25,   -25,   -25, -1291,
     -25,   157,  1257,   331,   341,   298,   298,   895,     0,   367,
     393,   136,   264,   447,   341,    83,   289,   341,   151,   341,
     139,   315,   341,   226,   106, -1291, -1291, -1291,   497,    49,
   -1291, -1291,    49, -1291, -1291,    49, -1291, -1291,   314,  1039,
   -1291, -1291, -1291, -1291, -1291, -1291, -1291,   508, -1291, -1291,
   -1291, -1291,   211,   223,   228,   895, -1291,   -25, -1291,   -25,
     157,   -25, -1291, -1291, -1291, -1291, -1291,   157,   -25,   157,
     -25, -1291,   157,   157,   -25,   -25, -1291, -1291, -1291, -1291,
     -25,   -25,   -25,   -25, -1291,   -25, -1291,   -25,   -25,   -25,
   -1291, -1291, -1291, -1291,   157,   157, -1291, -1291,   -25,   157,
   -1291, -1291,   157,   -25,   -25, -1291,   -25, -1291, -1291,   157,
   -1291, -1291,   -25,   -25,   157,   -25,   157, -1291, -1291,   -25,
     -25,   157,   -25,   -25, -1291, -1291,   -25,   -25,   157, -1291,
   -1291,   -25,   -25,   157, -1291, -1291,   -25,   -25,   157,   -25,
     157, -1291, -1291,   -25,   157, -1291,   -25, -1291, -1291, -1291,
   -1291,   157, -1291,   -25,   157,   -25,   -25,   -25,   -25,   -25,
   -1291,   -25,   157,   341, -1291,   420,   508, -1291, -1291,   409,
     341,   121, -1291,   341,     0,   334,     0,    73,   355,    90,
       0,     0,   339,   339,    73,    90,   339,   339,   895,   420,
     341,   395,     0,     0,   198,   341,     0,     0,   116,   407,
     339,     0,   413, -1291,    95,     0,   413,   508,   407,   339,
       0, -1291,   413,   407,   339,     0,   407,   339,     0, -1291,
   -1291,   425,    65, -1291,     0,   339,     0,   106,   508,   140,
   -1291,     0,   314,   140, -1291,   410,   140, -1291,   314,   385,
    1039, -1291,   508, -1291, -1291, -1291, -1291, -1291, -1291, -1291,
   -1291,   -25,   -25,   157, -1291,   157, -1291, -1291,   -25,   -25,
     157, -1291, -1291,   -25,   -25,   -25,   -25, -1291,   -25,   157,
   -1291, -1291, -1291,   157,   341,   -25, -1291,   -25,   -25, -1291,
     -25,   157,   -25,   -25,   341,   -25,   -25, -1291,   -25,   -25,
     895,   341, -1291,   -25,   -25,   -25, -1291,   -25,   -25,   157,
   -1291, -1291,   -25,   -25,   -25,   157,   341,   -25,   -25,   -25,
     -25,   -25, -1291,   341,   -25,   228,   341,   341, -1291, -1291,
   -1291, -1291,   157,   -25,   -25,   -25,   341,   -25,   -25,   341,
     -25,   -25, -1291, -1291,   285, -1291,   341,   -25, -1291,   341,
     -25,   -25,   -25,   228,   341, -1291,   341,   -25, -1291,   -25,
     157,   -25, -1291,   -25,   157,   341, -1291,   440,   446,     0,
       0, -1291,   413, -1291,  1350,   413,   341, -1291,   331,    90,
     612,   341,   508,  1113, -1291,   407,    81,    81,   407,     0,
     407,    90, -1291,   407,   407,   336,   341,   353, -1291, -1291,
   -1291,   407,    81,    81, -1291, -1291,     0,   341,   426,   407,
    1113, -1291,   407,   133, -1291, -1291,   413, -1291, -1291,   508,
   -1291, -1291,  1386,   407,    64, -1291, -1291,   407,   132, -1291,
     407,    37, -1291, -1291,   508, -1291, -1291,   508, -1291, -1291,
   -1291,   407,   393,  1734,   341,   508, -1291, -1291,   410,  1568,
     341,     0,   452,   734,   341,     0, -1291,   -25, -1291, -1291,
     341, -1291,   341, -1291,   -25, -1291,   341, -1291,   -25, -1291,
     -25,   341, -1291, -1291, -1291,   157, -1291,   228,   341, -1291,
   -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291,   -25, -1291,
     -25,   -25,   -25,   -25,   341, -1291,   -25,   341,   341,   341,
     341, -1291, -1291,   -25,   -25,   157, -1291, -1291, -1291,   -25,
     157,   341,   -25,   -25,   -25,   -25, -1291,   -25, -1291,   -25,
     341,   -25,   341,   -25,   -25,   -25, -1291,   -25, -1291, -1291,
   -1291, -1291,   -25,   -25,   -25,   341,   -25,   341,   -25,   341,
     -25,   330,   340, -1291,  1023,   341, -1291, -1291, -1291, -1291,
     -25, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291,
   -1291, -1291,   -25,   157,   -25,   157, -1291,   -25,   157,   -25,
     157,   -25,   157,   -25,   157,   -25, -1291,   157,   -25, -1291,
   -1291,   -25, -1291, -1291, -1291,   157,   -25,   -25,   157,   -25,
     157, -1291, -1291,   -25, -1291,   157, -1291, -1291,   -25,   446,
   -1291, -1291, -1291, -1291, -1291, -1291,     0,   508, -1291,   455,
      73,   104,   341,    73,   341, -1291,   410, -1291, -1291, -1291,
   -1291, -1291, -1291,     0, -1291,     0, -1291,    73,    82,   341,
      73,   341,   420,   628, -1291,   455, -1291,   116,   341, -1291,
     455,   455,   455,   455, -1291,   341, -1291,   341, -1291,   341,
   -1291,   508, -1291, -1291,   508,   508, -1291,   358, -1291, -1291,
   -1291, -1291,   395,   325,   502,   377, -1291,     0,   442, -1291,
       0,   449, -1291,  1350,   323, -1291,  1350,   379, -1291,   425,
   -1291,   386, -1291,   875,   341,     0, -1291, -1291,     0, -1291,
    1350,   413,   341,   346,    99, -1291, -1291, -1291, -1291,   -25,
   -1291, -1291, -1291, -1291,   -25,   -25,   -25,   -25, -1291,   -25,
   -1291,   -25, -1291,   -25,   -25,   -25,   -25, -1291,   -25,   -25,
   -1291,   -25, -1291, -1291,   -25,   -25,   -25,   -25, -1291,   -25,
     -25,   -25,   -25, -1291, -1291, -1291, -1291,   382,   358, -1291,
    1023,   508, -1291,   -25, -1291,   -25, -1291,   -25, -1291,   -25,
   -1291, -1291,   341,   -25,   -25,   -25, -1291,   341,   -25,   -25,
   -1291,   -25,   -25, -1291,   -25, -1291, -1291,   -25, -1291,   341,
   -1291, -1291,   -25,   -25,   -25,   157,   -25, -1291,   -25,   -25,
     341, -1291, -1291, -1291, -1291, -1291, -1291,   341,   -25,   -25,
     341,   341,   341, -1291, -1291,   341,   194,   341,   895,   895,
     341, -1291,   353, -1291, -1291,   341,   825,   341,   341,   341,
     341,   341,   341,   341, -1291, -1291,   508, -1291, -1291, -1291,
     990,   341, -1291,   503, -1291, -1291,   449, -1291,   323, -1291,
   -1291, -1291,   323, -1291, -1291,  1350, -1291,  1350,   425, -1291,
   -1291, -1291,  1206, -1291,  1039, -1291,   420,     0, -1291,   -25,
     971,   341,   410, -1291, -1291,   -25, -1291, -1291,   -25,   -25,
     -25, -1291, -1291,   -25,   -25, -1291,   -25, -1291, -1291, -1291,
   -1291, -1291,   -25, -1291,   157,   -25, -1291,   -25, -1291,   -25,
     -25,   -25,   -25, -1291, -1291,   925, -1291, -1291,   157, -1291,
     341,   -25,   -25,   -25, -1291,   -25,   -25,   -25,   -25, -1291,
     -25, -1291,   -25, -1291, -1291,   341,   -25,   341,   -25, -1291,
     -25,   452, -1291,   157,   -25,   -25, -1291,   545, -1291, -1291,
   -1291,   341,   341, -1291, -1291,     0, -1291,   545,   545,   545,
     545,   545,   783, -1291,  1113, -1291,   436,   640,   535,   323,
   -1291, -1291, -1291, -1291,  1350,   241,   341, -1291, -1291, -1291,
     704,   341,   341,   157,     0, -1291, -1291, -1291,   -25,   157,
   -1291, -1291,   -25,   -25,   -25,   157,   -25,   -25,   -25,   157,
     559,   783, -1291,   -25,   -25, -1291,   -25, -1291, -1291,   -25,
   -1291,   -25, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291,
     -25,   -25, -1291,   157, -1291, -1291,   346,   -25,  1267, -1291,
       0,   895,  1084, -1291,  1448, -1291,     0,  1318, -1291, -1291,
     844, -1291,     0, -1291,   640,   535,   535, -1291,  1350, -1291,
   -1291,     0,   341,   420, -1291, -1291, -1291, -1291, -1291, -1291,
     157, -1291, -1291,   -25, -1291, -1291, -1291, -1291,   157, -1291,
     157,   -25, -1291,   -25,   -25, -1291,   -25,   -25, -1291, -1291,
     -25,   -25,   157, -1291,   -25,   -25, -1291,   -25,   -25, -1291,
     -25,   -25,   -25, -1291, -1291, -1291, -1291,   341,   -25,   -25,
     -25,     0, -1291,     0,     0,   520, -1291,   520, -1291,  2271,
     341,  1287, -1291,  1287, -1291,     0,   734,  2416, -1291, -1291,
   -1291,  2583,   535, -1291,   895,   341,  1354,   341,   341, -1291,
     -25,   -25,   -25, -1291, -1291,   -25, -1291, -1291, -1291, -1291,
   -1291, -1291, -1291,   -25, -1291, -1291, -1291, -1291, -1291, -1291,
   -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291,
   -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291,   -25,   -25,
   -1291, -1291, -1291, -1291, -1291,   341, -1291,   -25, -1291,   -25,
   -1291,   -25, -1291,   -25, -1291,   -25,   -25,   -25,   -25,   -25,
     341, -1291,   -25, -1291,   -25, -1291, -1291,   -25,   157, -1291,
   -1291,   341,   895,   341,   611,   611,   843,   843, -1291,   595,
   -1291, -1291,   341,   281,   341,   443,   611,   521,   521, -1291,
      96, -1291, -1291,   346, -1291,   -25, -1291, -1291, -1291,   -25,
     -25, -1291,   -25,   157,   -25,   157, -1291, -1291,   -25,   -25,
   -1291,   -25,   157,   -25, -1291, -1291, -1291,   -25,   -25, -1291,
     -25, -1291,   -25,   157,   -25,   157, -1291,   -25,   -25, -1291,
     -25, -1291, -1291,   -25, -1291,   -25,   -25, -1291,   -25, -1291,
     -25,   -25, -1291,   -25, -1291,   -25, -1291,   341,   341, -1291,
   -1291,   726, -1291,  1350,   729, -1291,   508, -1291, -1291,   726,
   -1291,  1350,   729, -1291, -1291,   621, -1291,   549, -1291,    62,
   -1291,  1350, -1291,  1350, -1291, -1291,   283, -1291, -1291,   460,
   -1291, -1291, -1291,   460, -1291, -1291, -1291, -1291,   -25, -1291,
     -25,   -25,   -25,   -25,   341,   -25,   -25,   341,   -25,   -25,
     -25,   -25,   -25,   341,   341,   -25,   -25,   -25, -1291, -1291,
     729, -1291,   542, -1291, -1291, -1291,   729, -1291, -1291, -1291,
      62, -1291, -1291, -1291,    69, -1291, -1291, -1291, -1291, -1291,
   -1291,   -25,   341,   -25,   -25, -1291,   -25,   157, -1291, -1291,
   -1291,    69, -1291, -1291,   630,   -25, -1291, -1291,   341, -1291,
   -1291
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_int16 yydefact[] =
{
       0,   454,   441,   430,   440,   161,   475,   498,   432,   530,
     533,   658,   702,   737,   740,   559,   552,   391,   614,   540,
     537,   549,   547,   669,   724,   431,   456,   476,   433,   455,
     531,   535,   534,   560,   541,   538,   550,     0,     4,     5,
       2,     0,    13,   381,   382,     0,   641,   420,   418,   419,
     421,   422,     0,     0,     3,     0,    12,   451,     0,   643,
       0,    11,     0,   645,   512,   513,     0,    14,     0,   647,
       0,    15,     0,   649,     0,    16,     0,   651,     0,    17,
       0,   642,   595,   593,   594,   596,   597,   644,     0,   646,
     648,   650,   652,    19,    18,     0,     7,     0,     8,     0,
       9,     0,    10,     0,     6,     0,     1,    73,    74,     0,
       0,     0,     0,     0,    77,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    78,   162,     0,   392,     0,   429,
       0,     0,   442,     0,   446,   447,   452,     0,   457,     0,
       0,   499,     0,     0,   458,     0,   542,     0,   542,     0,
     554,   615,     0,   659,     0,   670,   684,   671,   685,   672,
     673,   687,   674,   675,   676,   677,   678,   679,   680,   681,
     682,   683,     0,   667,     0,   703,     0,     0,     0,   708,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    75,    76,   666,     0,     0,
     691,   693,     0,   715,   717,     0,   725,   727,     0,     0,
      40,    20,    37,    38,    39,    41,    42,     0,   163,    21,
      22,    26,     0,    25,    35,     0,   164,   154,   396,     0,
       0,     0,   383,   490,   491,   404,   435,     0,     0,     0,
       0,   434,     0,     0,     0,     0,   599,   602,   600,   603,
       0,     0,     0,     0,   443,     0,   448,     0,   458,     0,
     477,   478,   479,   480,     0,     0,   502,   501,   495,     0,
     630,   517,     0,     0,     0,   516,     0,   626,   627,     0,
     467,   470,   190,   459,     0,   460,     0,   532,   633,     0,
       0,     0,   190,   543,   539,   636,     0,     0,     0,   548,
     639,     0,     0,     0,   566,   562,   190,   190,     0,   190,
       0,   553,   620,     0,     0,   653,     0,   654,   661,   662,
     668,     0,   705,     0,     0,     0,     0,     0,     0,     0,
     710,     0,     0,     0,    34,    27,     0,    33,    23,     0,
       0,     0,   385,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    27,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   462,     0,     0,     0,     0,     0,     0,
       0,   544,     0,     0,     0,     0,     0,     0,     0,   558,
     557,     0,     0,   555,     0,     0,     0,     0,     0,     0,
     692,     0,     0,     0,   716,     0,     0,   726,     0,     0,
       0,   707,     0,    29,    31,    28,    36,   168,   171,   165,
     166,   155,   158,     0,   160,     0,   153,   400,     0,   386,
       0,   388,   397,   394,   386,     0,     0,   406,   410,     0,
     238,   428,   208,     0,     0,     0,   492,     0,     0,   573,
       0,     0,     0,     0,     0,     0,     0,   444,   437,   190,
       0,     0,   453,     0,     0,     0,   508,   190,   495,     0,
     494,   503,   190,     0,     0,     0,     0,     0,     0,   190,
     190,   461,   468,     0,   190,   471,     0,     0,   204,   205,
     206,   207,     0,     0,     0,   190,     0,     0,     0,     0,
       0,     0,    50,   563,    48,   564,     0,   190,   567,     0,
       0,     0,   655,   663,     0,   706,     0,     0,   576,   719,
       0,     0,   749,    80,     0,     0,    32,     0,     0,     0,
       0,   390,     0,   389,     0,     0,     0,   384,     0,     0,
       0,     0,     0,     0,   423,     0,     0,     0,     0,     0,
       0,     0,   425,     0,     0,     0,     0,     0,   450,    24,
     445,     0,     0,     0,   496,   497,     0,     0,     0,     0,
       0,   514,     0,     0,   191,   464,     0,   466,   463,   472,
     469,   536,     0,     0,     0,   545,   546,     0,     0,   551,
       0,     0,    44,    58,     0,    45,    49,     0,   561,   556,
     565,     0,     0,     0,     0,   664,   660,   704,     0,     0,
       0,     0,     0,     0,     0,     0,   709,   156,   159,   169,
       0,   172,     0,   402,   386,   401,     0,   398,   394,   393,
       0,     0,   415,   416,   411,     0,   403,   407,     0,   239,
     240,   241,   242,   243,   244,   245,   246,   247,     0,   427,
       0,   607,     0,   607,     0,   574,     0,     0,     0,     0,
       0,   199,   198,   190,   190,     0,   436,   197,   196,   190,
       0,     0,     0,   482,     0,   482,   509,     0,   500,     0,
       0,     0,     0,     0,   190,   190,   473,     0,   248,   249,
     250,   251,     0,     0,     0,     0,   190,     0,   190,     0,
     190,    48,     0,    59,     0,     0,   621,   622,   623,   624,
       0,   174,   100,   133,   136,   144,   148,    98,   657,    82,
      88,    89,    93,     0,    85,     0,    92,    85,     0,    85,
       0,    85,     0,    85,     0,    85,    84,     0,   655,   640,
     665,   695,   591,   714,   723,     0,   719,   719,     0,    80,
       0,   718,   577,   413,   738,     0,    81,   739,     0,     0,
     167,   170,   387,   399,   395,   424,     0,   408,   405,     0,
       0,     0,     0,     0,     0,   598,     0,   601,   426,   604,
     605,   439,   438,     0,   449,     0,   474,     0,     0,     0,
       0,     0,    27,     0,   515,     0,   625,     0,     0,   465,
       0,     0,     0,     0,   631,     0,   634,     0,   637,     0,
      46,     0,    43,    68,     0,     0,    53,    71,    55,    66,
      67,   613,     0,     0,     0,     0,    91,     0,     0,   117,
       0,     0,   118,     0,     0,   119,     0,     0,   120,     0,
      83,     0,   656,     0,     0,     0,   720,   721,     0,   722,
       0,     0,     0,     0,     0,   741,   743,   157,   417,   413,
     409,   252,   253,   254,   190,   607,   190,   190,   606,   607,
     611,   569,   202,     0,     0,   482,   190,   493,   190,   190,
     481,   482,   488,   510,   505,     0,   190,   190,   628,   190,
     190,   190,   190,   632,   635,   638,    52,    48,    71,    60,
       0,     0,    70,   617,    96,    85,    94,     0,    90,    85,
      87,   101,     0,    85,    85,    85,   134,     0,    85,    85,
     137,     0,    85,   145,     0,   149,   150,     0,    79,     0,
     712,   701,   695,   695,    80,     0,    80,   694,     0,     0,
       0,   414,   575,   731,   732,   729,   730,     0,   745,     0,
       0,     0,     0,   609,   608,     0,     0,     0,     0,     0,
       0,   486,     0,   483,   485,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    47,    69,     0,    54,    57,    72,
       0,     0,    95,     0,    86,    99,     0,   121,     0,   122,
     123,   132,     0,   124,   125,     0,   126,     0,     0,   173,
     696,   697,     0,   698,     0,   700,    27,     0,   713,     0,
       0,     0,     0,   742,   412,     0,   610,   612,   190,   569,
     569,   568,   203,   190,   190,   487,   190,   489,   188,   186,
     185,   187,   190,   511,     0,   190,   504,     0,   629,     0,
       0,     0,     0,    64,    56,     0,   619,   618,     0,   616,
       0,   102,   103,   104,   105,    85,    85,    85,    85,   138,
       0,   146,   142,   151,   152,     0,    80,     0,   579,   592,
     413,     0,   748,     0,   745,   745,   744,     0,   572,   570,
     571,     0,     0,   484,   506,     0,   507,     0,     0,     0,
       0,     0,     0,    63,     0,    97,     0,     0,     0,     0,
     127,   128,   129,   130,     0,     0,     0,   147,   699,   711,
       0,     0,     0,     0,     0,   747,   746,   258,   224,     0,
     192,   189,     0,   519,   229,     0,   211,    80,   235,     0,
      65,     0,    61,   106,   107,   108,   109,   110,   111,    85,
     139,     0,   143,   141,   589,   584,   585,   586,   587,   588,
     190,   190,   582,     0,   578,   590,     0,     0,     0,   210,
       0,     0,     0,   518,     0,   228,     0,     0,   209,   233,
       0,   234,     0,    62,     0,     0,     0,   131,     0,   581,
     580,     0,     0,    27,   183,   180,   179,   182,   200,   181,
       0,   201,   221,    85,   223,   380,   175,   177,     0,   176,
       0,   219,   227,   224,   215,   259,     0,   190,   528,   523,
      80,   524,     0,   232,   230,     0,   214,   211,    80,   237,
     235,     0,   112,   113,   114,   115,   140,     0,   194,   733,
     190,     0,   222,     0,     0,     0,   226,     0,   225,     0,
       0,     0,   520,     0,   522,     0,     0,     0,   213,   212,
     236,     0,     0,   135,     0,     0,     0,     0,     0,   218,
     413,     0,   194,   220,   216,   261,   321,   322,   323,   324,
     325,   326,   327,   263,   328,   329,   330,   331,   332,   333,
     334,   335,   336,   337,   338,   339,   340,   341,   342,   343,
     344,   345,   346,   347,   348,   349,   350,   351,   265,   267,
     352,   353,   354,   355,   356,     0,   260,   285,   184,   524,
     526,   524,   529,   413,   231,   314,   293,   295,   297,   299,
       0,   292,   309,   116,   190,   583,   736,    80,     0,   728,
     750,     0,     0,     0,     0,     0,     0,     0,   255,     0,
     521,   525,     0,     0,     0,     0,     0,     0,     0,   257,
       0,   195,   735,     0,   217,   190,   193,   358,   362,   190,
     190,   262,   190,     0,   190,     0,   264,   360,   190,   190,
     266,   190,     0,   190,   268,   373,   375,   190,   367,   286,
     367,   288,   190,     0,   190,     0,   527,   190,   367,   315,
     367,   317,   256,   190,   294,   190,   190,   296,   190,   298,
     190,   190,   300,   190,   310,   367,   313,     0,     0,   269,
     276,     0,   273,     0,     0,   275,     0,   277,   284,     0,
     281,     0,     0,   283,   287,     0,   291,     0,   289,     0,
     363,     0,   364,     0,   316,   320,     0,   318,   301,     0,
     303,   304,   305,     0,   307,   308,   311,   312,   733,   178,
     190,   190,     0,   190,     0,   190,   190,     0,   190,   190,
     190,   367,   190,     0,     0,   367,   190,   190,   734,   272,
       0,   270,     0,   274,   361,   280,     0,   278,   359,   282,
       0,   368,   369,   290,     0,   365,   372,   374,   319,   302,
     306,   190,     0,   190,   190,   377,   190,     0,   271,   357,
     279,     0,   370,   366,     0,   190,   378,   379,     0,   371,
     376
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
   -1291, -1291, -1291,  -224,  -241,  -164, -1291,   219,  -196,   249,
   -1291, -1291, -1291, -1291, -1291, -1291,   -81,  -333,  -633,  -113,
    -790,  -629, -1291, -1291, -1291, -1291, -1291, -1291, -1291,  -560,
    -300, -1291, -1291, -1291,  -857, -1291, -1291,  -286,   -45,  1494,
     498,  2252, -1291,  -755,  -593,  -620, -1069, -1291,  -208, -1291,
   -1291,  -207, -1291, -1291, -1291,  -203,  -359, -1291, -1291,  -787,
   -1291, -1291, -1291, -1291, -1291,  -783, -1291, -1291, -1291, -1291,
    -743, -1291, -1291, -1291,   677, -1291, -1291, -1291, -1291, -1291,
     102, -1291, -1291,  -503, -1291, -1291,  -750, -1291, -1291,  -434,
   -1291, -1291, -1291, -1291,  -575,  1610,  -479, -1290,  -623, -1291,
   -1291, -1291,  -767,  -935,   -62,   109,  -331, -1291,  -558, -1291,
   -1291, -1291,  -740,  -536, -1291, -1291, -1291, -1291,  -544,  -322,
    -487, -1291, -1291,   113,  -877,  -411,  -486,  -988,  -866, -1291,
   -1192,  -649, -1291, -1291, -1291, -1291,  -658, -1291, -1291, -1291,
   -1291,  -963,  -646, -1291,  -608, -1291,  -843, -1291, -1141, -1260,
   -1018, -1291, -1046, -1291,  -797, -1291, -1291,  -656, -1291,   731,
    -101,  -354,   524,  -416,    51,  -229,  -320,   107, -1291, -1291,
   -1291,   387, -1291,  -126, -1291,  -121, -1291, -1291, -1291, -1291,
   -1291, -1291,  -855, -1291, -1291, -1291, -1291,   638,   643,   658,
     664,  -278,   770, -1291, -1291,   -82,    80, -1291, -1291, -1291,
   -1291, -1291,  -111, -1291, -1291, -1291, -1291,    16, -1291,   835,
     479,   589, -1291, -1291,   408, -1291, -1291,   675, -1291, -1291,
   -1291,  -624, -1291, -1291, -1291,   605,   610,   195,  -170,     6,
     328, -1291, -1291, -1291, -1291, -1291, -1291, -1291,  -366,  -806,
    -962, -1291, -1291,   684,   688, -1291,   232, -1291,  -730,  -636,
   -1291, -1291, -1291,  -191, -1291,   693, -1291,  -163, -1291,   667,
     699, -1291,  -168, -1291,   700, -1291,  -176, -1291, -1291,   418,
   -1291, -1291, -1291, -1291, -1291,   746,  -305, -1291, -1291,  -377,
   -1291, -1291,  -789, -1291, -1291, -1291, -1291,  -785, -1291, -1291,
     706, -1291, -1291,   637, -1291,   661, -1291, -1291,   236,  -603,
     237,   240,   244,   738, -1291, -1291, -1291, -1291, -1291, -1291,
     739, -1291, -1291, -1291, -1291,   740, -1291, -1291,   743, -1291,
   -1291,   744, -1291, -1291,   748,  -175,  -351,   105, -1291, -1291,
   -1291, -1291, -1291, -1291, -1291, -1291, -1291, -1291,   878, -1291,
     536,  -172, -1291,  -112,  -213, -1291, -1291,   -78, -1291,    -7,
   -1291, -1291, -1291,  -837, -1291, -1291, -1291,   539,    18,   881,
   -1291, -1291,   538, -1093,  -578, -1291, -1019,   894, -1291, -1291,
   -1291,   -73,  -297, -1291, -1291
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
      -1,    37,    38,    39,   237,   645,   239,   903,   240,   480,
     241,   242,   433,   434,   243,   357,   244,   245,   921,   614,
     523,   615,   524,   722,   917,   616,   836,   997,   617,   837,
     920,  1064,  1065,  1151,   838,   839,   840,   922,   109,   217,
     392,   466,   949,   634,   776,   846,   739,   740,   741,   742,
     743,   744,   745,   932,  1070,   746,   747,   748,   937,   749,
     750,   941,  1080,  1161,  1247,   751,  1126,   752,   944,  1082,
     753,   754,   947,  1085,   499,   360,    41,   136,   247,   441,
     442,   443,   640,   444,   445,   642,   756,   757,  1215,  1216,
    1217,  1218,  1053,  1054,   897,   393,   684,  1219,  1275,   690,
     685,  1220,   893,  1043,   507,   461,   508,   462,  1188,  1258,
    1221,  1280,  1222,  1179,   509,  1185,   510,   511,  1191,   512,
     712,   713,   714,   884,  1138,  1144,  1148,  1139,  1226,  1325,
    1381,  1390,  1326,  1399,  1340,  1414,  1419,  1341,  1424,  1364,
    1409,  1327,  1382,  1383,  1391,  1392,  1384,  1385,  1401,  1446,
    1402,  1403,  1404,  1405,  1516,  1517,  1528,  1224,    42,   256,
     362,   553,    44,   363,   257,   138,   249,   556,   250,   453,
     648,   449,   450,   646,   644,   258,   259,   458,   459,   658,
     561,   654,   872,   655,   879,    46,    47,    48,    49,    50,
      51,   464,   140,    52,    53,   260,   251,   576,    55,   143,
     275,   478,   465,   147,   277,   481,    56,   261,    58,   149,
     204,   303,   304,   503,   305,   306,   506,    59,    60,   279,
     280,   809,   281,   282,   283,   262,   263,   467,   899,   963,
     385,    62,   152,   288,   289,   492,   488,   987,   765,   697,
     904,  1055,    63,    64,    65,   294,   496,  1183,  1264,  1231,
    1232,  1333,    66,    67,    68,    69,    70,    71,    72,   207,
      73,    74,    75,    76,    77,    78,    79,   212,    80,   327,
     328,   526,   329,   330,   529,   964,   977,   471,   676,   968,
     540,   773,   766,  1131,  1171,  1172,  1173,   767,   768,  1090,
      81,    82,    83,   264,    84,   265,    85,    86,   266,   792,
     267,   268,   269,    87,    88,   162,   333,  1001,   334,   730,
      89,   296,   297,   298,   299,    90,   310,   311,    91,   317,
     318,    92,   322,   323,    93,    94,   336,   624,    95,   164,
     340,   341,   534,    96,   182,    97,   183,   184,   965,   220,
     221,   864,    99,   186,   343,   344,   536,   345,   191,   351,
     352,   954,   955,   769,   770,   100,   223,   224,   630,   966,
     102,   226,   227,   967,  1277,   103,   775,   337,   105,   543,
     875,   876,  1031,   544,  1095
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
     110,   274,   689,   356,   146,   353,    61,   111,   112,   346,
     113,   295,   348,   116,   869,   117,    57,   118,   489,   187,
     188,   119,   435,   120,   970,   121,   956,   122,   238,   123,
     738,   124,   551,   125,   321,   126,   764,   194,   894,   335,
     316,   448,   309,   127,  1044,   638,   435,   463,   539,   358,
     128,    45,   129,   463,   130,   190,   131,   724,   132,   958,
     133,   933,   959,   998,  1088,   934,   532,   495,   938,   945,
     794,   811,  1133,  1408,   952,   270,   668,     1,   953,     5,
      54,   293,     5,  1202,   144,     1,     5,    15,   144,   687,
     189,   835,   830,   926,   688,   930,   144,   474,   930,     5,
     557,   930,   198,   701,   930,   935,   460,   930,   939,  1212,
     144,   942,     5,     5,   271,   106,     3,   325,  1378,   301,
    1145,     5,  1145,  1149,     6,   538,     2,   849,   342,   852,
      19,   855,    33,   858,     8,   860,     4,   455,   255,   286,
    1448,     9,    17,  1396,   347,   114,   229,   349,  1455,    19,
    1457,  1515,    10,    14,   134,   308,   354,  -686,   493,    16,
      26,    34,   287,  1386,    29,  1467,  1485,   145,    26,    10,
      18,   145,    29,    20,  1417,    22,  1395,  1396,   342,   145,
      34,   189,   477,   248,   447,  1086,  1212,  1039,  1212,    15,
     255,  1040,    25,   145,    27,   439,    28,   494,    30,  1023,
      21,  1025,  1149,   486,    35,   364,   545,   486,  1113,    31,
      32,     5,   366,   320,   368,     5,  1072,   370,   371,    21,
    1073,  1503,  1411,  1075,   315,  1508,    31,    32,  1083,  1426,
    1143,    36,  1146,   762,    33,  1132,  -688,   463,   650,   382,
     383,  1250,   335,   463,   386,  1094,  1227,   387,   681,   463,
      36,   761,   682,   292,   391,  1152,   930,  1348,   930,   395,
    1074,   397,   930,  1076,   599,  1077,   400,   495,   699,  1078,
     951,   980,   758,   405,   918,   436,   877,   985,   408,   278,
    1427,   651,   972,   411,   579,   414,   975,   996,   994,   416,
     782,   332,   625,   678,  1193,  1208,   418,  1208,     5,   421,
      10,  1223,     1,  1378,  1335,  1002,     5,   430,  1342,  1004,
    1237,  -689,   300,  1007,  1009,  1010,   505,   253,  1013,  1014,
    1235,   255,  1016,   107,   108,    17,  1241,   254,  1177,  1153,
     522,  1128,     5,  1154,  1156,   290,     2,   533,  1142,  1344,
       5,   516,     5,   681,   519,   347,     4,   682,   349,  -690,
     731,   546,   731,     5,   194,   255,   350,    31,    32,    17,
     687,  1395,  1396,  1395,  1396,   688,   324,   325,   355,   930,
       5,     7,     2,  1155,  1157,  1158,  1159,  1211,   736,  1211,
     -30,   737,     4,   456,   231,    26,    12,   483,   469,    29,
     473,   834,  1189,   248,     5,   469,     5,   447,   549,  1480,
     550,  1480,   704,  1482,   731,   554,   731,  1066,   733,   734,
    1480,  1242,   253,   457,   562,   537,   787,  1375,   563,   891,
       7,   541,   254,   460,    24,  1351,   569,  1479,  1214,  1479,
       5,   735,   736,   542,  1248,   720,   538,  1092,  1479,  1347,
     718,   716,   437,   438,   586,   670,   672,   612,   335,   613,
     590,  1243,  1244,  1245,  1514,  1120,  1121,  1122,  1123,     5,
       5,   692,   694,  1377,     5,  1378,     5,   602,   734,   731,
     300,   301,   703,   437,   734,  1262,   731,     5,  1362,   435,
     438,   657,  1378,  1269,   671,   673,  1281,  1282,   248,   551,
     735,   736,   -51,   905,   613,   631,   735,   736,   774,   635,
     693,   695,   612,   735,   736,  1214,   674,  1214,   677,   215,
     216,   679,   680,  1393,  1393,  1283,  1400,  1284,   706,   691,
    1410,   832,  1416,   778,  1421,  1421,  1230,  1425,  1234,  1343,
     702,  1238,   732,   721,   733,   734,   723,     5,     5,  1197,
     948,   715,  1387,  1378,   760,   717,   613,   731,   719,   687,
     732,   733,   734,  1137,   688,   115,  1204,   735,   736,   725,
    1205,  1206,  1207,   134,   230,   231,   468,   232,   233,   234,
     235,   236,   338,   339,   735,   736,   737,   484,   485,   134,
     230,   235,   236,   232,   233,   234,   498,   834,  1471,  1229,
     736,  1233,  1372,  1252,  1236,   514,  1476,  1239,   479,  1360,
     518,  1361,  1204,   521,   432,  1210,  1205,  1206,  1207,   831,
     786,   531,     5,   881,   882,   883,  1486,  1378,  1204,  1209,
    1487,   999,  1205,  1206,  1207,  1329,   907,  1331,     5,  1395,
    1396,  1377,   995,  1378,   192,   924,   193,   927,   195,   196,
     803,   197,   928,   723,  1071,   805,  1162,   199,   200,   637,
     201,   202,   203,   205,   206,   208,   206,   210,   211,  1353,
     213,   969,   214,   230,  1093,  1209,   232,   233,   234,  1067,
    1330,   652,   653,  1334,   669,  1395,  1396,    40,  1068,  1268,
     887,  1209,   218,  1346,   219,   222,   225,  1256,   228,   940,
     248,   447,   943,   435,   735,   736,  1270,   898,   844,  1147,
     847,  1395,  1396,   850,  1240,   853,   880,   856,  1394,   859,
    1422,  1164,   861,  1526,  1527,  1165,  1166,  1167,  1168,  1415,
     865,     5,   -59,   868,  1525,   870,   291,   -59,   -59,   -59,
     873,    43,   834,  1204,  1099,  1100,  1204,  1205,  1206,  1207,
    1205,  1206,  1207,     5,   960,   359,     5,   361,  1378,   365,
     916,     5,   454,   723,   919,   784,   367,   380,   369,  1420,
    1420,   731,   372,   373,   732,   733,   734,   165,   374,   375,
     376,   377,   166,   378,  1169,   379,   203,   381,   522,   834,
    1020,  1021,  1042,  1042,   866,   867,   384,   167,   735,   736,
     737,   388,   389,   168,   390,   313,  1209,  1135,  1136,  1209,
    1087,   394,   501,   396,   169,   284,   885,   398,   399,   889,
     285,   402,  1046,   170,   403,   404,   585,   171,   834,   406,
     407,   700,   172,   895,  1112,   209,   901,   412,   173,   174,
     527,   415,  1048,   272,   417,   175,  1049,  1050,  1051,   723,
     919,   419,     5,   422,   423,   425,   426,   428,   923,   429,
    1204,  1079,  1137,  1081,  1205,  1206,  1207,   273,   726,   727,
       5,     5,   728,   862,  1387,  1378,   729,   176,   177,   178,
     435,   731,   179,   180,   732,   733,   734,   181,    98,   420,
     302,   101,   141,   312,   486,   424,   427,   150,   326,   153,
    1488,   155,     5,   157,   104,   159,  1033,     0,   735,   736,
     737,     0,   731,     0,     0,   732,   733,   734,     0,     0,
    1024,     0,     0,  1209,   762,  1063,     0,  1512,   950,   763,
    1380,  1380,  1389,  1389,     0,  1398,     0,     0,   906,   735,
     736,   737,  1380,   909,   910,   911,   912,   522,   230,   547,
     548,   232,   233,   234,   235,   236,   361,   552,   833,     0,
       0,   555,   552,   558,   559,   154,   560,   156,     0,   158,
    1160,   160,     0,   565,     0,   566,   567,     0,   568,     0,
     570,   571,     0,   573,   574,     0,   575,   577,     0,     0,
     486,   581,   582,   583,   723,  1042,   384,     0,     5,     0,
       0,   588,   589,     0,     0,   592,   593,  1470,   731,   596,
    1473,   732,   733,   734,     0,  1475,   460,     5,  1478,  1105,
     762,   603,   604,  1481,     0,   607,   608,   731,   610,   611,
     732,   733,   734,  1114,     0,   735,   736,   737,   621,   622,
     623,  1150,     0,     0,  1246,   628,     0,   629,     0,   632,
       0,   633,     0,     0,   735,   736,   737,     0,  1134,     3,
     230,   231,     0,   232,   233,   234,  1511,     6,     0,     0,
       0,   487,  1513,     0,     0,     0,     0,     8,     0,     0,
     723,   500,     0,   504,     9,     0,     0,    11,  1042,   515,
     230,   231,     0,   232,   233,   234,   235,   236,  1176,     0,
     833,  1204,    16,     0,  1180,  1205,  1206,  1207,  1208,     0,
    1186,     5,     0,    18,  1192,     0,    20,     0,    22,     0,
       0,   731,     0,     0,   732,   733,   734,     0,   470,   472,
       0,  1228,   475,   476,     0,    25,     0,    27,  1201,    28,
       0,    30,     0,     0,     0,   779,   497,    35,   735,   736,
     737,     0,   552,     0,     0,   513,   555,     0,   558,     0,
     517,     0,     0,   520,  1209,     0,  1042,     0,     0,     0,
       0,   530,     0,     0,     0,  1251,   789,     0,   790,   791,
     793,   791,     0,  1253,   796,  1254,     0,     0,   230,     0,
    1211,   232,   233,   234,   235,   236,   832,  1265,   833,     0,
     807,   808,   810,   808,     0,   812,     0,   813,     0,   815,
       0,   817,     0,     0,     0,   820,     0,     0,     0,     0,
     821,   822,   823,   659,   660,   661,   662,   663,   664,   665,
     666,   667,     0,     5,     0,     0,     0,     0,   842,   643,
       0,     0,   647,   731,     0,     0,   732,   733,   734,     0,
     843,     0,   845,     0,     0,   848,     0,   851,     0,   854,
     763,   857,   683,   857,     0,     0,   623,     0,     0,   863,
     735,   736,   737,     0,   629,   629,     0,   633,     0,  1472,
       0,   871,     0,   705,  1204,  1137,   874,  1477,  1205,  1206,
    1207,  1208,     0,     0,     5,     0,     0,  1483,     0,  1484,
       0,     0,     0,     0,   731,     0,     0,   732,   733,   734,
     755,     0,     0,  1373,     5,     0,   755,     0,     0,     0,
     755,     0,     0,     0,   731,     0,     0,   732,   733,   734,
       0,   735,   736,   737,  1228,  1204,  1137,     0,     0,  1205,
    1206,  1207,  1208,     0,     0,     5,     0,  1209,  1433,     0,
    1436,   735,   736,   737,     0,   731,     0,  1441,   732,   733,
     734,     0,  1210,     0,     0,  1474,     0,     0,  1451,     0,
    1453,     0,     0,  1211,     0,     0,     0,     0,     0,     0,
       0,     5,   735,   736,   737,     0,     0,   871,     0,     0,
       0,   731,     0,   791,   732,   733,   734,   791,  1209,   976,
       0,   978,   979,   808,     0,     0,   982,     0,   763,   808,
     774,     0,   986,   813,     0,     0,     0,     0,   735,   736,
     737,   134,   230,   231,  1211,   232,   233,   234,   235,   236,
       0,  1000,     0,   857,     0,  1003,     0,   857,     0,     0,
       0,  1006,  1008,   857,     0,     0,  1012,   857,     0,  1015,
     857,     0,  1017,     0,     0,  1018,     0,     0,     0,     0,
     863,   863,  1022,     0,   633,  1204,  1026,  1027,     0,  1205,
    1206,  1207,  1208,     0,     0,     5,  1030,  1032,   886,     0,
       0,     0,  1524,     0,     0,   731,     0,     0,   732,   733,
     734,     0,     0,     0,     0,   896,   659,   660,   661,   662,
     663,   664,   665,   666,   667,   707,   708,   709,   710,   711,
       0,     0,   735,   736,   737,   230,   231,     0,   232,   233,
     234,   235,   236,     0,     0,     0,     0,     0,  1209,     0,
     925,     0,   929,     0,     0,   929,     0,  1091,   929,     0,
       0,   929,     0,  1097,   929,     0,     0,   976,   976,     0,
     755,     0,     0,     0,  1211,     0,     0,     0,   961,     0,
       0,     0,     0,     0,     0,  1107,     0,  1108,  1109,  1110,
    1111,     0,     0,     0,     0,     0,     0,     0,     0,  1116,
    1117,  1118,     0,  1119,   857,   857,   857,   486,  1124,     0,
    1125,     0,     0,     0,   633,     5,  1130,     0,   871,     0,
       0,     0,  1030,  1030,     0,   731,     0,     0,   732,   733,
     734,     0,     0,     0,     0,     0,     0,   762,     0,     0,
       0,     0,   763,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   735,   736,   737,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,  1178,     0,     0,     0,
    1181,  1182,  1184,     0,  1187,   633,  1190,     0,     0,     0,
       0,  1194,  1195,  1038,  1196,     0,     0,   857,     0,  1198,
       0,     0,     0,  1052,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,  1203,     0,   755,     0,     0,
       0,     0,     0,   929,     0,   929,     0,     0,   252,   929,
       0,     0,     0,     0,     0,     0,     0,     0,   307,   755,
       0,   314,     0,   319,     0,     0,   331,   755,     0,     0,
       0,   857,     0,     0,     0,     0,     0,     0,     0,  1255,
       0,  1178,  1257,     0,  1259,     0,     0,     0,  1261,  1263,
       0,     0,  1266,  1267,     0,  1187,   633,     0,  1190,  1271,
    1272,     0,     0,     0,     3,     0,  1274,  1276,     0,     0,
       0,     5,     6,     0,     0,     0,     0,     0,     0,     0,
       0,   731,     8,     0,   732,   733,   734,     0,     0,     9,
       0,     0,     0,     0,     0,     0,     0,     0,   871,  1352,
    1274,    14,     0,  1354,     0,     0,     0,    16,   735,   736,
     737,  1355,     0,     0,     0,     0,   929,     0,    18,     0,
       0,    20,     0,    22,     0,     0,     0,  1170,     0,     0,
       0,     0,     0,     0,     0,     0,  1356,  1357,     0,     0,
      25,     0,    27,     0,    28,  1359,    30,  1263,     0,  1263,
       0,   871,    35,  1363,  1365,  1366,  1367,  1368,     0,     0,
    1370,     0,     0,     0,     0,   633,     0,   431,     0,     0,
       0,     0,     0,     0,   446,  1213,     0,   451,     0,   755,
       0,   755,     0,     0,   755,     0,     0,   755,     0,     0,
       0,     0,     0,     0,   482,     0,     0,     0,     0,   490,
    1431,     0,  1434,     0,     0,     0,     0,     0,     0,  1439,
       0,  1442,     0,     0,     0,     0,  1445,     0,  1447,     0,
    1449,     0,     0,     0,     0,     0,  1445,     0,  1456,     0,
       0,     0,     0,  1459,     0,     0,     0,     0,  1463,     0,
       0,     0,   401,  1445,     0,     0,     0,     0,     0,     0,
       0,     0,  1213,     0,  1213,     0,   409,   410,   755,   413,
     755,     0,     0,   755,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   755,     0,     0,     0,     0,   564,     0,
       0,     0,     0,     0,     0,     0,  1276,     0,   572,  1490,
    1492,     0,     0,     0,  1496,   580,     0,  1500,     0,  1445,
    1504,     0,     0,  1445,     0,     0,     0,     0,     0,     0,
     591,     0,     0,     0,     0,     0,     0,   597,     0,     0,
     600,   601,     0,     0,     0,     0,     0,     0,     0,     0,
     606,     0,  1521,   609,     0,     0,     0,     0,     0,     0,
     618,     0,     0,   620,     0,     0,     0,     0,   626,     0,
     627,  1379,  1379,  1388,  1388,     0,  1397,     0,     0,   636,
    1407,     0,  1413,  1379,  1418,  1418,     0,  1423,     0,     0,
     649,     0,     0,     0,     0,   656,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     686,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   698,     0,     0,     0,     0,     0,     0,     0,   578,
       0,     0,     0,     0,     0,     0,     0,   584,     0,     0,
       0,     0,   587,     0,     0,     0,     0,     0,     0,   594,
     595,     0,     0,     0,   598,     0,     0,     0,   759,     0,
       0,     0,     0,     0,   771,   605,     0,     0,   777,     0,
       0,     0,     0,     0,   780,     0,   781,   619,     0,     0,
     783,     0,     0,     0,     0,   785,     0,     0,     0,     0,
       0,     0,   788,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   795,     1,
       2,   797,   798,   799,   800,     3,     0,     0,     0,     0,
       4,     0,     5,     6,     0,   806,     0,     0,     0,     0,
       7,     0,     0,     8,   814,     0,   816,     0,     0,     0,
       9,    10,     0,    11,     0,    12,     0,     0,     0,   824,
      13,   826,    14,   828,     0,    15,     0,     0,    16,   841,
       0,     0,     0,     0,     0,     0,    17,     0,     0,    18,
       0,    19,    20,    21,    22,     0,     0,     0,     0,     0,
       0,     0,    23,    24,     0,     0,     0,     0,     0,     0,
       0,    25,    26,    27,     0,    28,    29,    30,    31,    32,
      33,     0,    34,    35,    36,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   888,     0,   890,     0,
       0,     0,     0,   801,   802,     0,     0,     0,     0,   804,
       0,     0,     0,   900,     0,   902,     0,     0,     0,     0,
       0,     0,   908,     0,   818,   819,     0,     0,     0,   913,
       0,   914,     0,   915,     0,     0,   825,     0,   827,     0,
     829,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   957,     0,
       0,   135,   137,   139,   139,   142,   962,     0,   148,   139,
     151,   139,   148,   139,   148,   139,   148,   139,   148,   161,
     163,     0,   185,   185,   185,  1285,  1286,  1287,  1288,  1289,
    1290,  1291,  1292,  1293,  1294,  1295,  1296,  1297,  1298,  1299,
    1300,  1301,  1302,  1303,  1304,  1305,  1306,  1307,  1308,  1309,
    1310,  1311,  1312,  1313,  1314,  1315,  1316,  1317,  1318,  1319,
    1320,  1321,  1322,  1323,  1324,     0,  1005,     0,     0,     0,
       0,  1011,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,  1019,   246,     0,     0,     0,     0,     0,
     276,     0,     0,     0,  1028,     0,     0,     0,     0,     0,
       0,  1029,     0,     0,  1034,  1035,  1036,     0,     0,  1037,
       0,  1041,     0,     0,  1045,     0,     0,     0,     0,  1047,
       0,  1056,  1057,  1058,  1059,  1060,  1061,  1062,     0,     0,
       0,     0,     0,     0,   971,  1069,   973,   974,     0,     0,
       0,     0,     0,     0,     0,     0,   981,     0,   983,   984,
       0,     0,     0,     0,     0,     0,   988,   989,     0,   990,
     991,   992,   993,     0,     0,  1096,     0,     0,     0,     0,
       0,  1286,  1287,  1288,  1289,  1290,  1291,  1292,     0,  1294,
    1295,  1296,  1297,  1298,  1299,  1300,  1301,  1302,  1303,  1304,
    1305,  1306,  1307,  1308,  1309,  1310,  1311,  1312,  1313,  1314,
    1315,  1316,  1317,     0,  1115,  1320,  1321,  1322,  1323,  1324,
       0,     0,     0,     0,     0,     0,     0,     0,     0,  1127,
       0,  1129,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,  1140,  1141,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   440,     0,     0,     0,     0,   452,     0,   137,     0,
    1163,     0,     0,     0,     0,  1174,  1175,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   491,   137,
       0,     0,     0,     0,     0,     0,     0,   502,  1098,     0,
       0,     0,     0,  1101,  1102,     0,  1103,     0,     0,     0,
       0,     0,  1104,   525,     0,  1106,   528,     0,     0,     0,
       0,     0,     0,   535,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,  1249,  1336,  1286,  1287,
    1288,  1289,  1290,  1291,  1292,  1337,  1294,  1295,  1296,  1297,
    1298,  1299,  1300,  1301,  1302,  1303,  1304,  1305,  1306,  1307,
    1308,  1309,  1310,  1311,  1312,  1313,  1314,  1315,  1316,  1317,
    1338,  1339,  1320,  1321,  1322,  1323,  1324,     0,     0,     0,
       0,  1273,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,  1328,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,  1345,
       0,  1349,  1350,     0,     0,     0,     0,     0,     0,     0,
    1199,  1200,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   639,   641,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,  1358,
       0,   675,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,  1369,     0,     0,  1260,   696,     0,
       0,     0,     0,     0,     0,  1374,     0,  1376,     0,     0,
       0,     0,     0,     0,     0,     0,  1406,     0,  1412,     0,
    1278,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   772,     0,     0,     0,   185,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,  1468,  1469,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,  1371,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,  1494,     0,
       0,  1498,     0,     0,     0,     0,     0,  1506,  1507,     0,
       0,     0,     0,     0,     0,  1428,     0,     0,     0,  1429,
    1430,     0,  1432,     0,  1435,     0,     0,     0,  1437,  1438,
       0,  1440,     0,  1443,     0,     0,  1519,  1444,     0,     0,
       0,     0,  1450,     0,  1452,     0,     0,  1454,     0,     0,
       0,     0,  1530,  1458,     0,  1460,  1461,     0,  1462,     0,
    1464,  1465,     0,  1466,     0,     0,     0,     0,   878,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   892,     0,   892,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
    1489,  1491,     0,  1493,     0,  1495,  1497,     0,  1499,  1501,
    1502,     0,  1505,     0,     0,     0,  1509,  1510,     0,   931,
       0,     0,   936,     0,     0,     0,     0,     0,     0,     0,
       0,   946,     0,     0,     0,     0,     0,   696,     0,     0,
     696,  1518,     0,  1520,  1522,     0,  1523,     0,     0,     0,
       0,     0,     0,     0,     0,  1529,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
    1084,     0,     0,     0,     0,     0,     0,     0,     0,  1089,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   892,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   696,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,  1225,     0,     0,     0,     0,     0,  1225,     0,
       0,     0,     0,     0,  1225,     0,     0,     0,     0,     0,
       0,     0,     0,   892,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,  1279,     0,   892,   892,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,  1332
};

static const yytype_int16 yycheck[] =
{
      45,   197,   577,   244,   115,   229,     0,    52,    53,   222,
      55,   202,   225,    58,   769,    60,     0,    62,   384,   131,
     132,    66,   355,    68,   879,    70,   863,    72,   192,    74,
     623,    76,   448,    78,   210,    80,   629,   138,   805,   214,
     208,   361,   205,    88,   979,   548,   379,   369,   425,   245,
      95,     0,    97,   375,    99,   133,   101,   617,   103,   865,
     105,   848,   868,   920,  1026,   848,   417,   389,   851,   859,
     673,   695,  1091,  1363,   863,   196,   563,     4,   863,    17,
       0,   202,    17,  1176,     6,     4,    17,    50,     6,     7,
      41,   724,   721,   843,    12,   845,     6,   375,   848,    17,
     454,   851,   147,   590,   854,   848,    16,   857,   851,  1178,
       6,   854,    17,    17,   196,     0,    10,    52,    22,    24,
    1108,    17,  1110,  1111,    18,    26,     5,   747,    79,   749,
      66,   751,    95,   753,    28,   755,    15,   366,    57,     3,
    1400,    35,    61,    81,   222,   170,   191,   225,  1408,    66,
    1410,    82,    36,    47,   154,    72,   237,   170,   387,    53,
      87,    97,    26,  1355,    91,  1425,  1456,    89,    87,    36,
      64,    89,    91,    67,  1366,    69,    80,    81,    79,    89,
      97,    41,   378,    62,    63,  1022,  1255,   976,  1257,    50,
      57,   976,    86,    89,    88,   359,    90,   388,    92,   954,
      68,   956,  1190,     9,    98,   250,   430,     9,  1065,    93,
      94,    17,   257,    74,   259,    17,  1003,   262,   263,    68,
    1003,  1481,  1363,  1006,    73,  1485,    93,    94,  1018,  1370,
    1107,    99,  1109,    39,    95,  1090,   170,   559,   558,   284,
     285,  1203,   417,   565,   289,  1030,  1181,   292,     7,   571,
      99,   628,    11,   202,   299,  1112,  1006,  1276,  1008,   304,
    1003,   306,  1012,  1006,   505,  1008,   311,   589,   588,  1012,
     863,   895,   623,   318,   834,   356,   779,   901,   323,   199,
    1373,   559,   885,   328,   480,   330,   889,   920,   917,   334,
     644,    65,   533,   571,  1151,    14,   341,    14,    17,   344,
      36,  1178,     4,    22,  1267,   925,    17,   352,  1271,   929,
    1187,   170,    23,   933,   934,   935,   397,    19,   938,   939,
    1186,    57,   942,   166,   167,    61,  1192,    29,  1134,  1116,
     411,  1086,    17,  1116,  1117,    71,     5,   418,  1105,  1274,
      17,   403,    17,     7,   406,   423,    15,    11,   426,   170,
      27,   432,    27,    17,   455,    57,    42,    93,    94,    61,
       7,    80,    81,    80,    81,    12,    51,    52,   157,  1119,
      17,    25,     5,  1116,  1117,  1118,  1119,    96,    55,    96,
     157,    56,    15,   367,   156,    87,    40,   381,    49,    91,
     374,   724,  1147,    62,    17,    49,    17,    63,   443,  1445,
     445,  1447,   593,  1449,    27,   450,    27,  1000,    31,    32,
    1456,  1194,    19,    58,   459,   422,   657,  1352,   463,   796,
      25,   428,    29,    16,    78,  1280,   471,  1445,  1178,  1447,
      17,    54,    55,    48,  1201,   611,    26,  1030,  1456,  1276,
     608,   604,    33,    34,   489,   566,   567,   162,   623,   164,
     495,  1194,  1195,  1196,  1500,  1075,  1076,  1077,  1078,    17,
      17,   582,   583,    20,    17,    22,    17,   512,    32,    27,
      23,    24,   593,    33,    32,  1230,    27,    17,  1333,   812,
      34,   562,    22,  1238,   566,   567,  1253,  1254,    62,   905,
      54,    55,   162,   813,   164,   540,    54,    55,    46,   544,
     582,   583,   162,    54,    55,  1255,   568,  1257,   570,   168,
     169,   573,   574,  1356,  1357,  1255,  1359,  1257,   599,   581,
    1363,   163,  1365,   635,  1367,  1368,  1182,  1370,  1184,  1272,
     592,  1187,    30,   614,    31,    32,   617,    17,    17,  1159,
     154,   603,    21,    22,   625,   607,   164,    27,   610,     7,
      30,    31,    32,     8,    12,    57,     7,    54,    55,   621,
      11,    12,    13,   154,   155,   156,   371,   158,   159,   160,
     161,   162,    75,    76,    54,    55,    56,   382,   383,   154,
     155,   161,   162,   158,   159,   160,   391,   920,  1431,  1182,
      55,  1184,  1347,  1213,  1187,   400,  1439,  1190,   379,  1329,
     405,  1331,     7,   408,   355,    85,    11,    12,    13,   722,
     655,   416,    17,   158,   159,   160,  1459,    22,     7,    70,
    1463,   921,    11,    12,    13,  1261,   817,  1263,    17,    80,
      81,    20,   918,    22,   136,   843,   138,   844,   140,   141,
     685,   143,   845,   724,  1003,   690,  1125,   149,   150,   547,
     152,   153,   154,   155,   156,   157,   158,   159,   160,  1282,
     162,   874,   164,   155,  1030,    70,   158,   159,   160,  1000,
    1263,    59,    60,  1266,   565,    80,    81,     0,  1000,  1237,
     791,    70,   184,  1276,   186,   187,   188,  1223,   190,   853,
      62,    63,   856,  1026,    54,    55,  1240,   808,   743,  1110,
     745,    80,    81,   748,  1190,   750,   787,   752,  1357,   754,
    1368,     7,   757,    83,    84,    11,    12,    13,    14,  1365,
     765,    17,   163,   768,  1521,   770,   202,   168,   169,   170,
     775,     0,  1065,     7,  1039,  1040,     7,    11,    12,    13,
      11,    12,    13,    17,   870,   247,    17,   249,    22,   251,
     831,    17,   365,   834,   835,   648,   258,   278,   260,  1367,
    1368,    27,   264,   265,    30,    31,    32,   129,   270,   271,
     272,   273,   129,   275,    70,   277,   278,   279,   859,  1112,
     952,   953,   978,   979,   766,   767,   288,   129,    54,    55,
      56,   293,   294,   129,   296,   206,    70,  1094,  1095,    70,
    1024,   303,   394,   305,   129,   200,   790,   309,   310,   793,
     200,   313,   982,   129,   316,   317,   488,   129,  1151,   321,
     322,   589,   129,   807,  1065,   158,   810,   329,   129,   129,
     412,   333,     7,   196,   336,   129,    11,    12,    13,   920,
     921,   343,    17,   345,   346,   347,   348,   349,   842,   351,
       7,  1015,     8,  1017,    11,    12,    13,   196,   622,   622,
      17,    17,   622,   758,    21,    22,   622,   129,   129,   129,
    1203,    27,   129,   129,    30,    31,    32,   129,     0,   343,
     203,     0,   112,   206,     9,   346,   348,   117,   211,   119,
    1468,   121,    17,   123,     0,   125,   969,    -1,    54,    55,
      56,    -1,    27,    -1,    -1,    30,    31,    32,    -1,    -1,
     955,    -1,    -1,    70,    39,   996,    -1,  1492,    43,    44,
    1354,  1355,  1356,  1357,    -1,  1359,    -1,    -1,   815,    54,
      55,    56,  1366,   820,   821,   822,   823,  1018,   155,   441,
     442,   158,   159,   160,   161,   162,   448,   449,   165,    -1,
      -1,   453,   454,   455,   456,   120,   458,   122,    -1,   124,
    1124,   126,    -1,   465,    -1,   467,   468,    -1,   470,    -1,
     472,   473,    -1,   475,   476,    -1,   478,   479,    -1,    -1,
       9,   483,   484,   485,  1065,  1181,   488,    -1,    17,    -1,
      -1,   493,   494,    -1,    -1,   497,   498,  1431,    27,   501,
    1434,    30,    31,    32,    -1,  1439,    16,    17,  1442,  1054,
      39,   513,   514,  1447,    -1,   517,   518,    27,   520,   521,
      30,    31,    32,  1068,    -1,    54,    55,    56,   530,   531,
     532,  1112,    -1,    -1,  1198,   537,    -1,   539,    -1,   541,
      -1,   543,    -1,    -1,    54,    55,    56,    -1,  1093,    10,
     155,   156,    -1,   158,   159,   160,  1490,    18,    -1,    -1,
      -1,   384,  1496,    -1,    -1,    -1,    -1,    28,    -1,    -1,
    1151,   394,    -1,   396,    35,    -1,    -1,    38,  1274,   402,
     155,   156,    -1,   158,   159,   160,   161,   162,  1133,    -1,
     165,     7,    53,    -1,  1139,    11,    12,    13,    14,    -1,
    1145,    17,    -1,    64,  1149,    -1,    67,    -1,    69,    -1,
      -1,    27,    -1,    -1,    30,    31,    32,    -1,   372,   373,
      -1,    37,   376,   377,    -1,    86,    -1,    88,  1173,    90,
      -1,    92,    -1,    -1,    -1,   637,   390,    98,    54,    55,
      56,    -1,   644,    -1,    -1,   399,   648,    -1,   650,    -1,
     404,    -1,    -1,   407,    70,    -1,  1352,    -1,    -1,    -1,
      -1,   415,    -1,    -1,    -1,  1210,   668,    -1,   670,   671,
     672,   673,    -1,  1218,   676,  1220,    -1,    -1,   155,    -1,
      96,   158,   159,   160,   161,   162,   163,  1232,   165,    -1,
     692,   693,   694,   695,    -1,   697,    -1,   699,    -1,   701,
      -1,   703,    -1,    -1,    -1,   707,    -1,    -1,    -1,    -1,
     712,   713,   714,   100,   101,   102,   103,   104,   105,   106,
     107,   108,    -1,    17,    -1,    -1,    -1,    -1,   730,   552,
      -1,    -1,   555,    27,    -1,    -1,    30,    31,    32,    -1,
     742,    -1,   744,    -1,    -1,   747,    -1,   749,    -1,   751,
      44,   753,   575,   755,    -1,    -1,   758,    -1,    -1,   761,
      54,    55,    56,    -1,   766,   767,    -1,   769,    -1,  1433,
      -1,   773,    -1,   596,     7,     8,   778,  1441,    11,    12,
      13,    14,    -1,    -1,    17,    -1,    -1,  1451,    -1,  1453,
      -1,    -1,    -1,    -1,    27,    -1,    -1,    30,    31,    32,
     623,    -1,    -1,  1348,    17,    -1,   629,    -1,    -1,    -1,
     633,    -1,    -1,    -1,    27,    -1,    -1,    30,    31,    32,
      -1,    54,    55,    56,    37,     7,     8,    -1,    -1,    11,
      12,    13,    14,    -1,    -1,    17,    -1,    70,  1383,    -1,
    1385,    54,    55,    56,    -1,    27,    -1,  1392,    30,    31,
      32,    -1,    85,    -1,    -1,  1436,    -1,    -1,  1403,    -1,
    1405,    -1,    -1,    96,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    17,    54,    55,    56,    -1,    -1,   879,    -1,    -1,
      -1,    27,    -1,   885,    30,    31,    32,   889,    70,   891,
      -1,   893,   894,   895,    -1,    -1,   898,    -1,    44,   901,
      46,    -1,   904,   905,    -1,    -1,    -1,    -1,    54,    55,
      56,   154,   155,   156,    96,   158,   159,   160,   161,   162,
      -1,   923,    -1,   925,    -1,   927,    -1,   929,    -1,    -1,
      -1,   933,   934,   935,    -1,    -1,   938,   939,    -1,   941,
     942,    -1,   944,    -1,    -1,   947,    -1,    -1,    -1,    -1,
     952,   953,   954,    -1,   956,     7,   958,   959,    -1,    11,
      12,    13,    14,    -1,    -1,    17,   968,   969,   791,    -1,
      -1,    -1,  1517,    -1,    -1,    27,    -1,    -1,    30,    31,
      32,    -1,    -1,    -1,    -1,   808,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
      -1,    -1,    54,    55,    56,   155,   156,    -1,   158,   159,
     160,   161,   162,    -1,    -1,    -1,    -1,    -1,    70,    -1,
     843,    -1,   845,    -1,    -1,   848,    -1,  1029,   851,    -1,
      -1,   854,    -1,  1035,   857,    -1,    -1,  1039,  1040,    -1,
     863,    -1,    -1,    -1,    96,    -1,    -1,    -1,   871,    -1,
      -1,    -1,    -1,    -1,    -1,  1057,    -1,  1059,  1060,  1061,
    1062,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1071,
    1072,  1073,    -1,  1075,  1076,  1077,  1078,     9,  1080,    -1,
    1082,    -1,    -1,    -1,  1086,    17,  1088,    -1,  1090,    -1,
      -1,    -1,  1094,  1095,    -1,    27,    -1,    -1,    30,    31,
      32,    -1,    -1,    -1,    -1,    -1,    -1,    39,    -1,    -1,
      -1,    -1,    44,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    54,    55,    56,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1138,    -1,    -1,    -1,
    1142,  1143,  1144,    -1,  1146,  1147,  1148,    -1,    -1,    -1,
      -1,  1153,  1154,   976,  1156,    -1,    -1,  1159,    -1,  1161,
      -1,    -1,    -1,   986,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,  1177,    -1,  1000,    -1,    -1,
      -1,    -1,    -1,  1006,    -1,  1008,    -1,    -1,   194,  1012,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   204,  1022,
      -1,   207,    -1,   209,    -1,    -1,   212,  1030,    -1,    -1,
      -1,  1213,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1221,
      -1,  1223,  1224,    -1,  1226,    -1,    -1,    -1,  1230,  1231,
      -1,    -1,  1234,  1235,    -1,  1237,  1238,    -1,  1240,  1241,
    1242,    -1,    -1,    -1,    10,    -1,  1248,  1249,    -1,    -1,
      -1,    17,    18,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    27,    28,    -1,    30,    31,    32,    -1,    -1,    35,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1280,  1281,
    1282,    47,    -1,  1285,    -1,    -1,    -1,    53,    54,    55,
      56,  1293,    -1,    -1,    -1,    -1,  1119,    -1,    64,    -1,
      -1,    67,    -1,    69,    -1,    -1,    -1,  1130,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1318,  1319,    -1,    -1,
      86,    -1,    88,    -1,    90,  1327,    92,  1329,    -1,  1331,
      -1,  1333,    98,  1335,  1336,  1337,  1338,  1339,    -1,    -1,
    1342,    -1,    -1,    -1,    -1,  1347,    -1,   353,    -1,    -1,
      -1,    -1,    -1,    -1,   360,  1178,    -1,   363,    -1,  1182,
      -1,  1184,    -1,    -1,  1187,    -1,    -1,  1190,    -1,    -1,
      -1,    -1,    -1,    -1,   380,    -1,    -1,    -1,    -1,   385,
    1382,    -1,  1384,    -1,    -1,    -1,    -1,    -1,    -1,  1391,
      -1,  1393,    -1,    -1,    -1,    -1,  1398,    -1,  1400,    -1,
    1402,    -1,    -1,    -1,    -1,    -1,  1408,    -1,  1410,    -1,
      -1,    -1,    -1,  1415,    -1,    -1,    -1,    -1,  1420,    -1,
      -1,    -1,   312,  1425,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,  1255,    -1,  1257,    -1,   326,   327,  1261,   329,
    1263,    -1,    -1,  1266,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,  1276,    -1,    -1,    -1,    -1,   464,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1468,    -1,   474,  1471,
    1472,    -1,    -1,    -1,  1476,   481,    -1,  1479,    -1,  1481,
    1482,    -1,    -1,  1485,    -1,    -1,    -1,    -1,    -1,    -1,
     496,    -1,    -1,    -1,    -1,    -1,    -1,   503,    -1,    -1,
     506,   507,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     516,    -1,  1514,   519,    -1,    -1,    -1,    -1,    -1,    -1,
     526,    -1,    -1,   529,    -1,    -1,    -1,    -1,   534,    -1,
     536,  1354,  1355,  1356,  1357,    -1,  1359,    -1,    -1,   545,
    1363,    -1,  1365,  1366,  1367,  1368,    -1,  1370,    -1,    -1,
     556,    -1,    -1,    -1,    -1,   561,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     576,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   587,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   479,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   487,    -1,    -1,
      -1,    -1,   492,    -1,    -1,    -1,    -1,    -1,    -1,   499,
     500,    -1,    -1,    -1,   504,    -1,    -1,    -1,   624,    -1,
      -1,    -1,    -1,    -1,   630,   515,    -1,    -1,   634,    -1,
      -1,    -1,    -1,    -1,   640,    -1,   642,   527,    -1,    -1,
     646,    -1,    -1,    -1,    -1,   651,    -1,    -1,    -1,    -1,
      -1,    -1,   658,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   674,     4,
       5,   677,   678,   679,   680,    10,    -1,    -1,    -1,    -1,
      15,    -1,    17,    18,    -1,   691,    -1,    -1,    -1,    -1,
      25,    -1,    -1,    28,   700,    -1,   702,    -1,    -1,    -1,
      35,    36,    -1,    38,    -1,    40,    -1,    -1,    -1,   715,
      45,   717,    47,   719,    -1,    50,    -1,    -1,    53,   725,
      -1,    -1,    -1,    -1,    -1,    -1,    61,    -1,    -1,    64,
      -1,    66,    67,    68,    69,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    77,    78,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    86,    87,    88,    -1,    90,    91,    92,    93,    94,
      95,    -1,    97,    98,    99,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   792,    -1,   794,    -1,
      -1,    -1,    -1,   683,   684,    -1,    -1,    -1,    -1,   689,
      -1,    -1,    -1,   809,    -1,   811,    -1,    -1,    -1,    -1,
      -1,    -1,   818,    -1,   704,   705,    -1,    -1,    -1,   825,
      -1,   827,    -1,   829,    -1,    -1,   716,    -1,   718,    -1,
     720,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   864,    -1,
      -1,   109,   110,   111,   112,   113,   872,    -1,   116,   117,
     118,   119,   120,   121,   122,   123,   124,   125,   126,   127,
     128,    -1,   130,   131,   132,   114,   115,   116,   117,   118,
     119,   120,   121,   122,   123,   124,   125,   126,   127,   128,
     129,   130,   131,   132,   133,   134,   135,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,    -1,   932,    -1,    -1,    -1,
      -1,   937,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   949,   192,    -1,    -1,    -1,    -1,    -1,
     198,    -1,    -1,    -1,   960,    -1,    -1,    -1,    -1,    -1,
      -1,   967,    -1,    -1,   970,   971,   972,    -1,    -1,   975,
      -1,   977,    -1,    -1,   980,    -1,    -1,    -1,    -1,   985,
      -1,   987,   988,   989,   990,   991,   992,   993,    -1,    -1,
      -1,    -1,    -1,    -1,   884,  1001,   886,   887,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   896,    -1,   898,   899,
      -1,    -1,    -1,    -1,    -1,    -1,   906,   907,    -1,   909,
     910,   911,   912,    -1,    -1,  1031,    -1,    -1,    -1,    -1,
      -1,   115,   116,   117,   118,   119,   120,   121,    -1,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,    -1,  1070,   149,   150,   151,   152,   153,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1085,
      -1,  1087,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,  1101,  1102,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   359,    -1,    -1,    -1,    -1,   364,    -1,   366,    -1,
    1126,    -1,    -1,    -1,    -1,  1131,  1132,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   386,   387,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   395,  1038,    -1,
      -1,    -1,    -1,  1043,  1044,    -1,  1046,    -1,    -1,    -1,
      -1,    -1,  1052,   411,    -1,  1055,   414,    -1,    -1,    -1,
      -1,    -1,    -1,   421,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1202,   114,   115,   116,
     117,   118,   119,   120,   121,   122,   123,   124,   125,   126,
     127,   128,   129,   130,   131,   132,   133,   134,   135,   136,
     137,   138,   139,   140,   141,   142,   143,   144,   145,   146,
     147,   148,   149,   150,   151,   152,   153,    -1,    -1,    -1,
      -1,  1247,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,  1260,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1275,
      -1,  1277,  1278,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
    1170,  1171,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   549,   550,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1325,
      -1,   569,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,  1340,    -1,    -1,  1227,   586,    -1,
      -1,    -1,    -1,    -1,    -1,  1351,    -1,  1353,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1362,    -1,  1364,    -1,
    1250,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   631,    -1,    -1,    -1,   635,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,  1427,  1428,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,  1344,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1474,    -1,
      -1,  1477,    -1,    -1,    -1,    -1,    -1,  1483,  1484,    -1,
      -1,    -1,    -1,    -1,    -1,  1375,    -1,    -1,    -1,  1379,
    1380,    -1,  1382,    -1,  1384,    -1,    -1,    -1,  1388,  1389,
      -1,  1391,    -1,  1393,    -1,    -1,  1512,  1397,    -1,    -1,
      -1,    -1,  1402,    -1,  1404,    -1,    -1,  1407,    -1,    -1,
      -1,    -1,  1528,  1413,    -1,  1415,  1416,    -1,  1418,    -1,
    1420,  1421,    -1,  1423,    -1,    -1,    -1,    -1,   786,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   803,    -1,   805,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
    1470,  1471,    -1,  1473,    -1,  1475,  1476,    -1,  1478,  1479,
    1480,    -1,  1482,    -1,    -1,    -1,  1486,  1487,    -1,   847,
      -1,    -1,   850,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   859,    -1,    -1,    -1,    -1,    -1,   865,    -1,    -1,
     868,  1511,    -1,  1513,  1514,    -1,  1516,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,  1525,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
    1018,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,  1027,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  1105,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1134,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,  1180,    -1,    -1,    -1,    -1,    -1,  1186,    -1,
      -1,    -1,    -1,    -1,  1192,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,  1201,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,  1251,    -1,  1253,  1254,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,  1265
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_int16 yystos[] =
{
       0,     4,     5,    10,    15,    17,    18,    25,    28,    35,
      36,    38,    40,    45,    47,    50,    53,    61,    64,    66,
      67,    68,    69,    77,    78,    86,    87,    88,    90,    91,
      92,    93,    94,    95,    97,    98,    99,   172,   173,   174,
     245,   247,   329,   330,   333,   335,   356,   357,   358,   359,
     360,   361,   364,   365,   367,   369,   377,   378,   379,   388,
     389,   400,   402,   413,   414,   415,   423,   424,   425,   426,
     427,   428,   429,   431,   432,   433,   434,   435,   436,   437,
     439,   461,   462,   463,   465,   467,   468,   474,   475,   481,
     486,   489,   492,   495,   496,   499,   504,   506,   509,   513,
     526,   530,   531,   536,   538,   539,     0,   166,   167,   209,
     209,   209,   209,   209,   170,   211,   209,   209,   209,   209,
     209,   209,   209,   209,   209,   209,   209,   209,   209,   209,
     209,   209,   209,   209,   154,   212,   248,   212,   336,   212,
     363,   363,   212,   370,     6,    89,   373,   374,   212,   380,
     363,   212,   403,   363,   380,   363,   380,   363,   380,   363,
     380,   212,   476,   212,   500,   358,   359,   360,   361,   388,
     414,   415,   426,   431,   435,   461,   474,   481,   486,   489,
     492,   495,   505,   507,   508,   212,   514,   514,   514,    41,
     518,   519,   211,   211,   331,   211,   211,   211,   209,   211,
     211,   211,   211,   211,   381,   211,   211,   430,   211,   430,
     211,   211,   438,   211,   211,   168,   169,   210,   211,   211,
     510,   511,   211,   527,   528,   211,   532,   533,   211,   209,
     155,   156,   158,   159,   160,   161,   162,   175,   176,   177,
     179,   181,   182,   185,   187,   188,   212,   249,    62,   337,
     339,   367,   210,    19,    29,    57,   330,   335,   346,   347,
     366,   378,   396,   397,   464,   466,   469,   471,   472,   473,
     346,   366,   464,   466,   179,   371,   212,   375,   367,   390,
     391,   393,   394,   395,   396,   397,     3,    26,   404,   405,
      71,   333,   335,   346,   416,   424,   482,   483,   484,   485,
      23,    24,   245,   382,   383,   385,   386,   210,    72,   428,
     487,   488,   245,   382,   210,    73,   433,   490,   491,   210,
      74,   437,   493,   494,    51,    52,   245,   440,   441,   443,
     444,   210,    65,   477,   479,   496,   497,   538,    75,    76,
     501,   502,    79,   515,   516,   518,   515,   518,   515,   518,
      42,   520,   521,   174,   187,   157,   175,   186,   179,   211,
     246,   211,   331,   334,   209,   211,   209,   211,   209,   211,
     209,   209,   211,   211,   211,   211,   211,   211,   211,   211,
     381,   211,   209,   209,   211,   401,   209,   209,   211,   211,
     211,   209,   211,   266,   211,   209,   211,   209,   211,   211,
     209,   266,   211,   211,   211,   209,   211,   211,   209,   266,
     266,   209,   211,   266,   209,   211,   209,   211,   209,   211,
     511,   209,   211,   211,   528,   211,   211,   533,   211,   211,
     209,   210,   180,   183,   184,   188,   187,    33,    34,   176,
     212,   250,   251,   252,   254,   255,   210,    63,   337,   342,
     343,   210,   212,   340,   342,   336,   378,    58,   348,   349,
      16,   276,   278,   290,   362,   373,   212,   398,   398,    49,
     446,   448,   446,   378,   362,   446,   446,   179,   372,   178,
     180,   376,   210,   400,   398,   398,     9,   245,   407,   409,
     210,   212,   406,   336,   424,   290,   417,   446,   398,   245,
     245,   385,   212,   384,   245,   187,   387,   275,   277,   285,
     287,   288,   290,   446,   398,   245,   275,   446,   398,   275,
     446,   398,   187,   191,   193,   212,   442,   440,   212,   445,
     446,   398,   497,   187,   503,   212,   517,   520,    26,   450,
     451,   520,    48,   540,   544,   174,   187,   211,   211,   209,
     209,   334,   211,   332,   209,   211,   338,   332,   211,   211,
     211,   351,   209,   209,   210,   211,   211,   211,   211,   209,
     211,   211,   210,   211,   211,   211,   368,   211,   266,   179,
     210,   211,   211,   211,   266,   401,   209,   266,   211,   211,
     209,   210,   211,   211,   266,   266,   211,   210,   266,   175,
     210,   210,   209,   211,   211,   266,   210,   211,   211,   210,
     211,   211,   162,   164,   190,   192,   196,   199,   210,   266,
     210,   211,   211,   211,   498,   175,   210,   210,   211,   211,
     529,   209,   211,   211,   214,   209,   210,   251,   254,   212,
     253,   212,   256,   245,   345,   176,   344,   245,   341,   210,
     337,   362,    59,    60,   352,   354,   210,   187,   350,   100,
     101,   102,   103,   104,   105,   106,   107,   108,   291,   276,
     346,   366,   346,   366,   275,   212,   449,   275,   362,   275,
     275,     7,    11,   245,   267,   271,   210,     7,    12,   265,
     270,   275,   346,   366,   346,   366,   212,   410,   210,   337,
     417,   291,   275,   346,   424,   245,   187,   109,   110,   111,
     112,   113,   291,   292,   293,   275,   428,   275,   433,   275,
     437,   187,   194,   187,   200,   275,   469,   471,   472,   473,
     480,    27,    30,    31,    32,    54,    55,    56,   215,   217,
     218,   219,   220,   221,   222,   223,   226,   227,   228,   230,
     231,   236,   238,   241,   242,   245,   257,   258,   497,   210,
     187,   450,    39,    44,   215,   409,   453,   458,   459,   524,
     525,   210,   212,   452,    46,   537,   215,   210,   514,   211,
     210,   210,   332,   210,   338,   210,   209,   175,   210,   211,
     211,   211,   470,   211,   470,   210,   211,   210,   210,   210,
     210,   266,   266,   209,   266,   209,   210,   211,   211,   392,
     211,   392,   211,   211,   210,   211,   210,   211,   266,   266,
     211,   211,   211,   211,   210,   266,   210,   266,   210,   266,
     192,   190,   163,   165,   188,   189,   197,   200,   205,   206,
     207,   210,   211,   211,   209,   211,   216,   209,   211,   216,
     209,   211,   216,   209,   211,   216,   209,   211,   216,   209,
     216,   209,   498,   211,   512,   209,   529,   529,   209,   214,
     209,   211,   353,   209,   211,   541,   542,   254,   212,   355,
     187,   158,   159,   160,   294,   378,   245,   373,   210,   378,
     210,   450,   212,   273,   273,   378,   245,   265,   373,   399,
     210,   378,   210,   178,   411,   337,   294,   424,   210,   294,
     294,   294,   294,   210,   210,   210,   187,   195,   200,   187,
     201,   189,   208,   400,   219,   245,   257,   222,   226,   245,
     257,   212,   224,   230,   236,   241,   212,   229,   236,   241,
     176,   232,   241,   176,   239,   191,   212,   243,   154,   213,
      43,   215,   453,   458,   522,   523,   524,   210,   410,   410,
     344,   245,   210,   400,   446,   509,   530,   534,   450,   515,
     353,   266,   470,   266,   266,   470,   211,   447,   211,   211,
     392,   266,   211,   266,   266,   392,   211,   408,   266,   266,
     266,   266,   266,   266,   192,   208,   189,   198,   205,   201,
     211,   478,   216,   211,   216,   210,   211,   216,   211,   216,
     216,   210,   211,   216,   216,   211,   216,   211,   211,   210,
     512,   512,   211,   214,   209,   214,   211,   211,   210,   210,
     211,   543,   211,   542,   210,   210,   210,   210,   245,   453,
     458,   210,   179,   274,   274,   210,   399,   210,     7,    11,
      12,    13,   245,   263,   264,   412,   210,   210,   210,   210,
     210,   210,   210,   187,   202,   203,   215,   277,   290,   210,
     225,   227,   230,   236,   241,   236,   241,   241,   241,   176,
     233,   176,   240,   191,   212,   244,   524,   174,   411,   212,
     460,   211,   215,   409,   458,   545,   210,   211,   266,   447,
     447,   266,   266,   266,   266,   209,   266,   211,   211,   211,
     211,   211,   175,   205,   209,   210,   211,   211,   211,   211,
     216,   216,   216,   216,   211,   211,   237,   210,   214,   210,
     211,   454,   353,   537,   209,   543,   543,     8,   295,   298,
     210,   210,   273,   295,   296,   298,   295,   296,   297,   298,
     187,   204,   205,   230,   236,   241,   236,   241,   241,   241,
     176,   234,   267,   210,     7,    11,    12,    13,    14,    70,
     245,   455,   456,   457,   210,   210,   209,   410,   211,   284,
     209,   211,   211,   418,   211,   286,   209,   211,   279,   214,
     211,   289,   209,   205,   211,   211,   211,   216,   211,   266,
     266,   209,   534,   211,     7,    11,    12,    13,    14,    70,
      85,    96,   217,   245,   257,   259,   260,   261,   262,   268,
     272,   281,   283,   295,   328,   212,   299,   274,    37,   215,
     328,   420,   421,   215,   328,   299,   215,   295,   328,   215,
     297,   299,   236,   241,   241,   241,   176,   235,   273,   210,
     411,   209,   216,   209,   209,   211,   284,   211,   280,   211,
     266,   211,   214,   211,   419,   209,   211,   211,   279,   214,
     289,   211,   211,   210,   211,   269,   211,   535,   266,   212,
     282,   273,   273,   283,   283,   114,   115,   116,   117,   118,
     119,   120,   121,   122,   123,   124,   125,   126,   127,   128,
     129,   130,   131,   132,   133,   134,   135,   136,   137,   138,
     139,   140,   141,   142,   143,   144,   145,   146,   147,   148,
     149,   150,   151,   152,   153,   300,   303,   312,   210,   420,
     215,   420,   212,   422,   215,   312,   114,   122,   147,   148,
     305,   308,   312,   241,   274,   210,   215,   524,   537,   210,
     210,   353,   211,   269,   211,   211,   211,   211,   210,   211,
     419,   419,   353,   211,   310,   211,   211,   211,   211,   210,
     211,   266,   214,   209,   210,   274,   210,    20,    22,   245,
     260,   301,   313,   314,   317,   318,   301,    21,   245,   260,
     302,   315,   316,   317,   302,    80,    81,   245,   260,   304,
     317,   319,   321,   322,   323,   324,   210,   245,   268,   311,
     317,   319,   210,   245,   306,   313,   317,   301,   245,   307,
     315,   317,   307,   245,   309,   317,   319,   534,   266,   266,
     266,   211,   266,   209,   211,   266,   209,   266,   266,   211,
     266,   209,   211,   266,   266,   211,   320,   211,   320,   211,
     266,   209,   266,   209,   266,   320,   211,   320,   266,   211,
     266,   266,   266,   211,   266,   266,   266,   320,   210,   210,
     260,   317,   176,   260,   187,   260,   317,   176,   260,   321,
     323,   260,   323,   176,   176,   268,   317,   317,   535,   266,
     211,   266,   211,   266,   210,   266,   211,   266,   210,   266,
     211,   266,   266,   320,   211,   266,   210,   210,   320,   266,
     266,   260,   265,   260,   323,    82,   325,   326,   266,   210,
     266,   211,   266,   266,   209,   325,    83,    84,   327,   266,
     210
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_int16 yyr1[] =
{
       0,   171,   172,   172,   172,   172,   172,   172,   172,   172,
     172,   172,   173,   173,   173,   173,   173,   173,   174,   174,
     175,   176,   176,   177,   178,   179,   179,   180,   180,   181,
     182,   183,   184,   185,   185,   186,   186,   187,   187,   187,
     187,   188,   188,   189,   190,   191,   191,   191,   192,   192,
     193,   194,   195,   196,   197,   197,   198,   198,   199,   200,
     201,   202,   202,   202,   203,   204,   205,   205,   206,   207,
     207,   208,   208,   209,   209,   210,   210,   211,   212,   213,
     214,   214,   215,   215,   215,   216,   216,   216,   217,   217,
     218,   218,   218,   219,   219,   219,   219,   220,   221,   222,
     223,   224,   225,   225,   225,   225,   225,   225,   225,   225,
     225,   225,   225,   225,   225,   225,   225,   226,   226,   226,
     226,   226,   226,   226,   226,   226,   226,   226,   226,   226,
     226,   226,   227,   228,   229,   230,   231,   232,   233,   234,
     235,   236,   237,   237,   238,   239,   240,   241,   242,   243,
     243,   244,   244,   245,   246,   246,   246,   246,   246,   246,
     246,   247,   248,   249,   249,   250,   250,   251,   252,   253,
     254,   255,   256,   257,   258,   259,   259,   260,   261,   262,
     262,   262,   262,   262,   263,   264,   264,   264,   264,   265,
     266,   266,   267,   268,   269,   269,   270,   270,   271,   271,
     272,   272,   273,   274,   275,   275,   275,   275,   276,   277,
     278,   279,   279,   279,   279,   280,   280,   281,   282,   283,
     283,   283,   283,   283,   284,   284,   284,   284,   285,   286,
     286,   286,   286,   287,   288,   289,   289,   289,   290,   291,
     291,   291,   291,   291,   291,   291,   291,   291,   292,   292,
     293,   293,   294,   294,   294,   295,   296,   297,   298,   299,
     300,   300,   300,   300,   300,   300,   300,   300,   300,   301,
     301,   301,   301,   301,   301,   301,   301,   302,   302,   302,
     302,   302,   302,   302,   302,   303,   303,   304,   304,   304,
     304,   304,   305,   305,   305,   305,   305,   305,   305,   305,
     305,   306,   306,   306,   306,   307,   307,   307,   307,   308,
     308,   309,   309,   309,   310,   310,   311,   311,   311,   311,
     311,   312,   312,   312,   312,   312,   312,   312,   312,   312,
     312,   312,   312,   312,   312,   312,   312,   312,   312,   312,
     312,   312,   312,   312,   312,   312,   312,   312,   312,   312,
     312,   312,   312,   312,   312,   312,   312,   313,   314,   315,
     316,   317,   318,   319,   319,   319,   319,   320,   320,   320,
     320,   320,   321,   322,   323,   324,   325,   326,   327,   327,
     328,   329,   329,   330,   331,   331,   332,   332,   333,   334,
     334,   335,   336,   337,   338,   338,   339,   340,   341,   342,
     343,   344,   345,   346,   347,   348,   349,   350,   350,   350,
     351,   351,   352,   353,   353,   354,   354,   355,   356,   356,
     356,   357,   357,   358,   359,   360,   361,   362,   362,   363,
     364,   364,   365,   365,   366,   366,   367,   368,   368,   368,
     369,   369,   370,   371,   372,   373,   374,   374,   375,   376,
     376,   377,   377,   378,   379,   379,   379,   380,   381,   381,
     381,   381,   381,   381,   381,   381,   382,   383,   384,   385,
     386,   387,   387,   387,   388,   389,   389,   390,   390,   390,
     390,   391,   392,   392,   392,   392,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   401,   401,   402,   403,
     404,   405,   405,   406,   407,   408,   408,   408,   409,   410,
     411,   412,   413,   413,   414,   415,   416,   416,   417,   418,
     418,   418,   418,   418,   419,   419,   419,   420,   421,   422,
     423,   423,   424,   425,   425,   425,   426,   427,   427,   428,
     429,   429,   430,   430,   430,   430,   431,   432,   433,   434,
     434,   435,   436,   437,   438,   438,   438,   438,   438,   439,
     439,   440,   441,   442,   442,   443,   444,   445,   446,   447,
     447,   447,   447,   448,   449,   450,   451,   452,   453,   454,
     454,   454,   455,   456,   457,   457,   457,   457,   457,   457,
     458,   459,   460,   461,   461,   461,   462,   462,   463,   464,
     464,   465,   466,   466,   467,   468,   469,   470,   470,   470,
     471,   472,   473,   474,   475,   476,   477,   478,   478,   478,
     479,   480,   480,   480,   480,   481,   482,   482,   483,   484,
     485,   486,   487,   488,   489,   490,   491,   492,   493,   494,
     495,   496,   496,   496,   496,   496,   496,   496,   496,   496,
     496,   496,   496,   497,   497,   498,   498,   498,   499,   500,
     501,   502,   502,   503,   503,   503,   504,   505,   505,   506,
     507,   507,   507,   507,   507,   507,   507,   507,   507,   507,
     507,   507,   507,   507,   508,   508,   508,   508,   508,   508,
     508,   509,   510,   510,   511,   512,   512,   512,   512,   512,
     512,   512,   513,   514,   515,   516,   517,   518,   519,   520,
     521,   522,   523,   524,   525,   526,   527,   527,   528,   529,
     529,   529,   529,   529,   530,   531,   532,   532,   533,   534,
     534,   534,   534,   535,   535,   535,   535,   536,   537,   538,
     539,   540,   541,   541,   542,   543,   543,   543,   543,   544,
     545
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     2,     2,     1,     1,     0,     1,     3,
       1,     1,     2,     2,     2,     0,     2,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     4,     6,     0,     1,
       1,     1,     1,     3,     3,     1,     2,     1,     1,     1,
       1,     3,     4,     2,     1,     1,     1,     1,     1,     3,
       2,     0,     2,     1,     1,     1,     1,     1,     1,     1,
       0,     2,     1,     2,     1,     0,     3,     2,     1,     1,
       3,     2,     1,     1,     3,     4,     3,     6,     1,     4,
       1,     1,     1,     1,     1,     1,     3,     3,     3,     3,
       3,     3,     5,     5,     5,     5,     7,     2,     2,     2,
       2,     4,     4,     4,     4,     4,     4,     6,     6,     6,
       6,     8,     4,     1,     1,    10,     1,     1,     1,     1,
       1,     7,     0,     2,     1,     1,     1,     6,     1,     1,
       1,     1,     1,     7,     0,     2,     4,     6,     2,     4,
       2,     1,     1,     1,     1,     1,     1,     4,     1,     1,
       4,     1,     1,     4,     1,     1,     1,     1,     7,     1,
       1,     1,     1,     1,     7,     1,     1,     1,     1,     7,
       0,     3,     7,     5,     0,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,    10,
      10,     0,     3,     3,     2,     0,     2,     5,     1,     1,
       3,     1,     2,     1,     0,     3,     3,     2,    10,     0,
       2,     4,     2,    10,    10,     0,     3,     2,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     6,     7,     6,     1,     1,
       1,     1,     3,     1,     3,     1,     3,     1,     3,     2,
       4,     6,     4,     2,     4,     2,     2,     2,     4,     6,
       4,     2,     4,     2,     2,     1,     3,     2,     1,     2,
       4,     2,     1,     1,     3,     1,     3,     1,     3,     1,
       3,     2,     4,     2,     2,     2,     4,     2,     2,     1,
       3,     2,     2,     1,     0,     2,     2,     1,     2,     4,
       2,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     6,     1,     4,
       1,     4,     1,     2,     2,     4,     6,     0,     3,     3,
       5,     7,     4,     1,     4,     1,     4,     1,     1,     1,
       1,     1,     1,     5,     5,     3,     0,     3,     7,     3,
       3,     1,     1,     5,     0,     3,     1,     1,     1,     4,
       1,     1,     1,     5,     1,     4,     1,     1,     2,     3,
       0,     2,     5,     0,     2,     1,     1,     1,     1,     1,
       1,     1,     1,     8,    10,     8,    10,     3,     1,     1,
       1,     1,     1,     1,     1,     1,     9,     0,     3,     3,
       1,     1,     1,     1,     1,     6,     1,     1,     1,     4,
       2,     1,     3,     7,     1,     1,     1,     1,     0,     2,
       2,     4,     3,     5,     5,     7,     4,     1,     1,     4,
       1,     1,     2,     3,    10,     1,     1,     1,     1,     1,
       1,     7,     0,     3,     5,     3,     3,     9,     7,     9,
       1,     1,     1,     1,     7,     0,     3,     3,     1,     1,
       5,     1,     1,     1,     7,     0,     3,     3,     1,     1,
       1,     1,     1,     1,     8,    10,     1,     1,    10,     0,
       3,     5,     3,     2,     0,     3,     2,     5,     1,     1,
       1,     1,     5,     1,     1,     1,     8,     1,     1,     5,
       1,     1,     0,     2,     3,     5,     8,     1,     5,     1,
       1,     8,     1,     5,     0,     3,     5,     3,     3,     1,
       1,     4,     1,     1,     1,     4,     1,     1,     7,     0,
       3,     3,     3,     1,     1,     5,     1,     1,     7,     0,
       3,     3,     1,     5,     1,     1,     1,     1,     1,     1,
       7,     1,     1,     1,     1,     1,     1,     1,    10,     1,
       1,    10,     1,     1,    10,    10,     7,     0,     3,     3,
       9,     7,     9,    10,     1,     1,     9,     0,     2,     2,
       1,     1,     1,     1,     1,    10,     1,     1,     7,     9,
       1,    10,     7,     1,    10,     7,     1,    10,     7,     1,
       9,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     0,     3,     2,     1,     1,
       4,     1,     1,     1,     2,     3,     4,     1,     3,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     4,     3,     1,     8,     0,     3,     3,     3,     5,
       3,     2,     1,     1,     4,     1,     1,     4,     1,     4,
       1,     4,     1,     4,     1,     4,     3,     1,     6,     0,
       3,     3,     3,     2,     1,     4,     3,     1,    16,     1,
       1,     1,     1,     0,     6,     3,     2,     1,     1,     9,
       1,     4,     3,     1,     4,     0,     3,     3,     2,     1,
       7
};


#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)
#define YYEMPTY         (-2)
#define YYEOF           0

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (context, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Error token number */
#define YYTERROR        1
#define YYERRCODE       256



/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)

/* This macro is provided for backward compatibility. */
#ifndef YY_LOCATION_PRINT
# define YY_LOCATION_PRINT(File, Loc) ((void) 0)
#endif


# define YY_SYMBOL_PRINT(Title, Type, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Type, Value, context); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo, int yytype, YYSTYPE const * const yyvaluep, pj_wkt2_parse_context *context)
{
  FILE *yyoutput = yyo;
  YYUSE (yyoutput);
  YYUSE (context);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyo, yytoknum[yytype], *yyvaluep);
# endif
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo, int yytype, YYSTYPE const * const yyvaluep, pj_wkt2_parse_context *context)
{
  YYFPRINTF (yyo, "%s %s (",
             yytype < YYNTOKENS ? "token" : "nterm", yytname[yytype]);

  yy_symbol_value_print (yyo, yytype, yyvaluep, context);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp, int yyrule, pj_wkt2_parse_context *context)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       yystos[+yyssp[yyi + 1 - yynrhs]],
                       &yyvsp[(yyi + 1) - (yynrhs)]
                                              , context);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, context); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
#  else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr && yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return (YYPTRDIFF_T)(yystpcpy (yyres, yystr) - yyres);
  else
    return yystrlen (yystr);
}
# endif

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                yy_state_t *yyssp, int yytoken)
{
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Actual size of YYARG. */
  int yycount = 0;
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[+*yyssp];
      YYPTRDIFF_T yysize0 = yytnamerr (YY_NULLPTR, yytname[yytoken]);
      yysize = yysize0;
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                {
                  YYPTRDIFF_T yysize1
                    = yysize + yytnamerr (YY_NULLPTR, yytname[yyx]);
                  if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
                    yysize = yysize1;
                  else
                    return 2;
                }
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  {
    /* Don't count the "%s"s in the final size, but reserve room for
       the terminator.  */
    YYPTRDIFF_T yysize1 = yysize + (yystrlen (yyformat) - 2 * yycount) + 1;
    if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
      yysize = yysize1;
    else
      return 2;
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, pj_wkt2_parse_context *context)
{
  YYUSE (yyvaluep);
  YYUSE (context);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YYUSE (yytype);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}




/*----------.
| yyparse.  |
`----------*/

int
yyparse (pj_wkt2_parse_context *context)
{
/* The lookahead symbol.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

    /* Number of syntax errors so far.  */
    /* int yynerrs; */

    yy_state_fast_t yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       'yyss': related to states.
       'yyvs': related to semantic values.

       Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss;
    yy_state_t *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    YYPTRDIFF_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken = 0;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yyssp = yyss = yyssa;
  yyvsp = yyvs = yyvsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  /* yynerrs = 0; */
  yychar = YYEMPTY; /* Cause a token to be read.  */
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    goto yyexhaustedlab;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = (YYPTRDIFF_T)(yyssp - yyss + 1);

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          goto yyexhaustedlab;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
# undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = yylex (&yylval, context);
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {


      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      /* ++yynerrs; */
#if ! YYERROR_VERBOSE
      yyerror (context, YY_("syntax error"));
#else
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *, YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (context, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }



  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, context);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
#if 0
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
#endif
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYTERROR;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  yystos[yystate], yyvsp, context);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;


#if !defined yyoverflow || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (context, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif


/*-----------------------------------------------------.
| yyreturn -- parsing is finished, return the result.  |
`-----------------------------------------------------*/
yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, context);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  yystos[+*yyssp], yyvsp, context);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  return yyresult;
}
