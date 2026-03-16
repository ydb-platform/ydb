/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
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
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

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

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1


/* Substitute the variable and function names.  */
#define yyparse         dceparse
#define yylex           dcelex
#define yyerror         dceerror
#define yydebug         dcedebug
#define yynerrs         dcenerrs

/* First part of user prologue.  */
#line 11 "dce.y"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif
#include <stdlib.h>
#include "netcdf.h"
#include "ncbytes.h"
#include "nclist.h"
#include "dceconstraints.h"
#include "dceparselex.h"

#line 88 "dcetab.c"

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

#include "dcetab.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_SCAN_WORD = 3,                  /* SCAN_WORD  */
  YYSYMBOL_SCAN_STRINGCONST = 4,           /* SCAN_STRINGCONST  */
  YYSYMBOL_SCAN_NUMBERCONST = 5,           /* SCAN_NUMBERCONST  */
  YYSYMBOL_6_ = 6,                         /* '?'  */
  YYSYMBOL_7_ = 7,                         /* ','  */
  YYSYMBOL_8_ = 8,                         /* '('  */
  YYSYMBOL_9_ = 9,                         /* ')'  */
  YYSYMBOL_10_ = 10,                       /* '.'  */
  YYSYMBOL_11_ = 11,                       /* '['  */
  YYSYMBOL_12_ = 12,                       /* ']'  */
  YYSYMBOL_13_ = 13,                       /* ':'  */
  YYSYMBOL_14_ = 14,                       /* '&'  */
  YYSYMBOL_15_ = 15,                       /* '{'  */
  YYSYMBOL_16_ = 16,                       /* '}'  */
  YYSYMBOL_17_ = 17,                       /* '='  */
  YYSYMBOL_18_ = 18,                       /* '>'  */
  YYSYMBOL_19_ = 19,                       /* '<'  */
  YYSYMBOL_20_ = 20,                       /* '!'  */
  YYSYMBOL_21_ = 21,                       /* '~'  */
  YYSYMBOL_YYACCEPT = 22,                  /* $accept  */
  YYSYMBOL_constraints = 23,               /* constraints  */
  YYSYMBOL_optquestionmark = 24,           /* optquestionmark  */
  YYSYMBOL_projections = 25,               /* projections  */
  YYSYMBOL_selections = 26,                /* selections  */
  YYSYMBOL_projectionlist = 27,            /* projectionlist  */
  YYSYMBOL_projection = 28,                /* projection  */
  YYSYMBOL_function = 29,                  /* function  */
  YYSYMBOL_segmentlist = 30,               /* segmentlist  */
  YYSYMBOL_segment = 31,                   /* segment  */
  YYSYMBOL_rangelist = 32,                 /* rangelist  */
  YYSYMBOL_range = 33,                     /* dap2_range  */
  YYSYMBOL_range1 = 34,                    /* range1  */
  YYSYMBOL_clauselist = 35,                /* clauselist  */
  YYSYMBOL_sel_clause = 36,                /* sel_clause  */
  YYSYMBOL_value_list = 37,                /* value_list  */
  YYSYMBOL_value = 38,                     /* value  */
  YYSYMBOL_constant = 39,                  /* constant  */
  YYSYMBOL_var = 40,                       /* var  */
  YYSYMBOL_indexpath = 41,                 /* indexpath  */
  YYSYMBOL_index = 42,                     /* index  */
  YYSYMBOL_array_indices = 43,             /* array_indices  */
  YYSYMBOL_boolfunction = 44,              /* boolfunction  */
  YYSYMBOL_arg_list = 45,                  /* arg_list  */
  YYSYMBOL_rel_op = 46,                    /* rel_op  */
  YYSYMBOL_ident = 47,                     /* ident  */
  YYSYMBOL_word = 48,                      /* word  */
  YYSYMBOL_number = 49,                    /* number  */
  YYSYMBOL_string = 50                     /* string  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




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

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
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
typedef yytype_int8 yy_state_t;

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
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
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

#if !defined yyoverflow

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
#endif /* !defined yyoverflow */

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
#define YYFINAL  4
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   83

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  22
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  29
/* YYNRULES -- Number of rules.  */
#define YYNRULES  59
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  87

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   260


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    20,     2,     2,     2,     2,    14,     2,
       8,     9,     2,     2,     7,     2,    10,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,    13,     2,
      19,    17,    18,     6,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    11,     2,    12,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    15,     2,    16,    21,     2,     2,     2,
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
       5
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_uint8 yyrline[] =
{
       0,    34,    34,    35,    36,    37,    40,    40,    43,    47,
      51,    53,    58,    60,    65,    67,    72,    74,    79,    81,
      86,    88,    93,    95,    97,   101,   107,   109,   114,   116,
     118,   123,   125,   130,   132,   134,   139,   141,   146,   151,
     153,   158,   160,   165,   167,   172,   174,   179,   181,   186,
     187,   188,   189,   190,   191,   192,   195,   199,   203,   207
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if YYDEBUG || 0
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "SCAN_WORD",
  "SCAN_STRINGCONST", "SCAN_NUMBERCONST", "'?'", "','", "'('", "')'",
  "'.'", "'['", "']'", "':'", "'&'", "'{'", "'}'", "'='", "'>'", "'<'",
  "'!'", "'~'", "$accept", "constraints", "optquestionmark", "projections",
  "selections", "projectionlist", "projection", "function", "segmentlist",
  "segment", "rangelist", "dap2_range", "range1", "clauselist", "sel_clause",
  "value_list", "value", "constant", "var", "indexpath", "index",
  "array_indices", "boolfunction", "arg_list", "rel_op", "ident", "word",
  "number", "string", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-36)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-57)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int8 yypact[] =
{
      10,   -36,     8,     4,   -36,   -36,    46,    12,   -36,    23,
     -36,   -36,    44,   -36,    12,   -36,     7,    -2,   -36,   -36,
     -36,    27,   -36,   -36,    45,   -36,   -36,    49,    17,   -36,
     -36,   -36,    36,    36,   -36,    18,    53,    48,   -36,    39,
      50,    51,    52,     9,    36,    31,    53,   -36,    54,   -36,
     -36,    48,   -36,    55,    57,    61,    29,   -36,   -36,   -36,
     -36,   -36,    46,   -36,   -36,    54,     3,    62,    60,   -36,
      46,   -36,   -36,    53,    13,   -36,     5,   -36,    64,    40,
      46,   -36,   -36,    53,   -36,    63,   -36
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       7,     6,     0,     0,     1,    57,     0,     2,     3,     8,
      10,    13,    12,    16,     9,    26,     0,    18,    59,    58,
      34,     0,    35,    33,    38,    39,    30,     0,    41,    36,
      37,     4,     0,     0,    27,     0,     0,    19,    20,    49,
      50,    51,     0,     0,     0,     0,     0,    43,    42,    11,
      17,    18,    14,     0,    31,     0,     0,    21,    55,    53,
      54,    52,     0,    29,    40,    41,    14,     0,     0,    44,
       0,    15,    22,     0,     0,    31,    15,    25,    32,     0,
       0,    28,    23,     0,    32,     0,    24
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -36,   -36,   -36,   -36,    56,   -36,    47,     1,   -36,    28,
     -36,    41,    32,   -36,    67,    14,    -6,   -36,   -36,   -36,
      33,   -36,   -36,    37,   -36,    77,    -1,   -35,   -36
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
       0,     2,     3,     7,     8,     9,    10,    20,    12,    13,
      37,    38,    47,    14,    15,    53,    54,    22,    23,    24,
      25,    48,    26,    55,    43,    16,    28,    29,    30
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int8 yytable[] =
{
      21,    56,    17,   -45,    11,   -46,   -56,     5,     4,    36,
      -5,    68,     5,    18,    19,    35,     1,   -45,     6,   -46,
      80,     5,    18,    19,    62,   -56,     6,    52,    46,    81,
      32,    17,    51,    11,     5,    18,    19,    63,    79,     5,
      66,    72,    73,    65,    39,    40,    41,    42,    85,     5,
      18,    19,    82,    83,    33,    44,    75,    45,    19,    36,
      58,    50,    70,    31,    78,    46,   -47,    59,    60,    61,
      71,    76,    77,   -48,    84,    86,    74,    64,    57,    49,
      69,    34,    67,    27
};

static const yytype_int8 yycheck[] =
{
       6,    36,     3,     0,     3,     0,     8,     3,     0,    11,
       0,    46,     3,     4,     5,     8,     6,    14,    14,    14,
       7,     3,     4,     5,    15,     8,    14,     9,    11,    16,
       7,    32,    33,    32,     3,     4,     5,    43,    73,     3,
       9,    12,    13,    44,    17,    18,    19,    20,    83,     3,
       4,     5,    12,    13,    10,    10,    62,     8,     5,    11,
      21,    33,     7,     7,    70,    11,     9,    17,    17,    17,
       9,     9,    12,     9,    80,    12,    62,    44,    37,    32,
      48,    14,    45,     6
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     6,    23,    24,     0,     3,    14,    25,    26,    27,
      28,    29,    30,    31,    35,    36,    47,    48,     4,     5,
      29,    38,    39,    40,    41,    42,    44,    47,    48,    49,
      50,    26,     7,    10,    36,     8,    11,    32,    33,    17,
      18,    19,    20,    46,    10,     8,    11,    34,    43,    28,
      31,    48,     9,    37,    38,    45,    49,    33,    21,    17,
      17,    17,    15,    38,    42,    48,     9,    45,    49,    34,
       7,     9,    12,    13,    37,    38,     9,    12,    38,    49,
       7,    16,    12,    13,    38,    49,    12
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    22,    23,    23,    23,    23,    24,    24,    25,    26,
      27,    27,    28,    28,    29,    29,    30,    30,    31,    31,
      32,    32,    33,    33,    33,    34,    35,    35,    36,    36,
      36,    37,    37,    38,    38,    38,    39,    39,    40,    41,
      41,    42,    42,    43,    43,    44,    44,    45,    45,    46,
      46,    46,    46,    46,    46,    46,    47,    48,    49,    50
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     2,     3,     0,     1,     0,     1,     1,
       1,     3,     1,     1,     3,     4,     1,     3,     1,     2,
       1,     2,     3,     5,     7,     3,     1,     2,     6,     4,
       2,     1,     3,     1,     1,     1,     1,     1,     1,     1,
       3,     1,     2,     1,     2,     3,     4,     1,     3,     1,
       1,     1,     2,     2,     2,     2,     1,     1,     1,     1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


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
        yyerror (parsestate, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF


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




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, parsestate); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, DCEparsestate* parsestate)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (parsestate);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, DCEparsestate* parsestate)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep, parsestate);
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
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp,
                 int yyrule, DCEparsestate* parsestate)
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
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)], parsestate);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, parsestate); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
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






/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, DCEparsestate* parsestate)
{
  YY_USE (yyvaluep);
  YY_USE (parsestate);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (DCEparsestate* parsestate)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

    /* Number of syntax errors so far.  */
    int yynerrs = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;



#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

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
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

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
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
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

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, parsestate);
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      goto yyerrlab1;
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
  case 8: /* projections: projectionlist  */
#line 43 "dce.y"
                       {projections(parsestate,yyvsp[0]);}
#line 1186 "dcetab.c"
    break;

  case 9: /* selections: clauselist  */
#line 47 "dce.y"
                   {selections(parsestate,yyvsp[0]);}
#line 1192 "dcetab.c"
    break;

  case 10: /* projectionlist: projection  */
#line 52 "dce.y"
            {yyval=projectionlist(parsestate,(Object)null,yyvsp[0]);}
#line 1198 "dcetab.c"
    break;

  case 11: /* projectionlist: projectionlist ',' projection  */
#line 54 "dce.y"
            {yyval=projectionlist(parsestate,yyvsp[-2],yyvsp[0]);}
#line 1204 "dcetab.c"
    break;

  case 12: /* projection: segmentlist  */
#line 59 "dce.y"
            {yyval=projection(parsestate,yyvsp[0]);}
#line 1210 "dcetab.c"
    break;

  case 13: /* projection: function  */
#line 61 "dce.y"
            {yyval=projection(parsestate,yyvsp[0]);}
#line 1216 "dcetab.c"
    break;

  case 14: /* function: ident '(' ')'  */
#line 66 "dce.y"
            {yyval=function(parsestate,yyvsp[-2],null);}
#line 1222 "dcetab.c"
    break;

  case 15: /* function: ident '(' arg_list ')'  */
#line 68 "dce.y"
            {yyval=function(parsestate,yyvsp[-3],yyvsp[-1]);}
#line 1228 "dcetab.c"
    break;

  case 16: /* segmentlist: segment  */
#line 73 "dce.y"
            {yyval=segmentlist(parsestate,null,yyvsp[0]);}
#line 1234 "dcetab.c"
    break;

  case 17: /* segmentlist: segmentlist '.' segment  */
#line 75 "dce.y"
            {yyval=segmentlist(parsestate,yyvsp[-2],yyvsp[0]);}
#line 1240 "dcetab.c"
    break;

  case 18: /* segment: word  */
#line 80 "dce.y"
            {yyval=segment(parsestate,yyvsp[0],null);}
#line 1246 "dcetab.c"
    break;

  case 19: /* segment: word rangelist  */
#line 82 "dce.y"
            {yyval=segment(parsestate,yyvsp[-1],yyvsp[0]);}
#line 1252 "dcetab.c"
    break;

  case 20: /* rangelist: dap2_range  */
#line 87 "dce.y"
            {yyval=rangelist(parsestate,null,yyvsp[0]);}
#line 1258 "dcetab.c"
    break;

  case 21: /* rangelist: rangelist dap2_range  */
#line 89 "dce.y"
            {yyval=rangelist(parsestate,yyvsp[-1],yyvsp[0]);}
#line 1264 "dcetab.c"
    break;

  case 22: /* dap2_range: '[' number ']'  */
#line 94 "dce.y"
            {yyval=dap2_range(parsestate,yyvsp[-1],null,null);}
#line 1270 "dcetab.c"
    break;

  case 23: /* dap2_range: '[' number ':' number ']'  */
#line 96 "dce.y"
            {yyval=dap2_range(parsestate,yyvsp[-3],null,yyvsp[-1]);}
#line 1276 "dcetab.c"
    break;

  case 24: /* dap2_range: '[' number ':' number ':' number ']'  */
#line 98 "dce.y"
            {yyval=dap2_range(parsestate,yyvsp[-5],yyvsp[-3],yyvsp[-1]);}
#line 1282 "dcetab.c"
    break;

  case 25: /* range1: '[' number ']'  */
#line 102 "dce.y"
            {yyval = range1(parsestate,yyvsp[-1]);}
#line 1288 "dcetab.c"
    break;

  case 26: /* clauselist: sel_clause  */
#line 108 "dce.y"
            {yyval=clauselist(parsestate,null,yyvsp[0]);}
#line 1294 "dcetab.c"
    break;

  case 27: /* clauselist: clauselist sel_clause  */
#line 110 "dce.y"
            {yyval=clauselist(parsestate,yyvsp[-1],yyvsp[0]);}
#line 1300 "dcetab.c"
    break;

  case 28: /* sel_clause: '&' value rel_op '{' value_list '}'  */
#line 115 "dce.y"
            {yyval=sel_clause(parsestate,1,yyvsp[-4],yyvsp[-3],yyvsp[-1]);}
#line 1306 "dcetab.c"
    break;

  case 29: /* sel_clause: '&' value rel_op value  */
#line 117 "dce.y"
            {yyval=sel_clause(parsestate,2,yyvsp[-2],yyvsp[-1],yyvsp[0]);}
#line 1312 "dcetab.c"
    break;

  case 30: /* sel_clause: '&' boolfunction  */
#line 119 "dce.y"
            {yyval=yyvsp[-1];}
#line 1318 "dcetab.c"
    break;

  case 31: /* value_list: value  */
#line 124 "dce.y"
            {yyval=value_list(parsestate,null,yyvsp[0]);}
#line 1324 "dcetab.c"
    break;

  case 32: /* value_list: value_list ',' value  */
#line 126 "dce.y"
            {yyval=value_list(parsestate,yyvsp[-2],yyvsp[0]);}
#line 1330 "dcetab.c"
    break;

  case 33: /* value: var  */
#line 131 "dce.y"
            {yyval=value(parsestate,yyvsp[0]);}
#line 1336 "dcetab.c"
    break;

  case 34: /* value: function  */
#line 133 "dce.y"
            {yyval=value(parsestate,yyvsp[0]);}
#line 1342 "dcetab.c"
    break;

  case 35: /* value: constant  */
#line 135 "dce.y"
            {yyval=value(parsestate,yyvsp[0]);}
#line 1348 "dcetab.c"
    break;

  case 36: /* constant: number  */
#line 140 "dce.y"
            {yyval=constant(parsestate,yyvsp[0],SCAN_NUMBERCONST);}
#line 1354 "dcetab.c"
    break;

  case 37: /* constant: string  */
#line 142 "dce.y"
            {yyval=constant(parsestate,yyvsp[0],SCAN_STRINGCONST);}
#line 1360 "dcetab.c"
    break;

  case 38: /* var: indexpath  */
#line 147 "dce.y"
            {yyval=var(parsestate,yyvsp[0]);}
#line 1366 "dcetab.c"
    break;

  case 39: /* indexpath: index  */
#line 152 "dce.y"
            {yyval=indexpath(parsestate,null,yyvsp[0]);}
#line 1372 "dcetab.c"
    break;

  case 40: /* indexpath: indexpath '.' index  */
#line 154 "dce.y"
            {yyval=indexpath(parsestate,yyvsp[-2],yyvsp[0]);}
#line 1378 "dcetab.c"
    break;

  case 41: /* index: word  */
#line 159 "dce.y"
            {yyval=indexer(parsestate,yyvsp[0],null);}
#line 1384 "dcetab.c"
    break;

  case 42: /* index: word array_indices  */
#line 161 "dce.y"
            {yyval=indexer(parsestate,yyvsp[-1],yyvsp[0]);}
#line 1390 "dcetab.c"
    break;

  case 43: /* array_indices: range1  */
#line 166 "dce.y"
            {yyval=array_indices(parsestate,null,yyvsp[0]);}
#line 1396 "dcetab.c"
    break;

  case 44: /* array_indices: array_indices range1  */
#line 168 "dce.y"
            {yyval=array_indices(parsestate,yyvsp[-1],yyvsp[0]);}
#line 1402 "dcetab.c"
    break;

  case 45: /* boolfunction: ident '(' ')'  */
#line 173 "dce.y"
            {yyval=function(parsestate,yyvsp[-2],null);}
#line 1408 "dcetab.c"
    break;

  case 46: /* boolfunction: ident '(' arg_list ')'  */
#line 175 "dce.y"
            {yyval=function(parsestate,yyvsp[-3],yyvsp[-1]);}
#line 1414 "dcetab.c"
    break;

  case 47: /* arg_list: value  */
#line 180 "dce.y"
            {yyval=arg_list(parsestate,null,yyvsp[0]);}
#line 1420 "dcetab.c"
    break;

  case 48: /* arg_list: value_list ',' value  */
#line 182 "dce.y"
            {yyval=arg_list(parsestate,yyvsp[-2],yyvsp[0]);}
#line 1426 "dcetab.c"
    break;

  case 49: /* rel_op: '='  */
#line 186 "dce.y"
                  {yyval=makeselectiontag(CEO_EQ);}
#line 1432 "dcetab.c"
    break;

  case 50: /* rel_op: '>'  */
#line 187 "dce.y"
                  {yyval=makeselectiontag(CEO_GT);}
#line 1438 "dcetab.c"
    break;

  case 51: /* rel_op: '<'  */
#line 188 "dce.y"
                  {yyval=makeselectiontag(CEO_LT);}
#line 1444 "dcetab.c"
    break;

  case 52: /* rel_op: '!' '='  */
#line 189 "dce.y"
                  {yyval=makeselectiontag(CEO_NEQ);}
#line 1450 "dcetab.c"
    break;

  case 53: /* rel_op: '>' '='  */
#line 190 "dce.y"
                  {yyval=makeselectiontag(CEO_GE);}
#line 1456 "dcetab.c"
    break;

  case 54: /* rel_op: '<' '='  */
#line 191 "dce.y"
                  {yyval=makeselectiontag(CEO_LE);}
#line 1462 "dcetab.c"
    break;

  case 55: /* rel_op: '=' '~'  */
#line 192 "dce.y"
                  {yyval=makeselectiontag(CEO_RE);}
#line 1468 "dcetab.c"
    break;

  case 56: /* ident: word  */
#line 196 "dce.y"
            {yyval = yyvsp[0];}
#line 1474 "dcetab.c"
    break;

  case 57: /* word: SCAN_WORD  */
#line 200 "dce.y"
            {yyval = checkobject(yyvsp[0]);}
#line 1480 "dcetab.c"
    break;

  case 58: /* number: SCAN_NUMBERCONST  */
#line 204 "dce.y"
            {yyval = checkobject(yyvsp[0]);}
#line 1486 "dcetab.c"
    break;

  case 59: /* string: SCAN_STRINGCONST  */
#line 208 "dce.y"
            {yyval = checkobject(yyvsp[0]);}
#line 1492 "dcetab.c"
    break;


#line 1496 "dcetab.c"

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
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

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
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      yyerror (parsestate, YY_("syntax error"));
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
                      yytoken, &yylval, parsestate);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

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
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
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
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, parsestate);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (parsestate, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, parsestate);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, parsestate);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif

  return yyresult;
}

#line 211 "dce.y"

