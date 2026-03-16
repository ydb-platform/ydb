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
#define yyparse         dapparse
#define yylex           daplex
#define yyerror         daperror
#define yydebug         dapdebug
#define yynerrs         dapnerrs

/* First part of user prologue.  */
#line 11 "dap.y"

#include "config.h"
#include "dapparselex.h"
#include "dapy.h"
int dapdebug = 0;

#line 83 "dapy.c"

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

#include "dapy.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_SCAN_ALIAS = 3,                 /* SCAN_ALIAS  */
  YYSYMBOL_SCAN_ARRAY = 4,                 /* SCAN_ARRAY  */
  YYSYMBOL_SCAN_ATTR = 5,                  /* SCAN_ATTR  */
  YYSYMBOL_SCAN_BYTE = 6,                  /* SCAN_BYTE  */
  YYSYMBOL_SCAN_CODE = 7,                  /* SCAN_CODE  */
  YYSYMBOL_SCAN_DATASET = 8,               /* SCAN_DATASET  */
  YYSYMBOL_SCAN_DATA = 9,                  /* SCAN_DATA  */
  YYSYMBOL_SCAN_ERROR = 10,                /* SCAN_ERROR  */
  YYSYMBOL_SCAN_FLOAT32 = 11,              /* SCAN_FLOAT32  */
  YYSYMBOL_SCAN_FLOAT64 = 12,              /* SCAN_FLOAT64  */
  YYSYMBOL_SCAN_GRID = 13,                 /* SCAN_GRID  */
  YYSYMBOL_SCAN_INT16 = 14,                /* SCAN_INT16  */
  YYSYMBOL_SCAN_INT32 = 15,                /* SCAN_INT32  */
  YYSYMBOL_SCAN_MAPS = 16,                 /* SCAN_MAPS  */
  YYSYMBOL_SCAN_MESSAGE = 17,              /* SCAN_MESSAGE  */
  YYSYMBOL_SCAN_SEQUENCE = 18,             /* SCAN_SEQUENCE  */
  YYSYMBOL_SCAN_STRING = 19,               /* SCAN_STRING  */
  YYSYMBOL_SCAN_STRUCTURE = 20,            /* SCAN_STRUCTURE  */
  YYSYMBOL_SCAN_UINT16 = 21,               /* SCAN_UINT16  */
  YYSYMBOL_SCAN_UINT32 = 22,               /* SCAN_UINT32  */
  YYSYMBOL_SCAN_URL = 23,                  /* SCAN_URL  */
  YYSYMBOL_SCAN_PTYPE = 24,                /* SCAN_PTYPE  */
  YYSYMBOL_SCAN_PROG = 25,                 /* SCAN_PROG  */
  YYSYMBOL_WORD_WORD = 26,                 /* WORD_WORD  */
  YYSYMBOL_WORD_STRING = 27,               /* WORD_STRING  */
  YYSYMBOL_28_ = 28,                       /* '{'  */
  YYSYMBOL_29_ = 29,                       /* '}'  */
  YYSYMBOL_30_ = 30,                       /* ';'  */
  YYSYMBOL_31_ = 31,                       /* ':'  */
  YYSYMBOL_32_ = 32,                       /* '['  */
  YYSYMBOL_33_ = 33,                       /* ']'  */
  YYSYMBOL_34_ = 34,                       /* '='  */
  YYSYMBOL_35_ = 35,                       /* ','  */
  YYSYMBOL_YYACCEPT = 36,                  /* $accept  */
  YYSYMBOL_start = 37,                     /* start  */
  YYSYMBOL_dataset = 38,                   /* dataset  */
  YYSYMBOL_attr = 39,                      /* attr  */
  YYSYMBOL_err = 40,                       /* err  */
  YYSYMBOL_datasetbody = 41,               /* datasetbody  */
  YYSYMBOL_declarations = 42,              /* declarations  */
  YYSYMBOL_declaration = 43,               /* declaration  */
  YYSYMBOL_base_type = 44,                 /* base_type  */
  YYSYMBOL_array_decls = 45,               /* array_decls  */
  YYSYMBOL_array_decl = 46,                /* array_decl  */
  YYSYMBOL_datasetname = 47,               /* datasetname  */
  YYSYMBOL_var_name = 48,                  /* var_name  */
  YYSYMBOL_attributebody = 49,             /* attributebody  */
  YYSYMBOL_attr_list = 50,                 /* attr_list  */
  YYSYMBOL_attribute = 51,                 /* attribute  */
  YYSYMBOL_bytes = 52,                     /* bytes  */
  YYSYMBOL_int16 = 53,                     /* int16  */
  YYSYMBOL_uint16 = 54,                    /* uint16  */
  YYSYMBOL_int32 = 55,                     /* int32  */
  YYSYMBOL_uint32 = 56,                    /* uint32  */
  YYSYMBOL_float32 = 57,                   /* float32  */
  YYSYMBOL_float64 = 58,                   /* float64  */
  YYSYMBOL_strs = 59,                      /* strs  */
  YYSYMBOL_urls = 60,                      /* urls  */
  YYSYMBOL_url = 61,                       /* url  */
  YYSYMBOL_str_or_id = 62,                 /* str_or_id  */
  YYSYMBOL_alias = 63,                     /* alias  */
  YYSYMBOL_errorbody = 64,                 /* errorbody  */
  YYSYMBOL_errorcode = 65,                 /* errorcode  */
  YYSYMBOL_errormsg = 66,                  /* errormsg  */
  YYSYMBOL_errorptype = 67,                /* errorptype  */
  YYSYMBOL_errorprog = 68,                 /* errorprog  */
  YYSYMBOL_name = 69                       /* name  */
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
typedef yytype_uint8 yy_state_t;

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

#if 1

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
#endif /* 1 */

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
#define YYFINAL  9
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   369

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  36
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  34
/* YYNRULES -- Number of rules.  */
#define YYNRULES  106
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  201

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   282


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
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,    35,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,    31,    30,
       2,    34,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    32,     2,    33,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    28,     2,    29,     2,     2,     2,     2,
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
      25,    26,    27
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,    54,    54,    55,    56,    57,    58,    62,    66,    70,
      75,    81,    82,    88,    90,    92,    94,    97,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   115,   116,   120,
     121,   122,   123,   128,   129,   133,   136,   137,   142,   143,
     147,   148,   150,   152,   154,   156,   158,   160,   162,   164,
     166,   167,   172,   173,   177,   178,   182,   183,   187,   188,
     192,   193,   196,   197,   200,   201,   204,   205,   209,   210,
     214,   218,   219,   230,   234,   238,   238,   239,   239,   240,
     240,   241,   241,   247,   248,   249,   250,   251,   252,   253,
     254,   255,   256,   257,   258,   259,   260,   261,   262,   263,
     264,   265,   266,   267,   268,   269,   270
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if 1
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "SCAN_ALIAS",
  "SCAN_ARRAY", "SCAN_ATTR", "SCAN_BYTE", "SCAN_CODE", "SCAN_DATASET",
  "SCAN_DATA", "SCAN_ERROR", "SCAN_FLOAT32", "SCAN_FLOAT64", "SCAN_GRID",
  "SCAN_INT16", "SCAN_INT32", "SCAN_MAPS", "SCAN_MESSAGE", "SCAN_SEQUENCE",
  "SCAN_STRING", "SCAN_STRUCTURE", "SCAN_UINT16", "SCAN_UINT32",
  "SCAN_URL", "SCAN_PTYPE", "SCAN_PROG", "WORD_WORD", "WORD_STRING", "'{'",
  "'}'", "';'", "':'", "'['", "']'", "'='", "','", "$accept", "start",
  "dataset", "attr", "err", "datasetbody", "declarations", "declaration",
  "base_type", "array_decls", "array_decl", "datasetname", "var_name",
  "attributebody", "attr_list", "attribute", "bytes", "int16", "uint16",
  "int32", "uint32", "float32", "float64", "strs", "urls", "url",
  "str_or_id", "alias", "errorbody", "errorcode", "errormsg", "errorptype",
  "errorprog", "name", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-91)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
       6,   -91,   -91,   -91,   -91,     9,   -22,     7,   -16,   -91,
     -91,    10,   -91,   -91,   -91,    20,   -91,    37,   -91,   191,
      -6,    14,   -91,   -91,   -91,   -91,    17,   -91,   -91,    18,
     -91,    19,   -91,   -91,   -91,   271,   -91,   320,   -91,    27,
     -91,   -91,   320,   -91,   -91,   -91,   -91,   320,   320,   -91,
     320,   320,   -91,   -91,   -91,   320,   -91,   320,   320,   320,
     -91,   -91,   -91,   -91,   -91,    24,    43,    35,    39,    50,
      74,   -91,   -91,   -91,   -91,   -91,   -91,   -91,   -91,   -91,
     -91,   -91,   -91,   -91,    55,   -91,   -91,   -91,    60,    67,
      68,    70,    71,    73,   295,    77,    78,   295,   -91,   -91,
      65,    79,    66,    80,    76,    69,   127,   -91,     4,   -91,
     -91,   -20,   -91,   -13,   -91,   -12,   -91,   -10,   -91,    -9,
     -91,    32,   -91,   -91,   -91,    33,   -91,    34,    42,   -91,
     -91,   218,   -91,    81,    82,    75,    83,   346,   320,   320,
     -91,   -91,   159,   -91,   -91,    84,   -91,    88,   -91,    89,
     -91,    90,   -91,    91,   -91,   295,   -91,    92,   -91,    93,
     -91,   295,   -91,   -91,    95,    94,    96,   105,    97,   -91,
      98,   103,   100,   -91,   -91,   -91,   -91,   -91,   -91,   -91,
     -91,   -91,   -91,   102,   -91,    99,   -91,    12,   -91,   111,
     109,   -91,   -91,   -91,   -91,   118,   244,   -91,   320,   106,
     -91
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,     6,     8,     7,     9,     0,     0,     0,     0,     1,
      11,     2,    37,    38,     4,    75,     5,     0,     3,     0,
       0,    77,    17,    18,    23,    24,     0,    19,    21,     0,
      26,     0,    20,    22,    25,     0,    12,     0,    51,    84,
      85,    86,    87,   103,    88,    89,    90,    91,    92,    93,
      94,    95,    96,   104,    97,    98,    99,   100,   101,   102,
     106,   105,    83,    36,    39,     0,     0,     0,     0,    79,
       0,    11,    11,    34,    84,    87,    91,    92,    94,    95,
      98,   100,   101,   102,     0,    33,    35,    27,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    40,    38,
       0,     0,     0,    81,     0,     0,     0,    10,     0,    73,
      52,     0,    62,     0,    64,     0,    54,     0,    58,     0,
      72,     0,    66,    71,    56,     0,    60,     0,     0,    68,
      70,     0,    76,     0,     0,     0,     0,     0,     0,     0,
      32,    13,     0,    28,    41,     0,    46,     0,    47,     0,
      42,     0,    44,     0,    48,     0,    43,     0,    45,     0,
      49,     0,    50,    78,     0,     0,     0,     0,     0,    27,
      83,     0,     0,    53,    63,    65,    55,    59,    67,    57,
      61,    69,    80,     0,    74,     0,    15,     0,    29,     0,
       0,    82,    11,    14,    30,     0,     0,    31,     0,     0,
      16
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -91,   -91,   -91,   -91,   -91,   -91,   -69,   -15,   -91,   -17,
     -91,   -91,   -37,   -91,    54,   -91,   -91,   -91,   -91,   -91,
     -91,   -91,   -91,   -91,   -91,    -7,   -90,   -91,   -91,   -91,
     -91,   -91,   -91,   -18
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,     5,     6,     7,     8,    11,    17,    36,    37,   108,
     143,    84,    85,    14,    19,    64,   111,   117,   125,   119,
     127,   113,   115,   121,   128,   129,   130,    65,    16,    21,
      69,   103,   136,    86
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_uint8 yytable[] =
{
      87,    66,   105,   106,   122,   140,    10,     1,    12,     9,
     144,     2,    15,   140,     3,   145,     4,   146,   148,    18,
     150,   152,   147,   149,    89,   151,   153,    20,    67,    90,
      91,    68,    92,    93,   141,    13,   142,    94,    22,    95,
      96,    97,   193,    23,   142,    70,    71,    72,    24,    25,
      26,    27,    28,    88,    98,    29,    30,    31,    32,    33,
      34,   100,   154,   156,   158,   178,    35,   155,   157,   159,
      22,    99,   160,   101,   102,    23,   123,   161,   104,   123,
      24,    25,    26,    27,    28,   107,   109,    29,    30,    31,
      32,    33,    34,   110,   112,   132,   114,   116,   138,   118,
     134,   168,   169,   124,   126,   135,   133,   137,   164,   165,
     173,   163,   166,    66,   174,   175,   176,   177,   179,   180,
     183,   185,   167,   196,   172,   182,   184,   186,    22,   189,
     192,   188,   191,    23,   190,   195,   200,   123,    24,    25,
      26,    27,    28,   123,   194,    29,    30,    31,    32,    33,
      34,   197,   187,   131,   181,     0,   139,     0,     0,     0,
       0,   199,    74,    40,    41,    75,    43,    44,    45,    46,
      76,    77,    49,    78,    79,    52,    53,    54,    80,    56,
      81,    82,    83,    60,    61,   170,     0,     0,     0,     0,
       0,     0,    38,   171,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,     0,    38,
      63,    39,    40,    41,    42,    43,    44,    45,    46,    47,
      48,    49,    50,    51,    52,    53,    54,    55,    56,    57,
      58,    59,    60,    61,    62,    22,     0,   162,     0,     0,
      23,     0,     0,     0,     0,    24,    25,    26,    27,    28,
       0,     0,    29,    30,    31,    32,    33,    34,     0,     0,
       0,     0,    73,   198,    74,    40,    41,    75,    43,    44,
      45,    46,    76,    77,    49,    78,    79,    52,    53,    54,
      80,    56,    81,    82,    83,    60,    61,    62,    74,    40,
      41,    75,    43,    44,    45,    46,    76,    77,    49,    78,
      79,    52,    53,    54,    80,    56,    81,    82,    83,    60,
      61,    62,   120,    74,    40,    41,    75,    43,    44,    45,
      46,    76,    77,    49,    78,    79,    52,    53,    54,    80,
      56,    81,    82,    83,    60,    61,    62,    22,     0,     0,
       0,     0,    23,     0,     0,     0,     0,    24,    25,    26,
      27,    28,     0,     0,    29,    30,    31,    32,    33,    34
};

static const yytype_int16 yycheck[] =
{
      37,    19,    71,    72,    94,     1,    28,     1,     1,     0,
      30,     5,    28,     1,     8,    35,    10,    30,    30,     9,
      30,    30,    35,    35,    42,    35,    35,     7,    34,    47,
      48,    17,    50,    51,    30,    28,    32,    55,     1,    57,
      58,    59,    30,     6,    32,    28,    28,    28,    11,    12,
      13,    14,    15,    26,    30,    18,    19,    20,    21,    22,
      23,    26,    30,    30,    30,   155,    29,    35,    35,    35,
       1,    28,    30,    34,    24,     6,    94,    35,     4,    97,
      11,    12,    13,    14,    15,    30,    26,    18,    19,    20,
      21,    22,    23,    26,    26,    30,    26,    26,    29,    26,
      34,   138,   139,    26,    26,    25,    27,    31,    26,    34,
      26,    30,    29,   131,    26,    26,    26,    26,    26,    26,
      26,    16,   137,   192,   142,    30,    30,    30,     1,    26,
      31,    33,    30,     6,    34,    26,    30,   155,    11,    12,
      13,    14,    15,   161,    33,    18,    19,    20,    21,    22,
      23,    33,   169,    99,   161,    -1,    29,    -1,    -1,    -1,
      -1,   198,     3,     4,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    -1,    -1,    -1,    -1,
      -1,    -1,     1,    34,     3,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    -1,     1,
      29,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,     1,    -1,    29,    -1,    -1,
       6,    -1,    -1,    -1,    -1,    11,    12,    13,    14,    15,
      -1,    -1,    18,    19,    20,    21,    22,    23,    -1,    -1,
      -1,    -1,     1,    29,     3,     4,     5,     6,     7,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,    18,
      19,    20,    21,    22,    23,    24,    25,    26,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,     3,     4,     5,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,    16,    17,    18,    19,
      20,    21,    22,    23,    24,    25,    26,     1,    -1,    -1,
      -1,    -1,     6,    -1,    -1,    -1,    -1,    11,    12,    13,
      14,    15,    -1,    -1,    18,    19,    20,    21,    22,    23
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     1,     5,     8,    10,    37,    38,    39,    40,     0,
      28,    41,     1,    28,    49,    28,    64,    42,     9,    50,
       7,    65,     1,     6,    11,    12,    13,    14,    15,    18,
      19,    20,    21,    22,    23,    29,    43,    44,     1,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    29,    51,    63,    69,    34,    17,    66,
      28,    28,    28,     1,     3,     6,    11,    12,    14,    15,
      19,    21,    22,    23,    47,    48,    69,    48,    26,    69,
      69,    69,    69,    69,    69,    69,    69,    69,    30,    28,
      26,    34,    24,    67,     4,    42,    42,    30,    45,    26,
      26,    52,    26,    57,    26,    58,    26,    53,    26,    55,
      27,    59,    62,    69,    26,    54,    26,    56,    60,    61,
      62,    50,    30,    27,    34,    25,    68,    31,    29,    29,
       1,    30,    32,    46,    30,    35,    30,    35,    30,    35,
      30,    35,    30,    35,    30,    35,    30,    35,    30,    35,
      30,    35,    29,    30,    26,    34,    29,    43,    48,    48,
      26,    34,    69,    26,    26,    26,    26,    26,    62,    26,
      26,    61,    30,    26,    30,    16,    30,    45,    33,    26,
      34,    30,    31,    30,    33,    26,    42,    33,    29,    48,
      30
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    36,    37,    37,    37,    37,    37,    38,    39,    40,
      41,    42,    42,    43,    43,    43,    43,    43,    44,    44,
      44,    44,    44,    44,    44,    44,    44,    45,    45,    46,
      46,    46,    46,    47,    47,    48,    49,    49,    50,    50,
      51,    51,    51,    51,    51,    51,    51,    51,    51,    51,
      51,    51,    52,    52,    53,    53,    54,    54,    55,    55,
      56,    56,    57,    57,    58,    58,    59,    59,    60,    60,
      61,    62,    62,    63,    64,    65,    65,    66,    66,    67,
      67,    68,    68,    69,    69,    69,    69,    69,    69,    69,
      69,    69,    69,    69,    69,    69,    69,    69,    69,    69,
      69,    69,    69,    69,    69,    69,    69
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     3,     2,     2,     1,     1,     1,     1,
       5,     0,     2,     4,     7,     6,    11,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     0,     2,     3,
       4,     5,     1,     1,     1,     1,     3,     1,     0,     2,
       2,     4,     4,     4,     4,     4,     4,     4,     4,     4,
       4,     1,     1,     3,     1,     3,     1,     3,     1,     3,
       1,     3,     1,     3,     1,     3,     1,     3,     1,     3,
       1,     1,     1,     3,     7,     0,     4,     0,     4,     0,
       4,     0,     4,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1
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
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, DAPparsestate* parsestate)
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
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, DAPparsestate* parsestate)
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
                 int yyrule, DAPparsestate* parsestate)
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


/* Context of a parse error.  */
typedef struct
{
  yy_state_t *yyssp;
  yysymbol_kind_t yytoken;
} yypcontext_t;

/* Put in YYARG at most YYARGN of the expected tokens given the
   current YYCTX, and return the number of tokens stored in YYARG.  If
   YYARG is null, return the number of expected tokens (guaranteed to
   be less than YYNTOKENS).  Return YYENOMEM on memory exhaustion.
   Return 0 if there are more than YYARGN expected tokens, yet fill
   YYARG up to YYARGN. */
static int
yypcontext_expected_tokens (const yypcontext_t *yyctx,
                            yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  int yyn = yypact[+*yyctx->yyssp];
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
        if (yycheck[yyx + yyn] == yyx && yyx != YYSYMBOL_YYerror
            && !yytable_value_is_error (yytable[yyx + yyn]))
          {
            if (!yyarg)
              ++yycount;
            else if (yycount == yyargn)
              return 0;
            else
              yyarg[yycount++] = YY_CAST (yysymbol_kind_t, yyx);
          }
    }
  if (yyarg && yycount == 0 && 0 < yyargn)
    yyarg[0] = YYSYMBOL_YYEMPTY;
  return yycount;
}




#ifndef yystrlen
# if defined __GLIBC__ && defined _STRING_H
#  define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
# else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
# endif
#endif

#ifndef yystpcpy
# if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#  define yystpcpy stpcpy
# else
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
# endif
#endif

#ifndef yytnamerr
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
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
#endif


static int
yy_syntax_error_arguments (const yypcontext_t *yyctx,
                           yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
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
  if (yyctx->yytoken != YYSYMBOL_YYEMPTY)
    {
      int yyn;
      if (yyarg)
        yyarg[yycount] = yyctx->yytoken;
      ++yycount;
      yyn = yypcontext_expected_tokens (yyctx,
                                        yyarg ? yyarg + 1 : yyarg, yyargn - 1);
      if (yyn == YYENOMEM)
        return YYENOMEM;
      else
        yycount += yyn;
    }
  return yycount;
}

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return -1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return YYENOMEM if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                const yypcontext_t *yyctx)
{
  enum { YYARGS_MAX = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  yysymbol_kind_t yyarg[YYARGS_MAX];
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* Actual size of YYARG. */
  int yycount = yy_syntax_error_arguments (yyctx, yyarg, YYARGS_MAX);
  if (yycount == YYENOMEM)
    return YYENOMEM;

  switch (yycount)
    {
#define YYCASE_(N, S)                       \
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
#undef YYCASE_
    }

  /* Compute error message size.  Don't count the "%s"s, but reserve
     room for the terminator.  */
  yysize = yystrlen (yyformat) - 2 * yycount + 1;
  {
    int yyi;
    for (yyi = 0; yyi < yycount; ++yyi)
      {
        YYPTRDIFF_T yysize1
          = yysize + yytnamerr (YY_NULLPTR, yytname[yyarg[yyi]]);
        if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
          yysize = yysize1;
        else
          return YYENOMEM;
      }
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return -1;
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
          yyp += yytnamerr (yyp, yytname[yyarg[yyi++]]);
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


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, DAPparsestate* parsestate)
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
yyparse (DAPparsestate* parsestate)
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

  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;

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
  case 6: /* start: error  */
#line 58 "dap.y"
                {dap_unrecognizedresponse(parsestate); YYABORT;}
#line 1585 "dapy.c"
    break;

  case 7: /* dataset: SCAN_DATASET  */
#line 63 "dap.y"
            {dap_tagparse(parsestate,SCAN_DATASET);}
#line 1591 "dapy.c"
    break;

  case 8: /* attr: SCAN_ATTR  */
#line 67 "dap.y"
            {dap_tagparse(parsestate,SCAN_ATTR);}
#line 1597 "dapy.c"
    break;

  case 9: /* err: SCAN_ERROR  */
#line 71 "dap.y"
            {dap_tagparse(parsestate,SCAN_ERROR);}
#line 1603 "dapy.c"
    break;

  case 10: /* datasetbody: '{' declarations '}' datasetname ';'  */
#line 76 "dap.y"
                {dap_datasetbody(parsestate,yyvsp[-1],yyvsp[-3]);}
#line 1609 "dapy.c"
    break;

  case 11: /* declarations: %empty  */
#line 81 "dap.y"
                      {yyval=dap_declarations(parsestate,null,null);}
#line 1615 "dapy.c"
    break;

  case 12: /* declarations: declarations declaration  */
#line 82 "dap.y"
                                   {yyval=dap_declarations(parsestate,yyvsp[-1],yyvsp[0]);}
#line 1621 "dapy.c"
    break;

  case 13: /* declaration: base_type var_name array_decls ';'  */
#line 89 "dap.y"
                {yyval=dap_makebase(parsestate,yyvsp[-2],yyvsp[-3],yyvsp[-1]);}
#line 1627 "dapy.c"
    break;

  case 14: /* declaration: SCAN_STRUCTURE '{' declarations '}' var_name array_decls ';'  */
#line 91 "dap.y"
            {if((yyval=dap_makestructure(parsestate,yyvsp[-2],yyvsp[-1],yyvsp[-4]))==null) {YYABORT;}}
#line 1633 "dapy.c"
    break;

  case 15: /* declaration: SCAN_SEQUENCE '{' declarations '}' var_name ';'  */
#line 93 "dap.y"
            {if((yyval=dap_makesequence(parsestate,yyvsp[-1],yyvsp[-3]))==null) {YYABORT;}}
#line 1639 "dapy.c"
    break;

  case 16: /* declaration: SCAN_GRID '{' SCAN_ARRAY ':' declaration SCAN_MAPS ':' declarations '}' var_name ';'  */
#line 96 "dap.y"
            {if((yyval=dap_makegrid(parsestate,yyvsp[-1],yyvsp[-6],yyvsp[-3]))==null) {YYABORT;}}
#line 1645 "dapy.c"
    break;

  case 17: /* declaration: error  */
#line 98 "dap.y"
            {dapsemanticerror(parsestate,OC_EBADTYPE,"Unrecognized type"); YYABORT;}
#line 1651 "dapy.c"
    break;

  case 18: /* base_type: SCAN_BYTE  */
#line 103 "dap.y"
                    {yyval=(Object)SCAN_BYTE;}
#line 1657 "dapy.c"
    break;

  case 19: /* base_type: SCAN_INT16  */
#line 104 "dap.y"
                     {yyval=(Object)SCAN_INT16;}
#line 1663 "dapy.c"
    break;

  case 20: /* base_type: SCAN_UINT16  */
#line 105 "dap.y"
                      {yyval=(Object)SCAN_UINT16;}
#line 1669 "dapy.c"
    break;

  case 21: /* base_type: SCAN_INT32  */
#line 106 "dap.y"
                     {yyval=(Object)SCAN_INT32;}
#line 1675 "dapy.c"
    break;

  case 22: /* base_type: SCAN_UINT32  */
#line 107 "dap.y"
                      {yyval=(Object)SCAN_UINT32;}
#line 1681 "dapy.c"
    break;

  case 23: /* base_type: SCAN_FLOAT32  */
#line 108 "dap.y"
                       {yyval=(Object)SCAN_FLOAT32;}
#line 1687 "dapy.c"
    break;

  case 24: /* base_type: SCAN_FLOAT64  */
#line 109 "dap.y"
                       {yyval=(Object)SCAN_FLOAT64;}
#line 1693 "dapy.c"
    break;

  case 25: /* base_type: SCAN_URL  */
#line 110 "dap.y"
                   {yyval=(Object)SCAN_URL;}
#line 1699 "dapy.c"
    break;

  case 26: /* base_type: SCAN_STRING  */
#line 111 "dap.y"
                      {yyval=(Object)SCAN_STRING;}
#line 1705 "dapy.c"
    break;

  case 27: /* array_decls: %empty  */
#line 115 "dap.y"
                      {yyval=dap_arraydecls(parsestate,null,null);}
#line 1711 "dapy.c"
    break;

  case 28: /* array_decls: array_decls array_decl  */
#line 116 "dap.y"
                                 {yyval=dap_arraydecls(parsestate,yyvsp[-1],yyvsp[0]);}
#line 1717 "dapy.c"
    break;

  case 29: /* array_decl: '[' WORD_WORD ']'  */
#line 120 "dap.y"
                             {yyval=dap_arraydecl(parsestate,null,yyvsp[-1]);}
#line 1723 "dapy.c"
    break;

  case 30: /* array_decl: '[' '=' WORD_WORD ']'  */
#line 121 "dap.y"
                                 {yyval=dap_arraydecl(parsestate,null,yyvsp[-1]);}
#line 1729 "dapy.c"
    break;

  case 31: /* array_decl: '[' name '=' WORD_WORD ']'  */
#line 122 "dap.y"
                                      {yyval=dap_arraydecl(parsestate,yyvsp[-3],yyvsp[-1]);}
#line 1735 "dapy.c"
    break;

  case 32: /* array_decl: error  */
#line 124 "dap.y"
            {dapsemanticerror(parsestate,OC_EDIMSIZE,"Illegal dimension declaration"); YYABORT;}
#line 1741 "dapy.c"
    break;

  case 33: /* datasetname: var_name  */
#line 128 "dap.y"
                   {yyval=yyvsp[0];}
#line 1747 "dapy.c"
    break;

  case 34: /* datasetname: error  */
#line 130 "dap.y"
            {dapsemanticerror(parsestate,OC_EDDS,"Illegal dataset declaration"); YYABORT;}
#line 1753 "dapy.c"
    break;

  case 35: /* var_name: name  */
#line 133 "dap.y"
               {yyval=yyvsp[0];}
#line 1759 "dapy.c"
    break;

  case 36: /* attributebody: '{' attr_list '}'  */
#line 136 "dap.y"
                            {dap_attributebody(parsestate,yyvsp[-1]);}
#line 1765 "dapy.c"
    break;

  case 37: /* attributebody: error  */
#line 138 "dap.y"
            {dapsemanticerror(parsestate,OC_EDAS,"Illegal DAS body"); YYABORT;}
#line 1771 "dapy.c"
    break;

  case 38: /* attr_list: %empty  */
#line 142 "dap.y"
                      {yyval=dap_attrlist(parsestate,null,null);}
#line 1777 "dapy.c"
    break;

  case 39: /* attr_list: attr_list attribute  */
#line 143 "dap.y"
                              {yyval=dap_attrlist(parsestate,yyvsp[-1],yyvsp[0]);}
#line 1783 "dapy.c"
    break;

  case 40: /* attribute: alias ';'  */
#line 147 "dap.y"
                    {yyval=null;}
#line 1789 "dapy.c"
    break;

  case 41: /* attribute: SCAN_BYTE name bytes ';'  */
#line 149 "dap.y"
            {yyval=dap_attribute(parsestate,yyvsp[-2],yyvsp[-1],(Object)SCAN_BYTE);}
#line 1795 "dapy.c"
    break;

  case 42: /* attribute: SCAN_INT16 name int16 ';'  */
#line 151 "dap.y"
            {yyval=dap_attribute(parsestate,yyvsp[-2],yyvsp[-1],(Object)SCAN_INT16);}
#line 1801 "dapy.c"
    break;

  case 43: /* attribute: SCAN_UINT16 name uint16 ';'  */
#line 153 "dap.y"
            {yyval=dap_attribute(parsestate,yyvsp[-2],yyvsp[-1],(Object)SCAN_UINT16);}
#line 1807 "dapy.c"
    break;

  case 44: /* attribute: SCAN_INT32 name int32 ';'  */
#line 155 "dap.y"
            {yyval=dap_attribute(parsestate,yyvsp[-2],yyvsp[-1],(Object)SCAN_INT32);}
#line 1813 "dapy.c"
    break;

  case 45: /* attribute: SCAN_UINT32 name uint32 ';'  */
#line 157 "dap.y"
            {yyval=dap_attribute(parsestate,yyvsp[-2],yyvsp[-1],(Object)SCAN_UINT32);}
#line 1819 "dapy.c"
    break;

  case 46: /* attribute: SCAN_FLOAT32 name float32 ';'  */
#line 159 "dap.y"
            {yyval=dap_attribute(parsestate,yyvsp[-2],yyvsp[-1],(Object)SCAN_FLOAT32);}
#line 1825 "dapy.c"
    break;

  case 47: /* attribute: SCAN_FLOAT64 name float64 ';'  */
#line 161 "dap.y"
            {yyval=dap_attribute(parsestate,yyvsp[-2],yyvsp[-1],(Object)SCAN_FLOAT64);}
#line 1831 "dapy.c"
    break;

  case 48: /* attribute: SCAN_STRING name strs ';'  */
#line 163 "dap.y"
            {yyval=dap_attribute(parsestate,yyvsp[-2],yyvsp[-1],(Object)SCAN_STRING);}
#line 1837 "dapy.c"
    break;

  case 49: /* attribute: SCAN_URL name urls ';'  */
#line 165 "dap.y"
            {yyval=dap_attribute(parsestate,yyvsp[-2],yyvsp[-1],(Object)SCAN_URL);}
#line 1843 "dapy.c"
    break;

  case 50: /* attribute: name '{' attr_list '}'  */
#line 166 "dap.y"
                                 {yyval=dap_attrset(parsestate,yyvsp[-3],yyvsp[-1]);}
#line 1849 "dapy.c"
    break;

  case 51: /* attribute: error  */
#line 168 "dap.y"
            {dapsemanticerror(parsestate,OC_EDAS,"Illegal attribute"); YYABORT;}
#line 1855 "dapy.c"
    break;

  case 52: /* bytes: WORD_WORD  */
#line 172 "dap.y"
                    {yyval=dap_attrvalue(parsestate,null,yyvsp[0],(Object)SCAN_BYTE);}
#line 1861 "dapy.c"
    break;

  case 53: /* bytes: bytes ',' WORD_WORD  */
#line 174 "dap.y"
                {yyval=dap_attrvalue(parsestate,yyvsp[-2],yyvsp[0],(Object)SCAN_BYTE);}
#line 1867 "dapy.c"
    break;

  case 54: /* int16: WORD_WORD  */
#line 177 "dap.y"
                    {yyval=dap_attrvalue(parsestate,null,yyvsp[0],(Object)SCAN_INT16);}
#line 1873 "dapy.c"
    break;

  case 55: /* int16: int16 ',' WORD_WORD  */
#line 179 "dap.y"
                {yyval=dap_attrvalue(parsestate,yyvsp[-2],yyvsp[0],(Object)SCAN_INT16);}
#line 1879 "dapy.c"
    break;

  case 56: /* uint16: WORD_WORD  */
#line 182 "dap.y"
                    {yyval=dap_attrvalue(parsestate,null,yyvsp[0],(Object)SCAN_UINT16);}
#line 1885 "dapy.c"
    break;

  case 57: /* uint16: uint16 ',' WORD_WORD  */
#line 184 "dap.y"
                {yyval=dap_attrvalue(parsestate,yyvsp[-2],yyvsp[0],(Object)SCAN_UINT16);}
#line 1891 "dapy.c"
    break;

  case 58: /* int32: WORD_WORD  */
#line 187 "dap.y"
                    {yyval=dap_attrvalue(parsestate,null,yyvsp[0],(Object)SCAN_INT32);}
#line 1897 "dapy.c"
    break;

  case 59: /* int32: int32 ',' WORD_WORD  */
#line 189 "dap.y"
                {yyval=dap_attrvalue(parsestate,yyvsp[-2],yyvsp[0],(Object)SCAN_INT32);}
#line 1903 "dapy.c"
    break;

  case 60: /* uint32: WORD_WORD  */
#line 192 "dap.y"
                    {yyval=dap_attrvalue(parsestate,null,yyvsp[0],(Object)SCAN_UINT32);}
#line 1909 "dapy.c"
    break;

  case 61: /* uint32: uint32 ',' WORD_WORD  */
#line 193 "dap.y"
                                {yyval=dap_attrvalue(parsestate,yyvsp[-2],yyvsp[0],(Object)SCAN_UINT32);}
#line 1915 "dapy.c"
    break;

  case 62: /* float32: WORD_WORD  */
#line 196 "dap.y"
                    {yyval=dap_attrvalue(parsestate,null,yyvsp[0],(Object)SCAN_FLOAT32);}
#line 1921 "dapy.c"
    break;

  case 63: /* float32: float32 ',' WORD_WORD  */
#line 197 "dap.y"
                                 {yyval=dap_attrvalue(parsestate,yyvsp[-2],yyvsp[0],(Object)SCAN_FLOAT32);}
#line 1927 "dapy.c"
    break;

  case 64: /* float64: WORD_WORD  */
#line 200 "dap.y"
                    {yyval=dap_attrvalue(parsestate,null,yyvsp[0],(Object)SCAN_FLOAT64);}
#line 1933 "dapy.c"
    break;

  case 65: /* float64: float64 ',' WORD_WORD  */
#line 201 "dap.y"
                                 {yyval=dap_attrvalue(parsestate,yyvsp[-2],yyvsp[0],(Object)SCAN_FLOAT64);}
#line 1939 "dapy.c"
    break;

  case 66: /* strs: str_or_id  */
#line 204 "dap.y"
                    {yyval=dap_attrvalue(parsestate,null,yyvsp[0],(Object)SCAN_STRING);}
#line 1945 "dapy.c"
    break;

  case 67: /* strs: strs ',' str_or_id  */
#line 205 "dap.y"
                             {yyval=dap_attrvalue(parsestate,yyvsp[-2],yyvsp[0],(Object)SCAN_STRING);}
#line 1951 "dapy.c"
    break;

  case 68: /* urls: url  */
#line 209 "dap.y"
              {yyval=dap_attrvalue(parsestate,null,yyvsp[0],(Object)SCAN_URL);}
#line 1957 "dapy.c"
    break;

  case 69: /* urls: urls ',' url  */
#line 210 "dap.y"
                       {yyval=dap_attrvalue(parsestate,yyvsp[-2],yyvsp[0],(Object)SCAN_URL);}
#line 1963 "dapy.c"
    break;

  case 70: /* url: str_or_id  */
#line 214 "dap.y"
                  {yyval=yyvsp[0];}
#line 1969 "dapy.c"
    break;

  case 71: /* str_or_id: name  */
#line 218 "dap.y"
               {yyval=yyvsp[0];}
#line 1975 "dapy.c"
    break;

  case 72: /* str_or_id: WORD_STRING  */
#line 219 "dap.y"
                      {yyval=yyvsp[0];}
#line 1981 "dapy.c"
    break;

  case 73: /* alias: SCAN_ALIAS WORD_WORD WORD_WORD  */
#line 230 "dap.y"
                                       {yyval=yyvsp[-1]; yyval=yyvsp[0]; yyval=null;}
#line 1987 "dapy.c"
    break;

  case 74: /* errorbody: '{' errorcode errormsg errorptype errorprog '}' ';'  */
#line 235 "dap.y"
                {dap_errorbody(parsestate,yyvsp[-5],yyvsp[-4],yyvsp[-3],yyvsp[-2]);}
#line 1993 "dapy.c"
    break;

  case 75: /* errorcode: %empty  */
#line 238 "dap.y"
                      {yyval=null;}
#line 1999 "dapy.c"
    break;

  case 76: /* errorcode: SCAN_CODE '=' WORD_WORD ';'  */
#line 238 "dap.y"
                                                                  {yyval=yyvsp[-1];}
#line 2005 "dapy.c"
    break;

  case 77: /* errormsg: %empty  */
#line 239 "dap.y"
                      {yyval=null;}
#line 2011 "dapy.c"
    break;

  case 78: /* errormsg: SCAN_MESSAGE '=' WORD_STRING ';'  */
#line 239 "dap.y"
                                                                    {yyval=yyvsp[-1];}
#line 2017 "dapy.c"
    break;

  case 79: /* errorptype: %empty  */
#line 240 "dap.y"
                      {yyval=null;}
#line 2023 "dapy.c"
    break;

  case 80: /* errorptype: SCAN_PTYPE '=' WORD_WORD ';'  */
#line 240 "dap.y"
                                                                  {yyval=yyvsp[-1];}
#line 2029 "dapy.c"
    break;

  case 81: /* errorprog: %empty  */
#line 241 "dap.y"
                      {yyval=null;}
#line 2035 "dapy.c"
    break;

  case 82: /* errorprog: SCAN_PROG '=' WORD_WORD ';'  */
#line 241 "dap.y"
                                                                  {yyval=yyvsp[-1];}
#line 2041 "dapy.c"
    break;

  case 83: /* name: WORD_WORD  */
#line 247 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2047 "dapy.c"
    break;

  case 84: /* name: SCAN_ALIAS  */
#line 248 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2053 "dapy.c"
    break;

  case 85: /* name: SCAN_ARRAY  */
#line 249 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2059 "dapy.c"
    break;

  case 86: /* name: SCAN_ATTR  */
#line 250 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2065 "dapy.c"
    break;

  case 87: /* name: SCAN_BYTE  */
#line 251 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2071 "dapy.c"
    break;

  case 88: /* name: SCAN_DATASET  */
#line 252 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2077 "dapy.c"
    break;

  case 89: /* name: SCAN_DATA  */
#line 253 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2083 "dapy.c"
    break;

  case 90: /* name: SCAN_ERROR  */
#line 254 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2089 "dapy.c"
    break;

  case 91: /* name: SCAN_FLOAT32  */
#line 255 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2095 "dapy.c"
    break;

  case 92: /* name: SCAN_FLOAT64  */
#line 256 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2101 "dapy.c"
    break;

  case 93: /* name: SCAN_GRID  */
#line 257 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2107 "dapy.c"
    break;

  case 94: /* name: SCAN_INT16  */
#line 258 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2113 "dapy.c"
    break;

  case 95: /* name: SCAN_INT32  */
#line 259 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2119 "dapy.c"
    break;

  case 96: /* name: SCAN_MAPS  */
#line 260 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2125 "dapy.c"
    break;

  case 97: /* name: SCAN_SEQUENCE  */
#line 261 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2131 "dapy.c"
    break;

  case 98: /* name: SCAN_STRING  */
#line 262 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2137 "dapy.c"
    break;

  case 99: /* name: SCAN_STRUCTURE  */
#line 263 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2143 "dapy.c"
    break;

  case 100: /* name: SCAN_UINT16  */
#line 264 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2149 "dapy.c"
    break;

  case 101: /* name: SCAN_UINT32  */
#line 265 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2155 "dapy.c"
    break;

  case 102: /* name: SCAN_URL  */
#line 266 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2161 "dapy.c"
    break;

  case 103: /* name: SCAN_CODE  */
#line 267 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2167 "dapy.c"
    break;

  case 104: /* name: SCAN_MESSAGE  */
#line 268 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2173 "dapy.c"
    break;

  case 105: /* name: SCAN_PROG  */
#line 269 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2179 "dapy.c"
    break;

  case 106: /* name: SCAN_PTYPE  */
#line 270 "dap.y"
                         {yyval=dapdecode(parsestate->lexstate,yyvsp[0]);}
#line 2185 "dapy.c"
    break;


#line 2189 "dapy.c"

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
      {
        yypcontext_t yyctx
          = {yyssp, yytoken};
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == -1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *,
                             YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (yymsg)
              {
                yysyntax_error_status
                  = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
                yymsgp = yymsg;
              }
            else
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = YYENOMEM;
              }
          }
        yyerror (parsestate, yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          YYNOMEM;
      }
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
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
  return yyresult;
}

#line 273 "dap.y"

