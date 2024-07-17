/* A Bison parser, made by GNU Bison 3.7.5.  */

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
#define YYBISON 30705

/* Bison version string.  */
#define YYBISON_VERSION "3.7.5"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1


/* Substitute the variable and function names.  */
#define yyparse         jsonpath_yyparse
#define yylex           jsonpath_yylex
#define yyerror         jsonpath_yyerror
#define yydebug         jsonpath_yydebug
#define yynerrs         jsonpath_yynerrs

/* First part of user prologue.  */
#line 1 "jsonpath_gram.y"

/*-------------------------------------------------------------------------
 *
 * jsonpath_gram.y
 *	 Grammar definitions for jsonpath datatype
 *
 * Transforms tokenized jsonpath into tree of JsonPathParseItem structs.
 *
 * Copyright (c) 2019-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_gram.y
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_collation.h"
#include "fmgr.h"
#include "jsonpath_internal.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "regex/regex.h"
#include "utils/builtins.h"

static JsonPathParseItem *makeItemType(JsonPathItemType type);
static JsonPathParseItem *makeItemString(JsonPathString *s);
static JsonPathParseItem *makeItemVariable(JsonPathString *s);
static JsonPathParseItem *makeItemKey(JsonPathString *s);
static JsonPathParseItem *makeItemNumeric(JsonPathString *s);
static JsonPathParseItem *makeItemBool(bool val);
static JsonPathParseItem *makeItemBinary(JsonPathItemType type,
										 JsonPathParseItem *la,
										 JsonPathParseItem *ra);
static JsonPathParseItem *makeItemUnary(JsonPathItemType type,
										JsonPathParseItem *a);
static JsonPathParseItem *makeItemList(List *list);
static JsonPathParseItem *makeIndexArray(List *list);
static JsonPathParseItem *makeAny(int first, int last);
static bool makeItemLikeRegex(JsonPathParseItem *expr,
							  JsonPathString *pattern,
							  JsonPathString *flags,
							  JsonPathParseItem ** result,
							  struct Node *escontext);

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.
 */
#define YYMALLOC palloc
#define YYFREE   pfree


#line 132 "jsonpath_gram.c"

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

#include "jsonpath_gram.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_TO_P = 3,                       /* TO_P  */
  YYSYMBOL_NULL_P = 4,                     /* NULL_P  */
  YYSYMBOL_TRUE_P = 5,                     /* TRUE_P  */
  YYSYMBOL_FALSE_P = 6,                    /* FALSE_P  */
  YYSYMBOL_IS_P = 7,                       /* IS_P  */
  YYSYMBOL_UNKNOWN_P = 8,                  /* UNKNOWN_P  */
  YYSYMBOL_EXISTS_P = 9,                   /* EXISTS_P  */
  YYSYMBOL_IDENT_P = 10,                   /* IDENT_P  */
  YYSYMBOL_STRING_P = 11,                  /* STRING_P  */
  YYSYMBOL_NUMERIC_P = 12,                 /* NUMERIC_P  */
  YYSYMBOL_INT_P = 13,                     /* INT_P  */
  YYSYMBOL_VARIABLE_P = 14,                /* VARIABLE_P  */
  YYSYMBOL_OR_P = 15,                      /* OR_P  */
  YYSYMBOL_AND_P = 16,                     /* AND_P  */
  YYSYMBOL_NOT_P = 17,                     /* NOT_P  */
  YYSYMBOL_LESS_P = 18,                    /* LESS_P  */
  YYSYMBOL_LESSEQUAL_P = 19,               /* LESSEQUAL_P  */
  YYSYMBOL_EQUAL_P = 20,                   /* EQUAL_P  */
  YYSYMBOL_NOTEQUAL_P = 21,                /* NOTEQUAL_P  */
  YYSYMBOL_GREATEREQUAL_P = 22,            /* GREATEREQUAL_P  */
  YYSYMBOL_GREATER_P = 23,                 /* GREATER_P  */
  YYSYMBOL_ANY_P = 24,                     /* ANY_P  */
  YYSYMBOL_STRICT_P = 25,                  /* STRICT_P  */
  YYSYMBOL_LAX_P = 26,                     /* LAX_P  */
  YYSYMBOL_LAST_P = 27,                    /* LAST_P  */
  YYSYMBOL_STARTS_P = 28,                  /* STARTS_P  */
  YYSYMBOL_WITH_P = 29,                    /* WITH_P  */
  YYSYMBOL_LIKE_REGEX_P = 30,              /* LIKE_REGEX_P  */
  YYSYMBOL_FLAG_P = 31,                    /* FLAG_P  */
  YYSYMBOL_ABS_P = 32,                     /* ABS_P  */
  YYSYMBOL_SIZE_P = 33,                    /* SIZE_P  */
  YYSYMBOL_TYPE_P = 34,                    /* TYPE_P  */
  YYSYMBOL_FLOOR_P = 35,                   /* FLOOR_P  */
  YYSYMBOL_DOUBLE_P = 36,                  /* DOUBLE_P  */
  YYSYMBOL_CEILING_P = 37,                 /* CEILING_P  */
  YYSYMBOL_KEYVALUE_P = 38,                /* KEYVALUE_P  */
  YYSYMBOL_DATETIME_P = 39,                /* DATETIME_P  */
  YYSYMBOL_40_ = 40,                       /* '+'  */
  YYSYMBOL_41_ = 41,                       /* '-'  */
  YYSYMBOL_42_ = 42,                       /* '*'  */
  YYSYMBOL_43_ = 43,                       /* '/'  */
  YYSYMBOL_44_ = 44,                       /* '%'  */
  YYSYMBOL_UMINUS = 45,                    /* UMINUS  */
  YYSYMBOL_46_ = 46,                       /* '('  */
  YYSYMBOL_47_ = 47,                       /* ')'  */
  YYSYMBOL_48_ = 48,                       /* '$'  */
  YYSYMBOL_49_ = 49,                       /* '@'  */
  YYSYMBOL_50_ = 50,                       /* ','  */
  YYSYMBOL_51_ = 51,                       /* '['  */
  YYSYMBOL_52_ = 52,                       /* ']'  */
  YYSYMBOL_53_ = 53,                       /* '{'  */
  YYSYMBOL_54_ = 54,                       /* '}'  */
  YYSYMBOL_55_ = 55,                       /* '.'  */
  YYSYMBOL_56_ = 56,                       /* '?'  */
  YYSYMBOL_YYACCEPT = 57,                  /* $accept  */
  YYSYMBOL_result = 58,                    /* result  */
  YYSYMBOL_expr_or_predicate = 59,         /* expr_or_predicate  */
  YYSYMBOL_mode = 60,                      /* mode  */
  YYSYMBOL_scalar_value = 61,              /* scalar_value  */
  YYSYMBOL_comp_op = 62,                   /* comp_op  */
  YYSYMBOL_delimited_predicate = 63,       /* delimited_predicate  */
  YYSYMBOL_predicate = 64,                 /* predicate  */
  YYSYMBOL_starts_with_initial = 65,       /* starts_with_initial  */
  YYSYMBOL_path_primary = 66,              /* path_primary  */
  YYSYMBOL_accessor_expr = 67,             /* accessor_expr  */
  YYSYMBOL_expr = 68,                      /* expr  */
  YYSYMBOL_index_elem = 69,                /* index_elem  */
  YYSYMBOL_index_list = 70,                /* index_list  */
  YYSYMBOL_array_accessor = 71,            /* array_accessor  */
  YYSYMBOL_any_level = 72,                 /* any_level  */
  YYSYMBOL_any_path = 73,                  /* any_path  */
  YYSYMBOL_accessor_op = 74,               /* accessor_op  */
  YYSYMBOL_datetime_template = 75,         /* datetime_template  */
  YYSYMBOL_opt_datetime_template = 76,     /* opt_datetime_template  */
  YYSYMBOL_key = 77,                       /* key  */
  YYSYMBOL_key_name = 78,                  /* key_name  */
  YYSYMBOL_method = 79                     /* method  */
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
#define YYFINAL  5
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   239

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  57
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  23
/* YYNRULES -- Number of rules.  */
#define YYNRULES  104
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  143

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   295


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
       2,     2,     2,     2,     2,     2,    48,    44,     2,     2,
      46,    47,    42,    40,    50,    41,    55,    43,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    56,    49,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    51,     2,    52,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    53,     2,    54,     2,     2,     2,     2,
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
      35,    36,    37,    38,    39,    45
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   117,   117,   123,   127,   128,   132,   133,   134,   138,
     139,   140,   141,   142,   143,   144,   148,   149,   150,   151,
     152,   153,   157,   158,   162,   163,   164,   165,   166,   167,
     169,   171,   178,   188,   189,   193,   194,   195,   196,   200,
     201,   202,   203,   207,   208,   209,   210,   211,   212,   213,
     214,   215,   219,   220,   224,   225,   229,   230,   234,   235,
     239,   240,   241,   246,   247,   248,   249,   250,   251,   253,
     257,   261,   262,   266,   270,   271,   272,   273,   274,   275,
     276,   277,   278,   279,   280,   281,   282,   283,   284,   285,
     286,   287,   288,   289,   290,   291,   292,   293,   297,   298,
     299,   300,   301,   302,   303
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
  "\"end of file\"", "error", "\"invalid token\"", "TO_P", "NULL_P",
  "TRUE_P", "FALSE_P", "IS_P", "UNKNOWN_P", "EXISTS_P", "IDENT_P",
  "STRING_P", "NUMERIC_P", "INT_P", "VARIABLE_P", "OR_P", "AND_P", "NOT_P",
  "LESS_P", "LESSEQUAL_P", "EQUAL_P", "NOTEQUAL_P", "GREATEREQUAL_P",
  "GREATER_P", "ANY_P", "STRICT_P", "LAX_P", "LAST_P", "STARTS_P",
  "WITH_P", "LIKE_REGEX_P", "FLAG_P", "ABS_P", "SIZE_P", "TYPE_P",
  "FLOOR_P", "DOUBLE_P", "CEILING_P", "KEYVALUE_P", "DATETIME_P", "'+'",
  "'-'", "'*'", "'/'", "'%'", "UMINUS", "'('", "')'", "'$'", "'@'", "','",
  "'['", "']'", "'{'", "'}'", "'.'", "'?'", "$accept", "result",
  "expr_or_predicate", "mode", "scalar_value", "comp_op",
  "delimited_predicate", "predicate", "starts_with_initial",
  "path_primary", "accessor_expr", "expr", "index_elem", "index_list",
  "array_accessor", "any_level", "any_path", "accessor_op",
  "datetime_template", "opt_datetime_template", "key", "key_name",
  "method", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#ifdef YYPRINT
/* YYTOKNUM[NUM] -- (External) token number corresponding to the
   (internal) symbol number NUM (which must be that of a token).  */
static const yytype_int16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,   287,   288,   289,   290,   291,   292,   293,   294,
      43,    45,    42,    47,    37,   295,    40,    41,    36,    64,
      44,    91,    93,   123,   125,    46,    63
};
#endif

#define YYPACT_NINF (-44)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-105)

#define yytable_value_is_error(Yyn) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
       7,   -44,   -44,    18,    51,   -44,   -44,   -44,   -44,   -43,
     -44,   -44,   -44,   -44,    -3,   -44,   114,   114,    51,   -44,
     -44,   -44,   -44,   -44,    10,   -44,   -35,   195,   114,    51,
     -44,    51,   -44,   -44,    14,   165,    51,    51,    68,   140,
      -9,   -44,   -44,   -44,   -44,   -44,   -44,   -44,   -44,    37,
      60,   114,   114,   114,   114,   114,   114,    46,    20,   195,
      30,     3,   -35,    59,   -44,    24,    -2,   -44,   -41,   -44,
     -44,   -44,   -44,   -44,   -44,   -44,   -44,   -44,    31,   -44,
     -44,   -44,   -44,   -44,   -44,   -44,    48,    50,    52,    61,
      67,    69,    78,    83,   -44,   -44,   -44,   -44,    84,    51,
      17,   100,    79,    79,   -44,   -44,   -44,    62,   -44,   -44,
     -35,    75,   -44,   -44,   -44,   114,   114,   -44,    -8,   121,
      86,    54,   -44,   -44,   -44,   123,   -44,    62,   -44,   -44,
     -44,    -1,   -44,   -44,    88,   -44,   -44,   -44,    -8,   -44,
     -44,    82,   -44
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       8,     6,     7,     0,     0,     1,    10,    11,    12,     0,
       9,    13,    14,    15,     0,    38,     0,     0,     0,    36,
      37,     2,    35,    24,     5,    39,    43,     4,     0,     0,
      28,     0,    45,    46,     0,     0,     0,     0,     0,     0,
       0,    65,    42,    18,    20,    16,    17,    21,    19,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    22,    44,    27,    26,     0,    52,    54,     0,    76,
      77,    78,    79,    80,    81,    82,    74,    75,    60,    83,
      84,    93,    94,    95,    96,    97,    85,    86,    87,    88,
      89,    90,    92,    91,    64,    66,    63,    73,     0,     0,
       0,    31,    47,    48,    49,    50,    51,    25,    23,    22,
       0,     0,    41,    40,    56,     0,     0,    57,     0,    72,
       0,     0,    33,    34,    30,     0,    29,    53,    55,    58,
      59,     0,    70,    71,     0,    67,    69,    32,     0,    61,
      68,     0,    62
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -44,   -44,   -44,   -44,   -44,   -44,   124,   -14,   -44,   -44,
     -44,    -4,    21,   -44,   -44,     1,   -44,   -18,   -44,   -44,
     -44,   -44,   -44
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,     3,    21,     4,    22,    56,    23,    24,   124,    25,
      26,    59,    67,    68,    41,   131,    95,   112,   133,   134,
      96,    97,    98
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int16 yytable[] =
{
      27,   115,   138,    28,    34,   129,     9,    -3,    42,   116,
     111,   117,    32,    33,    35,    58,    38,    60,     5,   130,
      39,    40,    63,    64,    57,    36,    37,    35,   122,    36,
      37,   123,     1,     2,    66,    36,    37,    99,    51,    52,
      53,    54,    55,    29,   113,    36,    37,   102,   103,   104,
     105,   106,   107,   139,    38,     6,     7,     8,    39,    40,
       9,    61,    10,    11,    12,    13,   100,   109,    14,    36,
      37,   101,     6,     7,     8,    37,   114,   110,    15,    10,
      11,    12,    13,   126,   118,   121,    51,    52,    53,    54,
      55,    16,    17,   108,   -98,    15,   -99,    18,  -100,    19,
      20,   136,    51,    52,    53,    54,    55,  -101,    16,    17,
      65,   127,    66,  -102,    31,  -103,    19,    20,     6,     7,
       8,    53,    54,    55,  -104,    10,    11,    12,    13,   119,
     120,   125,   132,   135,   137,   140,   142,   128,    30,   141,
       0,    15,     0,    69,    70,    71,    72,    73,    74,    75,
      76,    77,     0,     0,    16,    17,     0,     0,     0,     0,
      31,     0,    19,    20,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
       0,     0,    94,    43,    44,    45,    46,    47,    48,     0,
       0,     0,     0,    49,     0,    50,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    51,    52,    53,    54,    55,
       0,     0,    62,    43,    44,    45,    46,    47,    48,     0,
       0,     0,     0,    49,     0,    50,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    51,    52,    53,    54,    55
};

static const yytype_int16 yycheck[] =
{
       4,     3,     3,    46,    18,    13,     9,     0,    26,    50,
       7,    52,    16,    17,    18,    29,    51,    31,     0,    27,
      55,    56,    36,    37,    28,    15,    16,    31,    11,    15,
      16,    14,    25,    26,    38,    15,    16,    46,    40,    41,
      42,    43,    44,    46,    62,    15,    16,    51,    52,    53,
      54,    55,    56,    54,    51,     4,     5,     6,    55,    56,
       9,    47,    11,    12,    13,    14,    29,    47,    17,    15,
      16,    11,     4,     5,     6,    16,    52,    47,    27,    11,
      12,    13,    14,     8,    53,    99,    40,    41,    42,    43,
      44,    40,    41,    47,    46,    27,    46,    46,    46,    48,
      49,    47,    40,    41,    42,    43,    44,    46,    40,    41,
      42,   115,   116,    46,    46,    46,    48,    49,     4,     5,
       6,    42,    43,    44,    46,    11,    12,    13,    14,    46,
      46,    31,    11,    47,    11,    47,    54,   116,    14,   138,
      -1,    27,    -1,     3,     4,     5,     6,     7,     8,     9,
      10,    11,    -1,    -1,    40,    41,    -1,    -1,    -1,    -1,
      46,    -1,    48,    49,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    39,
      -1,    -1,    42,    18,    19,    20,    21,    22,    23,    -1,
      -1,    -1,    -1,    28,    -1,    30,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    40,    41,    42,    43,    44,
      -1,    -1,    47,    18,    19,    20,    21,    22,    23,    -1,
      -1,    -1,    -1,    28,    -1,    30,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    40,    41,    42,    43,    44
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,    25,    26,    58,    60,     0,     4,     5,     6,     9,
      11,    12,    13,    14,    17,    27,    40,    41,    46,    48,
      49,    59,    61,    63,    64,    66,    67,    68,    46,    46,
      63,    46,    68,    68,    64,    68,    15,    16,    51,    55,
      56,    71,    74,    18,    19,    20,    21,    22,    23,    28,
      30,    40,    41,    42,    43,    44,    62,    68,    64,    68,
      64,    47,    47,    64,    64,    42,    68,    69,    70,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    39,    42,    73,    77,    78,    79,    46,
      29,    11,    68,    68,    68,    68,    68,    68,    47,    47,
      47,     7,    74,    74,    52,     3,    50,    52,    53,    46,
      46,    64,    11,    14,    65,    31,     8,    68,    69,    13,
      27,    72,    11,    75,    76,    47,    47,    11,     3,    54,
      47,    72,    54
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_int8 yyr1[] =
{
       0,    57,    58,    58,    59,    59,    60,    60,    60,    61,
      61,    61,    61,    61,    61,    61,    62,    62,    62,    62,
      62,    62,    63,    63,    64,    64,    64,    64,    64,    64,
      64,    64,    64,    65,    65,    66,    66,    66,    66,    67,
      67,    67,    67,    68,    68,    68,    68,    68,    68,    68,
      68,    68,    69,    69,    70,    70,    71,    71,    72,    72,
      73,    73,    73,    74,    74,    74,    74,    74,    74,    74,
      75,    76,    76,    77,    78,    78,    78,    78,    78,    78,
      78,    78,    78,    78,    78,    78,    78,    78,    78,    78,
      78,    78,    78,    78,    78,    78,    78,    78,    79,    79,
      79,    79,    79,    79,    79
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     0,     1,     1,     1,     1,     0,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     3,     4,     1,     3,     3,     3,     2,     5,
       4,     3,     5,     1,     1,     1,     1,     1,     1,     1,
       4,     4,     2,     1,     3,     2,     2,     3,     3,     3,
       3,     3,     1,     3,     1,     3,     3,     3,     1,     1,
       1,     4,     6,     2,     2,     1,     2,     4,     5,     4,
       1,     1,     0,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

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
        yyerror (result, escontext, YY_("syntax error: cannot back up")); \
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

/* This macro is provided for backward compatibility. */
# ifndef YY_LOCATION_PRINT
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif


# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, result, escontext); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, JsonPathParseResult **result, struct Node *escontext)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (result);
  YY_USE (escontext);
  if (!yyvaluep)
    return;
# ifdef YYPRINT
  if (yykind < YYNTOKENS)
    YYPRINT (yyo, yytoknum[yykind], *yyvaluep);
# endif
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, JsonPathParseResult **result, struct Node *escontext)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep, result, escontext);
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
                 int yyrule, JsonPathParseResult **result, struct Node *escontext)
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
                       &yyvsp[(yyi + 1) - (yynrhs)], result, escontext);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, result, escontext); \
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
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, JsonPathParseResult **result, struct Node *escontext)
{
  YY_USE (yyvaluep);
  YY_USE (result);
  YY_USE (escontext);
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
yyparse (JsonPathParseResult **result, struct Node *escontext)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static __thread YYSTYPE yyval_default;)
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
    goto yyexhaustedlab;
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
      yychar = yylex (&yylval, result, escontext);
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
  case 2: /* result: mode expr_or_predicate  */
#line 117 "jsonpath_gram.y"
                                                {
										*result = palloc(sizeof(JsonPathParseResult));
										(*result)->expr = (yyvsp[0].value);
										(*result)->lax = (yyvsp[-1].boolean);
										(void) yynerrs;
									}
#line 1350 "jsonpath_gram.c"
    break;

  case 3: /* result: %empty  */
#line 123 "jsonpath_gram.y"
                                                        { *result = NULL; }
#line 1356 "jsonpath_gram.c"
    break;

  case 4: /* expr_or_predicate: expr  */
#line 127 "jsonpath_gram.y"
                                                                { (yyval.value) = (yyvsp[0].value); }
#line 1362 "jsonpath_gram.c"
    break;

  case 5: /* expr_or_predicate: predicate  */
#line 128 "jsonpath_gram.y"
                                                                { (yyval.value) = (yyvsp[0].value); }
#line 1368 "jsonpath_gram.c"
    break;

  case 6: /* mode: STRICT_P  */
#line 132 "jsonpath_gram.y"
                                                                { (yyval.boolean) = false; }
#line 1374 "jsonpath_gram.c"
    break;

  case 7: /* mode: LAX_P  */
#line 133 "jsonpath_gram.y"
                                                                { (yyval.boolean) = true; }
#line 1380 "jsonpath_gram.c"
    break;

  case 8: /* mode: %empty  */
#line 134 "jsonpath_gram.y"
                                                        { (yyval.boolean) = true; }
#line 1386 "jsonpath_gram.c"
    break;

  case 9: /* scalar_value: STRING_P  */
#line 138 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemString(&(yyvsp[0].str)); }
#line 1392 "jsonpath_gram.c"
    break;

  case 10: /* scalar_value: NULL_P  */
#line 139 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemString(NULL); }
#line 1398 "jsonpath_gram.c"
    break;

  case 11: /* scalar_value: TRUE_P  */
#line 140 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemBool(true); }
#line 1404 "jsonpath_gram.c"
    break;

  case 12: /* scalar_value: FALSE_P  */
#line 141 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemBool(false); }
#line 1410 "jsonpath_gram.c"
    break;

  case 13: /* scalar_value: NUMERIC_P  */
#line 142 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemNumeric(&(yyvsp[0].str)); }
#line 1416 "jsonpath_gram.c"
    break;

  case 14: /* scalar_value: INT_P  */
#line 143 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemNumeric(&(yyvsp[0].str)); }
#line 1422 "jsonpath_gram.c"
    break;

  case 15: /* scalar_value: VARIABLE_P  */
#line 144 "jsonpath_gram.y"
                                                        { (yyval.value) = makeItemVariable(&(yyvsp[0].str)); }
#line 1428 "jsonpath_gram.c"
    break;

  case 16: /* comp_op: EQUAL_P  */
#line 148 "jsonpath_gram.y"
                                                                { (yyval.optype) = jpiEqual; }
#line 1434 "jsonpath_gram.c"
    break;

  case 17: /* comp_op: NOTEQUAL_P  */
#line 149 "jsonpath_gram.y"
                                                        { (yyval.optype) = jpiNotEqual; }
#line 1440 "jsonpath_gram.c"
    break;

  case 18: /* comp_op: LESS_P  */
#line 150 "jsonpath_gram.y"
                                                                { (yyval.optype) = jpiLess; }
#line 1446 "jsonpath_gram.c"
    break;

  case 19: /* comp_op: GREATER_P  */
#line 151 "jsonpath_gram.y"
                                                                { (yyval.optype) = jpiGreater; }
#line 1452 "jsonpath_gram.c"
    break;

  case 20: /* comp_op: LESSEQUAL_P  */
#line 152 "jsonpath_gram.y"
                                                        { (yyval.optype) = jpiLessOrEqual; }
#line 1458 "jsonpath_gram.c"
    break;

  case 21: /* comp_op: GREATEREQUAL_P  */
#line 153 "jsonpath_gram.y"
                                                        { (yyval.optype) = jpiGreaterOrEqual; }
#line 1464 "jsonpath_gram.c"
    break;

  case 22: /* delimited_predicate: '(' predicate ')'  */
#line 157 "jsonpath_gram.y"
                                                        { (yyval.value) = (yyvsp[-1].value); }
#line 1470 "jsonpath_gram.c"
    break;

  case 23: /* delimited_predicate: EXISTS_P '(' expr ')'  */
#line 158 "jsonpath_gram.y"
                                                { (yyval.value) = makeItemUnary(jpiExists, (yyvsp[-1].value)); }
#line 1476 "jsonpath_gram.c"
    break;

  case 24: /* predicate: delimited_predicate  */
#line 162 "jsonpath_gram.y"
                                                        { (yyval.value) = (yyvsp[0].value); }
#line 1482 "jsonpath_gram.c"
    break;

  case 25: /* predicate: expr comp_op expr  */
#line 163 "jsonpath_gram.y"
                                                        { (yyval.value) = makeItemBinary((yyvsp[-1].optype), (yyvsp[-2].value), (yyvsp[0].value)); }
#line 1488 "jsonpath_gram.c"
    break;

  case 26: /* predicate: predicate AND_P predicate  */
#line 164 "jsonpath_gram.y"
                                                { (yyval.value) = makeItemBinary(jpiAnd, (yyvsp[-2].value), (yyvsp[0].value)); }
#line 1494 "jsonpath_gram.c"
    break;

  case 27: /* predicate: predicate OR_P predicate  */
#line 165 "jsonpath_gram.y"
                                                { (yyval.value) = makeItemBinary(jpiOr, (yyvsp[-2].value), (yyvsp[0].value)); }
#line 1500 "jsonpath_gram.c"
    break;

  case 28: /* predicate: NOT_P delimited_predicate  */
#line 166 "jsonpath_gram.y"
                                                { (yyval.value) = makeItemUnary(jpiNot, (yyvsp[0].value)); }
#line 1506 "jsonpath_gram.c"
    break;

  case 29: /* predicate: '(' predicate ')' IS_P UNKNOWN_P  */
#line 168 "jsonpath_gram.y"
                                                                        { (yyval.value) = makeItemUnary(jpiIsUnknown, (yyvsp[-3].value)); }
#line 1512 "jsonpath_gram.c"
    break;

  case 30: /* predicate: expr STARTS_P WITH_P starts_with_initial  */
#line 170 "jsonpath_gram.y"
                                                                        { (yyval.value) = makeItemBinary(jpiStartsWith, (yyvsp[-3].value), (yyvsp[0].value)); }
#line 1518 "jsonpath_gram.c"
    break;

  case 31: /* predicate: expr LIKE_REGEX_P STRING_P  */
#line 172 "jsonpath_gram.y"
        {
		JsonPathParseItem *jppitem;
		if (! makeItemLikeRegex((yyvsp[-2].value), &(yyvsp[0].str), NULL, &jppitem, escontext))
			YYABORT;
		(yyval.value) = jppitem;
	}
#line 1529 "jsonpath_gram.c"
    break;

  case 32: /* predicate: expr LIKE_REGEX_P STRING_P FLAG_P STRING_P  */
#line 179 "jsonpath_gram.y"
        {
		JsonPathParseItem *jppitem;
		if (! makeItemLikeRegex((yyvsp[-4].value), &(yyvsp[-2].str), &(yyvsp[0].str), &jppitem, escontext))
			YYABORT;
		(yyval.value) = jppitem;
	}
#line 1540 "jsonpath_gram.c"
    break;

  case 33: /* starts_with_initial: STRING_P  */
#line 188 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemString(&(yyvsp[0].str)); }
#line 1546 "jsonpath_gram.c"
    break;

  case 34: /* starts_with_initial: VARIABLE_P  */
#line 189 "jsonpath_gram.y"
                                                        { (yyval.value) = makeItemVariable(&(yyvsp[0].str)); }
#line 1552 "jsonpath_gram.c"
    break;

  case 35: /* path_primary: scalar_value  */
#line 193 "jsonpath_gram.y"
                                                        { (yyval.value) = (yyvsp[0].value); }
#line 1558 "jsonpath_gram.c"
    break;

  case 36: /* path_primary: '$'  */
#line 194 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemType(jpiRoot); }
#line 1564 "jsonpath_gram.c"
    break;

  case 37: /* path_primary: '@'  */
#line 195 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemType(jpiCurrent); }
#line 1570 "jsonpath_gram.c"
    break;

  case 38: /* path_primary: LAST_P  */
#line 196 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemType(jpiLast); }
#line 1576 "jsonpath_gram.c"
    break;

  case 39: /* accessor_expr: path_primary  */
#line 200 "jsonpath_gram.y"
                                                        { (yyval.elems) = list_make1((yyvsp[0].value)); }
#line 1582 "jsonpath_gram.c"
    break;

  case 40: /* accessor_expr: '(' expr ')' accessor_op  */
#line 201 "jsonpath_gram.y"
                                                { (yyval.elems) = list_make2((yyvsp[-2].value), (yyvsp[0].value)); }
#line 1588 "jsonpath_gram.c"
    break;

  case 41: /* accessor_expr: '(' predicate ')' accessor_op  */
#line 202 "jsonpath_gram.y"
                                        { (yyval.elems) = list_make2((yyvsp[-2].value), (yyvsp[0].value)); }
#line 1594 "jsonpath_gram.c"
    break;

  case 42: /* accessor_expr: accessor_expr accessor_op  */
#line 203 "jsonpath_gram.y"
                                                { (yyval.elems) = lappend((yyvsp[-1].elems), (yyvsp[0].value)); }
#line 1600 "jsonpath_gram.c"
    break;

  case 43: /* expr: accessor_expr  */
#line 207 "jsonpath_gram.y"
                                                        { (yyval.value) = makeItemList((yyvsp[0].elems)); }
#line 1606 "jsonpath_gram.c"
    break;

  case 44: /* expr: '(' expr ')'  */
#line 208 "jsonpath_gram.y"
                                                        { (yyval.value) = (yyvsp[-1].value); }
#line 1612 "jsonpath_gram.c"
    break;

  case 45: /* expr: '+' expr  */
#line 209 "jsonpath_gram.y"
                                                { (yyval.value) = makeItemUnary(jpiPlus, (yyvsp[0].value)); }
#line 1618 "jsonpath_gram.c"
    break;

  case 46: /* expr: '-' expr  */
#line 210 "jsonpath_gram.y"
                                                { (yyval.value) = makeItemUnary(jpiMinus, (yyvsp[0].value)); }
#line 1624 "jsonpath_gram.c"
    break;

  case 47: /* expr: expr '+' expr  */
#line 211 "jsonpath_gram.y"
                                                        { (yyval.value) = makeItemBinary(jpiAdd, (yyvsp[-2].value), (yyvsp[0].value)); }
#line 1630 "jsonpath_gram.c"
    break;

  case 48: /* expr: expr '-' expr  */
#line 212 "jsonpath_gram.y"
                                                        { (yyval.value) = makeItemBinary(jpiSub, (yyvsp[-2].value), (yyvsp[0].value)); }
#line 1636 "jsonpath_gram.c"
    break;

  case 49: /* expr: expr '*' expr  */
#line 213 "jsonpath_gram.y"
                                                        { (yyval.value) = makeItemBinary(jpiMul, (yyvsp[-2].value), (yyvsp[0].value)); }
#line 1642 "jsonpath_gram.c"
    break;

  case 50: /* expr: expr '/' expr  */
#line 214 "jsonpath_gram.y"
                                                        { (yyval.value) = makeItemBinary(jpiDiv, (yyvsp[-2].value), (yyvsp[0].value)); }
#line 1648 "jsonpath_gram.c"
    break;

  case 51: /* expr: expr '%' expr  */
#line 215 "jsonpath_gram.y"
                                                        { (yyval.value) = makeItemBinary(jpiMod, (yyvsp[-2].value), (yyvsp[0].value)); }
#line 1654 "jsonpath_gram.c"
    break;

  case 52: /* index_elem: expr  */
#line 219 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemBinary(jpiSubscript, (yyvsp[0].value), NULL); }
#line 1660 "jsonpath_gram.c"
    break;

  case 53: /* index_elem: expr TO_P expr  */
#line 220 "jsonpath_gram.y"
                                                        { (yyval.value) = makeItemBinary(jpiSubscript, (yyvsp[-2].value), (yyvsp[0].value)); }
#line 1666 "jsonpath_gram.c"
    break;

  case 54: /* index_list: index_elem  */
#line 224 "jsonpath_gram.y"
                                                                { (yyval.indexs) = list_make1((yyvsp[0].value)); }
#line 1672 "jsonpath_gram.c"
    break;

  case 55: /* index_list: index_list ',' index_elem  */
#line 225 "jsonpath_gram.y"
                                                { (yyval.indexs) = lappend((yyvsp[-2].indexs), (yyvsp[0].value)); }
#line 1678 "jsonpath_gram.c"
    break;

  case 56: /* array_accessor: '[' '*' ']'  */
#line 229 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemType(jpiAnyArray); }
#line 1684 "jsonpath_gram.c"
    break;

  case 57: /* array_accessor: '[' index_list ']'  */
#line 230 "jsonpath_gram.y"
                                                { (yyval.value) = makeIndexArray((yyvsp[-1].indexs)); }
#line 1690 "jsonpath_gram.c"
    break;

  case 58: /* any_level: INT_P  */
#line 234 "jsonpath_gram.y"
                                                                { (yyval.integer) = pg_strtoint32((yyvsp[0].str).val); }
#line 1696 "jsonpath_gram.c"
    break;

  case 59: /* any_level: LAST_P  */
#line 235 "jsonpath_gram.y"
                                                                { (yyval.integer) = -1; }
#line 1702 "jsonpath_gram.c"
    break;

  case 60: /* any_path: ANY_P  */
#line 239 "jsonpath_gram.y"
                                                                { (yyval.value) = makeAny(0, -1); }
#line 1708 "jsonpath_gram.c"
    break;

  case 61: /* any_path: ANY_P '{' any_level '}'  */
#line 240 "jsonpath_gram.y"
                                                { (yyval.value) = makeAny((yyvsp[-1].integer), (yyvsp[-1].integer)); }
#line 1714 "jsonpath_gram.c"
    break;

  case 62: /* any_path: ANY_P '{' any_level TO_P any_level '}'  */
#line 242 "jsonpath_gram.y"
                                                                        { (yyval.value) = makeAny((yyvsp[-3].integer), (yyvsp[-1].integer)); }
#line 1720 "jsonpath_gram.c"
    break;

  case 63: /* accessor_op: '.' key  */
#line 246 "jsonpath_gram.y"
                                                                { (yyval.value) = (yyvsp[0].value); }
#line 1726 "jsonpath_gram.c"
    break;

  case 64: /* accessor_op: '.' '*'  */
#line 247 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemType(jpiAnyKey); }
#line 1732 "jsonpath_gram.c"
    break;

  case 65: /* accessor_op: array_accessor  */
#line 248 "jsonpath_gram.y"
                                                        { (yyval.value) = (yyvsp[0].value); }
#line 1738 "jsonpath_gram.c"
    break;

  case 66: /* accessor_op: '.' any_path  */
#line 249 "jsonpath_gram.y"
                                                        { (yyval.value) = (yyvsp[0].value); }
#line 1744 "jsonpath_gram.c"
    break;

  case 67: /* accessor_op: '.' method '(' ')'  */
#line 250 "jsonpath_gram.y"
                                                { (yyval.value) = makeItemType((yyvsp[-2].optype)); }
#line 1750 "jsonpath_gram.c"
    break;

  case 68: /* accessor_op: '.' DATETIME_P '(' opt_datetime_template ')'  */
#line 252 "jsonpath_gram.y"
                                                                        { (yyval.value) = makeItemUnary(jpiDatetime, (yyvsp[-1].value)); }
#line 1756 "jsonpath_gram.c"
    break;

  case 69: /* accessor_op: '?' '(' predicate ')'  */
#line 253 "jsonpath_gram.y"
                                                { (yyval.value) = makeItemUnary(jpiFilter, (yyvsp[-1].value)); }
#line 1762 "jsonpath_gram.c"
    break;

  case 70: /* datetime_template: STRING_P  */
#line 257 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemString(&(yyvsp[0].str)); }
#line 1768 "jsonpath_gram.c"
    break;

  case 71: /* opt_datetime_template: datetime_template  */
#line 261 "jsonpath_gram.y"
                                                        { (yyval.value) = (yyvsp[0].value); }
#line 1774 "jsonpath_gram.c"
    break;

  case 72: /* opt_datetime_template: %empty  */
#line 262 "jsonpath_gram.y"
                                                        { (yyval.value) = NULL; }
#line 1780 "jsonpath_gram.c"
    break;

  case 73: /* key: key_name  */
#line 266 "jsonpath_gram.y"
                                                                { (yyval.value) = makeItemKey(&(yyvsp[0].str)); }
#line 1786 "jsonpath_gram.c"
    break;

  case 98: /* method: ABS_P  */
#line 297 "jsonpath_gram.y"
                                                                { (yyval.optype) = jpiAbs; }
#line 1792 "jsonpath_gram.c"
    break;

  case 99: /* method: SIZE_P  */
#line 298 "jsonpath_gram.y"
                                                                { (yyval.optype) = jpiSize; }
#line 1798 "jsonpath_gram.c"
    break;

  case 100: /* method: TYPE_P  */
#line 299 "jsonpath_gram.y"
                                                                { (yyval.optype) = jpiType; }
#line 1804 "jsonpath_gram.c"
    break;

  case 101: /* method: FLOOR_P  */
#line 300 "jsonpath_gram.y"
                                                                { (yyval.optype) = jpiFloor; }
#line 1810 "jsonpath_gram.c"
    break;

  case 102: /* method: DOUBLE_P  */
#line 301 "jsonpath_gram.y"
                                                                { (yyval.optype) = jpiDouble; }
#line 1816 "jsonpath_gram.c"
    break;

  case 103: /* method: CEILING_P  */
#line 302 "jsonpath_gram.y"
                                                                { (yyval.optype) = jpiCeiling; }
#line 1822 "jsonpath_gram.c"
    break;

  case 104: /* method: KEYVALUE_P  */
#line 303 "jsonpath_gram.y"
                                                        { (yyval.optype) = jpiKeyValue; }
#line 1828 "jsonpath_gram.c"
    break;


#line 1832 "jsonpath_gram.c"

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
      yyerror (result, escontext, YY_("syntax error"));
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
                      yytoken, &yylval, result, escontext);
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
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, result, escontext);
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
  goto yyreturn;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;


#if !defined yyoverflow
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (result, escontext, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturn;
#endif


/*-------------------------------------------------------.
| yyreturn -- parsing is finished, clean up and return.  |
`-------------------------------------------------------*/
yyreturn:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, result, escontext);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, result, escontext);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif

  return yyresult;
}

#line 305 "jsonpath_gram.y"


/*
 * The helper functions below allocate and fill JsonPathParseItem's of various
 * types.
 */

static JsonPathParseItem *
makeItemType(JsonPathItemType type)
{
	JsonPathParseItem *v = palloc(sizeof(*v));

	CHECK_FOR_INTERRUPTS();

	v->type = type;
	v->next = NULL;

	return v;
}

static JsonPathParseItem *
makeItemString(JsonPathString *s)
{
	JsonPathParseItem *v;

	if (s == NULL)
	{
		v = makeItemType(jpiNull);
	}
	else
	{
		v = makeItemType(jpiString);
		v->value.string.val = s->val;
		v->value.string.len = s->len;
	}

	return v;
}

static JsonPathParseItem *
makeItemVariable(JsonPathString *s)
{
	JsonPathParseItem *v;

	v = makeItemType(jpiVariable);
	v->value.string.val = s->val;
	v->value.string.len = s->len;

	return v;
}

static JsonPathParseItem *
makeItemKey(JsonPathString *s)
{
	JsonPathParseItem *v;

	v = makeItemString(s);
	v->type = jpiKey;

	return v;
}

static JsonPathParseItem *
makeItemNumeric(JsonPathString *s)
{
	JsonPathParseItem *v;

	v = makeItemType(jpiNumeric);
	v->value.numeric =
		DatumGetNumeric(DirectFunctionCall3(numeric_in,
											CStringGetDatum(s->val),
											ObjectIdGetDatum(InvalidOid),
											Int32GetDatum(-1)));

	return v;
}

static JsonPathParseItem *
makeItemBool(bool val)
{
	JsonPathParseItem *v = makeItemType(jpiBool);

	v->value.boolean = val;

	return v;
}

static JsonPathParseItem *
makeItemBinary(JsonPathItemType type, JsonPathParseItem *la, JsonPathParseItem *ra)
{
	JsonPathParseItem *v = makeItemType(type);

	v->value.args.left = la;
	v->value.args.right = ra;

	return v;
}

static JsonPathParseItem *
makeItemUnary(JsonPathItemType type, JsonPathParseItem *a)
{
	JsonPathParseItem *v;

	if (type == jpiPlus && a->type == jpiNumeric && !a->next)
		return a;

	if (type == jpiMinus && a->type == jpiNumeric && !a->next)
	{
		v = makeItemType(jpiNumeric);
		v->value.numeric =
			DatumGetNumeric(DirectFunctionCall1(numeric_uminus,
												NumericGetDatum(a->value.numeric)));
		return v;
	}

	v = makeItemType(type);

	v->value.arg = a;

	return v;
}

static JsonPathParseItem *
makeItemList(List *list)
{
	JsonPathParseItem *head,
			   *end;
	ListCell   *cell;

	head = end = (JsonPathParseItem *) linitial(list);

	if (list_length(list) == 1)
		return head;

	/* append items to the end of already existing list */
	while (end->next)
		end = end->next;

	for_each_from(cell, list, 1)
	{
		JsonPathParseItem *c = (JsonPathParseItem *) lfirst(cell);

		end->next = c;
		end = c;
	}

	return head;
}

static JsonPathParseItem *
makeIndexArray(List *list)
{
	JsonPathParseItem *v = makeItemType(jpiIndexArray);
	ListCell   *cell;
	int			i = 0;

	Assert(list != NIL);
	v->value.array.nelems = list_length(list);

	v->value.array.elems = palloc(sizeof(v->value.array.elems[0]) *
								  v->value.array.nelems);

	foreach(cell, list)
	{
		JsonPathParseItem *jpi = lfirst(cell);

		Assert(jpi->type == jpiSubscript);

		v->value.array.elems[i].from = jpi->value.args.left;
		v->value.array.elems[i++].to = jpi->value.args.right;
	}

	return v;
}

static JsonPathParseItem *
makeAny(int first, int last)
{
	JsonPathParseItem *v = makeItemType(jpiAny);

	v->value.anybounds.first = (first >= 0) ? first : PG_UINT32_MAX;
	v->value.anybounds.last = (last >= 0) ? last : PG_UINT32_MAX;

	return v;
}

static bool
makeItemLikeRegex(JsonPathParseItem *expr, JsonPathString *pattern,
				  JsonPathString *flags, JsonPathParseItem ** result,
				  struct Node *escontext)
{
	JsonPathParseItem *v = makeItemType(jpiLikeRegex);
	int			i;
	int			cflags;

	v->value.like_regex.expr = expr;
	v->value.like_regex.pattern = pattern->val;
	v->value.like_regex.patternlen = pattern->len;

	/* Parse the flags string, convert to bitmask.  Duplicate flags are OK. */
	v->value.like_regex.flags = 0;
	for (i = 0; flags && i < flags->len; i++)
	{
		switch (flags->val[i])
		{
			case 'i':
				v->value.like_regex.flags |= JSP_REGEX_ICASE;
				break;
			case 's':
				v->value.like_regex.flags |= JSP_REGEX_DOTALL;
				break;
			case 'm':
				v->value.like_regex.flags |= JSP_REGEX_MLINE;
				break;
			case 'x':
				v->value.like_regex.flags |= JSP_REGEX_WSPACE;
				break;
			case 'q':
				v->value.like_regex.flags |= JSP_REGEX_QUOTE;
				break;
			default:
				ereturn(escontext, false,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input syntax for type %s", "jsonpath"),
						 errdetail("Unrecognized flag character \"%.*s\" in LIKE_REGEX predicate.",
								   pg_mblen(flags->val + i), flags->val + i)));
				break;
		}
	}

	/* Convert flags to what pg_regcomp needs */
	if ( !jspConvertRegexFlags(v->value.like_regex.flags, &cflags, escontext))
		 return false;

	/* check regex validity */
	{
		regex_t     re_tmp;
		pg_wchar   *wpattern;
		int         wpattern_len;
		int         re_result;

		wpattern = (pg_wchar *) palloc((pattern->len + 1) * sizeof(pg_wchar));
		wpattern_len = pg_mb2wchar_with_len(pattern->val,
											wpattern,
											pattern->len);

		if ((re_result = pg_regcomp(&re_tmp, wpattern, wpattern_len, cflags,
									DEFAULT_COLLATION_OID)) != REG_OKAY)
		{
			char        errMsg[100];

			pg_regerror(re_result, &re_tmp, errMsg, sizeof(errMsg));
			ereturn(escontext, false,
					(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
					 errmsg("invalid regular expression: %s", errMsg)));
		}

		pg_regfree(&re_tmp);
	}

	*result = v;

	return true;
}

/*
 * Convert from XQuery regex flags to those recognized by our regex library.
 */
bool
jspConvertRegexFlags(uint32 xflags, int *result, struct Node *escontext)
{
	/* By default, XQuery is very nearly the same as Spencer's AREs */
	int			cflags = REG_ADVANCED;

	/* Ignore-case means the same thing, too, modulo locale issues */
	if (xflags & JSP_REGEX_ICASE)
		cflags |= REG_ICASE;

	/* Per XQuery spec, if 'q' is specified then 'm', 's', 'x' are ignored */
	if (xflags & JSP_REGEX_QUOTE)
	{
		cflags &= ~REG_ADVANCED;
		cflags |= REG_QUOTE;
	}
	else
	{
		/* Note that dotall mode is the default in POSIX */
		if (!(xflags & JSP_REGEX_DOTALL))
			cflags |= REG_NLSTOP;
		if (xflags & JSP_REGEX_MLINE)
			cflags |= REG_NLANCH;

		/*
		 * XQuery's 'x' mode is related to Spencer's expanded mode, but it's
		 * not really enough alike to justify treating JSP_REGEX_WSPACE as
		 * REG_EXPANDED.  For now we treat 'x' as unimplemented; perhaps in
		 * future we'll modify the regex library to have an option for
		 * XQuery-style ignore-whitespace mode.
		 */
		if (xflags & JSP_REGEX_WSPACE)
			ereturn(escontext, false,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("XQuery \"x\" flag (expanded regular expressions) is not implemented")));
	}

	*result = cflags;

	return true;
}
