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
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1


/* Substitute the variable and function names.  */
#define yyparse         boot_yyparse
#define yylex           boot_yylex
#define yyerror         boot_yyerror
#define yydebug         boot_yydebug
#define yynerrs         boot_yynerrs
#define yylval          boot_yylval
#define yychar          boot_yychar

/* First part of user prologue.  */
#line 1 "bootparse.y"

/*-------------------------------------------------------------------------
 *
 * bootparse.y
 *	  yacc grammar for the "bootstrap" mode (BKI file format)
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/bootstrap/bootparse.y
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "bootstrap/bootstrap.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_am.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_tablespace.h"
#include "catalog/toasting.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/memutils.h"


/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.
 */
#define YYMALLOC palloc
#define YYFREE   pfree

static __thread MemoryContext per_line_ctx = NULL;

static void
do_start(void)
{
	Assert(CurrentMemoryContext == CurTransactionContext);
	/* First time through, create the per-line working context */
	if (per_line_ctx == NULL)
		per_line_ctx = AllocSetContextCreate(CurTransactionContext,
											 "bootstrap per-line processing",
											 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(per_line_ctx);
}


static void
do_end(void)
{
	/* Reclaim memory allocated while processing this line */
	MemoryContextSwitchTo(CurTransactionContext);
	MemoryContextReset(per_line_ctx);
	CHECK_FOR_INTERRUPTS();		/* allow SIGINT to kill bootstrap run */
	if (isatty(0))
	{
		printf("bootstrap> ");
		fflush(stdout);
	}
}


static __thread int num_columns_read = 0;


#line 155 "bootparse.c"

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

#include "bootparse.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_ID = 3,                         /* ID  */
  YYSYMBOL_COMMA = 4,                      /* COMMA  */
  YYSYMBOL_EQUALS = 5,                     /* EQUALS  */
  YYSYMBOL_LPAREN = 6,                     /* LPAREN  */
  YYSYMBOL_RPAREN = 7,                     /* RPAREN  */
  YYSYMBOL_NULLVAL = 8,                    /* NULLVAL  */
  YYSYMBOL_OPEN = 9,                       /* OPEN  */
  YYSYMBOL_XCLOSE = 10,                    /* XCLOSE  */
  YYSYMBOL_XCREATE = 11,                   /* XCREATE  */
  YYSYMBOL_INSERT_TUPLE = 12,              /* INSERT_TUPLE  */
  YYSYMBOL_XDECLARE = 13,                  /* XDECLARE  */
  YYSYMBOL_INDEX = 14,                     /* INDEX  */
  YYSYMBOL_ON = 15,                        /* ON  */
  YYSYMBOL_USING = 16,                     /* USING  */
  YYSYMBOL_XBUILD = 17,                    /* XBUILD  */
  YYSYMBOL_INDICES = 18,                   /* INDICES  */
  YYSYMBOL_UNIQUE = 19,                    /* UNIQUE  */
  YYSYMBOL_XTOAST = 20,                    /* XTOAST  */
  YYSYMBOL_OBJ_ID = 21,                    /* OBJ_ID  */
  YYSYMBOL_XBOOTSTRAP = 22,                /* XBOOTSTRAP  */
  YYSYMBOL_XSHARED_RELATION = 23,          /* XSHARED_RELATION  */
  YYSYMBOL_XROWTYPE_OID = 24,              /* XROWTYPE_OID  */
  YYSYMBOL_XFORCE = 25,                    /* XFORCE  */
  YYSYMBOL_XNOT = 26,                      /* XNOT  */
  YYSYMBOL_XNULL = 27,                     /* XNULL  */
  YYSYMBOL_YYACCEPT = 28,                  /* $accept  */
  YYSYMBOL_TopLevel = 29,                  /* TopLevel  */
  YYSYMBOL_Boot_Queries = 30,              /* Boot_Queries  */
  YYSYMBOL_Boot_Query = 31,                /* Boot_Query  */
  YYSYMBOL_Boot_OpenStmt = 32,             /* Boot_OpenStmt  */
  YYSYMBOL_Boot_CloseStmt = 33,            /* Boot_CloseStmt  */
  YYSYMBOL_Boot_CreateStmt = 34,           /* Boot_CreateStmt  */
  YYSYMBOL_35_1 = 35,                      /* $@1  */
  YYSYMBOL_36_2 = 36,                      /* $@2  */
  YYSYMBOL_Boot_InsertStmt = 37,           /* Boot_InsertStmt  */
  YYSYMBOL_38_3 = 38,                      /* $@3  */
  YYSYMBOL_Boot_DeclareIndexStmt = 39,     /* Boot_DeclareIndexStmt  */
  YYSYMBOL_Boot_DeclareUniqueIndexStmt = 40, /* Boot_DeclareUniqueIndexStmt  */
  YYSYMBOL_Boot_DeclareToastStmt = 41,     /* Boot_DeclareToastStmt  */
  YYSYMBOL_Boot_BuildIndsStmt = 42,        /* Boot_BuildIndsStmt  */
  YYSYMBOL_boot_index_params = 43,         /* boot_index_params  */
  YYSYMBOL_boot_index_param = 44,          /* boot_index_param  */
  YYSYMBOL_optbootstrap = 45,              /* optbootstrap  */
  YYSYMBOL_optsharedrelation = 46,         /* optsharedrelation  */
  YYSYMBOL_optrowtypeoid = 47,             /* optrowtypeoid  */
  YYSYMBOL_boot_column_list = 48,          /* boot_column_list  */
  YYSYMBOL_boot_column_def = 49,           /* boot_column_def  */
  YYSYMBOL_boot_column_nullness = 50,      /* boot_column_nullness  */
  YYSYMBOL_oidspec = 51,                   /* oidspec  */
  YYSYMBOL_boot_column_val_list = 52,      /* boot_column_val_list  */
  YYSYMBOL_boot_column_val = 53,           /* boot_column_val  */
  YYSYMBOL_boot_ident = 54                 /* boot_ident  */
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
#define YYFINAL  46
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   169

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  28
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  27
/* YYNRULES -- Number of rules.  */
#define YYNRULES  65
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  110

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
       0,   112,   112,   113,   117,   118,   122,   123,   124,   125,
     126,   127,   128,   129,   133,   142,   152,   162,   151,   249,
     248,   267,   320,   373,   385,   395,   396,   400,   416,   417,
     421,   422,   426,   427,   431,   432,   436,   445,   446,   447,
     451,   455,   456,   457,   461,   463,   468,   469,   470,   471,
     472,   473,   474,   475,   476,   477,   478,   479,   480,   481,
     482,   483,   484,   485,   486,   487
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
  "\"end of file\"", "error", "\"invalid token\"", "ID", "COMMA",
  "EQUALS", "LPAREN", "RPAREN", "NULLVAL", "OPEN", "XCLOSE", "XCREATE",
  "INSERT_TUPLE", "XDECLARE", "INDEX", "ON", "USING", "XBUILD", "INDICES",
  "UNIQUE", "XTOAST", "OBJ_ID", "XBOOTSTRAP", "XSHARED_RELATION",
  "XROWTYPE_OID", "XFORCE", "XNOT", "XNULL", "$accept", "TopLevel",
  "Boot_Queries", "Boot_Query", "Boot_OpenStmt", "Boot_CloseStmt",
  "Boot_CreateStmt", "$@1", "$@2", "Boot_InsertStmt", "$@3",
  "Boot_DeclareIndexStmt", "Boot_DeclareUniqueIndexStmt",
  "Boot_DeclareToastStmt", "Boot_BuildIndsStmt", "boot_index_params",
  "boot_index_param", "optbootstrap", "optsharedrelation", "optrowtypeoid",
  "boot_column_list", "boot_column_def", "boot_column_nullness", "oidspec",
  "boot_column_val_list", "boot_column_val", "boot_ident", YY_NULLPTR
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
     275,   276,   277,   278,   279,   280,   281,   282
};
#endif

#define YYPACT_NINF (-53)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int16 yypact[] =
{
      -4,   142,   142,   142,   -53,     2,   -14,    25,    -4,   -53,
     -53,   -53,   -53,   -53,   -53,   -53,   -53,   -53,   -53,   -53,
     -53,   -53,   -53,   -53,   -53,   -53,   -53,   -53,   -53,   -53,
     -53,   -53,   -53,   -53,   -53,   -53,   -53,   -53,   -53,   -53,
     142,    20,   142,    13,   142,   -53,   -53,   -53,     6,   -53,
     117,   142,   142,   142,   -53,     8,   -53,    92,   -53,   -53,
      14,   142,    17,   -53,     9,   117,   -53,   -53,   142,    19,
     142,   142,    29,   -53,    21,   142,   -53,   -53,   -53,   142,
      22,   142,    30,   142,    35,   -53,    37,   142,    34,   142,
      36,   142,    10,   -53,   142,   142,   -53,   -53,    23,   142,
     -53,   -53,    11,    -3,   -53,   -53,   -53,    18,   -53,   -53
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       3,     0,     0,     0,    19,     0,     0,     0,     2,     4,
       6,     7,     8,     9,    10,    11,    12,    13,    46,    47,
      48,    49,    50,    51,    52,    53,    54,    55,    56,    57,
      58,    59,    60,    61,    62,    63,    64,    65,    14,    15,
       0,     0,     0,     0,     0,    24,     1,     5,    29,    40,
       0,     0,     0,     0,    28,    31,    45,     0,    41,    44,
       0,     0,     0,    30,    33,     0,    20,    42,     0,     0,
       0,     0,     0,    43,     0,     0,    23,    32,    16,     0,
       0,     0,     0,     0,    17,    34,     0,     0,     0,     0,
       0,     0,     0,    26,     0,     0,    35,    18,    39,     0,
      21,    27,     0,     0,    36,    25,    22,     0,    38,    37
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -53,   -53,   -53,    38,   -53,   -53,   -53,   -53,   -53,   -53,
     -53,   -53,   -53,   -53,   -53,   -51,   -52,   -53,   -53,   -53,
     -53,   -39,   -53,   -41,   -53,   -46,    -1
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
       0,     7,     8,     9,    10,    11,    12,    81,    90,    13,
      41,    14,    15,    16,    17,    92,    93,    55,    64,    72,
      84,    85,   104,    48,    57,    58,    49
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int8 yytable[] =
{
      38,    39,    40,    53,    45,     1,     2,     3,     4,     5,
      60,    67,    62,     6,    99,    99,    42,   100,   106,    73,
      69,    43,    44,   107,   108,    46,    50,    52,    54,    68,
      77,    63,    70,    71,    75,    78,    87,    79,    83,    89,
      95,    51,    91,    97,   102,   109,    47,   105,   103,    59,
      96,    61,     0,     0,     0,     0,    59,     0,     0,     0,
       0,     0,     0,     0,    59,     0,     0,    74,     0,    76,
       0,     0,     0,     0,    80,     0,     0,     0,    82,     0,
      86,     0,    88,     0,     0,     0,    94,     0,    86,     0,
      98,     0,     0,   101,    94,    18,    65,     0,    94,    66,
      56,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      18,     0,     0,     0,     0,    56,    19,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    36,    37,    18,     0,     0,     0,     0,
       0,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37
};

static const yytype_int8 yycheck[] =
{
       1,     2,     3,    44,    18,     9,    10,    11,    12,    13,
      51,    57,    53,    17,     4,     4,    14,     7,     7,    65,
      61,    19,    20,    26,    27,     0,     6,    14,    22,    15,
      71,    23,    15,    24,    15,     6,     6,    16,    16,     4,
       6,    42,     5,     7,    95,    27,     8,    99,    25,    50,
      89,    52,    -1,    -1,    -1,    -1,    57,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    65,    -1,    -1,    68,    -1,    70,
      -1,    -1,    -1,    -1,    75,    -1,    -1,    -1,    79,    -1,
      81,    -1,    83,    -1,    -1,    -1,    87,    -1,    89,    -1,
      91,    -1,    -1,    94,    95,     3,     4,    -1,    99,     7,
       8,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
       3,    -1,    -1,    -1,    -1,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    27,     3,    -1,    -1,    -1,    -1,
      -1,     9,    10,    11,    12,    13,    14,    15,    16,    17,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     9,    10,    11,    12,    13,    17,    29,    30,    31,
      32,    33,    34,    37,    39,    40,    41,    42,     3,     9,
      10,    11,    12,    13,    14,    15,    16,    17,    18,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    54,    54,
      54,    38,    14,    19,    20,    18,     0,    31,    51,    54,
       6,    54,    14,    51,    22,    45,     8,    52,    53,    54,
      51,    54,    51,    23,    46,     4,     7,    53,    15,    51,
      15,    24,    47,    53,    54,    15,    54,    51,     6,    16,
      54,    35,    54,    16,    48,    49,    54,     6,    54,     4,
      36,     5,    43,    44,    54,     6,    49,     7,    54,     4,
       7,    54,    43,    25,    50,    44,     7,    26,    27,    27
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_int8 yyr1[] =
{
       0,    28,    29,    29,    30,    30,    31,    31,    31,    31,
      31,    31,    31,    31,    32,    33,    35,    36,    34,    38,
      37,    39,    40,    41,    42,    43,    43,    44,    45,    45,
      46,    46,    47,    47,    48,    48,    49,    50,    50,    50,
      51,    52,    52,    52,    53,    53,    54,    54,    54,    54,
      54,    54,    54,    54,    54,    54,    54,    54,    54,    54,
      54,    54,    54,    54,    54,    54
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     1,     0,     1,     2,     1,     1,     1,     1,
       1,     1,     1,     1,     2,     2,     0,     0,    11,     0,
       5,    11,    12,     6,     2,     3,     1,     2,     1,     0,
       1,     0,     2,     0,     1,     3,     4,     3,     2,     0,
       1,     1,     2,     3,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1
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
        yyerror (YY_("syntax error: cannot back up")); \
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
                  Kind, Value); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
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
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep);
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
                 int yyrule)
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
                       &yyvsp[(yyi + 1) - (yynrhs)]);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule); \
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
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep)
{
  YY_USE (yyvaluep);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/* Lookahead token kind.  */
__thread int yychar;

/* The semantic value of the lookahead symbol.  */
__thread YYSTYPE boot_yylval;
/* Number of syntax errors so far.  */
__thread int yynerrs;




/*----------.
| yyparse.  |
`----------*/

int
yyparse (void)
{
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
      yychar = yylex ();
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
  case 14: /* Boot_OpenStmt: OPEN boot_ident  */
#line 134 "bootparse.y"
                                {
					do_start();
					boot_openrel((yyvsp[0].str));
					do_end();
				}
#line 1292 "bootparse.c"
    break;

  case 15: /* Boot_CloseStmt: XCLOSE boot_ident  */
#line 143 "bootparse.y"
                                {
					do_start();
					closerel((yyvsp[0].str));
					do_end();
				}
#line 1302 "bootparse.c"
    break;

  case 16: /* $@1: %empty  */
#line 152 "bootparse.y"
                                {
					do_start();
					numattr = 0;
					elog(DEBUG4, "creating%s%s relation %s %u",
						 (yyvsp[-3].ival) ? " bootstrap" : "",
						 (yyvsp[-2].ival) ? " shared" : "",
						 (yyvsp[-5].str),
						 (yyvsp[-4].oidval));
				}
#line 1316 "bootparse.c"
    break;

  case 17: /* $@2: %empty  */
#line 162 "bootparse.y"
                                {
					do_end();
				}
#line 1324 "bootparse.c"
    break;

  case 18: /* Boot_CreateStmt: XCREATE boot_ident oidspec optbootstrap optsharedrelation optrowtypeoid LPAREN $@1 boot_column_list $@2 RPAREN  */
#line 166 "bootparse.y"
                                {
					TupleDesc	tupdesc;
					bool		shared_relation;
					bool		mapped_relation;

					do_start();

					tupdesc = CreateTupleDesc(numattr, attrtypes);

					shared_relation = (yyvsp[-6].ival);

					/*
					 * The catalogs that use the relation mapper are the
					 * bootstrap catalogs plus the shared catalogs.  If this
					 * ever gets more complicated, we should invent a BKI
					 * keyword to mark the mapped catalogs, but for now a
					 * quick hack seems the most appropriate thing.  Note in
					 * particular that all "nailed" heap rels (see formrdesc
					 * in relcache.c) must be mapped.
					 */
					mapped_relation = ((yyvsp[-7].ival) || shared_relation);

					if ((yyvsp[-7].ival))
					{
						TransactionId relfrozenxid;
						MultiXactId relminmxid;

						if (boot_reldesc)
						{
							elog(DEBUG4, "create bootstrap: warning, open relation exists, closing first");
							closerel(NULL);
						}

						boot_reldesc = heap_create((yyvsp[-9].str),
												   PG_CATALOG_NAMESPACE,
												   shared_relation ? GLOBALTABLESPACE_OID : 0,
												   (yyvsp[-8].oidval),
												   InvalidOid,
												   HEAP_TABLE_AM_OID,
												   tupdesc,
												   RELKIND_RELATION,
												   RELPERSISTENCE_PERMANENT,
												   shared_relation,
												   mapped_relation,
												   true,
												   &relfrozenxid,
												   &relminmxid,
												   true);
						elog(DEBUG4, "bootstrap relation created");
					}
					else
					{
						Oid			id;

						id = heap_create_with_catalog((yyvsp[-9].str),
													  PG_CATALOG_NAMESPACE,
													  shared_relation ? GLOBALTABLESPACE_OID : 0,
													  (yyvsp[-8].oidval),
													  (yyvsp[-5].oidval),
													  InvalidOid,
													  BOOTSTRAP_SUPERUSERID,
													  HEAP_TABLE_AM_OID,
													  tupdesc,
													  NIL,
													  RELKIND_RELATION,
													  RELPERSISTENCE_PERMANENT,
													  shared_relation,
													  mapped_relation,
													  ONCOMMIT_NOOP,
													  (Datum) 0,
													  false,
													  true,
													  false,
													  InvalidOid,
													  NULL);
						elog(DEBUG4, "relation created with OID %u", id);
					}
					do_end();
				}
#line 1408 "bootparse.c"
    break;

  case 19: /* $@3: %empty  */
#line 249 "bootparse.y"
                                {
					do_start();
					elog(DEBUG4, "inserting row");
					num_columns_read = 0;
				}
#line 1418 "bootparse.c"
    break;

  case 20: /* Boot_InsertStmt: INSERT_TUPLE $@3 LPAREN boot_column_val_list RPAREN  */
#line 255 "bootparse.y"
                                {
					if (num_columns_read != numattr)
						elog(ERROR, "incorrect number of columns in row (expected %d, got %d)",
							 numattr, num_columns_read);
					if (boot_reldesc == NULL)
						elog(FATAL, "relation not open");
					InsertOneTuple();
					do_end();
				}
#line 1432 "bootparse.c"
    break;

  case 21: /* Boot_DeclareIndexStmt: XDECLARE INDEX boot_ident oidspec ON boot_ident USING boot_ident LPAREN boot_index_params RPAREN  */
#line 268 "bootparse.y"
                                {
					IndexStmt  *stmt = makeNode(IndexStmt);
					Oid			relationId;

					elog(DEBUG4, "creating index \"%s\"", (yyvsp[-8].str));

					do_start();

					stmt->idxname = (yyvsp[-8].str);
					stmt->relation = makeRangeVar(NULL, (yyvsp[-5].str), -1);
					stmt->accessMethod = (yyvsp[-3].str);
					stmt->tableSpace = NULL;
					stmt->indexParams = (yyvsp[-1].list);
					stmt->indexIncludingParams = NIL;
					stmt->options = NIL;
					stmt->whereClause = NULL;
					stmt->excludeOpNames = NIL;
					stmt->idxcomment = NULL;
					stmt->indexOid = InvalidOid;
					stmt->oldNumber = InvalidRelFileNumber;
					stmt->oldCreateSubid = InvalidSubTransactionId;
					stmt->oldFirstRelfilelocatorSubid = InvalidSubTransactionId;
					stmt->unique = false;
					stmt->primary = false;
					stmt->isconstraint = false;
					stmt->deferrable = false;
					stmt->initdeferred = false;
					stmt->transformed = false;
					stmt->concurrent = false;
					stmt->if_not_exists = false;
					stmt->reset_default_tblspc = false;

					/* locks and races need not concern us in bootstrap mode */
					relationId = RangeVarGetRelid(stmt->relation, NoLock,
												  false);

					DefineIndex(relationId,
								stmt,
								(yyvsp[-7].oidval),
								InvalidOid,
								InvalidOid,
								-1,
								false,
								false,
								false,
								true, /* skip_build */
								false);
					do_end();
				}
#line 1486 "bootparse.c"
    break;

  case 22: /* Boot_DeclareUniqueIndexStmt: XDECLARE UNIQUE INDEX boot_ident oidspec ON boot_ident USING boot_ident LPAREN boot_index_params RPAREN  */
#line 321 "bootparse.y"
                                {
					IndexStmt  *stmt = makeNode(IndexStmt);
					Oid			relationId;

					elog(DEBUG4, "creating unique index \"%s\"", (yyvsp[-8].str));

					do_start();

					stmt->idxname = (yyvsp[-8].str);
					stmt->relation = makeRangeVar(NULL, (yyvsp[-5].str), -1);
					stmt->accessMethod = (yyvsp[-3].str);
					stmt->tableSpace = NULL;
					stmt->indexParams = (yyvsp[-1].list);
					stmt->indexIncludingParams = NIL;
					stmt->options = NIL;
					stmt->whereClause = NULL;
					stmt->excludeOpNames = NIL;
					stmt->idxcomment = NULL;
					stmt->indexOid = InvalidOid;
					stmt->oldNumber = InvalidRelFileNumber;
					stmt->oldCreateSubid = InvalidSubTransactionId;
					stmt->oldFirstRelfilelocatorSubid = InvalidSubTransactionId;
					stmt->unique = true;
					stmt->primary = false;
					stmt->isconstraint = false;
					stmt->deferrable = false;
					stmt->initdeferred = false;
					stmt->transformed = false;
					stmt->concurrent = false;
					stmt->if_not_exists = false;
					stmt->reset_default_tblspc = false;

					/* locks and races need not concern us in bootstrap mode */
					relationId = RangeVarGetRelid(stmt->relation, NoLock,
												  false);

					DefineIndex(relationId,
								stmt,
								(yyvsp[-7].oidval),
								InvalidOid,
								InvalidOid,
								-1,
								false,
								false,
								false,
								true, /* skip_build */
								false);
					do_end();
				}
#line 1540 "bootparse.c"
    break;

  case 23: /* Boot_DeclareToastStmt: XDECLARE XTOAST oidspec oidspec ON boot_ident  */
#line 374 "bootparse.y"
                                {
					elog(DEBUG4, "creating toast table for table \"%s\"", (yyvsp[0].str));

					do_start();

					BootstrapToastTable((yyvsp[0].str), (yyvsp[-3].oidval), (yyvsp[-2].oidval));
					do_end();
				}
#line 1553 "bootparse.c"
    break;

  case 24: /* Boot_BuildIndsStmt: XBUILD INDICES  */
#line 386 "bootparse.y"
                                {
					do_start();
					build_indices();
					do_end();
				}
#line 1563 "bootparse.c"
    break;

  case 25: /* boot_index_params: boot_index_params COMMA boot_index_param  */
#line 395 "bootparse.y"
                                                                { (yyval.list) = lappend((yyvsp[-2].list), (yyvsp[0].ielem)); }
#line 1569 "bootparse.c"
    break;

  case 26: /* boot_index_params: boot_index_param  */
#line 396 "bootparse.y"
                                                                                        { (yyval.list) = list_make1((yyvsp[0].ielem)); }
#line 1575 "bootparse.c"
    break;

  case 27: /* boot_index_param: boot_ident boot_ident  */
#line 401 "bootparse.y"
                                {
					IndexElem  *n = makeNode(IndexElem);

					n->name = (yyvsp[-1].str);
					n->expr = NULL;
					n->indexcolname = NULL;
					n->collation = NIL;
					n->opclass = list_make1(makeString((yyvsp[0].str)));
					n->ordering = SORTBY_DEFAULT;
					n->nulls_ordering = SORTBY_NULLS_DEFAULT;
					(yyval.ielem) = n;
				}
#line 1592 "bootparse.c"
    break;

  case 28: /* optbootstrap: XBOOTSTRAP  */
#line 416 "bootparse.y"
                                        { (yyval.ival) = 1; }
#line 1598 "bootparse.c"
    break;

  case 29: /* optbootstrap: %empty  */
#line 417 "bootparse.y"
                                                { (yyval.ival) = 0; }
#line 1604 "bootparse.c"
    break;

  case 30: /* optsharedrelation: XSHARED_RELATION  */
#line 421 "bootparse.y"
                                                { (yyval.ival) = 1; }
#line 1610 "bootparse.c"
    break;

  case 31: /* optsharedrelation: %empty  */
#line 422 "bootparse.y"
                                                                { (yyval.ival) = 0; }
#line 1616 "bootparse.c"
    break;

  case 32: /* optrowtypeoid: XROWTYPE_OID oidspec  */
#line 426 "bootparse.y"
                                                { (yyval.oidval) = (yyvsp[0].oidval); }
#line 1622 "bootparse.c"
    break;

  case 33: /* optrowtypeoid: %empty  */
#line 427 "bootparse.y"
                                                                        { (yyval.oidval) = InvalidOid; }
#line 1628 "bootparse.c"
    break;

  case 36: /* boot_column_def: boot_ident EQUALS boot_ident boot_column_nullness  */
#line 437 "bootparse.y"
                                {
				   if (++numattr > MAXATTR)
						elog(FATAL, "too many columns");
				   DefineAttr((yyvsp[-3].str), (yyvsp[-1].str), numattr-1, (yyvsp[0].ival));
				}
#line 1638 "bootparse.c"
    break;

  case 37: /* boot_column_nullness: XFORCE XNOT XNULL  */
#line 445 "bootparse.y"
                                                { (yyval.ival) = BOOTCOL_NULL_FORCE_NOT_NULL; }
#line 1644 "bootparse.c"
    break;

  case 38: /* boot_column_nullness: XFORCE XNULL  */
#line 446 "bootparse.y"
                                                {  (yyval.ival) = BOOTCOL_NULL_FORCE_NULL; }
#line 1650 "bootparse.c"
    break;

  case 39: /* boot_column_nullness: %empty  */
#line 447 "bootparse.y"
                  { (yyval.ival) = BOOTCOL_NULL_AUTO; }
#line 1656 "bootparse.c"
    break;

  case 40: /* oidspec: boot_ident  */
#line 451 "bootparse.y"
                                                                                        { (yyval.oidval) = atooid((yyvsp[0].str)); }
#line 1662 "bootparse.c"
    break;

  case 44: /* boot_column_val: boot_ident  */
#line 462 "bootparse.y"
                        { InsertOneValue((yyvsp[0].str), num_columns_read++); }
#line 1668 "bootparse.c"
    break;

  case 45: /* boot_column_val: NULLVAL  */
#line 464 "bootparse.y"
                        { InsertOneNull(num_columns_read++); }
#line 1674 "bootparse.c"
    break;

  case 46: /* boot_ident: ID  */
#line 468 "bootparse.y"
                                        { (yyval.str) = (yyvsp[0].str); }
#line 1680 "bootparse.c"
    break;

  case 47: /* boot_ident: OPEN  */
#line 469 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1686 "bootparse.c"
    break;

  case 48: /* boot_ident: XCLOSE  */
#line 470 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1692 "bootparse.c"
    break;

  case 49: /* boot_ident: XCREATE  */
#line 471 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1698 "bootparse.c"
    break;

  case 50: /* boot_ident: INSERT_TUPLE  */
#line 472 "bootparse.y"
                                { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1704 "bootparse.c"
    break;

  case 51: /* boot_ident: XDECLARE  */
#line 473 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1710 "bootparse.c"
    break;

  case 52: /* boot_ident: INDEX  */
#line 474 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1716 "bootparse.c"
    break;

  case 53: /* boot_ident: ON  */
#line 475 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1722 "bootparse.c"
    break;

  case 54: /* boot_ident: USING  */
#line 476 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1728 "bootparse.c"
    break;

  case 55: /* boot_ident: XBUILD  */
#line 477 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1734 "bootparse.c"
    break;

  case 56: /* boot_ident: INDICES  */
#line 478 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1740 "bootparse.c"
    break;

  case 57: /* boot_ident: UNIQUE  */
#line 479 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1746 "bootparse.c"
    break;

  case 58: /* boot_ident: XTOAST  */
#line 480 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1752 "bootparse.c"
    break;

  case 59: /* boot_ident: OBJ_ID  */
#line 481 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1758 "bootparse.c"
    break;

  case 60: /* boot_ident: XBOOTSTRAP  */
#line 482 "bootparse.y"
                                { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1764 "bootparse.c"
    break;

  case 61: /* boot_ident: XSHARED_RELATION  */
#line 483 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1770 "bootparse.c"
    break;

  case 62: /* boot_ident: XROWTYPE_OID  */
#line 484 "bootparse.y"
                                { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1776 "bootparse.c"
    break;

  case 63: /* boot_ident: XFORCE  */
#line 485 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1782 "bootparse.c"
    break;

  case 64: /* boot_ident: XNOT  */
#line 486 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1788 "bootparse.c"
    break;

  case 65: /* boot_ident: XNULL  */
#line 487 "bootparse.y"
                                        { (yyval.str) = pstrdup((yyvsp[0].kw)); }
#line 1794 "bootparse.c"
    break;


#line 1798 "bootparse.c"

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
      yyerror (YY_("syntax error"));
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
                      yytoken, &yylval);
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
                  YY_ACCESSING_SYMBOL (yystate), yyvsp);
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
  yyerror (YY_("memory exhausted"));
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
                  yytoken, &yylval);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif

  return yyresult;
}

#line 489 "bootparse.y"

