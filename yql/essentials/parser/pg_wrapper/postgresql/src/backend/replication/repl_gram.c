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
#define yyparse         replication_yyparse
#define yylex           replication_yylex
#define yyerror         replication_yyerror
#define yydebug         replication_yydebug
#define yynerrs         replication_yynerrs
#define yylval          replication_yylval
#define yychar          replication_yychar

/* First part of user prologue.  */
#line 1 "repl_gram.y"

/*-------------------------------------------------------------------------
 *
 * repl_gram.y				- Parser for the replication commands
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/replication/repl_gram.y
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlogdefs.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/replnodes.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"


/* Result of the parsing is returned here */
__thread Node *replication_parse_result;


/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.
 */
#define YYMALLOC palloc
#define YYFREE   pfree


#line 117 "repl_gram.c"

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

#include "repl_gram.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_SCONST = 3,                     /* SCONST  */
  YYSYMBOL_IDENT = 4,                      /* IDENT  */
  YYSYMBOL_UCONST = 5,                     /* UCONST  */
  YYSYMBOL_RECPTR = 6,                     /* RECPTR  */
  YYSYMBOL_K_BASE_BACKUP = 7,              /* K_BASE_BACKUP  */
  YYSYMBOL_K_IDENTIFY_SYSTEM = 8,          /* K_IDENTIFY_SYSTEM  */
  YYSYMBOL_K_READ_REPLICATION_SLOT = 9,    /* K_READ_REPLICATION_SLOT  */
  YYSYMBOL_K_SHOW = 10,                    /* K_SHOW  */
  YYSYMBOL_K_START_REPLICATION = 11,       /* K_START_REPLICATION  */
  YYSYMBOL_K_CREATE_REPLICATION_SLOT = 12, /* K_CREATE_REPLICATION_SLOT  */
  YYSYMBOL_K_DROP_REPLICATION_SLOT = 13,   /* K_DROP_REPLICATION_SLOT  */
  YYSYMBOL_K_TIMELINE_HISTORY = 14,        /* K_TIMELINE_HISTORY  */
  YYSYMBOL_K_WAIT = 15,                    /* K_WAIT  */
  YYSYMBOL_K_TIMELINE = 16,                /* K_TIMELINE  */
  YYSYMBOL_K_PHYSICAL = 17,                /* K_PHYSICAL  */
  YYSYMBOL_K_LOGICAL = 18,                 /* K_LOGICAL  */
  YYSYMBOL_K_SLOT = 19,                    /* K_SLOT  */
  YYSYMBOL_K_RESERVE_WAL = 20,             /* K_RESERVE_WAL  */
  YYSYMBOL_K_TEMPORARY = 21,               /* K_TEMPORARY  */
  YYSYMBOL_K_TWO_PHASE = 22,               /* K_TWO_PHASE  */
  YYSYMBOL_K_EXPORT_SNAPSHOT = 23,         /* K_EXPORT_SNAPSHOT  */
  YYSYMBOL_K_NOEXPORT_SNAPSHOT = 24,       /* K_NOEXPORT_SNAPSHOT  */
  YYSYMBOL_K_USE_SNAPSHOT = 25,            /* K_USE_SNAPSHOT  */
  YYSYMBOL_26_ = 26,                       /* ';'  */
  YYSYMBOL_27_ = 27,                       /* '.'  */
  YYSYMBOL_28_ = 28,                       /* '('  */
  YYSYMBOL_29_ = 29,                       /* ')'  */
  YYSYMBOL_30_ = 30,                       /* ','  */
  YYSYMBOL_YYACCEPT = 31,                  /* $accept  */
  YYSYMBOL_firstcmd = 32,                  /* firstcmd  */
  YYSYMBOL_opt_semicolon = 33,             /* opt_semicolon  */
  YYSYMBOL_command = 34,                   /* command  */
  YYSYMBOL_identify_system = 35,           /* identify_system  */
  YYSYMBOL_read_replication_slot = 36,     /* read_replication_slot  */
  YYSYMBOL_show = 37,                      /* show  */
  YYSYMBOL_var_name = 38,                  /* var_name  */
  YYSYMBOL_base_backup = 39,               /* base_backup  */
  YYSYMBOL_create_replication_slot = 40,   /* create_replication_slot  */
  YYSYMBOL_create_slot_options = 41,       /* create_slot_options  */
  YYSYMBOL_create_slot_legacy_opt_list = 42, /* create_slot_legacy_opt_list  */
  YYSYMBOL_create_slot_legacy_opt = 43,    /* create_slot_legacy_opt  */
  YYSYMBOL_drop_replication_slot = 44,     /* drop_replication_slot  */
  YYSYMBOL_start_replication = 45,         /* start_replication  */
  YYSYMBOL_start_logical_replication = 46, /* start_logical_replication  */
  YYSYMBOL_timeline_history = 47,          /* timeline_history  */
  YYSYMBOL_opt_physical = 48,              /* opt_physical  */
  YYSYMBOL_opt_temporary = 49,             /* opt_temporary  */
  YYSYMBOL_opt_slot = 50,                  /* opt_slot  */
  YYSYMBOL_opt_timeline = 51,              /* opt_timeline  */
  YYSYMBOL_plugin_options = 52,            /* plugin_options  */
  YYSYMBOL_plugin_opt_list = 53,           /* plugin_opt_list  */
  YYSYMBOL_plugin_opt_elem = 54,           /* plugin_opt_elem  */
  YYSYMBOL_plugin_opt_arg = 55,            /* plugin_opt_arg  */
  YYSYMBOL_generic_option_list = 56,       /* generic_option_list  */
  YYSYMBOL_generic_option = 57,            /* generic_option  */
  YYSYMBOL_ident_or_keyword = 58           /* ident_or_keyword  */
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
#define YYFINAL  29
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   71

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  31
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  28
/* YYNRULES -- Number of rules.  */
#define YYNRULES  76
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  99

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   280


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
      28,    29,     2,     2,    30,     2,    27,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    26,
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
      25
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,    97,    97,   103,   104,   108,   109,   110,   111,   112,
     113,   114,   115,   116,   123,   133,   145,   152,   153,   161,
     167,   176,   187,   201,   202,   206,   209,   213,   218,   223,
     228,   233,   242,   250,   264,   279,   294,   311,   312,   316,
     317,   321,   324,   328,   336,   341,   342,   346,   350,   357,
     364,   365,   369,   371,   376,   380,   384,   388,   395,   396,
     397,   398,   399,   400,   401,   402,   403,   404,   405,   406,
     407,   408,   409,   410,   411,   412,   413
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
  "\"end of file\"", "error", "\"invalid token\"", "SCONST", "IDENT",
  "UCONST", "RECPTR", "K_BASE_BACKUP", "K_IDENTIFY_SYSTEM",
  "K_READ_REPLICATION_SLOT", "K_SHOW", "K_START_REPLICATION",
  "K_CREATE_REPLICATION_SLOT", "K_DROP_REPLICATION_SLOT",
  "K_TIMELINE_HISTORY", "K_WAIT", "K_TIMELINE", "K_PHYSICAL", "K_LOGICAL",
  "K_SLOT", "K_RESERVE_WAL", "K_TEMPORARY", "K_TWO_PHASE",
  "K_EXPORT_SNAPSHOT", "K_NOEXPORT_SNAPSHOT", "K_USE_SNAPSHOT", "';'",
  "'.'", "'('", "')'", "','", "$accept", "firstcmd", "opt_semicolon",
  "command", "identify_system", "read_replication_slot", "show",
  "var_name", "base_backup", "create_replication_slot",
  "create_slot_options", "create_slot_legacy_opt_list",
  "create_slot_legacy_opt", "drop_replication_slot", "start_replication",
  "start_logical_replication", "timeline_history", "opt_physical",
  "opt_temporary", "opt_slot", "opt_timeline", "plugin_options",
  "plugin_opt_list", "plugin_opt_elem", "plugin_opt_arg",
  "generic_option_list", "generic_option", "ident_or_keyword", YY_NULLPTR
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
     275,   276,   277,   278,   279,   280,    59,    46,    40,    41,
      44
};
#endif

#define YYPACT_NINF (-29)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int8 yypact[] =
{
      15,   -23,   -29,    27,    27,    26,    42,    43,    44,    48,
      24,   -29,   -29,   -29,   -29,   -29,   -29,   -29,   -29,   -29,
      -4,   -29,    25,    25,    47,    36,    34,    39,   -29,   -29,
     -29,   -29,   -29,   -29,   -29,   -29,   -29,   -29,   -29,   -29,
     -29,   -29,   -29,   -29,   -29,   -29,   -29,   -29,   -29,   -29,
     -29,   -28,   -29,    33,    52,    40,   -29,    51,   -29,    22,
     -29,   -29,    -4,   -29,   -29,   -29,   -29,    53,    45,    32,
      58,   -29,    35,    59,   -29,    -4,   -29,    10,    32,    61,
     -29,   -29,    12,   -29,   -29,   -29,   -29,   -29,   -29,   -29,
      63,    14,   -29,   -29,   -29,   -29,   -29,    61,   -29
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,    20,    14,     0,     0,    42,     0,     0,     0,     0,
       4,     5,    11,    13,     6,     9,    10,     7,     8,    12,
       0,    17,    15,    16,     0,    38,    40,    32,    36,     1,
       3,     2,    58,    59,    60,    61,    62,    63,    64,    65,
      66,    67,    68,    69,    70,    71,    72,    73,    74,    75,
      76,     0,    53,    54,     0,    41,    37,     0,    39,     0,
      33,    19,     0,    56,    55,    57,    18,     0,    44,    26,
       0,    52,    46,     0,    34,     0,    21,    24,    26,     0,
      35,    43,     0,    30,    31,    27,    28,    29,    25,    22,
      51,     0,    47,    23,    50,    49,    45,     0,    48
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -29,   -29,   -29,   -29,   -29,   -29,   -29,    64,   -29,   -29,
     -11,   -29,   -29,   -29,   -29,   -29,   -29,   -29,   -29,   -29,
     -29,   -29,   -29,   -27,   -29,    -6,     9,   -29
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
       0,     9,    31,    10,    11,    12,    13,    22,    14,    15,
      76,    77,    88,    16,    17,    18,    19,    57,    59,    25,
      74,    80,    91,    92,    95,    51,    52,    53
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int8 yytable[] =
{
      32,    61,    62,    33,    34,    20,    35,    36,    37,    38,
      39,    40,    41,    42,    43,    44,    45,    46,    47,    48,
      49,    50,     1,     2,     3,     4,     5,     6,     7,     8,
      83,    21,    84,    85,    86,    87,    63,    64,    65,    69,
      70,    93,    62,    96,    97,    24,    26,    27,    29,    28,
      30,    55,    54,    56,    60,    58,    66,    68,    67,    72,
      75,    73,    78,    79,    81,    90,    94,    89,    23,    82,
      98,    71
};

static const yytype_int8 yycheck[] =
{
       4,    29,    30,     7,     8,    28,    10,    11,    12,    13,
      14,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,     7,     8,     9,    10,    11,    12,    13,    14,
      20,     4,    22,    23,    24,    25,     3,     4,     5,    17,
      18,    29,    30,    29,    30,    19,     4,     4,     0,     5,
      26,     4,    27,    17,    15,    21,     4,     6,    18,     6,
      28,    16,     4,    28,     5,     4,     3,    78,     4,    75,
      97,    62
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     7,     8,     9,    10,    11,    12,    13,    14,    32,
      34,    35,    36,    37,    39,    40,    44,    45,    46,    47,
      28,     4,    38,    38,    19,    50,     4,     4,     5,     0,
      26,    33,     4,     7,     8,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    56,    57,    58,    27,     4,    17,    48,    21,    49,
      15,    29,    30,     3,     4,     5,     4,    18,     6,    17,
      18,    57,     6,    16,    51,    28,    41,    42,     4,    28,
      52,     5,    56,    20,    22,    23,    24,    25,    43,    41,
       4,    53,    54,    29,     3,    55,    29,    30,    54
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_int8 yyr1[] =
{
       0,    31,    32,    33,    33,    34,    34,    34,    34,    34,
      34,    34,    34,    34,    35,    36,    37,    38,    38,    39,
      39,    40,    40,    41,    41,    42,    42,    43,    43,    43,
      43,    43,    44,    44,    45,    46,    47,    48,    48,    49,
      49,    50,    50,    51,    51,    52,    52,    53,    53,    54,
      55,    55,    56,    56,    57,    57,    57,    57,    58,    58,
      58,    58,    58,    58,    58,    58,    58,    58,    58,    58,
      58,    58,    58,    58,    58,    58,    58
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     1,     0,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     2,     2,     1,     3,     4,
       1,     5,     6,     3,     1,     2,     0,     1,     1,     1,
       1,     1,     2,     3,     5,     6,     2,     1,     0,     1,
       0,     2,     0,     2,     0,     3,     0,     1,     3,     2,
       1,     0,     3,     1,     1,     2,     2,     2,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1
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
__thread YYSTYPE replication_yylval;
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
  case 2: /* firstcmd: command opt_semicolon  */
#line 98 "repl_gram.y"
                                {
					replication_parse_result = (yyvsp[-1].node);
				}
#line 1243 "repl_gram.c"
    break;

  case 14: /* identify_system: K_IDENTIFY_SYSTEM  */
#line 124 "repl_gram.y"
                                {
					(yyval.node) = (Node *) makeNode(IdentifySystemCmd);
				}
#line 1251 "repl_gram.c"
    break;

  case 15: /* read_replication_slot: K_READ_REPLICATION_SLOT var_name  */
#line 134 "repl_gram.y"
                                {
					ReadReplicationSlotCmd *n = makeNode(ReadReplicationSlotCmd);
					n->slotname = (yyvsp[0].str);
					(yyval.node) = (Node *) n;
				}
#line 1261 "repl_gram.c"
    break;

  case 16: /* show: K_SHOW var_name  */
#line 146 "repl_gram.y"
                                {
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = (yyvsp[0].str);
					(yyval.node) = (Node *) n;
				}
#line 1271 "repl_gram.c"
    break;

  case 17: /* var_name: IDENT  */
#line 152 "repl_gram.y"
                        { (yyval.str) = (yyvsp[0].str); }
#line 1277 "repl_gram.c"
    break;

  case 18: /* var_name: var_name '.' IDENT  */
#line 154 "repl_gram.y"
                                { (yyval.str) = psprintf("%s.%s", (yyvsp[-2].str), (yyvsp[0].str)); }
#line 1283 "repl_gram.c"
    break;

  case 19: /* base_backup: K_BASE_BACKUP '(' generic_option_list ')'  */
#line 162 "repl_gram.y"
                                {
					BaseBackupCmd *cmd = makeNode(BaseBackupCmd);
					cmd->options = (yyvsp[-1].list);
					(yyval.node) = (Node *) cmd;
				}
#line 1293 "repl_gram.c"
    break;

  case 20: /* base_backup: K_BASE_BACKUP  */
#line 168 "repl_gram.y"
                                {
					BaseBackupCmd *cmd = makeNode(BaseBackupCmd);
					(yyval.node) = (Node *) cmd;
				}
#line 1302 "repl_gram.c"
    break;

  case 21: /* create_replication_slot: K_CREATE_REPLICATION_SLOT IDENT opt_temporary K_PHYSICAL create_slot_options  */
#line 177 "repl_gram.y"
                                {
					CreateReplicationSlotCmd *cmd;
					cmd = makeNode(CreateReplicationSlotCmd);
					cmd->kind = REPLICATION_KIND_PHYSICAL;
					cmd->slotname = (yyvsp[-3].str);
					cmd->temporary = (yyvsp[-2].boolval);
					cmd->options = (yyvsp[0].list);
					(yyval.node) = (Node *) cmd;
				}
#line 1316 "repl_gram.c"
    break;

  case 22: /* create_replication_slot: K_CREATE_REPLICATION_SLOT IDENT opt_temporary K_LOGICAL IDENT create_slot_options  */
#line 188 "repl_gram.y"
                                {
					CreateReplicationSlotCmd *cmd;
					cmd = makeNode(CreateReplicationSlotCmd);
					cmd->kind = REPLICATION_KIND_LOGICAL;
					cmd->slotname = (yyvsp[-4].str);
					cmd->temporary = (yyvsp[-3].boolval);
					cmd->plugin = (yyvsp[-1].str);
					cmd->options = (yyvsp[0].list);
					(yyval.node) = (Node *) cmd;
				}
#line 1331 "repl_gram.c"
    break;

  case 23: /* create_slot_options: '(' generic_option_list ')'  */
#line 201 "repl_gram.y"
                                                                        { (yyval.list) = (yyvsp[-1].list); }
#line 1337 "repl_gram.c"
    break;

  case 24: /* create_slot_options: create_slot_legacy_opt_list  */
#line 202 "repl_gram.y"
                                                                { (yyval.list) = (yyvsp[0].list); }
#line 1343 "repl_gram.c"
    break;

  case 25: /* create_slot_legacy_opt_list: create_slot_legacy_opt_list create_slot_legacy_opt  */
#line 207 "repl_gram.y"
                                { (yyval.list) = lappend((yyvsp[-1].list), (yyvsp[0].defelt)); }
#line 1349 "repl_gram.c"
    break;

  case 26: /* create_slot_legacy_opt_list: %empty  */
#line 209 "repl_gram.y"
                                { (yyval.list) = NIL; }
#line 1355 "repl_gram.c"
    break;

  case 27: /* create_slot_legacy_opt: K_EXPORT_SNAPSHOT  */
#line 214 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("snapshot",
								   (Node *) makeString("export"), -1);
				}
#line 1364 "repl_gram.c"
    break;

  case 28: /* create_slot_legacy_opt: K_NOEXPORT_SNAPSHOT  */
#line 219 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("snapshot",
								   (Node *) makeString("nothing"), -1);
				}
#line 1373 "repl_gram.c"
    break;

  case 29: /* create_slot_legacy_opt: K_USE_SNAPSHOT  */
#line 224 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("snapshot",
								   (Node *) makeString("use"), -1);
				}
#line 1382 "repl_gram.c"
    break;

  case 30: /* create_slot_legacy_opt: K_RESERVE_WAL  */
#line 229 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("reserve_wal",
								   (Node *) makeBoolean(true), -1);
				}
#line 1391 "repl_gram.c"
    break;

  case 31: /* create_slot_legacy_opt: K_TWO_PHASE  */
#line 234 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("two_phase",
								   (Node *) makeBoolean(true), -1);
				}
#line 1400 "repl_gram.c"
    break;

  case 32: /* drop_replication_slot: K_DROP_REPLICATION_SLOT IDENT  */
#line 243 "repl_gram.y"
                                {
					DropReplicationSlotCmd *cmd;
					cmd = makeNode(DropReplicationSlotCmd);
					cmd->slotname = (yyvsp[0].str);
					cmd->wait = false;
					(yyval.node) = (Node *) cmd;
				}
#line 1412 "repl_gram.c"
    break;

  case 33: /* drop_replication_slot: K_DROP_REPLICATION_SLOT IDENT K_WAIT  */
#line 251 "repl_gram.y"
                                {
					DropReplicationSlotCmd *cmd;
					cmd = makeNode(DropReplicationSlotCmd);
					cmd->slotname = (yyvsp[-1].str);
					cmd->wait = true;
					(yyval.node) = (Node *) cmd;
				}
#line 1424 "repl_gram.c"
    break;

  case 34: /* start_replication: K_START_REPLICATION opt_slot opt_physical RECPTR opt_timeline  */
#line 265 "repl_gram.y"
                                {
					StartReplicationCmd *cmd;

					cmd = makeNode(StartReplicationCmd);
					cmd->kind = REPLICATION_KIND_PHYSICAL;
					cmd->slotname = (yyvsp[-3].str);
					cmd->startpoint = (yyvsp[-1].recptr);
					cmd->timeline = (yyvsp[0].uintval);
					(yyval.node) = (Node *) cmd;
				}
#line 1439 "repl_gram.c"
    break;

  case 35: /* start_logical_replication: K_START_REPLICATION K_SLOT IDENT K_LOGICAL RECPTR plugin_options  */
#line 280 "repl_gram.y"
                                {
					StartReplicationCmd *cmd;
					cmd = makeNode(StartReplicationCmd);
					cmd->kind = REPLICATION_KIND_LOGICAL;
					cmd->slotname = (yyvsp[-3].str);
					cmd->startpoint = (yyvsp[-1].recptr);
					cmd->options = (yyvsp[0].list);
					(yyval.node) = (Node *) cmd;
				}
#line 1453 "repl_gram.c"
    break;

  case 36: /* timeline_history: K_TIMELINE_HISTORY UCONST  */
#line 295 "repl_gram.y"
                                {
					TimeLineHistoryCmd *cmd;

					if ((yyvsp[0].uintval) <= 0)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("invalid timeline %u", (yyvsp[0].uintval))));

					cmd = makeNode(TimeLineHistoryCmd);
					cmd->timeline = (yyvsp[0].uintval);

					(yyval.node) = (Node *) cmd;
				}
#line 1471 "repl_gram.c"
    break;

  case 39: /* opt_temporary: K_TEMPORARY  */
#line 316 "repl_gram.y"
                                                                                { (yyval.boolval) = true; }
#line 1477 "repl_gram.c"
    break;

  case 40: /* opt_temporary: %empty  */
#line 317 "repl_gram.y"
                                                                        { (yyval.boolval) = false; }
#line 1483 "repl_gram.c"
    break;

  case 41: /* opt_slot: K_SLOT IDENT  */
#line 322 "repl_gram.y"
                                { (yyval.str) = (yyvsp[0].str); }
#line 1489 "repl_gram.c"
    break;

  case 42: /* opt_slot: %empty  */
#line 324 "repl_gram.y"
                                { (yyval.str) = NULL; }
#line 1495 "repl_gram.c"
    break;

  case 43: /* opt_timeline: K_TIMELINE UCONST  */
#line 329 "repl_gram.y"
                                {
					if ((yyvsp[0].uintval) <= 0)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("invalid timeline %u", (yyvsp[0].uintval))));
					(yyval.uintval) = (yyvsp[0].uintval);
				}
#line 1507 "repl_gram.c"
    break;

  case 44: /* opt_timeline: %empty  */
#line 336 "repl_gram.y"
                                                                { (yyval.uintval) = 0; }
#line 1513 "repl_gram.c"
    break;

  case 45: /* plugin_options: '(' plugin_opt_list ')'  */
#line 341 "repl_gram.y"
                                                                { (yyval.list) = (yyvsp[-1].list); }
#line 1519 "repl_gram.c"
    break;

  case 46: /* plugin_options: %empty  */
#line 342 "repl_gram.y"
                                                                        { (yyval.list) = NIL; }
#line 1525 "repl_gram.c"
    break;

  case 47: /* plugin_opt_list: plugin_opt_elem  */
#line 347 "repl_gram.y"
                                {
					(yyval.list) = list_make1((yyvsp[0].defelt));
				}
#line 1533 "repl_gram.c"
    break;

  case 48: /* plugin_opt_list: plugin_opt_list ',' plugin_opt_elem  */
#line 351 "repl_gram.y"
                                {
					(yyval.list) = lappend((yyvsp[-2].list), (yyvsp[0].defelt));
				}
#line 1541 "repl_gram.c"
    break;

  case 49: /* plugin_opt_elem: IDENT plugin_opt_arg  */
#line 358 "repl_gram.y"
                                {
					(yyval.defelt) = makeDefElem((yyvsp[-1].str), (yyvsp[0].node), -1);
				}
#line 1549 "repl_gram.c"
    break;

  case 50: /* plugin_opt_arg: SCONST  */
#line 364 "repl_gram.y"
                                                                                { (yyval.node) = (Node *) makeString((yyvsp[0].str)); }
#line 1555 "repl_gram.c"
    break;

  case 51: /* plugin_opt_arg: %empty  */
#line 365 "repl_gram.y"
                                                                        { (yyval.node) = NULL; }
#line 1561 "repl_gram.c"
    break;

  case 52: /* generic_option_list: generic_option_list ',' generic_option  */
#line 370 "repl_gram.y"
                                { (yyval.list) = lappend((yyvsp[-2].list), (yyvsp[0].defelt)); }
#line 1567 "repl_gram.c"
    break;

  case 53: /* generic_option_list: generic_option  */
#line 372 "repl_gram.y"
                                { (yyval.list) = list_make1((yyvsp[0].defelt)); }
#line 1573 "repl_gram.c"
    break;

  case 54: /* generic_option: ident_or_keyword  */
#line 377 "repl_gram.y"
                                {
					(yyval.defelt) = makeDefElem((yyvsp[0].str), NULL, -1);
				}
#line 1581 "repl_gram.c"
    break;

  case 55: /* generic_option: ident_or_keyword IDENT  */
#line 381 "repl_gram.y"
                                {
					(yyval.defelt) = makeDefElem((yyvsp[-1].str), (Node *) makeString((yyvsp[0].str)), -1);
				}
#line 1589 "repl_gram.c"
    break;

  case 56: /* generic_option: ident_or_keyword SCONST  */
#line 385 "repl_gram.y"
                                {
					(yyval.defelt) = makeDefElem((yyvsp[-1].str), (Node *) makeString((yyvsp[0].str)), -1);
				}
#line 1597 "repl_gram.c"
    break;

  case 57: /* generic_option: ident_or_keyword UCONST  */
#line 389 "repl_gram.y"
                                {
					(yyval.defelt) = makeDefElem((yyvsp[-1].str), (Node *) makeInteger((yyvsp[0].uintval)), -1);
				}
#line 1605 "repl_gram.c"
    break;

  case 58: /* ident_or_keyword: IDENT  */
#line 395 "repl_gram.y"
                                                                                { (yyval.str) = (yyvsp[0].str); }
#line 1611 "repl_gram.c"
    break;

  case 59: /* ident_or_keyword: K_BASE_BACKUP  */
#line 396 "repl_gram.y"
                                                                        { (yyval.str) = "base_backup"; }
#line 1617 "repl_gram.c"
    break;

  case 60: /* ident_or_keyword: K_IDENTIFY_SYSTEM  */
#line 397 "repl_gram.y"
                                                                        { (yyval.str) = "identify_system"; }
#line 1623 "repl_gram.c"
    break;

  case 61: /* ident_or_keyword: K_SHOW  */
#line 398 "repl_gram.y"
                                                                                { (yyval.str) = "show"; }
#line 1629 "repl_gram.c"
    break;

  case 62: /* ident_or_keyword: K_START_REPLICATION  */
#line 399 "repl_gram.y"
                                                                { (yyval.str) = "start_replication"; }
#line 1635 "repl_gram.c"
    break;

  case 63: /* ident_or_keyword: K_CREATE_REPLICATION_SLOT  */
#line 400 "repl_gram.y"
                                                        { (yyval.str) = "create_replication_slot"; }
#line 1641 "repl_gram.c"
    break;

  case 64: /* ident_or_keyword: K_DROP_REPLICATION_SLOT  */
#line 401 "repl_gram.y"
                                                                { (yyval.str) = "drop_replication_slot"; }
#line 1647 "repl_gram.c"
    break;

  case 65: /* ident_or_keyword: K_TIMELINE_HISTORY  */
#line 402 "repl_gram.y"
                                                                { (yyval.str) = "timeline_history"; }
#line 1653 "repl_gram.c"
    break;

  case 66: /* ident_or_keyword: K_WAIT  */
#line 403 "repl_gram.y"
                                                                                { (yyval.str) = "wait"; }
#line 1659 "repl_gram.c"
    break;

  case 67: /* ident_or_keyword: K_TIMELINE  */
#line 404 "repl_gram.y"
                                                                        { (yyval.str) = "timeline"; }
#line 1665 "repl_gram.c"
    break;

  case 68: /* ident_or_keyword: K_PHYSICAL  */
#line 405 "repl_gram.y"
                                                                        { (yyval.str) = "physical"; }
#line 1671 "repl_gram.c"
    break;

  case 69: /* ident_or_keyword: K_LOGICAL  */
#line 406 "repl_gram.y"
                                                                                { (yyval.str) = "logical"; }
#line 1677 "repl_gram.c"
    break;

  case 70: /* ident_or_keyword: K_SLOT  */
#line 407 "repl_gram.y"
                                                                                { (yyval.str) = "slot"; }
#line 1683 "repl_gram.c"
    break;

  case 71: /* ident_or_keyword: K_RESERVE_WAL  */
#line 408 "repl_gram.y"
                                                                        { (yyval.str) = "reserve_wal"; }
#line 1689 "repl_gram.c"
    break;

  case 72: /* ident_or_keyword: K_TEMPORARY  */
#line 409 "repl_gram.y"
                                                                        { (yyval.str) = "temporary"; }
#line 1695 "repl_gram.c"
    break;

  case 73: /* ident_or_keyword: K_TWO_PHASE  */
#line 410 "repl_gram.y"
                                                                        { (yyval.str) = "two_phase"; }
#line 1701 "repl_gram.c"
    break;

  case 74: /* ident_or_keyword: K_EXPORT_SNAPSHOT  */
#line 411 "repl_gram.y"
                                                                        { (yyval.str) = "export_snapshot"; }
#line 1707 "repl_gram.c"
    break;

  case 75: /* ident_or_keyword: K_NOEXPORT_SNAPSHOT  */
#line 412 "repl_gram.y"
                                                                { (yyval.str) = "noexport_snapshot"; }
#line 1713 "repl_gram.c"
    break;

  case 76: /* ident_or_keyword: K_USE_SNAPSHOT  */
#line 413 "repl_gram.y"
                                                                        { (yyval.str) = "use_snapshot"; }
#line 1719 "repl_gram.c"
    break;


#line 1723 "repl_gram.c"

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

#line 416 "repl_gram.y"

