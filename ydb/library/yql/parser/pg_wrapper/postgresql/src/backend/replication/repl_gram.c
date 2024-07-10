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
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
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
#include "nodes/replnodes.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"


/* Result of the parsing is returned here */
__thread Node *replication_parse_result;


/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree


#line 119 "repl_gram.c"

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


/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int replication_yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    SCONST = 258,                  /* SCONST  */
    IDENT = 259,                   /* IDENT  */
    UCONST = 260,                  /* UCONST  */
    RECPTR = 261,                  /* RECPTR  */
    K_BASE_BACKUP = 262,           /* K_BASE_BACKUP  */
    K_IDENTIFY_SYSTEM = 263,       /* K_IDENTIFY_SYSTEM  */
    K_SHOW = 264,                  /* K_SHOW  */
    K_START_REPLICATION = 265,     /* K_START_REPLICATION  */
    K_CREATE_REPLICATION_SLOT = 266, /* K_CREATE_REPLICATION_SLOT  */
    K_DROP_REPLICATION_SLOT = 267, /* K_DROP_REPLICATION_SLOT  */
    K_TIMELINE_HISTORY = 268,      /* K_TIMELINE_HISTORY  */
    K_LABEL = 269,                 /* K_LABEL  */
    K_PROGRESS = 270,              /* K_PROGRESS  */
    K_FAST = 271,                  /* K_FAST  */
    K_WAIT = 272,                  /* K_WAIT  */
    K_NOWAIT = 273,                /* K_NOWAIT  */
    K_MAX_RATE = 274,              /* K_MAX_RATE  */
    K_WAL = 275,                   /* K_WAL  */
    K_TABLESPACE_MAP = 276,        /* K_TABLESPACE_MAP  */
    K_NOVERIFY_CHECKSUMS = 277,    /* K_NOVERIFY_CHECKSUMS  */
    K_TIMELINE = 278,              /* K_TIMELINE  */
    K_PHYSICAL = 279,              /* K_PHYSICAL  */
    K_LOGICAL = 280,               /* K_LOGICAL  */
    K_SLOT = 281,                  /* K_SLOT  */
    K_RESERVE_WAL = 282,           /* K_RESERVE_WAL  */
    K_TEMPORARY = 283,             /* K_TEMPORARY  */
    K_EXPORT_SNAPSHOT = 284,       /* K_EXPORT_SNAPSHOT  */
    K_NOEXPORT_SNAPSHOT = 285,     /* K_NOEXPORT_SNAPSHOT  */
    K_USE_SNAPSHOT = 286,          /* K_USE_SNAPSHOT  */
    K_MANIFEST = 287,              /* K_MANIFEST  */
    K_MANIFEST_CHECKSUMS = 288     /* K_MANIFEST_CHECKSUMS  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 45 "repl_gram.y"

		char					*str;
		bool					boolval;
		uint32					uintval;

		XLogRecPtr				recptr;
		Node					*node;
		List					*list;
		DefElem					*defelt;

#line 210 "repl_gram.c"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern __thread YYSTYPE replication_yylval;

int replication_yyparse (void);


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
  YYSYMBOL_K_SHOW = 9,                     /* K_SHOW  */
  YYSYMBOL_K_START_REPLICATION = 10,       /* K_START_REPLICATION  */
  YYSYMBOL_K_CREATE_REPLICATION_SLOT = 11, /* K_CREATE_REPLICATION_SLOT  */
  YYSYMBOL_K_DROP_REPLICATION_SLOT = 12,   /* K_DROP_REPLICATION_SLOT  */
  YYSYMBOL_K_TIMELINE_HISTORY = 13,        /* K_TIMELINE_HISTORY  */
  YYSYMBOL_K_LABEL = 14,                   /* K_LABEL  */
  YYSYMBOL_K_PROGRESS = 15,                /* K_PROGRESS  */
  YYSYMBOL_K_FAST = 16,                    /* K_FAST  */
  YYSYMBOL_K_WAIT = 17,                    /* K_WAIT  */
  YYSYMBOL_K_NOWAIT = 18,                  /* K_NOWAIT  */
  YYSYMBOL_K_MAX_RATE = 19,                /* K_MAX_RATE  */
  YYSYMBOL_K_WAL = 20,                     /* K_WAL  */
  YYSYMBOL_K_TABLESPACE_MAP = 21,          /* K_TABLESPACE_MAP  */
  YYSYMBOL_K_NOVERIFY_CHECKSUMS = 22,      /* K_NOVERIFY_CHECKSUMS  */
  YYSYMBOL_K_TIMELINE = 23,                /* K_TIMELINE  */
  YYSYMBOL_K_PHYSICAL = 24,                /* K_PHYSICAL  */
  YYSYMBOL_K_LOGICAL = 25,                 /* K_LOGICAL  */
  YYSYMBOL_K_SLOT = 26,                    /* K_SLOT  */
  YYSYMBOL_K_RESERVE_WAL = 27,             /* K_RESERVE_WAL  */
  YYSYMBOL_K_TEMPORARY = 28,               /* K_TEMPORARY  */
  YYSYMBOL_K_EXPORT_SNAPSHOT = 29,         /* K_EXPORT_SNAPSHOT  */
  YYSYMBOL_K_NOEXPORT_SNAPSHOT = 30,       /* K_NOEXPORT_SNAPSHOT  */
  YYSYMBOL_K_USE_SNAPSHOT = 31,            /* K_USE_SNAPSHOT  */
  YYSYMBOL_K_MANIFEST = 32,                /* K_MANIFEST  */
  YYSYMBOL_K_MANIFEST_CHECKSUMS = 33,      /* K_MANIFEST_CHECKSUMS  */
  YYSYMBOL_34_ = 34,                       /* ';'  */
  YYSYMBOL_35_ = 35,                       /* '.'  */
  YYSYMBOL_36_ = 36,                       /* '('  */
  YYSYMBOL_37_ = 37,                       /* ')'  */
  YYSYMBOL_38_ = 38,                       /* ','  */
  YYSYMBOL_YYACCEPT = 39,                  /* $accept  */
  YYSYMBOL_firstcmd = 40,                  /* firstcmd  */
  YYSYMBOL_opt_semicolon = 41,             /* opt_semicolon  */
  YYSYMBOL_command = 42,                   /* command  */
  YYSYMBOL_identify_system = 43,           /* identify_system  */
  YYSYMBOL_show = 44,                      /* show  */
  YYSYMBOL_var_name = 45,                  /* var_name  */
  YYSYMBOL_base_backup = 46,               /* base_backup  */
  YYSYMBOL_base_backup_opt_list = 47,      /* base_backup_opt_list  */
  YYSYMBOL_base_backup_opt = 48,           /* base_backup_opt  */
  YYSYMBOL_create_replication_slot = 49,   /* create_replication_slot  */
  YYSYMBOL_create_slot_opt_list = 50,      /* create_slot_opt_list  */
  YYSYMBOL_create_slot_opt = 51,           /* create_slot_opt  */
  YYSYMBOL_drop_replication_slot = 52,     /* drop_replication_slot  */
  YYSYMBOL_start_replication = 53,         /* start_replication  */
  YYSYMBOL_start_logical_replication = 54, /* start_logical_replication  */
  YYSYMBOL_timeline_history = 55,          /* timeline_history  */
  YYSYMBOL_opt_physical = 56,              /* opt_physical  */
  YYSYMBOL_opt_temporary = 57,             /* opt_temporary  */
  YYSYMBOL_opt_slot = 58,                  /* opt_slot  */
  YYSYMBOL_opt_timeline = 59,              /* opt_timeline  */
  YYSYMBOL_plugin_options = 60,            /* plugin_options  */
  YYSYMBOL_plugin_opt_list = 61,           /* plugin_opt_list  */
  YYSYMBOL_plugin_opt_elem = 62,           /* plugin_opt_elem  */
  YYSYMBOL_plugin_opt_arg = 63             /* plugin_opt_arg  */
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
#define YYFINAL  26
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   52

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  39
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  25
/* YYNRULES -- Number of rules.  */
#define YYNRULES  57
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  78

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   288


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
      36,    37,     2,     2,    38,     2,    35,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,    34,
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
      25,    26,    27,    28,    29,    30,    31,    32,    33
};

#if YYDEBUG
  /* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   107,   107,   113,   114,   118,   119,   120,   121,   122,
     123,   124,   125,   132,   142,   149,   150,   160,   169,   172,
     176,   181,   186,   191,   196,   201,   206,   211,   216,   221,
     230,   241,   255,   258,   262,   267,   272,   277,   286,   294,
     308,   323,   338,   355,   356,   360,   361,   365,   368,   372,
     380,   385,   386,   390,   394,   401,   408,   409
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
  "UCONST", "RECPTR", "K_BASE_BACKUP", "K_IDENTIFY_SYSTEM", "K_SHOW",
  "K_START_REPLICATION", "K_CREATE_REPLICATION_SLOT",
  "K_DROP_REPLICATION_SLOT", "K_TIMELINE_HISTORY", "K_LABEL", "K_PROGRESS",
  "K_FAST", "K_WAIT", "K_NOWAIT", "K_MAX_RATE", "K_WAL",
  "K_TABLESPACE_MAP", "K_NOVERIFY_CHECKSUMS", "K_TIMELINE", "K_PHYSICAL",
  "K_LOGICAL", "K_SLOT", "K_RESERVE_WAL", "K_TEMPORARY",
  "K_EXPORT_SNAPSHOT", "K_NOEXPORT_SNAPSHOT", "K_USE_SNAPSHOT",
  "K_MANIFEST", "K_MANIFEST_CHECKSUMS", "';'", "'.'", "'('", "')'", "','",
  "$accept", "firstcmd", "opt_semicolon", "command", "identify_system",
  "show", "var_name", "base_backup", "base_backup_opt_list",
  "base_backup_opt", "create_replication_slot", "create_slot_opt_list",
  "create_slot_opt", "drop_replication_slot", "start_replication",
  "start_logical_replication", "timeline_history", "opt_physical",
  "opt_temporary", "opt_slot", "opt_timeline", "plugin_options",
  "plugin_opt_list", "plugin_opt_elem", "plugin_opt_arg", YY_NULLPTR
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
     285,   286,   287,   288,    59,    46,    40,    41,    44
};
#endif

#define YYPACT_NINF (-25)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

  /* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
     STATE-NUM.  */
static const yytype_int8 yypact[] =
{
       2,   -25,   -25,    -1,    -5,    23,    24,    25,    29,    -3,
     -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -14,   -25,
      -2,    28,    10,     7,    19,   -25,   -25,   -25,   -25,    34,
     -25,   -25,   -25,    33,   -25,   -25,   -25,    36,    37,   -25,
      38,    16,   -25,    39,   -25,    -8,   -25,   -25,   -25,   -25,
     -25,   -25,    40,    20,   -25,    43,     8,    44,   -25,    -7,
     -25,    46,   -25,   -25,   -25,   -25,   -25,   -25,   -25,    -7,
      45,   -12,   -25,   -25,   -25,   -25,    46,   -25
};

  /* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
     Performed when YYTABLE does not specify something else to do.  Zero
     means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       0,    19,    13,     0,    48,     0,     0,     0,     0,     4,
       5,    12,     6,     9,    10,     7,     8,    11,    17,    15,
      14,     0,    44,    46,    38,    42,     1,     3,     2,     0,
      21,    22,    24,     0,    23,    26,    27,     0,     0,    18,
       0,    47,    43,     0,    45,     0,    39,    20,    25,    28,
      29,    16,     0,    50,    33,     0,    52,     0,    40,    30,
      33,     0,    41,    49,    37,    34,    35,    36,    32,    31,
      57,     0,    53,    56,    55,    51,     0,    54
};

  /* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,    -9,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,   -25,   -25,   -24,   -25
};

  /* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
       0,     8,    28,     9,    10,    11,    20,    12,    18,    39,
      13,    59,    68,    14,    15,    16,    17,    43,    45,    22,
      58,    62,    71,    72,    74
};

  /* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
     positive, shift that token.  If negative, reduce the rule whose
     number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int8 yytable[] =
{
      29,    30,    31,    19,    32,    33,    34,    35,    36,     1,
       2,     3,     4,     5,     6,     7,    54,    55,    37,    38,
      64,    21,    65,    66,    67,    75,    76,    23,    24,    26,
      25,    27,    41,    40,    42,    44,    46,    47,    48,    49,
      50,    52,    51,    57,    61,    53,    56,    60,    73,    63,
      70,    69,    77
};

static const yytype_int8 yycheck[] =
{
      14,    15,    16,     4,    18,    19,    20,    21,    22,     7,
       8,     9,    10,    11,    12,    13,    24,    25,    32,    33,
      27,    26,    29,    30,    31,    37,    38,     4,     4,     0,
       5,    34,     4,    35,    24,    28,    17,     3,     5,     3,
       3,    25,     4,    23,    36,     6,     6,     4,     3,     5,
       4,    60,    76
};

  /* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
     symbol of state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     7,     8,     9,    10,    11,    12,    13,    40,    42,
      43,    44,    46,    49,    52,    53,    54,    55,    47,     4,
      45,    26,    58,     4,     4,     5,     0,    34,    41,    14,
      15,    16,    18,    19,    20,    21,    22,    32,    33,    48,
      35,     4,    24,    56,    28,    57,    17,     3,     5,     3,
       3,     4,    25,     6,    24,    25,     6,    23,    59,    50,
       4,    36,    60,     5,    27,    29,    30,    31,    51,    50,
       4,    61,    62,     3,    63,    37,    38,    62
};

  /* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_int8 yyr1[] =
{
       0,    39,    40,    41,    41,    42,    42,    42,    42,    42,
      42,    42,    42,    43,    44,    45,    45,    46,    47,    47,
      48,    48,    48,    48,    48,    48,    48,    48,    48,    48,
      49,    49,    50,    50,    51,    51,    51,    51,    52,    52,
      53,    54,    55,    56,    56,    57,    57,    58,    58,    59,
      59,    60,    60,    61,    61,    62,    63,    63
};

  /* YYR2[YYN] -- Number of symbols on the right hand side of rule YYN.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     1,     0,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     2,     1,     3,     2,     2,     0,
       2,     1,     1,     1,     1,     2,     1,     1,     2,     2,
       5,     6,     2,     0,     1,     1,     1,     1,     2,     3,
       5,     6,     2,     1,     0,     1,     0,     2,     0,     2,
       0,     3,     0,     1,     3,     2,     1,     0
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
#line 108 "repl_gram.y"
                                {
					replication_parse_result = (yyvsp[-1].node);
				}
#line 1315 "repl_gram.c"
    break;

  case 13: /* identify_system: K_IDENTIFY_SYSTEM  */
#line 133 "repl_gram.y"
                                {
					(yyval.node) = (Node *) makeNode(IdentifySystemCmd);
				}
#line 1323 "repl_gram.c"
    break;

  case 14: /* show: K_SHOW var_name  */
#line 143 "repl_gram.y"
                                {
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = (yyvsp[0].str);
					(yyval.node) = (Node *) n;
				}
#line 1333 "repl_gram.c"
    break;

  case 15: /* var_name: IDENT  */
#line 149 "repl_gram.y"
                        { (yyval.str) = (yyvsp[0].str); }
#line 1339 "repl_gram.c"
    break;

  case 16: /* var_name: var_name '.' IDENT  */
#line 151 "repl_gram.y"
                                { (yyval.str) = psprintf("%s.%s", (yyvsp[-2].str), (yyvsp[0].str)); }
#line 1345 "repl_gram.c"
    break;

  case 17: /* base_backup: K_BASE_BACKUP base_backup_opt_list  */
#line 161 "repl_gram.y"
                                {
					BaseBackupCmd *cmd = makeNode(BaseBackupCmd);
					cmd->options = (yyvsp[0].list);
					(yyval.node) = (Node *) cmd;
				}
#line 1355 "repl_gram.c"
    break;

  case 18: /* base_backup_opt_list: base_backup_opt_list base_backup_opt  */
#line 170 "repl_gram.y"
                                { (yyval.list) = lappend((yyvsp[-1].list), (yyvsp[0].defelt)); }
#line 1361 "repl_gram.c"
    break;

  case 19: /* base_backup_opt_list: %empty  */
#line 172 "repl_gram.y"
                                { (yyval.list) = NIL; }
#line 1367 "repl_gram.c"
    break;

  case 20: /* base_backup_opt: K_LABEL SCONST  */
#line 177 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("label",
								   (Node *)makeString((yyvsp[0].str)), -1);
				}
#line 1376 "repl_gram.c"
    break;

  case 21: /* base_backup_opt: K_PROGRESS  */
#line 182 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("progress",
								   (Node *)makeInteger(true), -1);
				}
#line 1385 "repl_gram.c"
    break;

  case 22: /* base_backup_opt: K_FAST  */
#line 187 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("fast",
								   (Node *)makeInteger(true), -1);
				}
#line 1394 "repl_gram.c"
    break;

  case 23: /* base_backup_opt: K_WAL  */
#line 192 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("wal",
								   (Node *)makeInteger(true), -1);
				}
#line 1403 "repl_gram.c"
    break;

  case 24: /* base_backup_opt: K_NOWAIT  */
#line 197 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("nowait",
								   (Node *)makeInteger(true), -1);
				}
#line 1412 "repl_gram.c"
    break;

  case 25: /* base_backup_opt: K_MAX_RATE UCONST  */
#line 202 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("max_rate",
								   (Node *)makeInteger((yyvsp[0].uintval)), -1);
				}
#line 1421 "repl_gram.c"
    break;

  case 26: /* base_backup_opt: K_TABLESPACE_MAP  */
#line 207 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("tablespace_map",
								   (Node *)makeInteger(true), -1);
				}
#line 1430 "repl_gram.c"
    break;

  case 27: /* base_backup_opt: K_NOVERIFY_CHECKSUMS  */
#line 212 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("noverify_checksums",
								   (Node *)makeInteger(true), -1);
				}
#line 1439 "repl_gram.c"
    break;

  case 28: /* base_backup_opt: K_MANIFEST SCONST  */
#line 217 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("manifest",
								   (Node *)makeString((yyvsp[0].str)), -1);
				}
#line 1448 "repl_gram.c"
    break;

  case 29: /* base_backup_opt: K_MANIFEST_CHECKSUMS SCONST  */
#line 222 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("manifest_checksums",
								   (Node *)makeString((yyvsp[0].str)), -1);
				}
#line 1457 "repl_gram.c"
    break;

  case 30: /* create_replication_slot: K_CREATE_REPLICATION_SLOT IDENT opt_temporary K_PHYSICAL create_slot_opt_list  */
#line 231 "repl_gram.y"
                                {
					CreateReplicationSlotCmd *cmd;
					cmd = makeNode(CreateReplicationSlotCmd);
					cmd->kind = REPLICATION_KIND_PHYSICAL;
					cmd->slotname = (yyvsp[-3].str);
					cmd->temporary = (yyvsp[-2].boolval);
					cmd->options = (yyvsp[0].list);
					(yyval.node) = (Node *) cmd;
				}
#line 1471 "repl_gram.c"
    break;

  case 31: /* create_replication_slot: K_CREATE_REPLICATION_SLOT IDENT opt_temporary K_LOGICAL IDENT create_slot_opt_list  */
#line 242 "repl_gram.y"
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
#line 1486 "repl_gram.c"
    break;

  case 32: /* create_slot_opt_list: create_slot_opt_list create_slot_opt  */
#line 256 "repl_gram.y"
                                { (yyval.list) = lappend((yyvsp[-1].list), (yyvsp[0].defelt)); }
#line 1492 "repl_gram.c"
    break;

  case 33: /* create_slot_opt_list: %empty  */
#line 258 "repl_gram.y"
                                { (yyval.list) = NIL; }
#line 1498 "repl_gram.c"
    break;

  case 34: /* create_slot_opt: K_EXPORT_SNAPSHOT  */
#line 263 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("export_snapshot",
								   (Node *)makeInteger(true), -1);
				}
#line 1507 "repl_gram.c"
    break;

  case 35: /* create_slot_opt: K_NOEXPORT_SNAPSHOT  */
#line 268 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("export_snapshot",
								   (Node *)makeInteger(false), -1);
				}
#line 1516 "repl_gram.c"
    break;

  case 36: /* create_slot_opt: K_USE_SNAPSHOT  */
#line 273 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("use_snapshot",
								   (Node *)makeInteger(true), -1);
				}
#line 1525 "repl_gram.c"
    break;

  case 37: /* create_slot_opt: K_RESERVE_WAL  */
#line 278 "repl_gram.y"
                                {
				  (yyval.defelt) = makeDefElem("reserve_wal",
								   (Node *)makeInteger(true), -1);
				}
#line 1534 "repl_gram.c"
    break;

  case 38: /* drop_replication_slot: K_DROP_REPLICATION_SLOT IDENT  */
#line 287 "repl_gram.y"
                                {
					DropReplicationSlotCmd *cmd;
					cmd = makeNode(DropReplicationSlotCmd);
					cmd->slotname = (yyvsp[0].str);
					cmd->wait = false;
					(yyval.node) = (Node *) cmd;
				}
#line 1546 "repl_gram.c"
    break;

  case 39: /* drop_replication_slot: K_DROP_REPLICATION_SLOT IDENT K_WAIT  */
#line 295 "repl_gram.y"
                                {
					DropReplicationSlotCmd *cmd;
					cmd = makeNode(DropReplicationSlotCmd);
					cmd->slotname = (yyvsp[-1].str);
					cmd->wait = true;
					(yyval.node) = (Node *) cmd;
				}
#line 1558 "repl_gram.c"
    break;

  case 40: /* start_replication: K_START_REPLICATION opt_slot opt_physical RECPTR opt_timeline  */
#line 309 "repl_gram.y"
                                {
					StartReplicationCmd *cmd;

					cmd = makeNode(StartReplicationCmd);
					cmd->kind = REPLICATION_KIND_PHYSICAL;
					cmd->slotname = (yyvsp[-3].str);
					cmd->startpoint = (yyvsp[-1].recptr);
					cmd->timeline = (yyvsp[0].uintval);
					(yyval.node) = (Node *) cmd;
				}
#line 1573 "repl_gram.c"
    break;

  case 41: /* start_logical_replication: K_START_REPLICATION K_SLOT IDENT K_LOGICAL RECPTR plugin_options  */
#line 324 "repl_gram.y"
                                {
					StartReplicationCmd *cmd;
					cmd = makeNode(StartReplicationCmd);
					cmd->kind = REPLICATION_KIND_LOGICAL;
					cmd->slotname = (yyvsp[-3].str);
					cmd->startpoint = (yyvsp[-1].recptr);
					cmd->options = (yyvsp[0].list);
					(yyval.node) = (Node *) cmd;
				}
#line 1587 "repl_gram.c"
    break;

  case 42: /* timeline_history: K_TIMELINE_HISTORY UCONST  */
#line 339 "repl_gram.y"
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
#line 1605 "repl_gram.c"
    break;

  case 45: /* opt_temporary: K_TEMPORARY  */
#line 360 "repl_gram.y"
                                                                                { (yyval.boolval) = true; }
#line 1611 "repl_gram.c"
    break;

  case 46: /* opt_temporary: %empty  */
#line 361 "repl_gram.y"
                                                                        { (yyval.boolval) = false; }
#line 1617 "repl_gram.c"
    break;

  case 47: /* opt_slot: K_SLOT IDENT  */
#line 366 "repl_gram.y"
                                { (yyval.str) = (yyvsp[0].str); }
#line 1623 "repl_gram.c"
    break;

  case 48: /* opt_slot: %empty  */
#line 368 "repl_gram.y"
                                { (yyval.str) = NULL; }
#line 1629 "repl_gram.c"
    break;

  case 49: /* opt_timeline: K_TIMELINE UCONST  */
#line 373 "repl_gram.y"
                                {
					if ((yyvsp[0].uintval) <= 0)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("invalid timeline %u", (yyvsp[0].uintval))));
					(yyval.uintval) = (yyvsp[0].uintval);
				}
#line 1641 "repl_gram.c"
    break;

  case 50: /* opt_timeline: %empty  */
#line 380 "repl_gram.y"
                                                                { (yyval.uintval) = 0; }
#line 1647 "repl_gram.c"
    break;

  case 51: /* plugin_options: '(' plugin_opt_list ')'  */
#line 385 "repl_gram.y"
                                                                { (yyval.list) = (yyvsp[-1].list); }
#line 1653 "repl_gram.c"
    break;

  case 52: /* plugin_options: %empty  */
#line 386 "repl_gram.y"
                                                                        { (yyval.list) = NIL; }
#line 1659 "repl_gram.c"
    break;

  case 53: /* plugin_opt_list: plugin_opt_elem  */
#line 391 "repl_gram.y"
                                {
					(yyval.list) = list_make1((yyvsp[0].defelt));
				}
#line 1667 "repl_gram.c"
    break;

  case 54: /* plugin_opt_list: plugin_opt_list ',' plugin_opt_elem  */
#line 395 "repl_gram.y"
                                {
					(yyval.list) = lappend((yyvsp[-2].list), (yyvsp[0].defelt));
				}
#line 1675 "repl_gram.c"
    break;

  case 55: /* plugin_opt_elem: IDENT plugin_opt_arg  */
#line 402 "repl_gram.y"
                                {
					(yyval.defelt) = makeDefElem((yyvsp[-1].str), (yyvsp[0].node), -1);
				}
#line 1683 "repl_gram.c"
    break;

  case 56: /* plugin_opt_arg: SCONST  */
#line 408 "repl_gram.y"
                                                                                { (yyval.node) = (Node *) makeString((yyvsp[0].str)); }
#line 1689 "repl_gram.c"
    break;

  case 57: /* plugin_opt_arg: %empty  */
#line 409 "repl_gram.y"
                                                                        { (yyval.node) = NULL; }
#line 1695 "repl_gram.c"
    break;


#line 1699 "repl_gram.c"

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

#line 412 "repl_gram.y"


#include "repl_scanner.c"
