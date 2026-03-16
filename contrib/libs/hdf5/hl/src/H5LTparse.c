#if defined (__GNUC__)                                            
#if ((__GNUC__ * 100) + __GNUC_MINOR__) >= 402                    
#pragma GCC diagnostic ignored "-Wconversion"                     
#pragma GCC diagnostic ignored "-Wimplicit-function-declaration"  
#pragma GCC diagnostic ignored "-Wmissing-prototypes"             
#pragma GCC diagnostic ignored "-Wnested-externs"                 
#pragma GCC diagnostic ignored "-Wold-style-definition"           
#pragma GCC diagnostic ignored "-Wredundant-decls"                
#pragma GCC diagnostic ignored "-Wsign-compare"                   
#pragma GCC diagnostic ignored "-Wsign-conversion"                
#pragma GCC diagnostic ignored "-Wstrict-overflow"                
#pragma GCC diagnostic ignored "-Wstrict-prototypes"              
#pragma GCC diagnostic ignored "-Wimplicit-fallthrough"           
#if !defined (__clang__)                                          
#pragma GCC diagnostic ignored "-Wlarger-than="                   
#pragma GCC diagnostic ignored "-Wsuggest-attribute=const"        
#pragma GCC diagnostic ignored "-Wsuggest-attribute=pure"         
#endif                                                            
#pragma GCC diagnostic ignored "-Wswitch-default"                 
#pragma GCC diagnostic ignored "-Wunused-function"                
#pragma GCC diagnostic ignored "-Wunused-macros"                  
#pragma GCC diagnostic ignored "-Wunused-parameter"               
#endif                                                            
#if ((__GNUC__ * 100) + __GNUC_MINOR__) >= 600                    
#pragma GCC diagnostic ignored "-Wnull-dereference"               
#endif                                                            
#elif defined __SUNPRO_CC                                         
#pragma disable_warn                                              
#elif defined _MSC_VER                                            
#pragma warning(push, 1)                                          
#endif                                                            
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
#define YYPURE 0

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1


/* Substitute the variable and function names.  */
#define yyparse         H5LTyyparse
#define yylex           H5LTyylex
#define yyerror         H5LTyyerror
#define yydebug         H5LTyydebug
#define yynerrs         H5LTyynerrs
#define yylval          H5LTyylval
#define yychar          H5LTyychar

/* First part of user prologue.  */
#line 19 "hl/src//H5LTparse.y"

#include <stdio.h>
#include <string.h>
#include <contrib/libs/hdf5/src/hdf5.h>

#include "H5private.h"

extern int yylex(void);
extern int yyerror(const char *);

#define STACK_SIZE      16

/*structure for compound type information*/
struct cmpd_info {
    hid_t       id;             /*type ID*/
    bool        is_field;       /*flag to lexer for compound member*/
    bool        first_memb;     /*flag for first compound member*/
};

/*stack for nested compound type*/
static struct cmpd_info cmpd_stack[STACK_SIZE] = {
    {0, 0, 1}, {0, 0, 1}, {0, 0, 1}, {0, 0, 1},
    {0, 0, 1}, {0, 0, 1}, {0, 0, 1}, {0, 0, 1},
    {0, 0, 1}, {0, 0, 1}, {0, 0, 1}, {0, 0, 1},
    {0, 0, 1}, {0, 0, 1}, {0, 0, 1}, {0, 0, 1} };

static int csindex = -1;                /*pointer to the top of compound stack*/

/*structure for array type information*/
struct arr_info {
    hsize_t             dims[H5S_MAX_RANK];     /*size of each dimension, limited to 32 dimensions*/
    unsigned            ndims;                  /*number of dimensions*/
    bool                is_dim;                 /*flag to lexer for dimension*/
};
/*stack for nested array type*/
static struct arr_info arr_stack[STACK_SIZE];
static int asindex = -1;               /*pointer to the top of array stack*/ 

static H5T_str_t   str_pad;                /*variable for string padding*/
static H5T_cset_t  str_cset;               /*variable for string character set*/
static bool        is_variable = 0;        /*variable for variable-length string*/
static size_t      str_size;               /*variable for string size*/
   
static hid_t       enum_id;                /*type ID*/
static bool        is_enum = 0;            /*flag to lexer for enum type*/
static bool        is_enum_memb = 0;       /*flag to lexer for enum member*/
static char*       enum_memb_symbol;       /*enum member symbol string*/


#line 128 "hl/src//H5LTparse.c"

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

#include "H5LTparse.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_H5T_STD_I8BE_TOKEN = 3,         /* H5T_STD_I8BE_TOKEN  */
  YYSYMBOL_H5T_STD_I8LE_TOKEN = 4,         /* H5T_STD_I8LE_TOKEN  */
  YYSYMBOL_H5T_STD_I16BE_TOKEN = 5,        /* H5T_STD_I16BE_TOKEN  */
  YYSYMBOL_H5T_STD_I16LE_TOKEN = 6,        /* H5T_STD_I16LE_TOKEN  */
  YYSYMBOL_H5T_STD_I32BE_TOKEN = 7,        /* H5T_STD_I32BE_TOKEN  */
  YYSYMBOL_H5T_STD_I32LE_TOKEN = 8,        /* H5T_STD_I32LE_TOKEN  */
  YYSYMBOL_H5T_STD_I64BE_TOKEN = 9,        /* H5T_STD_I64BE_TOKEN  */
  YYSYMBOL_H5T_STD_I64LE_TOKEN = 10,       /* H5T_STD_I64LE_TOKEN  */
  YYSYMBOL_H5T_STD_U8BE_TOKEN = 11,        /* H5T_STD_U8BE_TOKEN  */
  YYSYMBOL_H5T_STD_U8LE_TOKEN = 12,        /* H5T_STD_U8LE_TOKEN  */
  YYSYMBOL_H5T_STD_U16BE_TOKEN = 13,       /* H5T_STD_U16BE_TOKEN  */
  YYSYMBOL_H5T_STD_U16LE_TOKEN = 14,       /* H5T_STD_U16LE_TOKEN  */
  YYSYMBOL_H5T_STD_U32BE_TOKEN = 15,       /* H5T_STD_U32BE_TOKEN  */
  YYSYMBOL_H5T_STD_U32LE_TOKEN = 16,       /* H5T_STD_U32LE_TOKEN  */
  YYSYMBOL_H5T_STD_U64BE_TOKEN = 17,       /* H5T_STD_U64BE_TOKEN  */
  YYSYMBOL_H5T_STD_U64LE_TOKEN = 18,       /* H5T_STD_U64LE_TOKEN  */
  YYSYMBOL_H5T_NATIVE_CHAR_TOKEN = 19,     /* H5T_NATIVE_CHAR_TOKEN  */
  YYSYMBOL_H5T_NATIVE_SCHAR_TOKEN = 20,    /* H5T_NATIVE_SCHAR_TOKEN  */
  YYSYMBOL_H5T_NATIVE_UCHAR_TOKEN = 21,    /* H5T_NATIVE_UCHAR_TOKEN  */
  YYSYMBOL_H5T_NATIVE_SHORT_TOKEN = 22,    /* H5T_NATIVE_SHORT_TOKEN  */
  YYSYMBOL_H5T_NATIVE_USHORT_TOKEN = 23,   /* H5T_NATIVE_USHORT_TOKEN  */
  YYSYMBOL_H5T_NATIVE_INT_TOKEN = 24,      /* H5T_NATIVE_INT_TOKEN  */
  YYSYMBOL_H5T_NATIVE_UINT_TOKEN = 25,     /* H5T_NATIVE_UINT_TOKEN  */
  YYSYMBOL_H5T_NATIVE_LONG_TOKEN = 26,     /* H5T_NATIVE_LONG_TOKEN  */
  YYSYMBOL_H5T_NATIVE_ULONG_TOKEN = 27,    /* H5T_NATIVE_ULONG_TOKEN  */
  YYSYMBOL_H5T_NATIVE_LLONG_TOKEN = 28,    /* H5T_NATIVE_LLONG_TOKEN  */
  YYSYMBOL_H5T_NATIVE_ULLONG_TOKEN = 29,   /* H5T_NATIVE_ULLONG_TOKEN  */
  YYSYMBOL_H5T_IEEE_F32BE_TOKEN = 30,      /* H5T_IEEE_F32BE_TOKEN  */
  YYSYMBOL_H5T_IEEE_F32LE_TOKEN = 31,      /* H5T_IEEE_F32LE_TOKEN  */
  YYSYMBOL_H5T_IEEE_F64BE_TOKEN = 32,      /* H5T_IEEE_F64BE_TOKEN  */
  YYSYMBOL_H5T_IEEE_F64LE_TOKEN = 33,      /* H5T_IEEE_F64LE_TOKEN  */
  YYSYMBOL_H5T_NATIVE_FLOAT_TOKEN = 34,    /* H5T_NATIVE_FLOAT_TOKEN  */
  YYSYMBOL_H5T_NATIVE_DOUBLE_TOKEN = 35,   /* H5T_NATIVE_DOUBLE_TOKEN  */
  YYSYMBOL_H5T_NATIVE_LDOUBLE_TOKEN = 36,  /* H5T_NATIVE_LDOUBLE_TOKEN  */
  YYSYMBOL_H5T_STRING_TOKEN = 37,          /* H5T_STRING_TOKEN  */
  YYSYMBOL_STRSIZE_TOKEN = 38,             /* STRSIZE_TOKEN  */
  YYSYMBOL_STRPAD_TOKEN = 39,              /* STRPAD_TOKEN  */
  YYSYMBOL_CSET_TOKEN = 40,                /* CSET_TOKEN  */
  YYSYMBOL_CTYPE_TOKEN = 41,               /* CTYPE_TOKEN  */
  YYSYMBOL_H5T_VARIABLE_TOKEN = 42,        /* H5T_VARIABLE_TOKEN  */
  YYSYMBOL_H5T_STR_NULLTERM_TOKEN = 43,    /* H5T_STR_NULLTERM_TOKEN  */
  YYSYMBOL_H5T_STR_NULLPAD_TOKEN = 44,     /* H5T_STR_NULLPAD_TOKEN  */
  YYSYMBOL_H5T_STR_SPACEPAD_TOKEN = 45,    /* H5T_STR_SPACEPAD_TOKEN  */
  YYSYMBOL_H5T_CSET_ASCII_TOKEN = 46,      /* H5T_CSET_ASCII_TOKEN  */
  YYSYMBOL_H5T_CSET_UTF8_TOKEN = 47,       /* H5T_CSET_UTF8_TOKEN  */
  YYSYMBOL_H5T_C_S1_TOKEN = 48,            /* H5T_C_S1_TOKEN  */
  YYSYMBOL_H5T_FORTRAN_S1_TOKEN = 49,      /* H5T_FORTRAN_S1_TOKEN  */
  YYSYMBOL_H5T_OPAQUE_TOKEN = 50,          /* H5T_OPAQUE_TOKEN  */
  YYSYMBOL_OPQ_SIZE_TOKEN = 51,            /* OPQ_SIZE_TOKEN  */
  YYSYMBOL_OPQ_TAG_TOKEN = 52,             /* OPQ_TAG_TOKEN  */
  YYSYMBOL_H5T_COMPOUND_TOKEN = 53,        /* H5T_COMPOUND_TOKEN  */
  YYSYMBOL_H5T_ENUM_TOKEN = 54,            /* H5T_ENUM_TOKEN  */
  YYSYMBOL_H5T_ARRAY_TOKEN = 55,           /* H5T_ARRAY_TOKEN  */
  YYSYMBOL_H5T_VLEN_TOKEN = 56,            /* H5T_VLEN_TOKEN  */
  YYSYMBOL_STRING = 57,                    /* STRING  */
  YYSYMBOL_NUMBER = 58,                    /* NUMBER  */
  YYSYMBOL_59_ = 59,                       /* '{'  */
  YYSYMBOL_60_ = 60,                       /* '}'  */
  YYSYMBOL_61_ = 61,                       /* '['  */
  YYSYMBOL_62_ = 62,                       /* ']'  */
  YYSYMBOL_63_ = 63,                       /* ':'  */
  YYSYMBOL_64_ = 64,                       /* ';'  */
  YYSYMBOL_YYACCEPT = 65,                  /* $accept  */
  YYSYMBOL_start = 66,                     /* start  */
  YYSYMBOL_ddl_type = 67,                  /* ddl_type  */
  YYSYMBOL_atomic_type = 68,               /* atomic_type  */
  YYSYMBOL_integer_type = 69,              /* integer_type  */
  YYSYMBOL_fp_type = 70,                   /* fp_type  */
  YYSYMBOL_compound_type = 71,             /* compound_type  */
  YYSYMBOL_72_1 = 72,                      /* $@1  */
  YYSYMBOL_memb_list = 73,                 /* memb_list  */
  YYSYMBOL_memb_def = 74,                  /* memb_def  */
  YYSYMBOL_75_2 = 75,                      /* $@2  */
  YYSYMBOL_field_name = 76,                /* field_name  */
  YYSYMBOL_field_offset = 77,              /* field_offset  */
  YYSYMBOL_offset = 78,                    /* offset  */
  YYSYMBOL_array_type = 79,                /* array_type  */
  YYSYMBOL_80_3 = 80,                      /* $@3  */
  YYSYMBOL_dim_list = 81,                  /* dim_list  */
  YYSYMBOL_dim = 82,                       /* dim  */
  YYSYMBOL_83_4 = 83,                      /* $@4  */
  YYSYMBOL_84_5 = 84,                      /* $@5  */
  YYSYMBOL_dimsize = 85,                   /* dimsize  */
  YYSYMBOL_vlen_type = 86,                 /* vlen_type  */
  YYSYMBOL_opaque_type = 87,               /* opaque_type  */
  YYSYMBOL_88_6 = 88,                      /* @6  */
  YYSYMBOL_89_7 = 89,                      /* $@7  */
  YYSYMBOL_opaque_size = 90,               /* opaque_size  */
  YYSYMBOL_opaque_tag = 91,                /* opaque_tag  */
  YYSYMBOL_string_type = 92,               /* string_type  */
  YYSYMBOL_93_8 = 93,                      /* $@8  */
  YYSYMBOL_94_9 = 94,                      /* $@9  */
  YYSYMBOL_95_10 = 95,                     /* $@10  */
  YYSYMBOL_96_11 = 96,                     /* @11  */
  YYSYMBOL_strsize = 97,                   /* strsize  */
  YYSYMBOL_strpad = 98,                    /* strpad  */
  YYSYMBOL_cset = 99,                      /* cset  */
  YYSYMBOL_ctype = 100,                    /* ctype  */
  YYSYMBOL_enum_type = 101,                /* enum_type  */
  YYSYMBOL_102_12 = 102,                   /* $@12  */
  YYSYMBOL_enum_list = 103,                /* enum_list  */
  YYSYMBOL_enum_def = 104,                 /* enum_def  */
  YYSYMBOL_105_13 = 105,                   /* $@13  */
  YYSYMBOL_enum_symbol = 106,              /* enum_symbol  */
  YYSYMBOL_enum_val = 107                  /* enum_val  */
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
#define YYFINAL  58
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   197

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  65
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  43
/* YYNRULES -- Number of rules.  */
#define YYNRULES  92
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  134

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   313


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
       2,     2,     2,     2,     2,     2,     2,     2,    63,    64,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,    61,     2,    62,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,    59,     2,    60,     2,     2,     2,     2,
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
      55,    56,    57,    58
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   101,   101,   102,   104,   105,   106,   107,   109,   110,
     111,   112,   113,   116,   117,   118,   119,   120,   121,   122,
     123,   124,   125,   126,   127,   128,   129,   130,   131,   132,
     133,   134,   135,   136,   137,   138,   139,   140,   141,   142,
     145,   146,   147,   148,   149,   150,   151,   155,   154,   163,
     164,   166,   166,   203,   211,   212,   215,   217,   217,   226,
     227,   229,   230,   229,   237,   240,   247,   252,   244,   259,
     261,   266,   273,   282,   289,   263,   313,   314,   316,   317,
     318,   320,   321,   323,   324,   328,   327,   332,   333,   335,
     335,   385,   387
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
  "\"end of file\"", "error", "\"invalid token\"", "H5T_STD_I8BE_TOKEN",
  "H5T_STD_I8LE_TOKEN", "H5T_STD_I16BE_TOKEN", "H5T_STD_I16LE_TOKEN",
  "H5T_STD_I32BE_TOKEN", "H5T_STD_I32LE_TOKEN", "H5T_STD_I64BE_TOKEN",
  "H5T_STD_I64LE_TOKEN", "H5T_STD_U8BE_TOKEN", "H5T_STD_U8LE_TOKEN",
  "H5T_STD_U16BE_TOKEN", "H5T_STD_U16LE_TOKEN", "H5T_STD_U32BE_TOKEN",
  "H5T_STD_U32LE_TOKEN", "H5T_STD_U64BE_TOKEN", "H5T_STD_U64LE_TOKEN",
  "H5T_NATIVE_CHAR_TOKEN", "H5T_NATIVE_SCHAR_TOKEN",
  "H5T_NATIVE_UCHAR_TOKEN", "H5T_NATIVE_SHORT_TOKEN",
  "H5T_NATIVE_USHORT_TOKEN", "H5T_NATIVE_INT_TOKEN",
  "H5T_NATIVE_UINT_TOKEN", "H5T_NATIVE_LONG_TOKEN",
  "H5T_NATIVE_ULONG_TOKEN", "H5T_NATIVE_LLONG_TOKEN",
  "H5T_NATIVE_ULLONG_TOKEN", "H5T_IEEE_F32BE_TOKEN",
  "H5T_IEEE_F32LE_TOKEN", "H5T_IEEE_F64BE_TOKEN", "H5T_IEEE_F64LE_TOKEN",
  "H5T_NATIVE_FLOAT_TOKEN", "H5T_NATIVE_DOUBLE_TOKEN",
  "H5T_NATIVE_LDOUBLE_TOKEN", "H5T_STRING_TOKEN", "STRSIZE_TOKEN",
  "STRPAD_TOKEN", "CSET_TOKEN", "CTYPE_TOKEN", "H5T_VARIABLE_TOKEN",
  "H5T_STR_NULLTERM_TOKEN", "H5T_STR_NULLPAD_TOKEN",
  "H5T_STR_SPACEPAD_TOKEN", "H5T_CSET_ASCII_TOKEN", "H5T_CSET_UTF8_TOKEN",
  "H5T_C_S1_TOKEN", "H5T_FORTRAN_S1_TOKEN", "H5T_OPAQUE_TOKEN",
  "OPQ_SIZE_TOKEN", "OPQ_TAG_TOKEN", "H5T_COMPOUND_TOKEN",
  "H5T_ENUM_TOKEN", "H5T_ARRAY_TOKEN", "H5T_VLEN_TOKEN", "STRING",
  "NUMBER", "'{'", "'}'", "'['", "']'", "':'", "';'", "$accept", "start",
  "ddl_type", "atomic_type", "integer_type", "fp_type", "compound_type",
  "$@1", "memb_list", "memb_def", "$@2", "field_name", "field_offset",
  "offset", "array_type", "$@3", "dim_list", "dim", "$@4", "$@5",
  "dimsize", "vlen_type", "opaque_type", "@6", "$@7", "opaque_size",
  "opaque_tag", "string_type", "$@8", "$@9", "$@10", "@11", "strsize",
  "strpad", "cset", "ctype", "enum_type", "$@12", "enum_list", "enum_def",
  "$@13", "enum_symbol", "enum_val", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-25)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     114,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,   -25,   -25,   -25,   -25,   -24,   -22,   -25,   -13,   -25,
     -11,    49,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,   -25,    18,    45,    38,   168,    39,   114,   -25,    -4,
      41,   -25,    36,   -25,    42,   -25,   -25,    37,   -25,    40,
      56,   -25,    -3,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,    43,   -25,    66,    55,    51,   -21,    57,   -25,     0,
      95,   -25,    50,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,   -25,    89,   -25,    90,    97,    92,    99,    52,   -25,
     -25,   -25,   -25,   -25,   -25,    94,   -25,   119,   100,   -25,
      -6,   -25,   -25,   -25,    98,   -25,   120,    46,   -25,   -25,
     101,   -25,   103,   -25
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       2,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    39,    40,    41,
      42,    43,    44,    45,    46,     0,     0,    47,     0,    57,
       0,     0,     3,     4,     8,     9,     5,     6,     7,    12,
      10,    11,     0,     0,     0,     0,     0,     0,     1,     0,
       0,    49,     0,    59,     0,    76,    77,     0,    69,     0,
       0,    85,     0,    65,    71,    66,    48,    51,    50,    87,
      61,     0,    60,     0,     0,     0,     0,     0,    58,     0,
       0,    53,    54,    91,    86,    88,    89,    64,    62,    78,
      79,    80,     0,    70,     0,     0,     0,     0,     0,    72,
      67,    56,    55,    52,    92,     0,    63,     0,     0,    90,
       0,    68,    81,    82,     0,    73,     0,     0,    83,    84,
       0,    74,     0,    75
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -25,   -25,   -15,   -25,   111,   -25,   -25,   -25,   -25,   -25,
     -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,   -25,
     -25,   -25,   -25
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,    41,    42,    43,    44,    45,    46,    54,    70,    78,
      85,    92,   106,   112,    47,    56,    72,    82,    87,   108,
      98,    48,    49,    84,   118,    69,   104,    50,    83,   117,
     126,   132,    67,   102,   124,   130,    51,    79,    86,    95,
     107,    96,   115
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_uint8 yytable[] =
{
       1,     2,     3,     4,     5,     6,     7,     8,     9,    10,
      11,    12,    13,    14,    15,    16,    17,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    52,    93,    53,    65,    94,
     122,   123,    64,    99,   100,   101,    55,    36,    57,    58,
      37,    38,    39,    40,    66,    77,    59,    81,    80,     1,
       2,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,   128,   129,    60,    61,    63,    68,
      71,    74,    73,    88,    75,    89,    36,    90,    91,    37,
      38,    39,    40,   105,   116,    97,    76,     1,     2,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,   103,   109,   110,   111,   113,   114,   119,   120,
     121,   127,   125,   133,    36,   131,    62,    37,    38,    39,
      40,     1,     2,     3,     4,     5,     6,     7,     8,     9,
      10,    11,    12,    13,    14,    15,    16,    17,    18,    19,
      20,    21,    22,    23,    24,    25,    26,    27
};

static const yytype_int8 yycheck[] =
{
       3,     4,     5,     6,     7,     8,     9,    10,    11,    12,
      13,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    36,    37,    59,    57,    59,    42,    60,
      46,    47,    57,    43,    44,    45,    59,    50,    59,     0,
      53,    54,    55,    56,    58,    70,    38,    72,    61,     3,
       4,     5,     6,     7,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    17,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    48,    49,    51,    59,    59,    58,
      64,    64,    60,    60,    64,    39,    50,    52,    57,    53,
      54,    55,    56,    63,    62,    58,    60,     3,     4,     5,
       6,     7,     8,     9,    10,    11,    12,    13,    14,    15,
      16,    17,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    57,    64,    64,    58,    64,    58,    64,    40,
      60,    41,    64,    60,    50,    64,    55,    53,    54,    55,
      56,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     3,     4,     5,     6,     7,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    50,    53,    54,    55,
      56,    66,    67,    68,    69,    70,    71,    79,    86,    87,
      92,   101,    59,    59,    72,    59,    80,    59,     0,    38,
      51,    59,    69,    59,    67,    42,    58,    97,    58,    90,
      73,    64,    81,    60,    64,    64,    60,    67,    74,   102,
      61,    67,    82,    93,    88,    75,   103,    83,    60,    39,
      52,    57,    76,    57,    60,   104,   106,    58,    85,    43,
      44,    45,    98,    57,    91,    63,    77,   105,    84,    64,
      64,    58,    78,    64,    58,   107,    62,    94,    89,    64,
      40,    60,    46,    47,    99,    64,    95,    41,    48,    49,
     100,    64,    96,    60
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    65,    66,    66,    67,    67,    67,    67,    68,    68,
      68,    68,    68,    69,    69,    69,    69,    69,    69,    69,
      69,    69,    69,    69,    69,    69,    69,    69,    69,    69,
      69,    69,    69,    69,    69,    69,    69,    69,    69,    69,
      70,    70,    70,    70,    70,    70,    70,    72,    71,    73,
      73,    75,    74,    76,    77,    77,    78,    80,    79,    81,
      81,    83,    84,    82,    85,    86,    88,    89,    87,    90,
      91,    93,    94,    95,    96,    92,    97,    97,    98,    98,
      98,    99,    99,   100,   100,   102,   101,   103,   103,   105,
     104,   106,   107
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     0,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     0,     5,     0,
       2,     0,     5,     1,     0,     2,     1,     0,     6,     0,
       2,     0,     0,     5,     1,     4,     0,     0,    11,     1,
       1,     0,     0,     0,     0,    19,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     0,     7,     0,     2,     0,
       4,     1,     1
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
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;
/* Number of syntax errors so far.  */
int yynerrs;




/*----------.
| yyparse.  |
`----------*/

hid_t
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
  case 2: /* start: %empty  */
#line 101 "hl/src//H5LTparse.y"
                { memset(arr_stack, 0, STACK_SIZE*sizeof(struct arr_info)); /*initialize here?*/ }
#line 1353 "hl/src//H5LTparse.c"
    break;

  case 3: /* start: ddl_type  */
#line 102 "hl/src//H5LTparse.y"
                          { return (yyval.hid);}
#line 1359 "hl/src//H5LTparse.c"
    break;

  case 13: /* integer_type: H5T_STD_I8BE_TOKEN  */
#line 116 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_I8BE); }
#line 1365 "hl/src//H5LTparse.c"
    break;

  case 14: /* integer_type: H5T_STD_I8LE_TOKEN  */
#line 117 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_I8LE); }
#line 1371 "hl/src//H5LTparse.c"
    break;

  case 15: /* integer_type: H5T_STD_I16BE_TOKEN  */
#line 118 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_I16BE); }
#line 1377 "hl/src//H5LTparse.c"
    break;

  case 16: /* integer_type: H5T_STD_I16LE_TOKEN  */
#line 119 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_I16LE); }
#line 1383 "hl/src//H5LTparse.c"
    break;

  case 17: /* integer_type: H5T_STD_I32BE_TOKEN  */
#line 120 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_I32BE); }
#line 1389 "hl/src//H5LTparse.c"
    break;

  case 18: /* integer_type: H5T_STD_I32LE_TOKEN  */
#line 121 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_I32LE); }
#line 1395 "hl/src//H5LTparse.c"
    break;

  case 19: /* integer_type: H5T_STD_I64BE_TOKEN  */
#line 122 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_I64BE); }
#line 1401 "hl/src//H5LTparse.c"
    break;

  case 20: /* integer_type: H5T_STD_I64LE_TOKEN  */
#line 123 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_I64LE); }
#line 1407 "hl/src//H5LTparse.c"
    break;

  case 21: /* integer_type: H5T_STD_U8BE_TOKEN  */
#line 124 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_U8BE); }
#line 1413 "hl/src//H5LTparse.c"
    break;

  case 22: /* integer_type: H5T_STD_U8LE_TOKEN  */
#line 125 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_U8LE); }
#line 1419 "hl/src//H5LTparse.c"
    break;

  case 23: /* integer_type: H5T_STD_U16BE_TOKEN  */
#line 126 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_U16BE); }
#line 1425 "hl/src//H5LTparse.c"
    break;

  case 24: /* integer_type: H5T_STD_U16LE_TOKEN  */
#line 127 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_U16LE); }
#line 1431 "hl/src//H5LTparse.c"
    break;

  case 25: /* integer_type: H5T_STD_U32BE_TOKEN  */
#line 128 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_U32BE); }
#line 1437 "hl/src//H5LTparse.c"
    break;

  case 26: /* integer_type: H5T_STD_U32LE_TOKEN  */
#line 129 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_U32LE); }
#line 1443 "hl/src//H5LTparse.c"
    break;

  case 27: /* integer_type: H5T_STD_U64BE_TOKEN  */
#line 130 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_U64BE); }
#line 1449 "hl/src//H5LTparse.c"
    break;

  case 28: /* integer_type: H5T_STD_U64LE_TOKEN  */
#line 131 "hl/src//H5LTparse.y"
                                            { (yyval.hid) = H5Tcopy(H5T_STD_U64LE); }
#line 1455 "hl/src//H5LTparse.c"
    break;

  case 29: /* integer_type: H5T_NATIVE_CHAR_TOKEN  */
#line 132 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_CHAR); }
#line 1461 "hl/src//H5LTparse.c"
    break;

  case 30: /* integer_type: H5T_NATIVE_SCHAR_TOKEN  */
#line 133 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_SCHAR); }
#line 1467 "hl/src//H5LTparse.c"
    break;

  case 31: /* integer_type: H5T_NATIVE_UCHAR_TOKEN  */
#line 134 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_UCHAR); }
#line 1473 "hl/src//H5LTparse.c"
    break;

  case 32: /* integer_type: H5T_NATIVE_SHORT_TOKEN  */
#line 135 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_SHORT); }
#line 1479 "hl/src//H5LTparse.c"
    break;

  case 33: /* integer_type: H5T_NATIVE_USHORT_TOKEN  */
#line 136 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_USHORT); }
#line 1485 "hl/src//H5LTparse.c"
    break;

  case 34: /* integer_type: H5T_NATIVE_INT_TOKEN  */
#line 137 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_INT); }
#line 1491 "hl/src//H5LTparse.c"
    break;

  case 35: /* integer_type: H5T_NATIVE_UINT_TOKEN  */
#line 138 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_UINT); }
#line 1497 "hl/src//H5LTparse.c"
    break;

  case 36: /* integer_type: H5T_NATIVE_LONG_TOKEN  */
#line 139 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_LONG); }
#line 1503 "hl/src//H5LTparse.c"
    break;

  case 37: /* integer_type: H5T_NATIVE_ULONG_TOKEN  */
#line 140 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_ULONG); }
#line 1509 "hl/src//H5LTparse.c"
    break;

  case 38: /* integer_type: H5T_NATIVE_LLONG_TOKEN  */
#line 141 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_LLONG); }
#line 1515 "hl/src//H5LTparse.c"
    break;

  case 39: /* integer_type: H5T_NATIVE_ULLONG_TOKEN  */
#line 142 "hl/src//H5LTparse.y"
                                                { (yyval.hid) = H5Tcopy(H5T_NATIVE_ULLONG); }
#line 1521 "hl/src//H5LTparse.c"
    break;

  case 40: /* fp_type: H5T_IEEE_F32BE_TOKEN  */
#line 145 "hl/src//H5LTparse.y"
                                             { (yyval.hid) = H5Tcopy(H5T_IEEE_F32BE); }
#line 1527 "hl/src//H5LTparse.c"
    break;

  case 41: /* fp_type: H5T_IEEE_F32LE_TOKEN  */
#line 146 "hl/src//H5LTparse.y"
                                             { (yyval.hid) = H5Tcopy(H5T_IEEE_F32LE); }
#line 1533 "hl/src//H5LTparse.c"
    break;

  case 42: /* fp_type: H5T_IEEE_F64BE_TOKEN  */
#line 147 "hl/src//H5LTparse.y"
                                             { (yyval.hid) = H5Tcopy(H5T_IEEE_F64BE); }
#line 1539 "hl/src//H5LTparse.c"
    break;

  case 43: /* fp_type: H5T_IEEE_F64LE_TOKEN  */
#line 148 "hl/src//H5LTparse.y"
                                             { (yyval.hid) = H5Tcopy(H5T_IEEE_F64LE); }
#line 1545 "hl/src//H5LTparse.c"
    break;

  case 44: /* fp_type: H5T_NATIVE_FLOAT_TOKEN  */
#line 149 "hl/src//H5LTparse.y"
                                                  { (yyval.hid) = H5Tcopy(H5T_NATIVE_FLOAT); }
#line 1551 "hl/src//H5LTparse.c"
    break;

  case 45: /* fp_type: H5T_NATIVE_DOUBLE_TOKEN  */
#line 150 "hl/src//H5LTparse.y"
                                                  { (yyval.hid) = H5Tcopy(H5T_NATIVE_DOUBLE); }
#line 1557 "hl/src//H5LTparse.c"
    break;

  case 46: /* fp_type: H5T_NATIVE_LDOUBLE_TOKEN  */
#line 151 "hl/src//H5LTparse.y"
                                                  { (yyval.hid) = H5Tcopy(H5T_NATIVE_LDOUBLE); }
#line 1563 "hl/src//H5LTparse.c"
    break;

  case 47: /* $@1: %empty  */
#line 155 "hl/src//H5LTparse.y"
                            { csindex++; cmpd_stack[csindex].id = H5Tcreate(H5T_COMPOUND, 1); /*temporarily set size to 1*/ }
#line 1569 "hl/src//H5LTparse.c"
    break;

  case 48: /* compound_type: H5T_COMPOUND_TOKEN $@1 '{' memb_list '}'  */
#line 157 "hl/src//H5LTparse.y"
                            { (yyval.hid) = cmpd_stack[csindex].id; 
                              cmpd_stack[csindex].id = 0;
                              cmpd_stack[csindex].first_memb = 1; 
                              csindex--;
                            }
#line 1579 "hl/src//H5LTparse.c"
    break;

  case 51: /* $@2: %empty  */
#line 166 "hl/src//H5LTparse.y"
                                 { cmpd_stack[csindex].is_field = 1; /*notify lexer a compound member is parsed*/ }
#line 1585 "hl/src//H5LTparse.c"
    break;

  case 52: /* memb_def: ddl_type $@2 field_name field_offset ';'  */
#line 168 "hl/src//H5LTparse.y"
                        {   
                            size_t origin_size, new_size;
                            hid_t dtype_id = cmpd_stack[csindex].id;

                            /*Adjust size and insert member, consider both member size and offset.*/
                            if(cmpd_stack[csindex].first_memb) { /*reclaim the size 1 temporarily set*/
                                new_size = H5Tget_size((yyvsp[-4].hid)) + (yyvsp[-1].ival);
                                H5Tset_size(dtype_id, new_size);
                                /*member name is saved in yylval.sval by lexer*/
                                H5Tinsert(dtype_id, (yyvsp[-2].sval), (yyvsp[-1].ival), (yyvsp[-4].hid));

                                cmpd_stack[csindex].first_memb = 0;
                            } else {
                                origin_size = H5Tget_size(dtype_id);
                                
                                if((yyvsp[-1].ival) == 0) {
                                    new_size = origin_size + H5Tget_size((yyvsp[-4].hid));
                                    H5Tset_size(dtype_id, new_size);
                                    H5Tinsert(dtype_id, (yyvsp[-2].sval), origin_size, (yyvsp[-4].hid));
                                } else {
                                    new_size = (yyvsp[-1].ival) + H5Tget_size((yyvsp[-4].hid));
                                    H5Tset_size(dtype_id, new_size);
                                    H5Tinsert(dtype_id, (yyvsp[-2].sval), (yyvsp[-1].ival), (yyvsp[-4].hid));
                                }
                            }
                            if((yyvsp[-2].sval)) {
                                free((yyvsp[-2].sval));
                                (yyvsp[-2].sval) = NULL;
                            }
                            cmpd_stack[csindex].is_field = 0;
                            H5Tclose((yyvsp[-4].hid));
                             
                            new_size = H5Tget_size(dtype_id);
                        }
#line 1624 "hl/src//H5LTparse.c"
    break;

  case 53: /* field_name: STRING  */
#line 204 "hl/src//H5LTparse.y"
                        {
                            (yyval.sval) = strdup(yylval.sval);
                            free(yylval.sval);
                            yylval.sval = NULL;
                        }
#line 1634 "hl/src//H5LTparse.c"
    break;

  case 54: /* field_offset: %empty  */
#line 211 "hl/src//H5LTparse.y"
                        { (yyval.ival) = 0; }
#line 1640 "hl/src//H5LTparse.c"
    break;

  case 55: /* field_offset: ':' offset  */
#line 213 "hl/src//H5LTparse.y"
                        { (yyval.ival) = yylval.ival; }
#line 1646 "hl/src//H5LTparse.c"
    break;

  case 57: /* $@3: %empty  */
#line 217 "hl/src//H5LTparse.y"
                                        { asindex++; /*pushd onto the stack*/ }
#line 1652 "hl/src//H5LTparse.c"
    break;

  case 58: /* array_type: H5T_ARRAY_TOKEN $@3 '{' dim_list ddl_type '}'  */
#line 219 "hl/src//H5LTparse.y"
                        { 
                          (yyval.hid) = H5Tarray_create2((yyvsp[-1].hid), arr_stack[asindex].ndims, arr_stack[asindex].dims);
                          arr_stack[asindex].ndims = 0;
                          asindex--;
                          H5Tclose((yyvsp[-1].hid));
                        }
#line 1663 "hl/src//H5LTparse.c"
    break;

  case 61: /* $@4: %empty  */
#line 229 "hl/src//H5LTparse.y"
                            { arr_stack[asindex].is_dim = 1; /*notice lexer of dimension size*/ }
#line 1669 "hl/src//H5LTparse.c"
    break;

  case 62: /* $@5: %empty  */
#line 230 "hl/src//H5LTparse.y"
                                { unsigned ndims = arr_stack[asindex].ndims;
                                  arr_stack[asindex].dims[ndims] = (hsize_t)yylval.ival; 
                                  arr_stack[asindex].ndims++;
                                  arr_stack[asindex].is_dim = 0; 
                                }
#line 1679 "hl/src//H5LTparse.c"
    break;

  case 65: /* vlen_type: H5T_VLEN_TOKEN '{' ddl_type '}'  */
#line 241 "hl/src//H5LTparse.y"
                            { (yyval.hid) = H5Tvlen_create((yyvsp[-1].hid)); H5Tclose((yyvsp[-1].hid)); }
#line 1685 "hl/src//H5LTparse.c"
    break;

  case 66: /* @6: %empty  */
#line 247 "hl/src//H5LTparse.y"
                            {   
                                size_t size = (size_t)yylval.ival;
                                (yyval.hid) = H5Tcreate(H5T_OPAQUE, size);
                            }
#line 1694 "hl/src//H5LTparse.c"
    break;

  case 67: /* $@7: %empty  */
#line 252 "hl/src//H5LTparse.y"
                            {  
                                H5Tset_tag((yyvsp[-3].hid), yylval.sval);
                                free(yylval.sval);
                                yylval.sval = NULL;
                            }
#line 1704 "hl/src//H5LTparse.c"
    break;

  case 68: /* opaque_type: H5T_OPAQUE_TOKEN '{' OPQ_SIZE_TOKEN opaque_size ';' @6 OPQ_TAG_TOKEN opaque_tag ';' $@7 '}'  */
#line 257 "hl/src//H5LTparse.y"
                            { (yyval.hid) = (yyvsp[-5].hid); }
#line 1710 "hl/src//H5LTparse.c"
    break;

  case 71: /* $@8: %empty  */
#line 266 "hl/src//H5LTparse.y"
                            {  
                                if((yyvsp[-1].ival) == H5T_VARIABLE_TOKEN)
                                    is_variable = 1;
                                else 
                                    str_size = yylval.ival;
                            }
#line 1721 "hl/src//H5LTparse.c"
    break;

  case 72: /* $@9: %empty  */
#line 273 "hl/src//H5LTparse.y"
                            {
                                if((yyvsp[-1].ival) == H5T_STR_NULLTERM_TOKEN)
                                    str_pad = H5T_STR_NULLTERM;
                                else if((yyvsp[-1].ival) == H5T_STR_NULLPAD_TOKEN)
                                    str_pad = H5T_STR_NULLPAD;
                                else if((yyvsp[-1].ival) == H5T_STR_SPACEPAD_TOKEN)
                                    str_pad = H5T_STR_SPACEPAD;
                            }
#line 1734 "hl/src//H5LTparse.c"
    break;

  case 73: /* $@10: %empty  */
#line 282 "hl/src//H5LTparse.y"
                            {  
                                if((yyvsp[-1].ival) == H5T_CSET_ASCII_TOKEN)
                                    str_cset = H5T_CSET_ASCII;
                                else if((yyvsp[-1].ival) == H5T_CSET_UTF8_TOKEN)
                                    str_cset = H5T_CSET_UTF8;
                            }
#line 1745 "hl/src//H5LTparse.c"
    break;

  case 74: /* @11: %empty  */
#line 289 "hl/src//H5LTparse.y"
                            {
                                if((yyvsp[-1].hid) == H5T_C_S1_TOKEN)
                                    (yyval.hid) = H5Tcopy(H5T_C_S1);
                                else if((yyvsp[-1].hid) == H5T_FORTRAN_S1_TOKEN)
                                    (yyval.hid) = H5Tcopy(H5T_FORTRAN_S1);
                            }
#line 1756 "hl/src//H5LTparse.c"
    break;

  case 75: /* string_type: H5T_STRING_TOKEN '{' STRSIZE_TOKEN strsize ';' $@8 STRPAD_TOKEN strpad ';' $@9 CSET_TOKEN cset ';' $@10 CTYPE_TOKEN ctype ';' @11 '}'  */
#line 296 "hl/src//H5LTparse.y"
                            {   
                                hid_t str_id = (yyvsp[-1].hid);

                                /*set string size*/
                                if(is_variable) {
                                    H5Tset_size(str_id, H5T_VARIABLE);
                                    is_variable = 0;
                                } else
                                    H5Tset_size(str_id, str_size);
                                
                                /*set string padding and character set*/
                                H5Tset_strpad(str_id, str_pad);
                                H5Tset_cset(str_id, str_cset);

                                (yyval.hid) = str_id; 
                            }
#line 1777 "hl/src//H5LTparse.c"
    break;

  case 76: /* strsize: H5T_VARIABLE_TOKEN  */
#line 313 "hl/src//H5LTparse.y"
                                               {(yyval.ival) = H5T_VARIABLE_TOKEN;}
#line 1783 "hl/src//H5LTparse.c"
    break;

  case 78: /* strpad: H5T_STR_NULLTERM_TOKEN  */
#line 316 "hl/src//H5LTparse.y"
                                               {(yyval.ival) = H5T_STR_NULLTERM_TOKEN;}
#line 1789 "hl/src//H5LTparse.c"
    break;

  case 79: /* strpad: H5T_STR_NULLPAD_TOKEN  */
#line 317 "hl/src//H5LTparse.y"
                                               {(yyval.ival) = H5T_STR_NULLPAD_TOKEN;}
#line 1795 "hl/src//H5LTparse.c"
    break;

  case 80: /* strpad: H5T_STR_SPACEPAD_TOKEN  */
#line 318 "hl/src//H5LTparse.y"
                                               {(yyval.ival) = H5T_STR_SPACEPAD_TOKEN;}
#line 1801 "hl/src//H5LTparse.c"
    break;

  case 81: /* cset: H5T_CSET_ASCII_TOKEN  */
#line 320 "hl/src//H5LTparse.y"
                                             {(yyval.ival) = H5T_CSET_ASCII_TOKEN;}
#line 1807 "hl/src//H5LTparse.c"
    break;

  case 82: /* cset: H5T_CSET_UTF8_TOKEN  */
#line 321 "hl/src//H5LTparse.y"
                                            {(yyval.ival) = H5T_CSET_UTF8_TOKEN;}
#line 1813 "hl/src//H5LTparse.c"
    break;

  case 83: /* ctype: H5T_C_S1_TOKEN  */
#line 323 "hl/src//H5LTparse.y"
                                               {(yyval.hid) = H5T_C_S1_TOKEN;}
#line 1819 "hl/src//H5LTparse.c"
    break;

  case 84: /* ctype: H5T_FORTRAN_S1_TOKEN  */
#line 324 "hl/src//H5LTparse.y"
                                               {(yyval.hid) = H5T_FORTRAN_S1_TOKEN;}
#line 1825 "hl/src//H5LTparse.c"
    break;

  case 85: /* $@12: %empty  */
#line 328 "hl/src//H5LTparse.y"
                            { is_enum = 1; enum_id = H5Tenum_create((yyvsp[-1].hid)); H5Tclose((yyvsp[-1].hid)); }
#line 1831 "hl/src//H5LTparse.c"
    break;

  case 86: /* enum_type: H5T_ENUM_TOKEN '{' integer_type ';' $@12 enum_list '}'  */
#line 330 "hl/src//H5LTparse.y"
                            { is_enum = 0; /*reset*/ (yyval.hid) = enum_id; }
#line 1837 "hl/src//H5LTparse.c"
    break;

  case 89: /* $@13: %empty  */
#line 335 "hl/src//H5LTparse.y"
                                            {
                                                is_enum_memb = 1; /*indicate member of enum*/
                                                enum_memb_symbol = strdup(yylval.sval); 
                                                free(yylval.sval);
                                                yylval.sval = NULL;
                                            }
#line 1848 "hl/src//H5LTparse.c"
    break;

  case 90: /* enum_def: enum_symbol $@13 enum_val ';'  */
#line 342 "hl/src//H5LTparse.y"
                            {
                                char char_val=(char)yylval.ival;
                                short short_val=(short)yylval.ival;
                                int int_val=(int)yylval.ival;
                                long long_val=(long)yylval.ival;
                                long long llong_val=(long long)yylval.ival;
                                hid_t super = H5Tget_super(enum_id);
                                hid_t native = H5Tget_native_type(super, H5T_DIR_ASCEND);
                                H5T_order_t super_order = H5Tget_order(super);
                                H5T_order_t native_order = H5Tget_order(native);
 
                                if(is_enum && is_enum_memb) { /*if it's an enum member*/
                                    /*To handle machines of different endianness*/
                                    if(H5Tequal(native, H5T_NATIVE_SCHAR) || H5Tequal(native, H5T_NATIVE_UCHAR)) {
                                        if(super_order != native_order)
                                            H5Tconvert(native, super, 1, &char_val, NULL, H5P_DEFAULT); 
                                        H5Tenum_insert(enum_id, enum_memb_symbol, &char_val);
                                    } else if(H5Tequal(native, H5T_NATIVE_SHORT) || H5Tequal(native, H5T_NATIVE_USHORT)) {
                                        if(super_order != native_order)
                                            H5Tconvert(native, super, 1, &short_val, NULL, H5P_DEFAULT); 
                                        H5Tenum_insert(enum_id, enum_memb_symbol, &short_val);
                                    } else if(H5Tequal(native, H5T_NATIVE_INT) || H5Tequal(native, H5T_NATIVE_UINT)) {
                                        if(super_order != native_order)
                                            H5Tconvert(native, super, 1, &int_val, NULL, H5P_DEFAULT); 
                                        H5Tenum_insert(enum_id, enum_memb_symbol, &int_val);
                                    } else if(H5Tequal(native, H5T_NATIVE_LONG) || H5Tequal(native, H5T_NATIVE_ULONG)) {
                                        if(super_order != native_order)
                                            H5Tconvert(native, super, 1, &long_val, NULL, H5P_DEFAULT); 
                                        H5Tenum_insert(enum_id, enum_memb_symbol, &long_val);
                                    } else if(H5Tequal(native, H5T_NATIVE_LLONG) || H5Tequal(native, H5T_NATIVE_ULLONG)) {
                                        if(super_order != native_order)
                                            H5Tconvert(native, super, 1, &llong_val, NULL, H5P_DEFAULT); 
                                        H5Tenum_insert(enum_id, enum_memb_symbol, &llong_val);
                                    }

                                    is_enum_memb = 0; 
                                    if(enum_memb_symbol) free(enum_memb_symbol);
                                }

                                H5Tclose(super);
                                H5Tclose(native);
                            }
#line 1895 "hl/src//H5LTparse.c"
    break;


#line 1899 "hl/src//H5LTparse.c"

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
  yyerror (YY_("memory exhausted"));
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

