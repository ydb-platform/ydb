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
   especially those whose name start with YY_ or grib_yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with grib_yy or YY, to avoid
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




/* First part of user prologue.  */
#line 12 "griby.y"


#include "grib_api_internal.h"

#include "action_class_alias.h"
#include "action_class_assert.h"
#include "action_class_close.h"
#include "action_class_concept.h"
#include "action_class_gen.h"
#include "action_class_hash_array.h"
#include "action_class_if.h"
#include "action_class_list.h"
#include "action_class_meta.h"
#include "action_class_modify.h"
#include "action_class_noop.h"
#include "action_class_print.h"
#include "action_class_put.h"
#include "action_class_remove.h"
#include "action_class_rename.h"
#include "action_class_section.h"
#include "action_class_set.h"
#include "action_class_set_darray.h"
#include "action_class_set_missing.h"
#include "action_class_set_sarray.h"
#include "action_class_switch.h"
#include "action_class_template.h"
#include "action_class_transient_darray.h"
#include "action_class_trigger.h"
#include "action_class_variable.h"
#include "action_class_when.h"
#include "action_class_while.h"
#include "action_class_write.h"

/* #include "grib_parser.h" */

extern int grib_yylex(void);
extern int grib_yyerror(const char*);
extern int grib_yylineno;
extern char* file_being_parsed(void);

extern   grib_action*           grib_parser_all_actions;
extern   grib_concept_value*    grib_parser_concept;
extern   grib_hash_array_value*    grib_parser_hash_array;
extern   grib_context*          grib_parser_context;
extern   grib_rule*             grib_parser_rules;

static grib_concept_value* reverse_concept(grib_concept_value* r);
static grib_concept_value *_reverse_concept(grib_concept_value *r,grib_concept_value *s);
static grib_hash_array_value* reverse_hash_array(grib_hash_array_value* r);
static grib_hash_array_value *_reverse_hash_array(grib_hash_array_value *r,grib_hash_array_value *s);

/* typedef int (*testp_proc)(long,long); */
/* typedef long (*grib_op_proc)(long,long);   */



#line 128 "y.tab.c"

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

/* Use api.header.include to #include this header
   instead of duplicating it here.  */
#ifndef YY_YY_Y_TAB_H_INCLUDED
# define YY_YY_Y_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int grib_yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum grib_yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    LOWERCASE = 258,               /* LOWERCASE  */
    IF = 259,                      /* IF  */
    IF_TRANSIENT = 260,            /* IF_TRANSIENT  */
    ELSE = 261,                    /* ELSE  */
    END = 262,                     /* END  */
    CLOSE = 263,                   /* CLOSE  */
    UNSIGNED = 264,                /* UNSIGNED  */
    TEMPLATE = 265,                /* TEMPLATE  */
    TEMPLATE_NOFAIL = 266,         /* TEMPLATE_NOFAIL  */
    TRIGGER = 267,                 /* TRIGGER  */
    ASCII = 268,                   /* ASCII  */
    GROUP = 269,                   /* GROUP  */
    NON_ALPHA = 270,               /* NON_ALPHA  */
    KSEC1EXPVER = 271,             /* KSEC1EXPVER  */
    LABEL = 272,                   /* LABEL  */
    LIST = 273,                    /* LIST  */
    IS_IN_LIST = 274,              /* IS_IN_LIST  */
    IS_IN_DICT = 275,              /* IS_IN_DICT  */
    IS_INTEGER = 276,              /* IS_INTEGER  */
    TO_INTEGER = 277,              /* TO_INTEGER  */
    TO_STRING = 278,               /* TO_STRING  */
    SEX2DEC = 279,                 /* SEX2DEC  */
    WHILE = 280,                   /* WHILE  */
    IBMFLOAT = 281,                /* IBMFLOAT  */
    SIGNED = 282,                  /* SIGNED  */
    UINT8 = 283,                   /* UINT8  */
    INT8 = 284,                    /* INT8  */
    UINT16 = 285,                  /* UINT16  */
    INT16 = 286,                   /* INT16  */
    UINT16_LITTLE_ENDIAN = 287,    /* UINT16_LITTLE_ENDIAN  */
    INT16_LITTLE_ENDIAN = 288,     /* INT16_LITTLE_ENDIAN  */
    UINT32 = 289,                  /* UINT32  */
    INT32 = 290,                   /* INT32  */
    UINT32_LITTLE_ENDIAN = 291,    /* UINT32_LITTLE_ENDIAN  */
    INT32_LITTLE_ENDIAN = 292,     /* INT32_LITTLE_ENDIAN  */
    UINT64 = 293,                  /* UINT64  */
    INT64 = 294,                   /* INT64  */
    UINT64_LITTLE_ENDIAN = 295,    /* UINT64_LITTLE_ENDIAN  */
    INT64_LITTLE_ENDIAN = 296,     /* INT64_LITTLE_ENDIAN  */
    BLOB = 297,                    /* BLOB  */
    BYTE = 298,                    /* BYTE  */
    CODETABLE = 299,               /* CODETABLE  */
    SMART_TABLE = 300,             /* SMART_TABLE  */
    DICTIONARY = 301,              /* DICTIONARY  */
    COMPLEX_CODETABLE = 302,       /* COMPLEX_CODETABLE  */
    LOOKUP = 303,                  /* LOOKUP  */
    ALIAS = 304,                   /* ALIAS  */
    UNALIAS = 305,                 /* UNALIAS  */
    META = 306,                    /* META  */
    POS = 307,                     /* POS  */
    INTCONST = 308,                /* INTCONST  */
    TRANS = 309,                   /* TRANS  */
    FLAGBIT = 310,                 /* FLAGBIT  */
    CONCEPT = 311,                 /* CONCEPT  */
    GETENV = 312,                  /* GETENV  */
    HASH_ARRAY = 313,              /* HASH_ARRAY  */
    CONCEPT_NOFAIL = 314,          /* CONCEPT_NOFAIL  */
    NIL = 315,                     /* NIL  */
    DUMMY = 316,                   /* DUMMY  */
    MODIFY = 317,                  /* MODIFY  */
    READ_ONLY = 318,               /* READ_ONLY  */
    STRING_TYPE = 319,             /* STRING_TYPE  */
    LONG_TYPE = 320,               /* LONG_TYPE  */
    DOUBLE_TYPE = 321,             /* DOUBLE_TYPE  */
    NO_COPY = 322,                 /* NO_COPY  */
    DUMP = 323,                    /* DUMP  */
    JSON = 324,                    /* JSON  */
    XML = 325,                     /* XML  */
    NO_FAIL = 326,                 /* NO_FAIL  */
    EDITION_SPECIFIC = 327,        /* EDITION_SPECIFIC  */
    OVERRIDE = 328,                /* OVERRIDE  */
    HIDDEN = 329,                  /* HIDDEN  */
    CAN_BE_MISSING = 330,          /* CAN_BE_MISSING  */
    MISSING = 331,                 /* MISSING  */
    CONSTRAINT = 332,              /* CONSTRAINT  */
    COPY_OK = 333,                 /* COPY_OK  */
    COPY_AS_INT = 334,             /* COPY_AS_INT  */
    COPY_IF_CHANGING_EDITION = 335, /* COPY_IF_CHANGING_EDITION  */
    WHEN = 336,                    /* WHEN  */
    SET = 337,                     /* SET  */
    SET_NOFAIL = 338,              /* SET_NOFAIL  */
    WRITE = 339,                   /* WRITE  */
    APPEND = 340,                  /* APPEND  */
    PRINT = 341,                   /* PRINT  */
    EXPORT = 342,                  /* EXPORT  */
    REMOVE = 343,                  /* REMOVE  */
    RENAME = 344,                  /* RENAME  */
    SKIP = 345,                    /* SKIP  */
    PAD = 346,                     /* PAD  */
    SECTION_PADDING = 347,         /* SECTION_PADDING  */
    MESSAGE = 348,                 /* MESSAGE  */
    MESSAGE_COPY = 349,            /* MESSAGE_COPY  */
    PADTO = 350,                   /* PADTO  */
    PADTOEVEN = 351,               /* PADTOEVEN  */
    PADTOMULTIPLE = 352,           /* PADTOMULTIPLE  */
    G1_HALF_BYTE = 353,            /* G1_HALF_BYTE  */
    G1_MESSAGE_LENGTH = 354,       /* G1_MESSAGE_LENGTH  */
    G1_SECTION4_LENGTH = 355,      /* G1_SECTION4_LENGTH  */
    SECTION_LENGTH = 356,          /* SECTION_LENGTH  */
    LENGTH = 357,                  /* LENGTH  */
    FLAG = 358,                    /* FLAG  */
    ITERATOR = 359,                /* ITERATOR  */
    NEAREST = 360,                 /* NEAREST  */
    BOX = 361,                     /* BOX  */
    KSEC = 362,                    /* KSEC  */
    ASSERT = 363,                  /* ASSERT  */
    SUBSTR = 364,                  /* SUBSTR  */
    CASE = 365,                    /* CASE  */
    SWITCH = 366,                  /* SWITCH  */
    DEFAULT = 367,                 /* DEFAULT  */
    EQ = 368,                      /* EQ  */
    NE = 369,                      /* NE  */
    GE = 370,                      /* GE  */
    LE = 371,                      /* LE  */
    LT = 372,                      /* LT  */
    GT = 373,                      /* GT  */
    BIT = 374,                     /* BIT  */
    BITOFF = 375,                  /* BITOFF  */
    AND = 376,                     /* AND  */
    OR = 377,                      /* OR  */
    NOT = 378,                     /* NOT  */
    IS = 379,                      /* IS  */
    ISNOT = 380,                   /* ISNOT  */
    IDENT = 381,                   /* IDENT  */
    STRING = 382,                  /* STRING  */
    INTEGER = 383,                 /* INTEGER  */
    FLOAT = 384                    /* FLOAT  */
  };
  typedef enum grib_yytokentype grib_yytoken_kind_t;
#endif
/* Token kinds.  */
#define YYEMPTY -2
#define YYEOF 0
#define YYerror 256
#define YYUNDEF 257
#define LOWERCASE 258
#define IF 259
#define IF_TRANSIENT 260
#define ELSE 261
#define END 262
#define CLOSE 263
#define UNSIGNED 264
#define TEMPLATE 265
#define TEMPLATE_NOFAIL 266
#define TRIGGER 267
#define ASCII 268
#define GROUP 269
#define NON_ALPHA 270
#define KSEC1EXPVER 271
#define LABEL 272
#define LIST 273
#define IS_IN_LIST 274
#define IS_IN_DICT 275
#define IS_INTEGER 276
#define TO_INTEGER 277
#define TO_STRING 278
#define SEX2DEC 279
#define WHILE 280
#define IBMFLOAT 281
#define SIGNED 282
#define UINT8 283
#define INT8 284
#define UINT16 285
#define INT16 286
#define UINT16_LITTLE_ENDIAN 287
#define INT16_LITTLE_ENDIAN 288
#define UINT32 289
#define INT32 290
#define UINT32_LITTLE_ENDIAN 291
#define INT32_LITTLE_ENDIAN 292
#define UINT64 293
#define INT64 294
#define UINT64_LITTLE_ENDIAN 295
#define INT64_LITTLE_ENDIAN 296
#define BLOB 297
#define BYTE 298
#define CODETABLE 299
#define SMART_TABLE 300
#define DICTIONARY 301
#define COMPLEX_CODETABLE 302
#define LOOKUP 303
#define ALIAS 304
#define UNALIAS 305
#define META 306
#define POS 307
#define INTCONST 308
#define TRANS 309
#define FLAGBIT 310
#define CONCEPT 311
#define GETENV 312
#define HASH_ARRAY 313
#define CONCEPT_NOFAIL 314
#define NIL 315
#define DUMMY 316
#define MODIFY 317
#define READ_ONLY 318
#define STRING_TYPE 319
#define LONG_TYPE 320
#define DOUBLE_TYPE 321
#define NO_COPY 322
#define DUMP 323
#define JSON 324
#define XML 325
#define NO_FAIL 326
#define EDITION_SPECIFIC 327
#define OVERRIDE 328
#define HIDDEN 329
#define CAN_BE_MISSING 330
#define MISSING 331
#define CONSTRAINT 332
#define COPY_OK 333
#define COPY_AS_INT 334
#define COPY_IF_CHANGING_EDITION 335
#define WHEN 336
#define SET 337
#define SET_NOFAIL 338
#define WRITE 339
#define APPEND 340
#define PRINT 341
#define EXPORT 342
#define REMOVE 343
#define RENAME 344
#define SKIP 345
#define PAD 346
#define SECTION_PADDING 347
#define MESSAGE 348
#define MESSAGE_COPY 349
#define PADTO 350
#define PADTOEVEN 351
#define PADTOMULTIPLE 352
#define G1_HALF_BYTE 353
#define G1_MESSAGE_LENGTH 354
#define G1_SECTION4_LENGTH 355
#define SECTION_LENGTH 356
#define LENGTH 357
#define FLAG 358
#define ITERATOR 359
#define NEAREST 360
#define BOX 361
#define KSEC 362
#define ASSERT 363
#define SUBSTR 364
#define CASE 365
#define SWITCH 366
#define DEFAULT 367
#define EQ 368
#define NE 369
#define GE 370
#define LE 371
#define LT 372
#define GT 373
#define BIT 374
#define BITOFF 375
#define AND 376
#define OR 377
#define NOT 378
#define IS 379
#define ISNOT 380
#define IDENT 381
#define STRING 382
#define INTEGER 383
#define FLOAT 384

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 69 "griby.y"

    char                    *str;
    long                    lval;
    double                  dval;
    grib_darray             *dvalue;
    grib_sarray             *svalue;
    grib_iarray             *ivalue;
    grib_action             *act;
    grib_arguments          *explist;
    grib_expression         *exp;
    grib_concept_condition  *concept_condition;
    grib_concept_value      *concept_value;
    grib_hash_array_value      *hash_array_value;
    grib_case               *case_value;
  grib_rule               *rules;
  grib_rule_entry         *rule_entry;

#line 457 "y.tab.c"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE grib_yylval;


int grib_yyparse (void);


#endif /* !YY_YY_Y_TAB_H_INCLUDED  */
/* Symbol kind.  */
enum grib_yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_LOWERCASE = 3,                  /* LOWERCASE  */
  YYSYMBOL_IF = 4,                         /* IF  */
  YYSYMBOL_IF_TRANSIENT = 5,               /* IF_TRANSIENT  */
  YYSYMBOL_ELSE = 6,                       /* ELSE  */
  YYSYMBOL_END = 7,                        /* END  */
  YYSYMBOL_CLOSE = 8,                      /* CLOSE  */
  YYSYMBOL_UNSIGNED = 9,                   /* UNSIGNED  */
  YYSYMBOL_TEMPLATE = 10,                  /* TEMPLATE  */
  YYSYMBOL_TEMPLATE_NOFAIL = 11,           /* TEMPLATE_NOFAIL  */
  YYSYMBOL_TRIGGER = 12,                   /* TRIGGER  */
  YYSYMBOL_ASCII = 13,                     /* ASCII  */
  YYSYMBOL_GROUP = 14,                     /* GROUP  */
  YYSYMBOL_NON_ALPHA = 15,                 /* NON_ALPHA  */
  YYSYMBOL_KSEC1EXPVER = 16,               /* KSEC1EXPVER  */
  YYSYMBOL_LABEL = 17,                     /* LABEL  */
  YYSYMBOL_LIST = 18,                      /* LIST  */
  YYSYMBOL_IS_IN_LIST = 19,                /* IS_IN_LIST  */
  YYSYMBOL_IS_IN_DICT = 20,                /* IS_IN_DICT  */
  YYSYMBOL_IS_INTEGER = 21,                /* IS_INTEGER  */
  YYSYMBOL_TO_INTEGER = 22,                /* TO_INTEGER  */
  YYSYMBOL_TO_STRING = 23,                 /* TO_STRING  */
  YYSYMBOL_SEX2DEC = 24,                   /* SEX2DEC  */
  YYSYMBOL_WHILE = 25,                     /* WHILE  */
  YYSYMBOL_IBMFLOAT = 26,                  /* IBMFLOAT  */
  YYSYMBOL_SIGNED = 27,                    /* SIGNED  */
  YYSYMBOL_UINT8 = 28,                     /* UINT8  */
  YYSYMBOL_INT8 = 29,                      /* INT8  */
  YYSYMBOL_UINT16 = 30,                    /* UINT16  */
  YYSYMBOL_INT16 = 31,                     /* INT16  */
  YYSYMBOL_UINT16_LITTLE_ENDIAN = 32,      /* UINT16_LITTLE_ENDIAN  */
  YYSYMBOL_INT16_LITTLE_ENDIAN = 33,       /* INT16_LITTLE_ENDIAN  */
  YYSYMBOL_UINT32 = 34,                    /* UINT32  */
  YYSYMBOL_INT32 = 35,                     /* INT32  */
  YYSYMBOL_UINT32_LITTLE_ENDIAN = 36,      /* UINT32_LITTLE_ENDIAN  */
  YYSYMBOL_INT32_LITTLE_ENDIAN = 37,       /* INT32_LITTLE_ENDIAN  */
  YYSYMBOL_UINT64 = 38,                    /* UINT64  */
  YYSYMBOL_INT64 = 39,                     /* INT64  */
  YYSYMBOL_UINT64_LITTLE_ENDIAN = 40,      /* UINT64_LITTLE_ENDIAN  */
  YYSYMBOL_INT64_LITTLE_ENDIAN = 41,       /* INT64_LITTLE_ENDIAN  */
  YYSYMBOL_BLOB = 42,                      /* BLOB  */
  YYSYMBOL_BYTE = 43,                      /* BYTE  */
  YYSYMBOL_CODETABLE = 44,                 /* CODETABLE  */
  YYSYMBOL_SMART_TABLE = 45,               /* SMART_TABLE  */
  YYSYMBOL_DICTIONARY = 46,                /* DICTIONARY  */
  YYSYMBOL_COMPLEX_CODETABLE = 47,         /* COMPLEX_CODETABLE  */
  YYSYMBOL_LOOKUP = 48,                    /* LOOKUP  */
  YYSYMBOL_ALIAS = 49,                     /* ALIAS  */
  YYSYMBOL_UNALIAS = 50,                   /* UNALIAS  */
  YYSYMBOL_META = 51,                      /* META  */
  YYSYMBOL_POS = 52,                       /* POS  */
  YYSYMBOL_INTCONST = 53,                  /* INTCONST  */
  YYSYMBOL_TRANS = 54,                     /* TRANS  */
  YYSYMBOL_FLAGBIT = 55,                   /* FLAGBIT  */
  YYSYMBOL_CONCEPT = 56,                   /* CONCEPT  */
  YYSYMBOL_GETENV = 57,                    /* GETENV  */
  YYSYMBOL_HASH_ARRAY = 58,                /* HASH_ARRAY  */
  YYSYMBOL_CONCEPT_NOFAIL = 59,            /* CONCEPT_NOFAIL  */
  YYSYMBOL_NIL = 60,                       /* NIL  */
  YYSYMBOL_DUMMY = 61,                     /* DUMMY  */
  YYSYMBOL_MODIFY = 62,                    /* MODIFY  */
  YYSYMBOL_READ_ONLY = 63,                 /* READ_ONLY  */
  YYSYMBOL_STRING_TYPE = 64,               /* STRING_TYPE  */
  YYSYMBOL_LONG_TYPE = 65,                 /* LONG_TYPE  */
  YYSYMBOL_DOUBLE_TYPE = 66,               /* DOUBLE_TYPE  */
  YYSYMBOL_NO_COPY = 67,                   /* NO_COPY  */
  YYSYMBOL_DUMP = 68,                      /* DUMP  */
  YYSYMBOL_JSON = 69,                      /* JSON  */
  YYSYMBOL_XML = 70,                       /* XML  */
  YYSYMBOL_NO_FAIL = 71,                   /* NO_FAIL  */
  YYSYMBOL_EDITION_SPECIFIC = 72,          /* EDITION_SPECIFIC  */
  YYSYMBOL_OVERRIDE = 73,                  /* OVERRIDE  */
  YYSYMBOL_HIDDEN = 74,                    /* HIDDEN  */
  YYSYMBOL_CAN_BE_MISSING = 75,            /* CAN_BE_MISSING  */
  YYSYMBOL_MISSING = 76,                   /* MISSING  */
  YYSYMBOL_CONSTRAINT = 77,                /* CONSTRAINT  */
  YYSYMBOL_COPY_OK = 78,                   /* COPY_OK  */
  YYSYMBOL_COPY_AS_INT = 79,               /* COPY_AS_INT  */
  YYSYMBOL_COPY_IF_CHANGING_EDITION = 80,  /* COPY_IF_CHANGING_EDITION  */
  YYSYMBOL_WHEN = 81,                      /* WHEN  */
  YYSYMBOL_SET = 82,                       /* SET  */
  YYSYMBOL_SET_NOFAIL = 83,                /* SET_NOFAIL  */
  YYSYMBOL_WRITE = 84,                     /* WRITE  */
  YYSYMBOL_APPEND = 85,                    /* APPEND  */
  YYSYMBOL_PRINT = 86,                     /* PRINT  */
  YYSYMBOL_EXPORT = 87,                    /* EXPORT  */
  YYSYMBOL_REMOVE = 88,                    /* REMOVE  */
  YYSYMBOL_RENAME = 89,                    /* RENAME  */
  YYSYMBOL_SKIP = 90,                      /* SKIP  */
  YYSYMBOL_PAD = 91,                       /* PAD  */
  YYSYMBOL_SECTION_PADDING = 92,           /* SECTION_PADDING  */
  YYSYMBOL_MESSAGE = 93,                   /* MESSAGE  */
  YYSYMBOL_MESSAGE_COPY = 94,              /* MESSAGE_COPY  */
  YYSYMBOL_PADTO = 95,                     /* PADTO  */
  YYSYMBOL_PADTOEVEN = 96,                 /* PADTOEVEN  */
  YYSYMBOL_PADTOMULTIPLE = 97,             /* PADTOMULTIPLE  */
  YYSYMBOL_G1_HALF_BYTE = 98,              /* G1_HALF_BYTE  */
  YYSYMBOL_G1_MESSAGE_LENGTH = 99,         /* G1_MESSAGE_LENGTH  */
  YYSYMBOL_G1_SECTION4_LENGTH = 100,       /* G1_SECTION4_LENGTH  */
  YYSYMBOL_SECTION_LENGTH = 101,           /* SECTION_LENGTH  */
  YYSYMBOL_LENGTH = 102,                   /* LENGTH  */
  YYSYMBOL_FLAG = 103,                     /* FLAG  */
  YYSYMBOL_ITERATOR = 104,                 /* ITERATOR  */
  YYSYMBOL_NEAREST = 105,                  /* NEAREST  */
  YYSYMBOL_BOX = 106,                      /* BOX  */
  YYSYMBOL_KSEC = 107,                     /* KSEC  */
  YYSYMBOL_ASSERT = 108,                   /* ASSERT  */
  YYSYMBOL_SUBSTR = 109,                   /* SUBSTR  */
  YYSYMBOL_CASE = 110,                     /* CASE  */
  YYSYMBOL_SWITCH = 111,                   /* SWITCH  */
  YYSYMBOL_DEFAULT = 112,                  /* DEFAULT  */
  YYSYMBOL_EQ = 113,                       /* EQ  */
  YYSYMBOL_NE = 114,                       /* NE  */
  YYSYMBOL_GE = 115,                       /* GE  */
  YYSYMBOL_LE = 116,                       /* LE  */
  YYSYMBOL_LT = 117,                       /* LT  */
  YYSYMBOL_GT = 118,                       /* GT  */
  YYSYMBOL_BIT = 119,                      /* BIT  */
  YYSYMBOL_BITOFF = 120,                   /* BITOFF  */
  YYSYMBOL_AND = 121,                      /* AND  */
  YYSYMBOL_OR = 122,                       /* OR  */
  YYSYMBOL_NOT = 123,                      /* NOT  */
  YYSYMBOL_IS = 124,                       /* IS  */
  YYSYMBOL_ISNOT = 125,                    /* ISNOT  */
  YYSYMBOL_IDENT = 126,                    /* IDENT  */
  YYSYMBOL_STRING = 127,                   /* STRING  */
  YYSYMBOL_INTEGER = 128,                  /* INTEGER  */
  YYSYMBOL_FLOAT = 129,                    /* FLOAT  */
  YYSYMBOL_130_ = 130,                     /* ','  */
  YYSYMBOL_131_ = 131,                     /* ';'  */
  YYSYMBOL_132_ = 132,                     /* '['  */
  YYSYMBOL_133_ = 133,                     /* ']'  */
  YYSYMBOL_134_ = 134,                     /* '('  */
  YYSYMBOL_135_ = 135,                     /* ')'  */
  YYSYMBOL_136_ = 136,                     /* '='  */
  YYSYMBOL_137_ = 137,                     /* '.'  */
  YYSYMBOL_138_ = 138,                     /* '{'  */
  YYSYMBOL_139_ = 139,                     /* '}'  */
  YYSYMBOL_140_ = 140,                     /* ':'  */
  YYSYMBOL_141_ = 141,                     /* '-'  */
  YYSYMBOL_142_ = 142,                     /* '^'  */
  YYSYMBOL_143_ = 143,                     /* '*'  */
  YYSYMBOL_144_ = 144,                     /* '/'  */
  YYSYMBOL_145_ = 145,                     /* '%'  */
  YYSYMBOL_146_ = 146,                     /* '+'  */
  YYSYMBOL_YYACCEPT = 147,                 /* $accept  */
  YYSYMBOL_all = 148,                      /* all  */
  YYSYMBOL_empty = 149,                    /* empty  */
  YYSYMBOL_dvalues = 150,                  /* dvalues  */
  YYSYMBOL_svalues = 151,                  /* svalues  */
  YYSYMBOL_integer_array = 152,            /* integer_array  */
  YYSYMBOL_instructions = 153,             /* instructions  */
  YYSYMBOL_instruction = 154,              /* instruction  */
  YYSYMBOL_semi = 155,                     /* semi  */
  YYSYMBOL_argument_list = 156,            /* argument_list  */
  YYSYMBOL_arguments = 157,                /* arguments  */
  YYSYMBOL_argument = 158,                 /* argument  */
  YYSYMBOL_simple = 159,                   /* simple  */
  YYSYMBOL_if_block = 160,                 /* if_block  */
  YYSYMBOL_when_block = 161,               /* when_block  */
  YYSYMBOL_set = 162,                      /* set  */
  YYSYMBOL_set_list = 163,                 /* set_list  */
  YYSYMBOL_default = 164,                  /* default  */
  YYSYMBOL_flags = 165,                    /* flags  */
  YYSYMBOL_flag_list = 166,                /* flag_list  */
  YYSYMBOL_flag = 167,                     /* flag  */
  YYSYMBOL_list_block = 168,               /* list_block  */
  YYSYMBOL_while_block = 169,              /* while_block  */
  YYSYMBOL_trigger_block = 170,            /* trigger_block  */
  YYSYMBOL_concept_block = 171,            /* concept_block  */
  YYSYMBOL_concept_list = 172,             /* concept_list  */
  YYSYMBOL_hash_array_list = 173,          /* hash_array_list  */
  YYSYMBOL_hash_array_block = 174,         /* hash_array_block  */
  YYSYMBOL_case_list = 175,                /* case_list  */
  YYSYMBOL_case_value = 176,               /* case_value  */
  YYSYMBOL_switch_block = 177,             /* switch_block  */
  YYSYMBOL_concept_value = 178,            /* concept_value  */
  YYSYMBOL_concept_conditions = 179,       /* concept_conditions  */
  YYSYMBOL_concept_condition = 180,        /* concept_condition  */
  YYSYMBOL_hash_array_value = 181,         /* hash_array_value  */
  YYSYMBOL_string_or_ident = 182,          /* string_or_ident  */
  YYSYMBOL_atom = 183,                     /* atom  */
  YYSYMBOL_power = 184,                    /* power  */
  YYSYMBOL_factor = 185,                   /* factor  */
  YYSYMBOL_term = 186,                     /* term  */
  YYSYMBOL_condition = 187,                /* condition  */
  YYSYMBOL_conjunction = 188,              /* conjunction  */
  YYSYMBOL_disjunction = 189,              /* disjunction  */
  YYSYMBOL_expression = 190,               /* expression  */
  YYSYMBOL_rule = 191,                     /* rule  */
  YYSYMBOL_rule_entry = 192,               /* rule_entry  */
  YYSYMBOL_rule_entries = 193,             /* rule_entries  */
  YYSYMBOL_fact = 194,                     /* fact  */
  YYSYMBOL_conditional_rule = 195,         /* conditional_rule  */
  YYSYMBOL_rules = 196                     /* rules  */
};
typedef enum grib_yysymbol_kind_t grib_yysymbol_kind_t;




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
typedef __INT_LEAST8_TYPE__ grib_yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t grib_yytype_int8;
#else
typedef signed char grib_yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ grib_yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t grib_yytype_int16;
#else
typedef short grib_yytype_int16;
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
typedef __UINT_LEAST8_TYPE__ grib_yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t grib_yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char grib_yytype_uint8;
#else
typedef short grib_yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ grib_yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t grib_yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short grib_yytype_uint16;
#else
typedef int grib_yytype_uint16;
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
typedef grib_yytype_int16 grib_yy_state_t;

/* State numbers in computations.  */
typedef int grib_yy_state_fast_t;

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

/* Suppress an incorrect diagnostic about grib_yylval being uninitialized.  */
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

#if !defined grib_yyoverflow

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
#endif /* !defined grib_yyoverflow */

#if (! defined grib_yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union grib_yyalloc
{
  grib_yy_state_t grib_yyss_alloc;
  YYSTYPE grib_yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union grib_yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (grib_yy_state_t) + YYSIZEOF (YYSTYPE)) \
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
        YYPTRDIFF_T grib_yynewbytes;                                         \
        YYCOPY (&grib_yyptr->Stack_alloc, Stack, grib_yysize);                    \
        Stack = &grib_yyptr->Stack_alloc;                                    \
        grib_yynewbytes = grib_yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        grib_yyptr += grib_yynewbytes / YYSIZEOF (*grib_yyptr);                        \
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
          YYPTRDIFF_T grib_yyi;                      \
          for (grib_yyi = 0; grib_yyi < (Count); grib_yyi++)   \
            (Dst)[grib_yyi] = (Src)[grib_yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  214
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   1845

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  147
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  50
/* YYNRULES -- Number of rules.  */
#define YYNRULES  272
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  921

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   384


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by grib_yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (grib_yysymbol_kind_t, grib_yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by grib_yylex.  */
static const grib_yytype_uint8 grib_yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,   145,     2,     2,
     134,   135,   143,   146,   130,   141,   137,   144,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,   140,   131,
       2,   136,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,   132,     2,   133,   142,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,   138,     2,   139,     2,     2,     2,     2,
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
     125,   126,   127,   128,   129
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const grib_yytype_int16 grib_yyrline[] =
{
       0,   295,   295,   297,   298,   299,   300,   302,   306,   309,
     310,   311,   312,   315,   316,   320,   321,   324,   325,   326,
     327,   330,   331,   332,   333,   334,   335,   336,   337,   338,
     342,   343,   346,   347,   350,   351,   354,   358,   361,   364,
     367,   370,   373,   376,   379,   382,   385,   388,   392,   395,
     398,   401,   404,   407,   410,   413,   416,   419,   430,   434,
     437,   440,   443,   446,   449,   452,   455,   458,   461,   464,
     467,   471,   474,   477,   480,   483,   486,   489,   492,   495,
     498,   501,   504,   507,   510,   513,   517,   520,   523,   526,
     529,   531,   534,   537,   540,   543,   546,   549,   552,   555,
     558,   561,   564,   567,   570,   573,   576,   578,   580,   583,
     586,   589,   593,   597,   600,   603,   615,   627,   639,   642,
     645,   647,   650,   653,   654,   655,   656,   658,   661,   662,
     663,   664,   665,   666,   667,   668,   670,   671,   672,   673,
     674,   678,   679,   680,   681,   685,   686,   687,   690,   691,
     694,   695,   699,   700,   703,   704,   707,   708,   711,   712,
     713,   714,   715,   716,   717,   718,   719,   720,   721,   722,
     723,   724,   725,   726,   729,   732,   735,   738,   739,   740,
     741,   742,   743,   745,   746,   747,   748,   749,   751,   752,
     753,   754,   755,   756,   757,   758,   762,   763,   766,   767,
     770,   771,   774,   775,   778,   782,   783,   784,   787,   789,
     791,   793,   797,   798,   801,   802,   806,   808,   812,   813,
     814,   815,   818,   819,   820,   822,   823,   824,   825,   826,
     827,   831,   832,   835,   836,   837,   838,   839,   840,   841,
     842,   843,   844,   845,   846,   849,   850,   851,   854,   856,
     857,   858,   859,   860,   861,   862,   867,   868,   871,   872,
     875,   876,   879,   885,   886,   889,   890,   893,   894,   897,
     901,   904,   905
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (grib_yysymbol_kind_t, grib_yystos[State])

#if YYDEBUG || 0
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *grib_yysymbol_name (grib_yysymbol_kind_t grib_yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const grib_yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "LOWERCASE", "IF",
  "IF_TRANSIENT", "ELSE", "END", "CLOSE", "UNSIGNED", "TEMPLATE",
  "TEMPLATE_NOFAIL", "TRIGGER", "ASCII", "GROUP", "NON_ALPHA",
  "KSEC1EXPVER", "LABEL", "LIST", "IS_IN_LIST", "IS_IN_DICT", "IS_INTEGER",
  "TO_INTEGER", "TO_STRING", "SEX2DEC", "WHILE", "IBMFLOAT", "SIGNED",
  "UINT8", "INT8", "UINT16", "INT16", "UINT16_LITTLE_ENDIAN",
  "INT16_LITTLE_ENDIAN", "UINT32", "INT32", "UINT32_LITTLE_ENDIAN",
  "INT32_LITTLE_ENDIAN", "UINT64", "INT64", "UINT64_LITTLE_ENDIAN",
  "INT64_LITTLE_ENDIAN", "BLOB", "BYTE", "CODETABLE", "SMART_TABLE",
  "DICTIONARY", "COMPLEX_CODETABLE", "LOOKUP", "ALIAS", "UNALIAS", "META",
  "POS", "INTCONST", "TRANS", "FLAGBIT", "CONCEPT", "GETENV", "HASH_ARRAY",
  "CONCEPT_NOFAIL", "NIL", "DUMMY", "MODIFY", "READ_ONLY", "STRING_TYPE",
  "LONG_TYPE", "DOUBLE_TYPE", "NO_COPY", "DUMP", "JSON", "XML", "NO_FAIL",
  "EDITION_SPECIFIC", "OVERRIDE", "HIDDEN", "CAN_BE_MISSING", "MISSING",
  "CONSTRAINT", "COPY_OK", "COPY_AS_INT", "COPY_IF_CHANGING_EDITION",
  "WHEN", "SET", "SET_NOFAIL", "WRITE", "APPEND", "PRINT", "EXPORT",
  "REMOVE", "RENAME", "SKIP", "PAD", "SECTION_PADDING", "MESSAGE",
  "MESSAGE_COPY", "PADTO", "PADTOEVEN", "PADTOMULTIPLE", "G1_HALF_BYTE",
  "G1_MESSAGE_LENGTH", "G1_SECTION4_LENGTH", "SECTION_LENGTH", "LENGTH",
  "FLAG", "ITERATOR", "NEAREST", "BOX", "KSEC", "ASSERT", "SUBSTR", "CASE",
  "SWITCH", "DEFAULT", "EQ", "NE", "GE", "LE", "LT", "GT", "BIT", "BITOFF",
  "AND", "OR", "NOT", "IS", "ISNOT", "IDENT", "STRING", "INTEGER", "FLOAT",
  "','", "';'", "'['", "']'", "'('", "')'", "'='", "'.'", "'{'", "'}'",
  "':'", "'-'", "'^'", "'*'", "'/'", "'%'", "'+'", "$accept", "all",
  "empty", "dvalues", "svalues", "integer_array", "instructions",
  "instruction", "semi", "argument_list", "arguments", "argument",
  "simple", "if_block", "when_block", "set", "set_list", "default",
  "flags", "flag_list", "flag", "list_block", "while_block",
  "trigger_block", "concept_block", "concept_list", "hash_array_list",
  "hash_array_block", "case_list", "case_value", "switch_block",
  "concept_value", "concept_conditions", "concept_condition",
  "hash_array_value", "string_or_ident", "atom", "power", "factor", "term",
  "condition", "conjunction", "disjunction", "expression", "rule",
  "rule_entry", "rule_entries", "fact", "conditional_rule", "rules", YY_NULLPTR
};

static const char *
grib_yysymbol_name (grib_yysymbol_kind_t grib_yysymbol)
{
  return grib_yytname[grib_yysymbol];
}
#endif

#define YYPACT_NINF (-655)

#define grib_yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-9)

#define grib_yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const grib_yytype_int16 grib_yypact[] =
{
    1398,  -655,  -120,   -81,     7,    64,   -68,    45,    40,   127,
     146,   160,   166,    34,   161,   174,    81,   212,   217,   238,
     252,   256,   258,   262,   264,   267,   269,   275,   277,   285,
     287,   300,   283,   306,   316,   313,   327,   332,   335,   336,
     357,   358,   368,   370,   371,   372,   373,   375,   361,   376,
     377,    23,    29,    39,   378,   757,   380,   374,   385,   386,
     383,   393,   394,   397,   398,   409,   410,   413,   414,   415,
     422,   424,   425,   427,   420,   421,     3,   423,   426,   -74,
     560,  -655,  -655,  1503,   430,  -655,  -655,  -655,  -655,  -655,
    -655,   281,    80,  -655,  -655,  -655,  -655,    14,  -655,  -655,
    -655,  -655,   757,   757,   437,   436,   438,   440,   442,   757,
     443,   124,   429,   448,  -655,  -655,   757,   -40,   449,   450,
     429,   429,   429,   429,   429,   429,   429,   429,   429,   429,
     429,   429,   429,   429,   447,   452,   -72,   451,   454,   455,
     173,   433,   -50,  -655,   460,   461,   467,   -56,   136,    48,
     444,   757,   468,   469,  -655,   458,  -655,   474,  -655,   196,
     475,   476,   478,   479,  -655,  -655,   480,   481,   757,   482,
    -655,  -655,  -655,   757,   187,  -655,  -655,  -655,   473,   229,
     477,  -655,   -55,   -33,   165,   487,   495,  -655,   506,  -655,
     499,   444,   493,   444,   500,   502,   503,  -655,   511,   513,
     515,   516,   512,   520,   523,   757,   757,   757,   524,    49,
     -38,   525,   -13,   527,  -655,   548,     8,   555,  1716,  -655,
    -655,   514,   549,   550,  -655,   551,   552,  -655,   561,   553,
    -655,   559,   562,   568,   563,   569,  -655,  -655,   570,   573,
     757,   757,  -655,   444,   444,   574,   575,   757,   557,   444,
     579,   578,   444,   444,   444,   444,   444,   444,   444,   444,
     444,   444,   444,   444,   444,   444,   757,   581,   582,   583,
     757,   584,   585,   558,   593,   594,   588,   598,   757,    82,
     757,   599,   601,   281,   602,    80,   604,   605,   281,   369,
    -655,  -655,   600,   497,   757,   603,   606,   607,   609,   757,
     610,   619,   620,   621,   235,   165,   271,   613,  -655,  -655,
     757,   -76,   -76,   187,   187,   187,   187,   187,   187,   466,
     466,   466,   466,   466,   466,   466,   466,   757,   757,   622,
     757,  -655,   616,  -655,   757,   757,   757,   617,   618,   623,
     624,   757,   757,   757,  -655,   625,   626,   757,   629,   630,
     631,   632,   636,   580,   627,   642,   580,   627,   627,   757,
     628,   444,   627,   757,    17,  -655,   595,   637,   647,   648,
     757,   757,   644,   645,  -655,   658,   663,   652,   260,   661,
    -655,  -655,  -655,   673,   665,   668,   429,  -655,   683,   684,
    -655,  -655,  -655,  -655,  -655,  -655,  -655,  -655,  -655,  -655,
    -655,  -655,  -655,  -655,   681,   686,   689,   690,   687,   693,
     695,   444,   692,  -655,   757,   705,   444,   289,   444,   697,
      58,   163,   -91,   703,   -80,    59,   195,   -84,  -655,  -655,
    -655,  -655,  -655,  -655,  -655,  -655,  -655,  -655,  -655,  -655,
    -655,  -655,  -655,  -655,   704,  -655,   -52,  -655,   123,  -655,
    -655,   709,   711,   716,   718,   713,   721,   723,    62,   719,
     725,   727,  -655,   726,  -655,  -655,  -655,  -655,  -655,  -655,
    -655,  -655,  -655,  -655,  -655,   -55,   -55,   -33,   -33,   -33,
     -33,   -33,   -33,   165,   487,   720,   728,   732,   734,   735,
     736,   741,   746,   747,   748,   742,   743,   744,  -655,   724,
     752,   757,   757,   757,   757,   757,  -655,   -12,   740,   749,
     627,  -655,    92,   751,   758,   763,   429,  -655,   760,   767,
     768,  1611,  1716,   198,   213,  1716,   429,   429,   429,   429,
    1716,   429,   444,   223,   231,   429,   243,   757,   766,   429,
     895,   774,  -655,   755,   775,   777,  -655,  -655,  -655,   -64,
    -655,   429,   298,   771,   756,   281,   444,   786,   444,   790,
     780,   793,   281,   444,   369,   794,   795,   348,   770,  -655,
     -46,   -19,  -655,  -655,  -655,  -655,  -655,   796,   797,   798,
    -655,  -655,   799,   800,  -655,   787,  -655,   444,  -655,  -655,
    -655,   791,   801,   429,   757,  -655,  -655,  -655,   819,   792,
     802,   803,   805,   806,   808,   804,  -655,   679,  -655,  -655,
    -655,  -655,  -655,   429,   444,  -655,   809,   811,     9,   807,
     -77,   812,   813,   757,   444,   757,   444,   814,   444,   444,
     444,   444,   815,   444,  -655,   757,   444,   757,   444,   444,
     757,   444,   429,   757,   429,   444,   757,   429,   757,   444,
     429,   757,   322,   444,   444,   820,   827,   281,    89,    20,
    -655,   828,  -655,   830,   281,    91,    26,  -655,  -655,   825,
     826,   770,   -59,  -655,   832,  -655,   817,  -655,   829,   831,
     114,   837,   838,  -655,  -655,   757,   757,  -655,   429,   757,
     -78,  -655,  1716,   444,   444,   444,   429,   429,  -655,   580,
     839,   444,  -655,  1716,   -77,   242,   925,  -655,  -655,   928,
     836,  -655,   840,  -655,  -655,  -655,  -655,  -655,  -655,  -655,
    -655,   841,  -655,   842,  -655,  -655,   843,  -655,   444,   844,
     613,   -63,  -655,   845,   444,   847,  -655,   444,   848,  -655,
    -655,  -655,  -655,   810,   846,    41,   326,   833,   444,   851,
     852,    75,   857,   849,   444,   757,   757,   832,   979,   770,
    -655,  -655,  -655,  -655,   858,  -655,   860,   861,   859,   863,
     444,   615,   853,  -655,  -655,   856,  -655,  -655,  -655,   444,
     444,   244,  -655,  -655,   854,   862,   429,   429,   429,   429,
     429,  -655,   429,   865,  -655,   429,  -655,   444,  -655,   429,
     866,   138,   444,   871,   872,   281,  -655,   873,   154,   444,
     875,   281,  -655,  -655,  -655,   868,   832,   874,   876,   877,
    -655,  -655,  -655,  1716,  1266,  -655,  -655,  -655,   879,  1716,
    1716,   444,   444,   444,   444,   444,   444,   881,   444,  -655,
     444,   888,   889,   444,  -655,   890,   891,   128,   893,   899,
     444,  -655,   900,   179,   348,  -655,  -655,  -655,  -655,  -655,
     892,  -655,   894,   896,  -655,  -655,  -655,  -655,  -655,  -655,
     897,  -655,  -655,   902,   182,  -655,   898,   189,   444,   903,
     904,  -655,   190,   444,   -54,  -655,  -655,  -655,   444,   444,
     901,   444,   908,   914,   444,  -655,   444,   444,   915,   444,
    -655,  -655,  -655,  -655,   913,  -655,   917,   918,  -655,  -655,
    -655,   920,  -655,   444,   444,   444,   444,  -655,  -655,  -655,
    -655
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const grib_yytype_int16 grib_yydefact[] =
{
       0,     7,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   129,   133,   140,     0,     8,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     2,     5,    17,     0,    22,    28,    23,    24,    25,
      26,     3,     4,    27,    29,   196,   198,   271,   269,   263,
     264,     6,     0,     0,     0,     0,     0,     0,     0,     8,
       0,     8,     8,     0,    68,    69,     0,     8,     0,     0,
       8,     8,     8,     8,     8,     8,     8,     8,     8,     8,
       8,     8,     8,     8,     0,     0,     0,     0,     0,     0,
       0,   110,     0,    88,     0,     0,     0,     0,     0,     0,
       8,     0,     0,     0,   128,     0,   132,     0,   137,     0,
       0,     0,     0,     0,   225,   226,     0,     0,     0,   219,
     221,   223,   224,     0,     0,    32,   119,    33,    34,   222,
     232,   238,   247,   257,   259,   261,   262,    36,     0,   266,
       0,     8,     0,     8,     0,     0,     0,    95,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     8,     0,     0,
       0,     0,     8,     0,     1,     0,     0,     0,    20,    18,
      21,     0,     0,     0,   197,     0,     0,   199,     0,     0,
     272,     0,     0,     0,     0,     0,   107,   108,     0,     0,
       8,     8,   152,     8,     8,     0,     0,     0,     0,     8,
       0,     0,     8,     8,     8,     8,     8,     8,     8,     8,
       8,     8,     8,     8,     8,     8,     8,     0,     0,     0,
       8,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       8,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     154,   122,     0,     0,     0,     0,     0,     0,     0,     8,
       0,     0,     0,     0,     0,   256,     0,     0,   222,   228,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       8,   106,     0,   105,     8,     8,     8,     0,     0,     0,
       0,     8,     8,     8,    99,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     8,     0,     0,     0,    19,     0,     0,     0,     0,
       0,     0,     0,     0,   136,     0,     0,     0,     0,     0,
     153,    42,    47,     0,     0,     0,     8,    70,     0,     0,
      72,    71,    74,    73,    76,    75,    78,    77,    80,    79,
      82,    81,    84,    83,     0,     0,     0,     0,     0,     0,
       0,     8,     0,   112,     8,     0,     8,     0,     8,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   159,   170,
     158,   171,   172,   173,   161,   160,   162,   164,   163,   165,
     166,   167,   168,   169,   155,   156,     0,   123,     0,   124,
     127,   131,   135,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   229,     0,   227,    35,   219,   254,   255,   231,
     236,   237,   233,   234,   235,   246,   245,   249,   253,   251,
     252,   250,   248,   258,   260,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   121,     0,
       0,     8,     8,     8,     8,     8,    15,     0,     0,     0,
     212,   265,     0,     0,     0,     0,     8,    92,     0,     0,
       0,     0,     0,     8,     8,     0,     8,     8,     8,     8,
       0,     8,     8,     8,     8,     8,     8,     0,     0,     8,
       0,     0,   109,     0,     0,     0,    89,    11,     9,     0,
      90,     8,     0,     0,     0,     0,     8,     0,     8,     0,
       0,     0,     0,     8,     0,     0,     0,     0,     0,    13,
       0,     0,   130,   134,   139,   138,   118,     0,     0,     0,
     244,   239,     0,     0,   230,     0,   100,     8,   101,   102,
     103,     0,     0,     8,     0,   115,   116,   117,     0,     0,
       0,     0,     0,     0,     0,     0,   217,     0,   209,   213,
     216,   208,   210,     8,     8,   211,     0,     0,     0,     0,
     267,     0,     0,     8,     8,     8,     8,     0,     8,     8,
       8,     8,     0,     8,    86,     8,     8,     8,     8,     8,
       8,     8,     8,     8,     8,     8,     8,     8,     8,     8,
       8,     8,     0,     8,     8,     0,     0,     0,     0,     0,
     177,     0,   200,     0,     0,     0,     0,   188,   157,     0,
       0,     0,     0,    30,   145,   125,     0,   126,     0,     0,
       0,     0,     0,   120,   104,     8,     8,    96,     8,     0,
       0,   202,     0,     8,     8,     8,     8,     8,    16,     0,
       0,     8,    93,     0,     0,     0,   141,   268,   270,   143,
       0,    37,     0,    39,   176,    41,    48,    43,    51,   175,
      87,     0,    52,     0,    54,    85,     0,    49,     8,     0,
      36,     8,    60,     0,     8,     0,   111,     8,     0,    12,
      10,    91,    67,     0,     0,     0,     0,     0,     8,     0,
       0,     0,     0,     0,     8,     0,     0,   150,   146,     0,
      31,    14,   240,   241,     0,   242,     0,     0,     0,     0,
       8,     0,     0,   207,   203,     0,    44,    46,    45,     8,
       8,     0,   214,    94,     0,     0,     8,     8,     8,     8,
       8,    57,     8,     0,    56,     8,    63,     8,   113,     8,
       0,     0,     8,     0,     0,     0,   186,     0,     0,     8,
       0,     0,   194,   148,   149,     0,   151,     0,     0,     0,
      97,    98,    65,     0,     0,   174,    61,    62,     0,     0,
       0,     8,     8,     8,     8,     8,     8,     0,     8,    66,
       8,     0,     0,     8,   178,     0,     0,     0,     0,     0,
       8,   189,     0,     0,     0,   243,   218,   220,   204,   206,
       0,   215,     0,     0,    38,    40,    53,    55,    50,    59,
       0,    64,   114,     0,     0,   182,     0,     0,     8,     0,
       0,   191,     0,     8,     0,   205,   142,   144,     8,     8,
       0,     8,     0,     0,     8,   187,     8,     8,     0,     8,
     195,   147,    58,   180,     0,   179,     0,     0,   185,   201,
     190,     0,   193,     8,     8,     8,     8,   181,   184,   183,
     192
};

/* YYPGOTO[NTERM-NUM].  */
static const grib_yytype_int16 grib_yypgoto[] =
{
    -655,  -655,     5,   310,  -655,  -349,     0,  -655,  -654,    86,
    -308,  -180,  -655,  -655,  -655,  -445,   176,   547,   211,  -655,
     486,  -655,  -655,  -655,  -655,  -268,   772,  -655,  -655,   366,
    -655,   -85,  -346,  -655,   -88,   -83,   884,   164,   137,   167,
    -165,   731,  -655,   -94,  -655,    10,   445,  -655,  -655,   963
};

/* YYDEFGOTO[NTERM-NUM].  */
static const grib_yytype_int16 grib_yydefgoto[] =
{
       0,    80,   290,   549,   571,   507,   619,    83,   674,   176,
     177,   178,    84,    85,    86,   671,   672,   243,   291,   444,
     445,    87,    88,    89,    90,    91,    92,    93,   690,   691,
      94,    95,   509,   510,    96,   179,   180,   181,   182,   183,
     184,   185,   186,   187,    97,   620,   621,    99,   100,   101
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const grib_yytype_int16 grib_yytable[] =
{
      82,   568,   465,   305,   227,    81,   224,   512,   231,   232,
      98,   513,   514,    57,   102,   422,   518,   757,   228,   793,
     427,   208,   246,   565,   566,   344,   208,   208,   565,   566,
     565,   566,   689,   167,   772,   221,   222,    78,   223,   348,
     349,   350,   221,   222,    78,   223,   225,   226,   556,   229,
     466,   170,   212,   103,   268,   563,   269,   292,   107,   558,
     175,   773,   213,   351,   314,   315,   652,   385,   161,   162,
     163,   348,   349,   350,   352,   653,   276,   289,   281,   307,
     758,   282,   283,   219,   652,   901,   567,   277,   316,   317,
     318,   308,   247,   675,   356,   351,   241,   248,   416,   418,
     357,   161,   162,   163,    57,   816,   352,    98,   319,   164,
     165,   676,   345,   320,   175,   355,   242,   242,   605,   359,
     677,   606,   242,   241,   360,   242,   242,   242,   242,   242,
     242,   242,   242,   242,   242,   242,   242,   242,   242,   209,
     229,   104,   164,   165,   364,   705,   221,   222,    78,   223,
     154,   166,   221,   222,    78,   223,   156,   155,   167,   748,
     114,   115,   483,   157,   609,   754,   158,   221,   222,    78,
     223,   108,   168,   159,   109,   169,   170,   171,   172,   515,
     802,   353,   286,   173,   166,   287,   288,   354,   552,   559,
     174,   167,   579,   553,   560,   238,   105,   580,   106,   449,
     450,   221,   222,    78,   223,   168,   225,   226,   169,   170,
     171,   172,   175,   118,   809,   119,   173,   242,   365,   746,
     417,   752,   605,   174,   747,   610,   753,   759,   467,   468,
     308,   308,   308,   308,   308,   308,   308,   308,   308,   308,
     308,   308,   308,   308,   764,   175,   175,   164,   165,   765,
     569,   547,   548,   500,   221,   222,    78,   223,   240,   110,
     241,   161,   162,   163,   348,   349,   350,   878,   842,   519,
     284,   175,   111,   843,   285,   175,   520,   355,   321,   322,
     323,   324,   325,   326,   849,   175,   112,   659,   351,   850,
     161,   162,   163,   346,   666,   116,   167,   554,   113,   352,
     117,   555,   164,   165,   175,   221,   222,    78,   223,   273,
     274,   175,   890,   169,   170,   171,   172,   891,   883,   893,
     898,   173,   297,   298,   894,   899,   379,   380,   174,   561,
     623,   164,   165,   562,   241,   175,   227,   224,   120,   175,
     175,   175,   224,   121,   166,   625,   175,   175,   175,   241,
     781,   167,   404,   311,   312,   635,   408,   642,   644,   241,
     647,   460,   461,   637,   122,   168,   419,   241,   169,   170,
     171,   172,   428,   166,   605,   640,   173,   828,   123,   241,
     167,   771,   124,   174,   125,   455,   526,   527,   126,   745,
     127,   242,   463,   128,   168,   129,   751,   169,   170,   171,
     172,   130,   331,   131,   333,   173,   462,   221,   222,    78,
     223,   132,   174,   133,   688,   135,   486,   547,   548,   175,
     488,   489,   490,   429,   655,   656,   134,   495,   496,   497,
     565,   566,   430,   431,   432,   433,   434,   435,   136,   759,
     436,   437,   137,   438,   439,   138,   440,   441,   442,   443,
     739,   740,   803,   804,   381,   382,   475,   476,   140,   139,
     387,   141,   142,   390,   391,   392,   393,   394,   395,   396,
     397,   398,   399,   400,   401,   402,   403,   469,   470,   471,
     472,   473,   474,   143,   144,   161,   162,   163,   477,   478,
     479,   480,   481,   482,   145,   151,   146,   147,   148,   149,
     544,   150,   152,   153,   160,   189,   175,   175,   175,   175,
     175,   190,   191,   700,   188,   192,   161,   162,   163,   193,
     194,   242,   622,   195,   196,   627,   164,   165,   242,   242,
     632,   242,   242,   242,   242,   197,   242,   847,   242,   242,
     242,   242,   198,   853,   242,   199,   200,   201,   202,   730,
     203,   204,   730,   205,   206,   207,   242,   164,   165,   210,
     214,   220,   211,   233,   234,   241,   235,   236,   166,   237,
     275,   239,   517,   447,   224,   167,   245,   250,   251,   266,
     267,   224,   271,   272,   289,   270,   295,   600,   601,   602,
     603,   604,   169,   170,   171,   172,   278,   279,   242,   166,
     173,   280,   296,   310,   293,   294,   167,   174,   327,   299,
     300,   355,   301,   302,   303,   304,   306,   328,   242,   313,
     168,   332,   542,   169,   170,   171,   172,   546,   175,   550,
     175,   173,   329,   330,   334,   448,   335,   336,   174,   337,
     175,   338,   175,   339,   340,   175,   341,   242,   175,   242,
     366,   175,   242,   175,   342,   242,   175,   343,   347,   244,
     224,   813,   814,   358,   249,   362,   224,   252,   253,   254,
     255,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   212,   363,   386,   411,   367,   213,   368,   369,   371,
     175,   175,   775,   242,   372,   370,   375,   373,   161,   162,
     163,   242,   242,   374,   376,   377,   378,   383,   506,   710,
     384,   712,   388,   389,   405,   406,   407,   409,   410,   412,
     413,   721,   414,   723,   415,   420,   726,   421,   423,   729,
     425,   426,   733,   354,   735,   446,   456,   738,   451,   164,
     165,   452,   453,   634,   454,   457,   458,   459,   464,   487,
     491,   492,   485,   508,   516,   823,   493,   494,   570,   361,
     498,   499,   224,   501,   502,   503,   504,   660,   224,   662,
     505,   768,   769,   511,   667,   357,   161,   162,   163,   353,
     356,   166,   521,   522,   523,   161,   162,   163,   167,   524,
     525,   242,   242,   242,   242,   242,   528,   242,   684,   529,
     242,   531,   168,   530,   242,   169,   170,   171,   172,   533,
     534,   699,   536,   173,   535,   537,   538,   164,   165,   540,
     174,   541,   539,   858,   860,   702,   164,   165,   543,   862,
     863,   545,   551,   557,   564,   711,   572,   713,   573,   715,
     716,   717,   718,   574,   720,   575,   585,   722,   576,   724,
     725,   577,   727,   578,   581,   582,   732,   583,   587,   166,
     736,   584,   598,   586,   741,   742,   167,   591,   166,   588,
     589,   590,   592,   593,   594,   167,   607,   595,   596,   597,
     168,   649,   658,   169,   170,   171,   172,   599,   608,   168,
     611,   173,   169,   170,   171,   172,   613,   612,   174,   615,
     643,   673,   616,   617,   776,   777,   778,   174,   648,   657,
     650,   651,   783,   661,   161,   162,   163,   663,   664,   665,
     669,   670,   683,   678,   679,   685,   680,   681,   682,   689,
     692,   784,   698,   532,   785,   686,   800,   693,   694,   791,
     695,   696,   794,   697,   761,   796,   706,   703,   798,   704,
     743,   708,   709,   714,   719,   164,   165,   744,   749,   806,
     750,   755,   756,   760,   762,   812,   763,   766,   767,   786,
     782,   805,   801,   787,   788,   789,   790,   807,   808,   792,
     795,   822,   797,   799,   810,   815,   817,   811,   818,   819,
     826,   827,   829,   824,   820,   825,   841,   166,   821,   837,
     830,   845,   846,   848,   167,   852,   854,   870,   839,   855,
     861,   856,   857,   844,   873,   874,   876,   877,   168,   879,
     851,   169,   170,   171,   172,   880,   882,   904,   892,   646,
     884,   885,   888,   886,   906,   887,   174,   889,   896,   897,
     907,   911,   864,   865,   866,   867,   868,   869,   913,   871,
     668,   872,   914,   915,   875,   916,   774,   424,   309,   484,
     230,   881,     0,   614,     0,   707,     0,     0,     0,     0,
     624,   626,     0,   628,   629,   630,   631,     0,   633,     0,
     636,   638,   639,   641,     0,     0,   645,     0,     0,   895,
       0,     0,     0,     0,   900,     0,     0,     0,   654,   902,
     903,     0,   905,     0,     0,   908,     0,   909,   910,     0,
     912,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   917,   918,   919,   920,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     687,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     701,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   728,
       0,   731,     0,     0,   734,     0,     0,   737,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   770,     0,     0,     0,     0,
       0,     0,     0,   779,   780,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     215,     3,     0,     0,     4,     5,     6,     7,     8,     9,
      10,    11,    12,    13,     0,     0,     0,     0,     0,     0,
       0,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,     0,    35,    36,    37,    38,    39,    40,    41,
      42,    43,    44,     0,    45,    46,     0,     0,    47,     0,
       0,     0,     0,   831,   832,   833,   834,   835,     0,   836,
       0,     0,   838,     0,     0,     0,   840,    48,    49,    50,
      51,    52,    53,    54,    55,    56,     0,    58,    59,    60,
      61,    62,    63,    64,    65,    66,    67,    68,     0,    69,
      70,    71,    72,    73,    74,     0,     0,    75,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   216,     0,     0,   217,     0,     0,    -8,     1,
       0,     0,     2,     3,     0,   859,     4,     5,     6,     7,
       8,     9,    10,    11,    12,    13,     0,     0,     0,     0,
       0,     0,     0,    14,    15,    16,    17,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,     0,    35,    36,    37,    38,    39,
      40,    41,    42,    43,    44,     0,    45,    46,     0,     0,
      47,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    48,
      49,    50,    51,    52,    53,    54,    55,    56,    57,    58,
      59,    60,    61,    62,    63,    64,    65,    66,    67,    68,
       0,    69,    70,    71,    72,    73,    74,   215,     3,    75,
       0,     4,     5,     6,     7,     8,     9,    10,    11,    12,
      13,     0,     0,     0,    76,    77,    78,    79,    14,    15,
      16,    17,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,     0,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
       0,    45,    46,     0,     0,    47,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    48,    49,    50,    51,    52,    53,
      54,    55,    56,     0,    58,    59,    60,    61,    62,    63,
      64,    65,    66,    67,    68,     0,    69,    70,    71,    72,
      73,    74,     0,     0,    75,   215,     3,     0,     0,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,   216,
       0,     0,   217,     0,   218,     0,    14,    15,    16,    17,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,     0,    35,    36,
      37,    38,    39,    40,    41,    42,    43,    44,     0,    45,
      46,     0,     0,    47,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    48,    49,    50,    51,    52,    53,    54,    55,
      56,    57,    58,    59,    60,    61,    62,    63,    64,    65,
      66,    67,    68,     0,    69,    70,    71,    72,    73,    74,
     215,     3,    75,     0,     4,     5,     6,     7,     8,     9,
      10,    11,    12,    13,     0,     0,     0,   618,     0,     0,
     217,    14,    15,    16,    17,    18,    19,    20,    21,    22,
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,     0,    35,    36,    37,    38,    39,    40,    41,
      42,    43,    44,     0,    45,    46,     0,     0,    47,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    48,    49,    50,
      51,    52,    53,    54,    55,    56,     0,    58,    59,    60,
      61,    62,    63,    64,    65,    66,    67,    68,     0,    69,
      70,    71,    72,    73,    74,     0,     0,    75,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   216,     0,     0,   217
};

static const grib_yytype_int16 grib_yycheck[] =
{
       0,   446,   310,   168,    92,     0,    91,   356,   102,   103,
       0,   357,   358,    90,   134,   283,   362,   671,     4,    82,
     288,    18,   116,    82,    83,   205,    18,    18,    82,    83,
      82,    83,   110,   109,   112,   126,   127,   128,   129,    22,
      23,    24,   126,   127,   128,   129,   126,   127,   139,   126,
     126,   127,   126,   134,   126,   139,   128,   151,   126,   139,
      55,   139,   136,    46,   119,   120,   130,   247,    19,    20,
      21,    22,    23,    24,    57,   139,   126,   140,   134,   173,
     139,   137,   138,    83,   130,   139,   138,   137,   143,   144,
     145,   174,   132,   139,   132,    46,   136,   137,   278,   279,
     138,    19,    20,    21,    90,   759,    57,    97,   141,    60,
      61,   130,   206,   146,   109,   209,   111,   112,   130,   132,
     139,   133,   117,   136,   137,   120,   121,   122,   123,   124,
     125,   126,   127,   128,   129,   130,   131,   132,   133,   136,
     126,   134,    60,    61,   136,   136,   126,   127,   128,   129,
     127,   102,   126,   127,   128,   129,   127,   134,   109,   139,
     126,   127,   327,   134,   510,   139,   127,   126,   127,   128,
     129,   126,   123,   134,   134,   126,   127,   128,   129,   359,
     139,   132,   134,   134,   102,   137,   138,   138,   130,   130,
     141,   109,   130,   135,   135,   109,   132,   135,   134,   293,
     294,   126,   127,   128,   129,   123,   126,   127,   126,   127,
     128,   129,   207,   132,   139,   134,   134,   212,   218,   130,
     138,   130,   130,   141,   135,   133,   135,   672,   311,   312,
     313,   314,   315,   316,   317,   318,   319,   320,   321,   322,
     323,   324,   325,   326,   130,   240,   241,    60,    61,   135,
     127,   128,   129,   347,   126,   127,   128,   129,   134,   132,
     136,    19,    20,    21,    22,    23,    24,   139,   130,   363,
     134,   266,   126,   135,   138,   270,   370,   371,   113,   114,
     115,   116,   117,   118,   130,   280,   126,   555,    46,   135,
      19,    20,    21,   207,   562,   134,   109,   134,   132,    57,
     126,   138,    60,    61,   299,   126,   127,   128,   129,   136,
     137,   306,   130,   126,   127,   128,   129,   135,   139,   130,
     130,   134,   126,   127,   135,   135,   240,   241,   141,   134,
     132,    60,    61,   138,   136,   330,   424,   422,   126,   334,
     335,   336,   427,   126,   102,   132,   341,   342,   343,   136,
     699,   109,   266,   124,   125,   132,   270,   537,   538,   136,
     540,   126,   127,   132,   126,   123,   280,   136,   126,   127,
     128,   129,     3,   102,   130,   132,   134,   133,   126,   136,
     109,   689,   126,   141,   126,   299,   126,   127,   126,   657,
     126,   386,   306,   126,   123,   126,   664,   126,   127,   128,
     129,   126,   191,   126,   193,   134,   135,   126,   127,   128,
     129,   126,   141,   126,   594,   132,   330,   128,   129,   414,
     334,   335,   336,    54,   126,   127,   126,   341,   342,   343,
      82,    83,    63,    64,    65,    66,    67,    68,   132,   884,
      71,    72,   126,    74,    75,   132,    77,    78,    79,    80,
     128,   129,   126,   127,   243,   244,   319,   320,   126,   132,
     249,   126,   126,   252,   253,   254,   255,   256,   257,   258,
     259,   260,   261,   262,   263,   264,   265,   313,   314,   315,
     316,   317,   318,   126,   126,    19,    20,    21,   321,   322,
     323,   324,   325,   326,   126,   134,   126,   126,   126,   126,
     414,   126,   126,   126,   126,   131,   501,   502,   503,   504,
     505,   126,   126,   607,   134,   132,    19,    20,    21,   126,
     126,   516,   522,   126,   126,   525,    60,    61,   523,   524,
     530,   526,   527,   528,   529,   126,   531,   805,   533,   534,
     535,   536,   132,   811,   539,   132,   132,   132,   126,   643,
     126,   126,   646,   126,   134,   134,   551,    60,    61,   136,
       0,   131,   136,   126,   128,   136,   128,   127,   102,   127,
     137,   128,   361,    76,   659,   109,   128,   128,   128,   132,
     128,   666,   128,   128,   140,   134,   128,   501,   502,   503,
     504,   505,   126,   127,   128,   129,   136,   136,   593,   102,
     134,   134,   128,   130,   136,   136,   109,   141,   121,   134,
     134,   705,   134,   134,   134,   134,   134,   122,   613,   142,
     123,   128,   411,   126,   127,   128,   129,   416,   623,   418,
     625,   134,   126,   134,   134,   138,   134,   134,   141,   128,
     635,   128,   637,   128,   128,   640,   134,   642,   643,   644,
     136,   646,   647,   648,   134,   650,   651,   134,   134,   112,
     745,   755,   756,   138,   117,   138,   751,   120,   121,   122,
     123,   124,   125,   126,   127,   128,   129,   130,   131,   132,
     133,   126,   134,   126,   126,   136,   136,   136,   136,   136,
     685,   686,   692,   688,   135,   134,   133,   135,    19,    20,
      21,   696,   697,   135,   135,   135,   133,   133,   128,   623,
     135,   625,   133,   135,   133,   133,   133,   133,   133,   126,
     126,   635,   134,   637,   126,   126,   640,   126,   126,   643,
     126,   126,   646,   138,   648,   135,   126,   651,   135,    60,
      61,   135,   135,   532,   135,   126,   126,   126,   135,   133,
     133,   133,   130,   126,   126,   140,   133,   133,   448,   212,
     135,   135,   847,   134,   134,   134,   134,   556,   853,   558,
     134,   685,   686,   131,   563,   138,    19,    20,    21,   132,
     132,   102,   138,   138,   126,    19,    20,    21,   109,   126,
     138,   786,   787,   788,   789,   790,   135,   792,   587,   126,
     795,   133,   123,   138,   799,   126,   127,   128,   129,   126,
     126,   132,   126,   134,   133,   126,   126,    60,    61,   126,
     141,   126,   135,   823,   824,   614,    60,    61,   136,   829,
     830,   126,   135,   130,   130,   624,   127,   626,   127,   628,
     629,   630,   631,   127,   633,   127,   126,   636,   135,   638,
     639,   130,   641,   130,   135,   130,   645,   130,   126,   102,
     649,   135,   138,   135,   653,   654,   109,   126,   102,   135,
     135,   135,   126,   126,   126,   109,   136,   135,   135,   135,
     123,   126,   126,   126,   127,   128,   129,   135,   139,   123,
     139,   134,   126,   127,   128,   129,   133,   139,   141,   139,
     134,   131,   135,   135,   693,   694,   695,   141,   134,   138,
     135,   134,   701,   127,    19,    20,    21,   127,   138,   126,
     126,   126,   135,   127,   127,   134,   128,   128,   128,   110,
     138,     6,   128,   386,     6,   134,   126,   135,   135,   728,
     135,   135,   731,   135,   127,   734,   139,   138,   737,   138,
     130,   139,   139,   139,   139,    60,    61,   130,   130,   748,
     130,   136,   136,   131,   135,   754,   135,   130,   130,   133,
     131,   138,   126,   133,   133,   133,   133,   126,   126,   135,
     135,   770,   135,   135,   127,     6,   128,   138,   128,   128,
     779,   780,   138,   140,   135,   139,   130,   102,   135,   134,
     138,   130,   130,   130,   109,   130,   138,   126,   797,   135,
     131,   135,   135,   802,   126,   126,   126,   126,   123,   126,
     809,   126,   127,   128,   129,   126,   126,   126,   130,   134,
     854,   139,   135,   139,   126,   139,   141,   135,   135,   135,
     126,   126,   831,   832,   833,   834,   835,   836,   135,   838,
     564,   840,   135,   135,   843,   135,   690,   285,   174,   328,
      97,   850,    -1,   516,    -1,   620,    -1,    -1,    -1,    -1,
     523,   524,    -1,   526,   527,   528,   529,    -1,   531,    -1,
     533,   534,   535,   536,    -1,    -1,   539,    -1,    -1,   878,
      -1,    -1,    -1,    -1,   883,    -1,    -1,    -1,   551,   888,
     889,    -1,   891,    -1,    -1,   894,    -1,   896,   897,    -1,
     899,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   913,   914,   915,   916,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     593,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     613,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   642,
      -1,   644,    -1,    -1,   647,    -1,    -1,   650,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   688,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   696,   697,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
       4,     5,    -1,    -1,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    17,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,    39,    40,    41,    42,    43,
      44,    45,    -1,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56,    -1,    58,    59,    -1,    -1,    62,    -1,
      -1,    -1,    -1,   786,   787,   788,   789,   790,    -1,   792,
      -1,    -1,   795,    -1,    -1,    -1,   799,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    -1,    91,    92,    93,
      94,    95,    96,    97,    98,    99,   100,   101,    -1,   103,
     104,   105,   106,   107,   108,    -1,    -1,   111,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   126,    -1,    -1,   129,    -1,    -1,     0,     1,
      -1,    -1,     4,     5,    -1,   139,     8,     9,    10,    11,
      12,    13,    14,    15,    16,    17,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    39,    40,    41,
      42,    43,    44,    45,    -1,    47,    48,    49,    50,    51,
      52,    53,    54,    55,    56,    -1,    58,    59,    -1,    -1,
      62,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,    98,    99,   100,   101,
      -1,   103,   104,   105,   106,   107,   108,     4,     5,   111,
      -1,     8,     9,    10,    11,    12,    13,    14,    15,    16,
      17,    -1,    -1,    -1,   126,   127,   128,   129,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    39,    40,    41,    42,    43,    44,    45,    -1,
      47,    48,    49,    50,    51,    52,    53,    54,    55,    56,
      -1,    58,    59,    -1,    -1,    62,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    -1,    91,    92,    93,    94,    95,    96,
      97,    98,    99,   100,   101,    -1,   103,   104,   105,   106,
     107,   108,    -1,    -1,   111,     4,     5,    -1,    -1,     8,
       9,    10,    11,    12,    13,    14,    15,    16,    17,   126,
      -1,    -1,   129,    -1,   131,    -1,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      39,    40,    41,    42,    43,    44,    45,    -1,    47,    48,
      49,    50,    51,    52,    53,    54,    55,    56,    -1,    58,
      59,    -1,    -1,    62,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,    98,
      99,   100,   101,    -1,   103,   104,   105,   106,   107,   108,
       4,     5,   111,    -1,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    17,    -1,    -1,    -1,   126,    -1,    -1,
     129,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,    39,    40,    41,    42,    43,
      44,    45,    -1,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56,    -1,    58,    59,    -1,    -1,    62,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    -1,    91,    92,    93,
      94,    95,    96,    97,    98,    99,   100,   101,    -1,   103,
     104,   105,   106,   107,   108,    -1,    -1,   111,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   126,    -1,    -1,   129
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const grib_yytype_uint8 grib_yystos[] =
{
       0,     1,     4,     5,     8,     9,    10,    11,    12,    13,
      14,    15,    16,    17,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,    39,    40,
      41,    42,    43,    44,    45,    47,    48,    49,    50,    51,
      52,    53,    54,    55,    56,    58,    59,    62,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    96,    97,    98,    99,   100,   101,   103,
     104,   105,   106,   107,   108,   111,   126,   127,   128,   129,
     148,   149,   153,   154,   159,   160,   161,   168,   169,   170,
     171,   172,   173,   174,   177,   178,   181,   191,   192,   194,
     195,   196,   134,   134,   134,   132,   134,   126,   126,   134,
     132,   126,   126,   132,   126,   127,   134,   126,   132,   134,
     126,   126,   126,   126,   126,   126,   126,   126,   126,   126,
     126,   126,   126,   126,   126,   132,   132,   126,   132,   132,
     126,   126,   126,   126,   126,   126,   126,   126,   126,   126,
     126,   134,   126,   126,   127,   134,   127,   134,   127,   134,
     126,    19,    20,    21,    60,    61,   102,   109,   123,   126,
     127,   128,   129,   134,   141,   149,   156,   157,   158,   182,
     183,   184,   185,   186,   187,   188,   189,   190,   134,   131,
     126,   126,   132,   126,   126,   126,   126,   126,   132,   132,
     132,   132,   126,   126,   126,   126,   134,   134,    18,   136,
     136,   136,   126,   136,     0,     4,   126,   129,   131,   153,
     131,   126,   127,   129,   178,   126,   127,   181,     4,   126,
     196,   190,   190,   126,   128,   128,   127,   127,   156,   128,
     134,   136,   149,   164,   164,   128,   190,   132,   137,   164,
     128,   128,   164,   164,   164,   164,   164,   164,   164,   164,
     164,   164,   164,   164,   164,   164,   132,   128,   126,   128,
     134,   128,   128,   136,   137,   137,   126,   137,   136,   136,
     134,   134,   137,   138,   134,   138,   134,   137,   138,   140,
     149,   165,   190,   136,   136,   128,   128,   126,   127,   134,
     134,   134,   134,   134,   134,   187,   134,   190,   182,   183,
     130,   124,   125,   142,   119,   120,   143,   144,   145,   141,
     146,   113,   114,   115,   116,   117,   118,   121,   122,   126,
     134,   165,   128,   165,   134,   134,   134,   128,   128,   128,
     128,   134,   134,   134,   158,   190,   156,   134,    22,    23,
      24,    46,    57,   132,   138,   190,   132,   138,   138,   132,
     137,   164,   138,   134,   136,   153,   136,   136,   136,   136,
     134,   136,   135,   135,   135,   133,   135,   135,   133,   156,
     156,   165,   165,   133,   135,   158,   126,   165,   133,   135,
     165,   165,   165,   165,   165,   165,   165,   165,   165,   165,
     165,   165,   165,   165,   156,   133,   133,   133,   156,   133,
     133,   126,   126,   126,   134,   126,   158,   138,   158,   156,
     126,   126,   172,   126,   173,   126,   126,   172,     3,    54,
      63,    64,    65,    66,    67,    68,    71,    72,    74,    75,
      77,    78,    79,    80,   166,   167,   135,    76,   138,   190,
     190,   135,   135,   135,   135,   156,   126,   126,   126,   126,
     126,   127,   135,   156,   135,   157,   126,   182,   182,   184,
     184,   184,   184,   184,   184,   185,   185,   186,   186,   186,
     186,   186,   186,   187,   188,   130,   156,   133,   156,   156,
     156,   133,   133,   133,   133,   156,   156,   156,   135,   135,
     190,   134,   134,   134,   134,   134,   128,   152,   126,   179,
     180,   131,   152,   179,   179,   158,   126,   165,   179,   190,
     190,   138,   138,   126,   126,   138,   126,   127,   135,   126,
     138,   133,   164,   126,   126,   133,   126,   126,   126,   135,
     126,   126,   165,   136,   156,   126,   165,   128,   129,   150,
     165,   135,   130,   135,   134,   138,   139,   130,   139,   130,
     135,   134,   138,   139,   130,    82,    83,   138,   162,   127,
     150,   151,   127,   127,   127,   127,   135,   130,   130,   130,
     135,   135,   130,   130,   135,   126,   135,   126,   135,   135,
     135,   126,   126,   126,   126,   135,   135,   135,   138,   135,
     156,   156,   156,   156,   156,   130,   133,   136,   139,   179,
     133,   139,   139,   133,   164,   139,   135,   135,   126,   153,
     192,   193,   153,   132,   164,   132,   164,   153,   164,   164,
     164,   164,   153,   164,   165,   132,   164,   132,   164,   164,
     132,   164,   158,   134,   158,   164,   134,   158,   134,   126,
     135,   134,   130,   139,   164,   126,   127,   138,   126,   172,
     165,   127,   165,   127,   138,   126,   172,   165,   167,   126,
     126,   162,   163,   131,   155,   139,   130,   139,   127,   127,
     128,   128,   128,   135,   165,   134,   134,   164,   158,   110,
     175,   176,   138,   135,   135,   135,   135,   135,   128,   132,
     190,   164,   165,   138,   138,   136,   139,   193,   139,   139,
     156,   165,   156,   165,   139,   165,   165,   165,   165,   139,
     165,   156,   165,   156,   165,   165,   156,   165,   164,   156,
     190,   164,   165,   156,   164,   156,   165,   164,   156,   128,
     129,   165,   165,   130,   130,   172,   130,   135,   139,   130,
     130,   172,   130,   135,   139,   136,   136,   155,   139,   162,
     131,   127,   135,   135,   130,   135,   130,   130,   156,   156,
     164,   157,   112,   139,   176,   153,   165,   165,   165,   164,
     164,   152,   131,   165,     6,     6,   133,   133,   133,   133,
     133,   165,   135,    82,   165,   135,   165,   135,   165,   135,
     126,   126,   139,   126,   127,   138,   165,   126,   126,   139,
     127,   138,   165,   190,   190,     6,   155,   128,   128,   128,
     135,   135,   165,   140,   140,   139,   165,   165,   133,   138,
     138,   164,   164,   164,   164,   164,   164,   134,   164,   165,
     164,   130,   130,   135,   165,   130,   130,   172,   130,   130,
     135,   165,   130,   172,   138,   135,   135,   135,   153,   139,
     153,   131,   153,   153,   165,   165,   165,   165,   165,   165,
     126,   165,   165,   126,   126,   165,   126,   126,   139,   126,
     126,   165,   126,   139,   163,   139,   139,   139,   135,   135,
     130,   135,   130,   130,   135,   165,   135,   135,   130,   135,
     165,   139,   165,   165,   126,   165,   126,   126,   165,   165,
     165,   126,   165,   135,   135,   135,   135,   165,   165,   165,
     165
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const grib_yytype_uint8 grib_yyr1[] =
{
       0,   147,   148,   148,   148,   148,   148,   148,   149,   150,
     150,   150,   150,   151,   151,   152,   152,   153,   153,   153,
     153,   154,   154,   154,   154,   154,   154,   154,   154,   154,
     155,   155,   156,   156,   157,   157,   158,   159,   159,   159,
     159,   159,   159,   159,   159,   159,   159,   159,   159,   159,
     159,   159,   159,   159,   159,   159,   159,   159,   159,   159,
     159,   159,   159,   159,   159,   159,   159,   159,   159,   159,
     159,   159,   159,   159,   159,   159,   159,   159,   159,   159,
     159,   159,   159,   159,   159,   159,   159,   159,   159,   159,
     159,   159,   159,   159,   159,   159,   159,   159,   159,   159,
     159,   159,   159,   159,   159,   159,   159,   159,   159,   159,
     159,   159,   159,   159,   159,   159,   159,   159,   159,   159,
     159,   159,   159,   159,   159,   159,   159,   159,   159,   159,
     159,   159,   159,   159,   159,   159,   159,   159,   159,   159,
     159,   160,   160,   160,   160,   161,   161,   161,   162,   162,
     163,   163,   164,   164,   165,   165,   166,   166,   167,   167,
     167,   167,   167,   167,   167,   167,   167,   167,   167,   167,
     167,   167,   167,   167,   168,   169,   170,   171,   171,   171,
     171,   171,   171,   171,   171,   171,   171,   171,   171,   171,
     171,   171,   171,   171,   171,   171,   172,   172,   173,   173,
     174,   174,   175,   175,   176,   177,   177,   177,   178,   178,
     178,   178,   179,   179,   180,   180,   181,   181,   182,   182,
     182,   182,   183,   183,   183,   183,   183,   183,   183,   183,
     183,   184,   184,   185,   185,   185,   185,   185,   185,   185,
     185,   185,   185,   185,   185,   186,   186,   186,   187,   187,
     187,   187,   187,   187,   187,   187,   187,   187,   188,   188,
     189,   189,   190,   191,   191,   192,   192,   193,   193,   194,
     195,   196,   196
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const grib_yytype_int8 grib_yyr2[] =
{
       0,     2,     1,     1,     1,     1,     1,     1,     0,     1,
       3,     1,     3,     1,     3,     1,     3,     1,     2,     3,
       2,     2,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     2,     1,     1,     1,     3,     1,     7,    10,     7,
      10,     7,     4,     7,     7,     7,     7,     4,     7,     7,
      10,     7,     7,    10,     7,    10,     8,     8,    12,    10,
       7,     8,     8,     8,    10,     8,     9,     7,     2,     2,
       4,     4,     4,     4,     4,     4,     4,     4,     4,     4,
       4,     4,     4,     4,     4,     7,     6,     7,     2,     5,
       5,     7,     4,     6,     7,     2,     6,     8,     8,     3,
       5,     5,     5,     5,     6,     3,     3,     3,     3,     5,
       2,     7,     4,     8,    10,     5,     5,     5,     5,     2,
       6,     4,     3,     4,     4,     6,     6,     4,     2,     1,
       5,     4,     2,     1,     5,     4,     4,     2,     5,     5,
       1,     7,    11,     7,    11,     6,     7,    11,     4,     4,
       2,     3,     1,     2,     1,     2,     1,     3,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     8,     7,     7,     6,     9,    12,
      12,    14,    10,    14,    14,    12,     8,    11,     6,     9,
      12,    10,    14,    12,     8,    11,     1,     2,     1,     2,
       6,    12,     1,     2,     4,    10,     9,     7,     5,     5,
       5,     5,     1,     2,     4,     6,     5,     5,     8,     1,
       8,     1,     1,     1,     1,     1,     1,     3,     2,     3,
       4,     3,     1,     3,     3,     3,     3,     3,     1,     4,
       6,     6,     6,     8,     4,     3,     3,     1,     3,     3,
       3,     3,     3,     3,     3,     3,     2,     1,     3,     1,
       3,     1,     1,     1,     1,     4,     2,     1,     2,     1,
       7,     1,     2
};


enum { YYENOMEM = -2 };

#define grib_yyerrok         (grib_yyerrstatus = 0)
#define grib_yyclearin       (grib_yychar = YYEMPTY)

#define YYACCEPT        goto grib_yyacceptlab
#define YYABORT         goto grib_yyabortlab
#define YYERROR         goto grib_yyerrorlab
#define YYNOMEM         goto grib_yyexhaustedlab


#define YYRECOVERING()  (!!grib_yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (grib_yychar == YYEMPTY)                                        \
      {                                                           \
        grib_yychar = (Token);                                         \
        grib_yylval = (Value);                                         \
        YYPOPSTACK (grib_yylen);                                       \
        grib_yystate = *grib_yyssp;                                         \
        goto grib_yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        grib_yyerror (YY_("syntax error: cannot back up")); \
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
  if (grib_yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (grib_yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      grib_yy_symbol_print (stderr,                                            \
                  Kind, Value); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
grib_yy_symbol_value_print (FILE *grib_yyo,
                       grib_yysymbol_kind_t grib_yykind, YYSTYPE const * const grib_yyvaluep)
{
  FILE *grib_yyoutput = grib_yyo;
  YY_USE (grib_yyoutput);
  if (!grib_yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (grib_yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
grib_yy_symbol_print (FILE *grib_yyo,
                 grib_yysymbol_kind_t grib_yykind, YYSTYPE const * const grib_yyvaluep)
{
  YYFPRINTF (grib_yyo, "%s %s (",
             grib_yykind < YYNTOKENS ? "token" : "nterm", grib_yysymbol_name (grib_yykind));

  grib_yy_symbol_value_print (grib_yyo, grib_yykind, grib_yyvaluep);
  YYFPRINTF (grib_yyo, ")");
}

/*------------------------------------------------------------------.
| grib_yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
grib_yy_stack_print (grib_yy_state_t *grib_yybottom, grib_yy_state_t *grib_yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; grib_yybottom <= grib_yytop; grib_yybottom++)
    {
      int grib_yybot = *grib_yybottom;
      YYFPRINTF (stderr, " %d", grib_yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (grib_yydebug)                                                  \
    grib_yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
grib_yy_reduce_print (grib_yy_state_t *grib_yyssp, YYSTYPE *grib_yyvsp,
                 int grib_yyrule)
{
  int grib_yylno = grib_yyrline[grib_yyrule];
  int grib_yynrhs = grib_yyr2[grib_yyrule];
  int grib_yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             grib_yyrule - 1, grib_yylno);
  /* The symbols being reduced.  */
  for (grib_yyi = 0; grib_yyi < grib_yynrhs; grib_yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", grib_yyi + 1);
      grib_yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+grib_yyssp[grib_yyi + 1 - grib_yynrhs]),
                       &grib_yyvsp[(grib_yyi + 1) - (grib_yynrhs)]);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (grib_yydebug)                          \
    grib_yy_reduce_print (grib_yyssp, grib_yyvsp, Rule); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int grib_yydebug;
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
grib_yydestruct (const char *grib_yymsg,
            grib_yysymbol_kind_t grib_yykind, YYSTYPE *grib_yyvaluep)
{
  YY_USE (grib_yyvaluep);
  if (!grib_yymsg)
    grib_yymsg = "Deleting";
  YY_SYMBOL_PRINT (grib_yymsg, grib_yykind, grib_yyvaluep, grib_yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (grib_yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/* Lookahead token kind.  */
int grib_yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE grib_yylval;
/* Number of syntax errors so far.  */
int grib_yynerrs;




/*----------.
| grib_yyparse.  |
`----------*/

int
grib_yyparse (void)
{
    grib_yy_state_fast_t grib_yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int grib_yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow grib_yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T grib_yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    grib_yy_state_t grib_yyssa[YYINITDEPTH];
    grib_yy_state_t *grib_yyss = grib_yyssa;
    grib_yy_state_t *grib_yyssp = grib_yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE grib_yyvsa[YYINITDEPTH];
    YYSTYPE *grib_yyvs = grib_yyvsa;
    YYSTYPE *grib_yyvsp = grib_yyvs;

  int grib_yyn;
  /* The return value of grib_yyparse.  */
  int grib_yyresult;
  /* Lookahead symbol kind.  */
  grib_yysymbol_kind_t grib_yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE grib_yyval;



#define YYPOPSTACK(N)   (grib_yyvsp -= (N), grib_yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int grib_yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  grib_yychar = YYEMPTY; /* Cause a token to be read.  */

  goto grib_yysetstate;


/*------------------------------------------------------------.
| grib_yynewstate -- push a new state, which is found in grib_yystate.  |
`------------------------------------------------------------*/
grib_yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  grib_yyssp++;


/*--------------------------------------------------------------------.
| grib_yysetstate -- set current state (the top of the stack) to grib_yystate.  |
`--------------------------------------------------------------------*/
grib_yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", grib_yystate));
  YY_ASSERT (0 <= grib_yystate && grib_yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *grib_yyssp = YY_CAST (grib_yy_state_t, grib_yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (grib_yyss, grib_yyssp);

  if (grib_yyss + grib_yystacksize - 1 <= grib_yyssp)
#if !defined grib_yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T grib_yysize = grib_yyssp - grib_yyss + 1;

# if defined grib_yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        grib_yy_state_t *grib_yyss1 = grib_yyss;
        YYSTYPE *grib_yyvs1 = grib_yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if grib_yyoverflow is a macro.  */
        grib_yyoverflow (YY_("memory exhausted"),
                    &grib_yyss1, grib_yysize * YYSIZEOF (*grib_yyssp),
                    &grib_yyvs1, grib_yysize * YYSIZEOF (*grib_yyvsp),
                    &grib_yystacksize);
        grib_yyss = grib_yyss1;
        grib_yyvs = grib_yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= grib_yystacksize)
        YYNOMEM;
      grib_yystacksize *= 2;
      if (YYMAXDEPTH < grib_yystacksize)
        grib_yystacksize = YYMAXDEPTH;

      {
        grib_yy_state_t *grib_yyss1 = grib_yyss;
        union grib_yyalloc *grib_yyptr =
          YY_CAST (union grib_yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (grib_yystacksize))));
        if (! grib_yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (grib_yyss_alloc, grib_yyss);
        YYSTACK_RELOCATE (grib_yyvs_alloc, grib_yyvs);
#  undef YYSTACK_RELOCATE
        if (grib_yyss1 != grib_yyssa)
          YYSTACK_FREE (grib_yyss1);
      }
# endif

      grib_yyssp = grib_yyss + grib_yysize - 1;
      grib_yyvsp = grib_yyvs + grib_yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, grib_yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (grib_yyss + grib_yystacksize - 1 <= grib_yyssp)
        YYABORT;
    }
#endif /* !defined grib_yyoverflow && !defined YYSTACK_RELOCATE */


  if (grib_yystate == YYFINAL)
    YYACCEPT;

  goto grib_yybackup;


/*-----------.
| grib_yybackup.  |
`-----------*/
grib_yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  grib_yyn = grib_yypact[grib_yystate];
  if (grib_yypact_value_is_default (grib_yyn))
    goto grib_yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (grib_yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      grib_yychar = grib_yylex ();
    }

  if (grib_yychar <= YYEOF)
    {
      grib_yychar = YYEOF;
      grib_yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (grib_yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      grib_yychar = YYUNDEF;
      grib_yytoken = YYSYMBOL_YYerror;
      goto grib_yyerrlab1;
    }
  else
    {
      grib_yytoken = YYTRANSLATE (grib_yychar);
      YY_SYMBOL_PRINT ("Next token is", grib_yytoken, &grib_yylval, &grib_yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  grib_yyn += grib_yytoken;
  if (grib_yyn < 0 || YYLAST < grib_yyn || grib_yycheck[grib_yyn] != grib_yytoken)
    goto grib_yydefault;
  grib_yyn = grib_yytable[grib_yyn];
  if (grib_yyn <= 0)
    {
      if (grib_yytable_value_is_error (grib_yyn))
        goto grib_yyerrlab;
      grib_yyn = -grib_yyn;
      goto grib_yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (grib_yyerrstatus)
    grib_yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", grib_yytoken, &grib_yylval, &grib_yylloc);
  grib_yystate = grib_yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++grib_yyvsp = grib_yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  grib_yychar = YYEMPTY;
  goto grib_yynewstate;


/*-----------------------------------------------------------.
| grib_yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
grib_yydefault:
  grib_yyn = grib_yydefact[grib_yystate];
  if (grib_yyn == 0)
    goto grib_yyerrlab;
  goto grib_yyreduce;


/*-----------------------------.
| grib_yyreduce -- do a reduction.  |
`-----------------------------*/
grib_yyreduce:
  /* grib_yyn is the number of a rule to reduce with.  */
  grib_yylen = grib_yyr2[grib_yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  grib_yyval = grib_yyvsp[1-grib_yylen];


  YY_REDUCE_PRINT (grib_yyn);
  switch (grib_yyn)
    {
  case 2: /* all: empty  */
#line 295 "griby.y"
                  { grib_parser_all_actions = 0;grib_parser_concept=0; 
                            grib_parser_hash_array=0;grib_parser_rules=0; }
#line 2397 "y.tab.c"
    break;

  case 3: /* all: concept_list  */
#line 297 "griby.y"
                          { grib_parser_concept     = reverse_concept((grib_yyvsp[0].concept_value)); }
#line 2403 "y.tab.c"
    break;

  case 4: /* all: hash_array_list  */
#line 298 "griby.y"
                             { grib_parser_hash_array     = reverse_hash_array((grib_yyvsp[0].hash_array_value)); }
#line 2409 "y.tab.c"
    break;

  case 5: /* all: instructions  */
#line 299 "griby.y"
                          { grib_parser_all_actions = (grib_yyvsp[0].act); }
#line 2415 "y.tab.c"
    break;

  case 6: /* all: rules  */
#line 300 "griby.y"
                          { grib_parser_rules       = (grib_yyvsp[0].rules); }
#line 2421 "y.tab.c"
    break;

  case 7: /* all: error  */
#line 302 "griby.y"
                      { grib_parser_all_actions = 0; grib_parser_concept=0; 
	                    grib_parser_hash_array=0; grib_parser_rules=0; }
#line 2428 "y.tab.c"
    break;

  case 9: /* dvalues: FLOAT  */
#line 309 "griby.y"
                 { (grib_yyval.dvalue)=grib_darray_push(0,(grib_yyvsp[0].dval));}
#line 2434 "y.tab.c"
    break;

  case 10: /* dvalues: dvalues ',' FLOAT  */
#line 310 "griby.y"
                         { (grib_yyval.dvalue)=grib_darray_push((grib_yyvsp[-2].dvalue),(grib_yyvsp[0].dval));}
#line 2440 "y.tab.c"
    break;

  case 11: /* dvalues: INTEGER  */
#line 311 "griby.y"
               { (grib_yyval.dvalue)=grib_darray_push(0,(grib_yyvsp[0].lval));}
#line 2446 "y.tab.c"
    break;

  case 12: /* dvalues: dvalues ',' INTEGER  */
#line 312 "griby.y"
                           { (grib_yyval.dvalue)=grib_darray_push((grib_yyvsp[-2].dvalue),(grib_yyvsp[0].lval));}
#line 2452 "y.tab.c"
    break;

  case 13: /* svalues: STRING  */
#line 315 "griby.y"
                { (grib_yyval.svalue)=grib_sarray_push(0,(grib_yyvsp[0].str));}
#line 2458 "y.tab.c"
    break;

  case 14: /* svalues: svalues ',' STRING  */
#line 316 "griby.y"
                          { (grib_yyval.svalue)=grib_sarray_push((grib_yyvsp[-2].svalue),(grib_yyvsp[0].str));}
#line 2464 "y.tab.c"
    break;

  case 15: /* integer_array: INTEGER  */
#line 320 "griby.y"
                         { (grib_yyval.ivalue)=grib_iarray_push(0,(grib_yyvsp[0].lval));}
#line 2470 "y.tab.c"
    break;

  case 16: /* integer_array: integer_array ',' INTEGER  */
#line 321 "griby.y"
                                 { (grib_yyval.ivalue)=grib_iarray_push((grib_yyvsp[-2].ivalue),(grib_yyvsp[0].lval));}
#line 2476 "y.tab.c"
    break;

  case 18: /* instructions: instruction instructions  */
#line 325 "griby.y"
                                    { (grib_yyvsp[-1].act)->next_ = (grib_yyvsp[0].act); (grib_yyval.act) = (grib_yyvsp[-1].act); }
#line 2482 "y.tab.c"
    break;

  case 19: /* instructions: instruction ';' instructions  */
#line 326 "griby.y"
                                         { (grib_yyvsp[-2].act)->next_ = (grib_yyvsp[0].act); (grib_yyval.act) = (grib_yyvsp[-2].act); }
#line 2488 "y.tab.c"
    break;

  case 20: /* instructions: instruction ';'  */
#line 327 "griby.y"
                            {  (grib_yyval.act) = (grib_yyvsp[-1].act);}
#line 2494 "y.tab.c"
    break;

  case 32: /* argument_list: empty  */
#line 346 "griby.y"
                           { (grib_yyval.explist) = 0; }
#line 2500 "y.tab.c"
    break;

  case 35: /* arguments: argument ',' arguments  */
#line 351 "griby.y"
                                       { (grib_yyvsp[-2].explist)->next_ = (grib_yyvsp[0].explist); (grib_yyval.explist) = (grib_yyvsp[-2].explist); }
#line 2506 "y.tab.c"
    break;

  case 36: /* argument: expression  */
#line 354 "griby.y"
                     { (grib_yyval.explist) = grib_arguments_new(grib_parser_context,(grib_yyvsp[0].exp),NULL); }
#line 2512 "y.tab.c"
    break;

  case 37: /* simple: UNSIGNED '[' INTEGER ']' IDENT default flags  */
#line 359 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"unsigned",(grib_yyvsp[-4].lval),NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);        free((grib_yyvsp[-2].str));  }
#line 2518 "y.tab.c"
    break;

  case 38: /* simple: UNSIGNED '[' INTEGER ']' IDENT '[' argument_list ']' default flags  */
#line 362 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"unsigned",(grib_yyvsp[-7].lval),(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);        free((grib_yyvsp[-5].str));  }
#line 2524 "y.tab.c"
    break;

  case 39: /* simple: UNSIGNED '(' INTEGER ')' IDENT default flags  */
#line 365 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"unsigned_bits",(grib_yyvsp[-4].lval),NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);        free((grib_yyvsp[-2].str));  }
#line 2530 "y.tab.c"
    break;

  case 40: /* simple: UNSIGNED '(' INTEGER ')' IDENT '[' argument_list ']' default flags  */
#line 368 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"unsigned_bits",(grib_yyvsp[-7].lval),(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);        free((grib_yyvsp[-5].str));  }
#line 2536 "y.tab.c"
    break;

  case 41: /* simple: ASCII '[' INTEGER ']' IDENT default flags  */
#line 371 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"ascii",(grib_yyvsp[-4].lval),NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);  free((grib_yyvsp[-2].str));  }
#line 2542 "y.tab.c"
    break;

  case 42: /* simple: GROUP IDENT default flags  */
#line 374 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"group",0,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);  free((grib_yyvsp[-2].str));  }
#line 2548 "y.tab.c"
    break;

  case 43: /* simple: GROUP IDENT '(' argument_list ')' default flags  */
#line 377 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"group",0,(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);  free((grib_yyvsp[-5].str));  }
#line 2554 "y.tab.c"
    break;

  case 44: /* simple: IDENT '=' TO_INTEGER '(' argument_list ')' flags  */
#line 380 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-6].str),"to_integer",0,(grib_yyvsp[-2].explist),0,(grib_yyvsp[0].lval),NULL,NULL);  free((grib_yyvsp[-6].str));  }
#line 2560 "y.tab.c"
    break;

  case 45: /* simple: IDENT '=' SEX2DEC '(' argument_list ')' flags  */
#line 383 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-6].str),"sexagesimal2decimal",0,(grib_yyvsp[-2].explist),0,(grib_yyvsp[0].lval),NULL,NULL);  free((grib_yyvsp[-6].str));  }
#line 2566 "y.tab.c"
    break;

  case 46: /* simple: IDENT '=' TO_STRING '(' argument_list ')' flags  */
#line 386 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-6].str),"to_string",0,(grib_yyvsp[-2].explist),0,(grib_yyvsp[0].lval),NULL,NULL);  free((grib_yyvsp[-6].str));  }
#line 2572 "y.tab.c"
    break;

  case 47: /* simple: NON_ALPHA IDENT default flags  */
#line 389 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"non_alpha",0,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);  free((grib_yyvsp[-2].str));  }
#line 2578 "y.tab.c"
    break;

  case 48: /* simple: ASCII '[' INTEGER ']' STRING default flags  */
#line 393 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"ascii",(grib_yyvsp[-4].lval),NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);  free((grib_yyvsp[-2].str));  }
#line 2584 "y.tab.c"
    break;

  case 49: /* simple: BYTE '[' INTEGER ']' IDENT default flags  */
#line 396 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"bytes",(grib_yyvsp[-4].lval),NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);      free((grib_yyvsp[-2].str));  }
#line 2590 "y.tab.c"
    break;

  case 50: /* simple: BYTE '[' INTEGER ']' IDENT '[' argument_list ']' default flags  */
#line 399 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"bytes",(grib_yyvsp[-7].lval),(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);      free((grib_yyvsp[-5].str));  }
#line 2596 "y.tab.c"
    break;

  case 51: /* simple: KSEC1EXPVER '[' INTEGER ']' IDENT default flags  */
#line 402 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"ksec1expver",(grib_yyvsp[-4].lval),NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);  free((grib_yyvsp[-2].str));  }
#line 2602 "y.tab.c"
    break;

  case 52: /* simple: SIGNED '[' INTEGER ']' IDENT default flags  */
#line 405 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"signed",(grib_yyvsp[-4].lval),NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);      free((grib_yyvsp[-2].str));  }
#line 2608 "y.tab.c"
    break;

  case 53: /* simple: SIGNED '[' INTEGER ']' IDENT '[' argument_list ']' default flags  */
#line 408 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"signed",(grib_yyvsp[-7].lval),(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);      free((grib_yyvsp[-5].str));  }
#line 2614 "y.tab.c"
    break;

  case 54: /* simple: SIGNED '(' INTEGER ')' IDENT default flags  */
#line 411 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"signed_bits",(grib_yyvsp[-4].lval),NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);      free((grib_yyvsp[-2].str));  }
#line 2620 "y.tab.c"
    break;

  case 55: /* simple: SIGNED '(' INTEGER ')' IDENT '[' argument_list ']' default flags  */
#line 414 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"signed_bits",(grib_yyvsp[-7].lval),(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);      free((grib_yyvsp[-5].str));  }
#line 2626 "y.tab.c"
    break;

  case 56: /* simple: CODETABLE '[' INTEGER ']' IDENT argument default flags  */
#line 417 "griby.y"
    { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-3].str),"codetable",(grib_yyvsp[-5].lval), (grib_yyvsp[-2].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);    free((grib_yyvsp[-3].str)); }
#line 2632 "y.tab.c"
    break;

  case 57: /* simple: CODETABLE '[' IDENT ']' IDENT argument default flags  */
#line 420 "griby.y"
    {
      /* ECC-485: Set length to 0 and prepend the new argument */
      grib_arguments* a = grib_arguments_new(grib_parser_context, new_accessor_expression(grib_parser_context,(grib_yyvsp[-5].str),0,0),NULL);
      a->next_ = (grib_yyvsp[-2].explist);
      (grib_yyval.act) = grib_action_create_gen(grib_parser_context, (grib_yyvsp[-3].str), "codetable",
                                  0, a, /* length=0 and additional argument */
                                  (grib_yyvsp[-1].explist), (grib_yyvsp[0].lval), NULL, NULL);
      free((grib_yyvsp[-3].str));
    }
#line 2646 "y.tab.c"
    break;

  case 58: /* simple: CODETABLE '[' INTEGER ']' IDENT argument default SET '(' IDENT ')' flags  */
#line 431 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-7].str),"codetable",(grib_yyvsp[-9].lval), (grib_yyvsp[-6].explist),(grib_yyvsp[-5].explist),(grib_yyvsp[0].lval),NULL,(grib_yyvsp[-2].str));
           free((grib_yyvsp[-7].str));free((grib_yyvsp[-2].str)); }
#line 2653 "y.tab.c"
    break;

  case 59: /* simple: CODETABLE '[' INTEGER ']' IDENT '(' argument_list ')' default flags  */
#line 435 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"codetable",(grib_yyvsp[-7].lval), (grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);    free((grib_yyvsp[-5].str)); }
#line 2659 "y.tab.c"
    break;

  case 60: /* simple: SMART_TABLE IDENT '(' argument_list ')' default flags  */
#line 438 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"smart_table",0,(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);    free((grib_yyvsp[-5].str)); }
#line 2665 "y.tab.c"
    break;

  case 61: /* simple: IDENT '=' DICTIONARY '(' argument_list ')' default flags  */
#line 441 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-7].str),"dictionary",0,(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);    free((grib_yyvsp[-7].str)); }
#line 2671 "y.tab.c"
    break;

  case 62: /* simple: IDENT '=' GETENV '(' argument_list ')' default flags  */
#line 444 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-7].str),"getenv",0,(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);    free((grib_yyvsp[-7].str)); }
#line 2677 "y.tab.c"
    break;

  case 63: /* simple: COMPLEX_CODETABLE '[' INTEGER ']' IDENT argument default flags  */
#line 447 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-3].str),"complex_codetable",(grib_yyvsp[-5].lval), (grib_yyvsp[-2].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);    free((grib_yyvsp[-3].str)); }
#line 2683 "y.tab.c"
    break;

  case 64: /* simple: COMPLEX_CODETABLE '[' INTEGER ']' IDENT '(' argument_list ')' default flags  */
#line 450 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"complex_codetable",(grib_yyvsp[-7].lval), (grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);    free((grib_yyvsp[-5].str)); }
#line 2689 "y.tab.c"
    break;

  case 65: /* simple: FLAG '[' INTEGER ']' IDENT argument default flags  */
#line 453 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-3].str),"codeflag",(grib_yyvsp[-5].lval), (grib_yyvsp[-2].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);  free((grib_yyvsp[-3].str)); }
#line 2695 "y.tab.c"
    break;

  case 66: /* simple: LOOKUP '[' INTEGER ']' IDENT '(' argument_list ')' flags  */
#line 456 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-4].str),"lookup",(grib_yyvsp[-6].lval),(grib_yyvsp[-2].explist),NULL,(grib_yyvsp[0].lval),NULL,NULL); free((grib_yyvsp[-4].str)); }
#line 2701 "y.tab.c"
    break;

  case 67: /* simple: FLAGBIT IDENT '(' argument_list ')' default flags  */
#line 459 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"bit",0,(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL); free((grib_yyvsp[-5].str)); }
#line 2707 "y.tab.c"
    break;

  case 68: /* simple: LABEL IDENT  */
#line 462 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[0].str),"label",0,NULL,NULL,0,NULL,NULL);   free((grib_yyvsp[0].str));  }
#line 2713 "y.tab.c"
    break;

  case 69: /* simple: LABEL STRING  */
#line 465 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[0].str),"label",0,NULL,NULL,0,NULL,NULL);   free((grib_yyvsp[0].str));  }
#line 2719 "y.tab.c"
    break;

  case 70: /* simple: IBMFLOAT IDENT default flags  */
#line 468 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"ibmfloat",4,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2725 "y.tab.c"
    break;

  case 71: /* simple: INT8 IDENT default flags  */
#line 472 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"int8",1,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2731 "y.tab.c"
    break;

  case 72: /* simple: UINT8 IDENT default flags  */
#line 475 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"uint8",1,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2737 "y.tab.c"
    break;

  case 73: /* simple: INT16 IDENT default flags  */
#line 478 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"int16",2,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2743 "y.tab.c"
    break;

  case 74: /* simple: UINT16 IDENT default flags  */
#line 481 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"uint16",2,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2749 "y.tab.c"
    break;

  case 75: /* simple: INT16_LITTLE_ENDIAN IDENT default flags  */
#line 484 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"int16_little_endian",2,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2755 "y.tab.c"
    break;

  case 76: /* simple: UINT16_LITTLE_ENDIAN IDENT default flags  */
#line 487 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"uint16_little_endian",2,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2761 "y.tab.c"
    break;

  case 77: /* simple: INT32 IDENT default flags  */
#line 490 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"int32",4,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2767 "y.tab.c"
    break;

  case 78: /* simple: UINT32 IDENT default flags  */
#line 493 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"uint32",4,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2773 "y.tab.c"
    break;

  case 79: /* simple: INT32_LITTLE_ENDIAN IDENT default flags  */
#line 496 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"int32_little_endian",4,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2779 "y.tab.c"
    break;

  case 80: /* simple: UINT32_LITTLE_ENDIAN IDENT default flags  */
#line 499 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"uint32_little_endian",4,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2785 "y.tab.c"
    break;

  case 81: /* simple: INT64 IDENT default flags  */
#line 502 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"int64",8,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2791 "y.tab.c"
    break;

  case 82: /* simple: UINT64 IDENT default flags  */
#line 505 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"uint64",8,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2797 "y.tab.c"
    break;

  case 83: /* simple: INT64_LITTLE_ENDIAN IDENT default flags  */
#line 508 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"int64_little_endian",8,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2803 "y.tab.c"
    break;

  case 84: /* simple: UINT64_LITTLE_ENDIAN IDENT default flags  */
#line 511 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"uint64_little_endian",8,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-2].str));  }
#line 2809 "y.tab.c"
    break;

  case 85: /* simple: BLOB IDENT '[' argument_list ']' default flags  */
#line 514 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"blob",0,(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);        free((grib_yyvsp[-5].str));  }
#line 2815 "y.tab.c"
    break;

  case 86: /* simple: IBMFLOAT IDENT '.' IDENT default flags  */
#line 518 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"ibmfloat",4,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),(grib_yyvsp[-4].str),NULL);free((grib_yyvsp[-2].str)); free((grib_yyvsp[-4].str)); }
#line 2821 "y.tab.c"
    break;

  case 87: /* simple: IBMFLOAT IDENT '[' argument ']' default flags  */
#line 521 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"ibmfloat",4,(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-5].str));  }
#line 2827 "y.tab.c"
    break;

  case 88: /* simple: POS IDENT  */
#line 524 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[0].str),"position",0,NULL,NULL,0,NULL,NULL);     free((grib_yyvsp[0].str));  }
#line 2833 "y.tab.c"
    break;

  case 89: /* simple: INTCONST IDENT '=' argument flags  */
#line 527 "griby.y"
        { (grib_yyval.act) = grib_action_create_variable(grib_parser_context,(grib_yyvsp[-3].str),"constant",0,(grib_yyvsp[-1].explist),NULL,(grib_yyvsp[0].lval),NULL);free((grib_yyvsp[-3].str)); }
#line 2839 "y.tab.c"
    break;

  case 90: /* simple: TRANS IDENT '=' argument flags  */
#line 530 "griby.y"
        { (grib_yyval.act) = grib_action_create_variable(grib_parser_context,(grib_yyvsp[-3].str),"transient",0,(grib_yyvsp[-1].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL);   free((grib_yyvsp[-3].str)); }
#line 2845 "y.tab.c"
    break;

  case 91: /* simple: TRANS IDENT '=' '{' dvalues '}' flags  */
#line 532 "griby.y"
        { (grib_yyval.act) = grib_action_create_transient_darray(grib_parser_context,(grib_yyvsp[-5].str),(grib_yyvsp[-2].dvalue),(grib_yyvsp[0].lval)); free((grib_yyvsp[-5].str)); }
#line 2851 "y.tab.c"
    break;

  case 92: /* simple: FLOAT IDENT default flags  */
#line 535 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"ieeefloat",4,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);   free((grib_yyvsp[-2].str));  }
#line 2857 "y.tab.c"
    break;

  case 93: /* simple: FLOAT IDENT '.' IDENT default flags  */
#line 538 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-2].str),"ieeefloat",4,NULL,(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),(grib_yyvsp[-4].str),NULL);  free((grib_yyvsp[-2].str));free((grib_yyvsp[-4].str));}
#line 2863 "y.tab.c"
    break;

  case 94: /* simple: FLOAT IDENT '[' argument ']' default flags  */
#line 541 "griby.y"
   { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-5].str),"ieeefloat",4,(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL,NULL);free((grib_yyvsp[-5].str));  }
#line 2869 "y.tab.c"
    break;

  case 95: /* simple: G1_HALF_BYTE IDENT  */
#line 544 "griby.y"
   { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[0].str),"g1_half_byte_codeflag",0,NULL,NULL,0,NULL,NULL);free((grib_yyvsp[0].str));  }
#line 2875 "y.tab.c"
    break;

  case 96: /* simple: SECTION_LENGTH '[' INTEGER ']' IDENT default  */
#line 547 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-1].str),"section_length",(grib_yyvsp[-3].lval),NULL,(grib_yyvsp[0].explist),0,NULL,NULL);free((grib_yyvsp[-1].str));  }
#line 2881 "y.tab.c"
    break;

  case 97: /* simple: G1_MESSAGE_LENGTH '[' INTEGER ']' IDENT '(' argument_list ')'  */
#line 550 "griby.y"
   { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-3].str),"g1_message_length",(grib_yyvsp[-5].lval),(grib_yyvsp[-1].explist),NULL,0,NULL,NULL);free((grib_yyvsp[-3].str));  }
#line 2887 "y.tab.c"
    break;

  case 98: /* simple: G1_SECTION4_LENGTH '[' INTEGER ']' IDENT '(' argument_list ')'  */
#line 553 "griby.y"
  { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-3].str),"g1_section4_length",(grib_yyvsp[-5].lval),(grib_yyvsp[-1].explist),NULL,0,NULL,NULL);free((grib_yyvsp[-3].str));  }
#line 2893 "y.tab.c"
    break;

  case 99: /* simple: KSEC IDENT argument  */
#line 556 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-1].str),"ksec",0,(grib_yyvsp[0].explist),NULL,0,NULL,NULL);free((grib_yyvsp[-1].str)); }
#line 2899 "y.tab.c"
    break;

  case 100: /* simple: PAD IDENT '(' argument_list ')'  */
#line 559 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-3].str),"pad",0,(grib_yyvsp[-1].explist),0,0,NULL,NULL);   free((grib_yyvsp[-3].str)); }
#line 2905 "y.tab.c"
    break;

  case 101: /* simple: PADTO IDENT '(' argument_list ')'  */
#line 562 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-3].str),"padto",0,(grib_yyvsp[-1].explist),0,0,NULL,NULL);   free((grib_yyvsp[-3].str)); }
#line 2911 "y.tab.c"
    break;

  case 102: /* simple: PADTOEVEN IDENT '(' argument_list ')'  */
#line 565 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-3].str),"padtoeven",0,(grib_yyvsp[-1].explist),0,0,NULL,NULL);   free((grib_yyvsp[-3].str)); }
#line 2917 "y.tab.c"
    break;

  case 103: /* simple: PADTOMULTIPLE IDENT '(' argument_list ')'  */
#line 568 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-3].str),"padtomultiple",0,(grib_yyvsp[-1].explist),0,0,NULL,NULL);   free((grib_yyvsp[-3].str)); }
#line 2923 "y.tab.c"
    break;

  case 104: /* simple: MESSAGE '[' INTEGER ']' IDENT flags  */
#line 571 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-1].str),"message",(grib_yyvsp[-3].lval),0,0,(grib_yyvsp[0].lval),NULL,NULL);   free((grib_yyvsp[-1].str));  }
#line 2929 "y.tab.c"
    break;

  case 105: /* simple: MESSAGE_COPY IDENT flags  */
#line 574 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-1].str),"message_copy",0,0,0,(grib_yyvsp[0].lval),NULL,NULL);   free((grib_yyvsp[-1].str));  }
#line 2935 "y.tab.c"
    break;

  case 106: /* simple: SECTION_PADDING IDENT flags  */
#line 577 "griby.y"
        { (grib_yyval.act) = grib_action_create_gen(grib_parser_context,(grib_yyvsp[-1].str),"section_padding",0,0,0,(grib_yyvsp[0].lval),NULL,NULL);   free((grib_yyvsp[-1].str));  }
#line 2941 "y.tab.c"
    break;

  case 107: /* simple: TEMPLATE IDENT STRING  */
#line 579 "griby.y"
        { (grib_yyval.act) = grib_action_create_template(grib_parser_context,0,(grib_yyvsp[-1].str),(grib_yyvsp[0].str),grib_yylineno); free((grib_yyvsp[-1].str)); free((grib_yyvsp[0].str));}
#line 2947 "y.tab.c"
    break;

  case 108: /* simple: TEMPLATE_NOFAIL IDENT STRING  */
#line 581 "griby.y"
    { (grib_yyval.act) = grib_action_create_template(grib_parser_context,1,(grib_yyvsp[-1].str),(grib_yyvsp[0].str),grib_yylineno); free((grib_yyvsp[-1].str)); free((grib_yyvsp[0].str));}
#line 2953 "y.tab.c"
    break;

  case 109: /* simple: ALIAS IDENT '=' IDENT flags  */
#line 584 "griby.y"
        { (grib_yyval.act) = grib_action_create_alias(grib_parser_context,(grib_yyvsp[-3].str),(grib_yyvsp[-1].str),NULL,(grib_yyvsp[0].lval));  free((grib_yyvsp[-3].str)); free((grib_yyvsp[-1].str)); }
#line 2959 "y.tab.c"
    break;

  case 110: /* simple: UNALIAS IDENT  */
#line 587 "griby.y"
        { (grib_yyval.act) = grib_action_create_alias(grib_parser_context,(grib_yyvsp[0].str),NULL,NULL,0);  free((grib_yyvsp[0].str)); }
#line 2965 "y.tab.c"
    break;

  case 111: /* simple: ALIAS IDENT '.' IDENT '=' IDENT flags  */
#line 590 "griby.y"
        {
         (grib_yyval.act) = grib_action_create_alias(grib_parser_context,(grib_yyvsp[-3].str),(grib_yyvsp[-1].str),(grib_yyvsp[-5].str),(grib_yyvsp[0].lval));  free((grib_yyvsp[-5].str)); free((grib_yyvsp[-3].str)); free((grib_yyvsp[-1].str));
    }
#line 2973 "y.tab.c"
    break;

  case 112: /* simple: UNALIAS IDENT '.' IDENT  */
#line 594 "griby.y"
        {
         (grib_yyval.act) = grib_action_create_alias(grib_parser_context,(grib_yyvsp[0].str),NULL,(grib_yyvsp[-2].str),0);  free((grib_yyvsp[-2].str)); free((grib_yyvsp[0].str)); 
    }
#line 2981 "y.tab.c"
    break;

  case 113: /* simple: META IDENT IDENT '(' argument_list ')' default flags  */
#line 598 "griby.y"
        { (grib_yyval.act) = grib_action_create_meta(grib_parser_context,(grib_yyvsp[-6].str),(grib_yyvsp[-5].str),(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),NULL); free((grib_yyvsp[-6].str));free((grib_yyvsp[-5].str));}
#line 2987 "y.tab.c"
    break;

  case 114: /* simple: META IDENT '.' IDENT IDENT '(' argument_list ')' default flags  */
#line 601 "griby.y"
    { (grib_yyval.act) = grib_action_create_meta(grib_parser_context,(grib_yyvsp[-6].str),(grib_yyvsp[-5].str),(grib_yyvsp[-3].explist),(grib_yyvsp[-1].explist),(grib_yyvsp[0].lval),(grib_yyvsp[-8].str)); free((grib_yyvsp[-6].str));free((grib_yyvsp[-5].str));free((grib_yyvsp[-8].str));}
#line 2993 "y.tab.c"
    break;

  case 115: /* simple: ITERATOR IDENT '(' argument_list ')'  */
#line 604 "griby.y"
        {
      grib_arguments* a = grib_arguments_new(
        grib_parser_context,
        new_accessor_expression(grib_parser_context,(grib_yyvsp[-3].str),0,0),
		NULL
        );
      a->next_=(grib_yyvsp[-1].explist);
      (grib_yyval.act) = grib_action_create_meta(grib_parser_context,
      "ITERATOR","iterator",a,NULL,
      GRIB_ACCESSOR_FLAG_HIDDEN|GRIB_ACCESSOR_FLAG_READ_ONLY,NULL); free((grib_yyvsp[-3].str));
    }
#line 3009 "y.tab.c"
    break;

  case 116: /* simple: NEAREST IDENT '(' argument_list ')'  */
#line 616 "griby.y"
        {
      grib_arguments* a = grib_arguments_new(
        grib_parser_context,
        new_accessor_expression(grib_parser_context,(grib_yyvsp[-3].str),0,0),
		NULL
        );
      a->next_=(grib_yyvsp[-1].explist);
      (grib_yyval.act) = grib_action_create_meta(grib_parser_context,
      "NEAREST","nearest",a,NULL,
      GRIB_ACCESSOR_FLAG_HIDDEN|GRIB_ACCESSOR_FLAG_READ_ONLY,NULL); free((grib_yyvsp[-3].str));
    }
#line 3025 "y.tab.c"
    break;

  case 117: /* simple: BOX IDENT '(' argument_list ')'  */
#line 628 "griby.y"
        {
      grib_arguments* a = grib_arguments_new(
        grib_parser_context,
        new_accessor_expression(grib_parser_context,(grib_yyvsp[-3].str),0,0),
		NULL
        );
      a->next_=(grib_yyvsp[-1].explist);
      (grib_yyval.act) = grib_action_create_meta(grib_parser_context,
      "BOX","box",a,NULL,
      GRIB_ACCESSOR_FLAG_HIDDEN|GRIB_ACCESSOR_FLAG_READ_ONLY,NULL); free((grib_yyvsp[-3].str));
    }
#line 3041 "y.tab.c"
    break;

  case 118: /* simple: EXPORT IDENT '(' argument_list ')'  */
#line 640 "griby.y"
       { (grib_yyval.act) = grib_action_create_put(grib_parser_context,(grib_yyvsp[-3].str),(grib_yyvsp[-1].explist));free((grib_yyvsp[-3].str));}
#line 3047 "y.tab.c"
    break;

  case 119: /* simple: REMOVE argument_list  */
#line 643 "griby.y"
       { (grib_yyval.act) = grib_action_create_remove(grib_parser_context,(grib_yyvsp[0].explist));}
#line 3053 "y.tab.c"
    break;

  case 120: /* simple: RENAME '(' IDENT ',' IDENT ')'  */
#line 645 "griby.y"
                                     { (grib_yyval.act) = grib_action_create_rename(grib_parser_context,(grib_yyvsp[-3].str),(grib_yyvsp[-1].str));free((grib_yyvsp[-3].str));free((grib_yyvsp[-1].str));}
#line 3059 "y.tab.c"
    break;

  case 121: /* simple: ASSERT '(' expression ')'  */
#line 648 "griby.y"
       { (grib_yyval.act) = grib_action_create_assert(grib_parser_context,(grib_yyvsp[-1].exp));}
#line 3065 "y.tab.c"
    break;

  case 122: /* simple: MODIFY IDENT flags  */
#line 651 "griby.y"
       { (grib_yyval.act) = grib_action_create_modify(grib_parser_context,(grib_yyvsp[-1].str),(grib_yyvsp[0].lval)); free((grib_yyvsp[-1].str));}
#line 3071 "y.tab.c"
    break;

  case 123: /* simple: SET IDENT '=' MISSING  */
#line 653 "griby.y"
                          { (grib_yyval.act) = grib_action_create_set_missing(grib_parser_context,(grib_yyvsp[-2].str)); free((grib_yyvsp[-2].str)); }
#line 3077 "y.tab.c"
    break;

  case 124: /* simple: SET IDENT '=' expression  */
#line 654 "griby.y"
                             { (grib_yyval.act) = grib_action_create_set(grib_parser_context,(grib_yyvsp[-2].str),(grib_yyvsp[0].exp),0); free((grib_yyvsp[-2].str)); }
#line 3083 "y.tab.c"
    break;

  case 125: /* simple: SET IDENT '=' '{' dvalues '}'  */
#line 655 "griby.y"
                                  { (grib_yyval.act) = grib_action_create_set_darray(grib_parser_context,(grib_yyvsp[-4].str),(grib_yyvsp[-1].dvalue)); free((grib_yyvsp[-4].str)); }
#line 3089 "y.tab.c"
    break;

  case 126: /* simple: SET IDENT '=' '{' svalues '}'  */
#line 656 "griby.y"
                                  { (grib_yyval.act) = grib_action_create_set_sarray(grib_parser_context,(grib_yyvsp[-4].str),(grib_yyvsp[-1].svalue)); free((grib_yyvsp[-4].str)); }
#line 3095 "y.tab.c"
    break;

  case 127: /* simple: SET_NOFAIL IDENT '=' expression  */
#line 658 "griby.y"
                                    { (grib_yyval.act) = grib_action_create_set(grib_parser_context,(grib_yyvsp[-2].str),(grib_yyvsp[0].exp),1); free((grib_yyvsp[-2].str)); }
#line 3101 "y.tab.c"
    break;

  case 128: /* simple: WRITE STRING  */
#line 661 "griby.y"
                 { (grib_yyval.act) = grib_action_create_write(grib_parser_context,(grib_yyvsp[0].str),0,0); free((grib_yyvsp[0].str));}
#line 3107 "y.tab.c"
    break;

  case 129: /* simple: WRITE  */
#line 662 "griby.y"
          { (grib_yyval.act) = grib_action_create_write(grib_parser_context,"",0,0); }
#line 3113 "y.tab.c"
    break;

  case 130: /* simple: WRITE '(' INTEGER ')' STRING  */
#line 663 "griby.y"
                                 { (grib_yyval.act) = grib_action_create_write(grib_parser_context,(grib_yyvsp[0].str),0,(grib_yyvsp[-2].lval)); free((grib_yyvsp[0].str));}
#line 3119 "y.tab.c"
    break;

  case 131: /* simple: WRITE '(' INTEGER ')'  */
#line 664 "griby.y"
                          { (grib_yyval.act) = grib_action_create_write(grib_parser_context,"",0,(grib_yyvsp[-1].lval)); }
#line 3125 "y.tab.c"
    break;

  case 132: /* simple: APPEND STRING  */
#line 665 "griby.y"
                  { (grib_yyval.act) = grib_action_create_write(grib_parser_context,(grib_yyvsp[0].str),1,0); free((grib_yyvsp[0].str));}
#line 3131 "y.tab.c"
    break;

  case 133: /* simple: APPEND  */
#line 666 "griby.y"
           { (grib_yyval.act) = grib_action_create_write(grib_parser_context,"",1,0); }
#line 3137 "y.tab.c"
    break;

  case 134: /* simple: APPEND '(' INTEGER ')' STRING  */
#line 667 "griby.y"
                                  { (grib_yyval.act) = grib_action_create_write(grib_parser_context,(grib_yyvsp[0].str),1,(grib_yyvsp[-2].lval)); free((grib_yyvsp[0].str));}
#line 3143 "y.tab.c"
    break;

  case 135: /* simple: APPEND '(' INTEGER ')'  */
#line 668 "griby.y"
                           { (grib_yyval.act) = grib_action_create_write(grib_parser_context,"",1,(grib_yyvsp[-1].lval)); }
#line 3149 "y.tab.c"
    break;

  case 136: /* simple: CLOSE '(' IDENT ')'  */
#line 670 "griby.y"
                        { (grib_yyval.act) = grib_action_create_close(grib_parser_context,(grib_yyvsp[-1].str)); free((grib_yyvsp[-1].str));}
#line 3155 "y.tab.c"
    break;

  case 137: /* simple: PRINT STRING  */
#line 671 "griby.y"
                 { (grib_yyval.act) = grib_action_create_print(grib_parser_context,(grib_yyvsp[0].str),0); free((grib_yyvsp[0].str)); }
#line 3161 "y.tab.c"
    break;

  case 138: /* simple: PRINT '(' STRING ')' STRING  */
#line 672 "griby.y"
                                { (grib_yyval.act) = grib_action_create_print(grib_parser_context,(grib_yyvsp[0].str),(grib_yyvsp[-2].str)); free((grib_yyvsp[0].str)); free((grib_yyvsp[-2].str));}
#line 3167 "y.tab.c"
    break;

  case 139: /* simple: PRINT '(' IDENT ')' STRING  */
#line 673 "griby.y"
                               { (grib_yyval.act) = grib_action_create_print(grib_parser_context,(grib_yyvsp[0].str),(grib_yyvsp[-2].str)); free((grib_yyvsp[0].str)); free((grib_yyvsp[-2].str));}
#line 3173 "y.tab.c"
    break;

  case 140: /* simple: PRINT  */
#line 674 "griby.y"
          { (grib_yyval.act) = grib_action_create_print(grib_parser_context,"",0);  }
#line 3179 "y.tab.c"
    break;

  case 141: /* if_block: IF '(' expression ')' '{' instructions '}'  */
#line 678 "griby.y"
                                             { (grib_yyval.act) = grib_action_create_if(grib_parser_context,(grib_yyvsp[-4].exp),(grib_yyvsp[-1].act),0,0,grib_yylineno,file_being_parsed()); }
#line 3185 "y.tab.c"
    break;

  case 142: /* if_block: IF '(' expression ')' '{' instructions '}' ELSE '{' instructions '}'  */
#line 679 "griby.y"
                                                                        { (grib_yyval.act) = grib_action_create_if(grib_parser_context,(grib_yyvsp[-8].exp),(grib_yyvsp[-5].act),(grib_yyvsp[-1].act),0,grib_yylineno,file_being_parsed()); }
#line 3191 "y.tab.c"
    break;

  case 143: /* if_block: IF_TRANSIENT '(' expression ')' '{' instructions '}'  */
#line 680 "griby.y"
                                                       { (grib_yyval.act) = grib_action_create_if(grib_parser_context,(grib_yyvsp[-4].exp),(grib_yyvsp[-1].act),0,1,grib_yylineno,file_being_parsed()); }
#line 3197 "y.tab.c"
    break;

  case 144: /* if_block: IF_TRANSIENT '(' expression ')' '{' instructions '}' ELSE '{' instructions '}'  */
#line 681 "griby.y"
                                                                                  { (grib_yyval.act) = grib_action_create_if(grib_parser_context,(grib_yyvsp[-8].exp),(grib_yyvsp[-5].act),(grib_yyvsp[-1].act),1,grib_yylineno,file_being_parsed()); }
#line 3203 "y.tab.c"
    break;

  case 145: /* when_block: WHEN '(' expression ')' set semi  */
#line 685 "griby.y"
                                     { (grib_yyval.act) = grib_action_create_when(grib_parser_context,(grib_yyvsp[-3].exp),(grib_yyvsp[-1].act),NULL); }
#line 3209 "y.tab.c"
    break;

  case 146: /* when_block: WHEN '(' expression ')' '{' set_list '}'  */
#line 686 "griby.y"
                                               { (grib_yyval.act) = grib_action_create_when(grib_parser_context,(grib_yyvsp[-4].exp),(grib_yyvsp[-1].act),NULL); }
#line 3215 "y.tab.c"
    break;

  case 147: /* when_block: WHEN '(' expression ')' '{' set_list '}' ELSE '{' set_list '}'  */
#line 687 "griby.y"
                                                                   { (grib_yyval.act) = grib_action_create_when(grib_parser_context,(grib_yyvsp[-8].exp),(grib_yyvsp[-5].act),(grib_yyvsp[-1].act)); }
#line 3221 "y.tab.c"
    break;

  case 148: /* set: SET IDENT '=' expression  */
#line 690 "griby.y"
                              { (grib_yyval.act) = grib_action_create_set(grib_parser_context,(grib_yyvsp[-2].str),(grib_yyvsp[0].exp),0); free((grib_yyvsp[-2].str)); }
#line 3227 "y.tab.c"
    break;

  case 149: /* set: SET_NOFAIL IDENT '=' expression  */
#line 691 "griby.y"
                                    { (grib_yyval.act) = grib_action_create_set(grib_parser_context,(grib_yyvsp[-2].str),(grib_yyvsp[0].exp),1); free((grib_yyvsp[-2].str)); }
#line 3233 "y.tab.c"
    break;

  case 151: /* set_list: set_list set semi  */
#line 695 "griby.y"
                             { (grib_yyvsp[-2].act)->next_ = (grib_yyvsp[-1].act); (grib_yyval.act) = (grib_yyvsp[-2].act); }
#line 3239 "y.tab.c"
    break;

  case 152: /* default: empty  */
#line 699 "griby.y"
               { (grib_yyval.explist) = NULL ;}
#line 3245 "y.tab.c"
    break;

  case 153: /* default: '=' argument_list  */
#line 700 "griby.y"
                       { (grib_yyval.explist) = (grib_yyvsp[0].explist) ;}
#line 3251 "y.tab.c"
    break;

  case 154: /* flags: empty  */
#line 703 "griby.y"
                     { (grib_yyval.lval) = 0 ; }
#line 3257 "y.tab.c"
    break;

  case 155: /* flags: ':' flag_list  */
#line 704 "griby.y"
                      { (grib_yyval.lval) = (grib_yyvsp[0].lval); }
#line 3263 "y.tab.c"
    break;

  case 157: /* flag_list: flag_list ',' flag  */
#line 708 "griby.y"
                        { (grib_yyval.lval) = (grib_yyvsp[-2].lval) | (grib_yyvsp[0].lval); }
#line 3269 "y.tab.c"
    break;

  case 158: /* flag: READ_ONLY  */
#line 711 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_READ_ONLY; }
#line 3275 "y.tab.c"
    break;

  case 159: /* flag: LOWERCASE  */
#line 712 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_LOWERCASE; }
#line 3281 "y.tab.c"
    break;

  case 160: /* flag: DUMP  */
#line 713 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_DUMP; }
#line 3287 "y.tab.c"
    break;

  case 161: /* flag: NO_COPY  */
#line 714 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_NO_COPY; }
#line 3293 "y.tab.c"
    break;

  case 162: /* flag: NO_FAIL  */
#line 715 "griby.y"
                             { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_NO_FAIL; }
#line 3299 "y.tab.c"
    break;

  case 163: /* flag: HIDDEN  */
#line 716 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_HIDDEN; }
#line 3305 "y.tab.c"
    break;

  case 164: /* flag: EDITION_SPECIFIC  */
#line 717 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_EDITION_SPECIFIC; }
#line 3311 "y.tab.c"
    break;

  case 165: /* flag: CAN_BE_MISSING  */
#line 718 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_CAN_BE_MISSING; }
#line 3317 "y.tab.c"
    break;

  case 166: /* flag: CONSTRAINT  */
#line 719 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_CONSTRAINT; }
#line 3323 "y.tab.c"
    break;

  case 167: /* flag: COPY_OK  */
#line 720 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_COPY_OK; }
#line 3329 "y.tab.c"
    break;

  case 168: /* flag: COPY_AS_INT  */
#line 721 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_COPY_AS_INT; }
#line 3335 "y.tab.c"
    break;

  case 169: /* flag: COPY_IF_CHANGING_EDITION  */
#line 722 "griby.y"
                               { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_COPY_IF_CHANGING_EDITION; }
#line 3341 "y.tab.c"
    break;

  case 170: /* flag: TRANS  */
#line 723 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_TRANSIENT; }
#line 3347 "y.tab.c"
    break;

  case 171: /* flag: STRING_TYPE  */
#line 724 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_STRING_TYPE; }
#line 3353 "y.tab.c"
    break;

  case 172: /* flag: LONG_TYPE  */
#line 725 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_LONG_TYPE; }
#line 3359 "y.tab.c"
    break;

  case 173: /* flag: DOUBLE_TYPE  */
#line 726 "griby.y"
                         { (grib_yyval.lval) = GRIB_ACCESSOR_FLAG_DOUBLE_TYPE; }
#line 3365 "y.tab.c"
    break;

  case 174: /* list_block: IDENT LIST '(' expression ')' '{' instructions '}'  */
#line 729 "griby.y"
                                                               { (grib_yyval.act) = grib_action_create_list(grib_parser_context,(grib_yyvsp[-7].str),(grib_yyvsp[-4].exp),(grib_yyvsp[-1].act)); free((grib_yyvsp[-7].str)); }
#line 3371 "y.tab.c"
    break;

  case 175: /* while_block: WHILE '(' expression ')' '{' instructions '}'  */
#line 732 "griby.y"
                                                           { (grib_yyval.act) = grib_action_create_while(grib_parser_context,(grib_yyvsp[-4].exp),(grib_yyvsp[-1].act));  }
#line 3377 "y.tab.c"
    break;

  case 176: /* trigger_block: TRIGGER '(' argument_list ')' '{' instructions '}'  */
#line 735 "griby.y"
                                                                  { (grib_yyval.act) = grib_action_create_trigger(grib_parser_context,(grib_yyvsp[-4].explist),(grib_yyvsp[-1].act));  }
#line 3383 "y.tab.c"
    break;

  case 177: /* concept_block: CONCEPT IDENT '{' concept_list '}' flags  */
#line 738 "griby.y"
                                                        { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-4].str),(grib_yyvsp[-2].concept_value),0,0,0,0,0,0,(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-4].str)); }
#line 3389 "y.tab.c"
    break;

  case 178: /* concept_block: CONCEPT IDENT '(' IDENT ')' '{' concept_list '}' flags  */
#line 739 "griby.y"
                                                            { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-7].str),(grib_yyvsp[-2].concept_value),0,0,(grib_yyvsp[-5].str),0,0,0,(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-7].str));free((grib_yyvsp[-5].str)); }
#line 3395 "y.tab.c"
    break;

  case 179: /* concept_block: CONCEPT IDENT '(' IDENT ',' STRING ',' IDENT ',' IDENT ')' flags  */
#line 740 "griby.y"
                                                                      { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-10].str),0,(grib_yyvsp[-6].str),0,(grib_yyvsp[-8].str),(grib_yyvsp[-4].str),(grib_yyvsp[-2].str),0,(grib_yyvsp[0].lval),0); free((grib_yyvsp[-10].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str));free((grib_yyvsp[-2].str)); }
#line 3401 "y.tab.c"
    break;

  case 180: /* concept_block: CONCEPT IDENT '(' IDENT ',' IDENT ',' IDENT ',' IDENT ')' flags  */
#line 741 "griby.y"
                                                                      { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-10].str),0,(grib_yyvsp[-6].str),0,(grib_yyvsp[-8].str),(grib_yyvsp[-4].str),(grib_yyvsp[-2].str),0,(grib_yyvsp[0].lval),0); free((grib_yyvsp[-10].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str));free((grib_yyvsp[-2].str)); }
#line 3407 "y.tab.c"
    break;

  case 181: /* concept_block: CONCEPT IDENT '(' IDENT ',' STRING ',' IDENT ',' IDENT ',' IDENT ')' flags  */
#line 742 "griby.y"
                                                                                { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-12].str),0,(grib_yyvsp[-8].str),0,(grib_yyvsp[-10].str),(grib_yyvsp[-6].str),(grib_yyvsp[-4].str),(grib_yyvsp[-2].str),(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-12].str));free((grib_yyvsp[-8].str));free((grib_yyvsp[-10].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-4].str));free((grib_yyvsp[-2].str)); }
#line 3413 "y.tab.c"
    break;

  case 182: /* concept_block: CONCEPT IDENT '(' IDENT ',' STRING ',' IDENT ')' flags  */
#line 743 "griby.y"
                                                            { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-8].str),0,(grib_yyvsp[-4].str),0,(grib_yyvsp[-6].str),(grib_yyvsp[-2].str),0,0,(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-2].str)); }
#line 3419 "y.tab.c"
    break;

  case 183: /* concept_block: CONCEPT IDENT '.' IDENT '(' IDENT ',' STRING ',' IDENT ',' IDENT ')' flags  */
#line 745 "griby.y"
                                                                                { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-10].str),0,(grib_yyvsp[-6].str),(grib_yyvsp[-12].str),(grib_yyvsp[-8].str),(grib_yyvsp[-4].str),(grib_yyvsp[-2].str),0,(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-10].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str)); free((grib_yyvsp[-2].str)); free((grib_yyvsp[-12].str));}
#line 3425 "y.tab.c"
    break;

  case 184: /* concept_block: CONCEPT IDENT '.' IDENT '(' IDENT ',' IDENT ',' IDENT ',' IDENT ')' flags  */
#line 746 "griby.y"
                                                                                { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-10].str),0,(grib_yyvsp[-6].str),(grib_yyvsp[-12].str),(grib_yyvsp[-8].str),(grib_yyvsp[-4].str),(grib_yyvsp[-2].str),0,(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-10].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str)); free((grib_yyvsp[-2].str)); free((grib_yyvsp[-12].str));}
#line 3431 "y.tab.c"
    break;

  case 185: /* concept_block: CONCEPT IDENT '.' IDENT '(' IDENT ',' STRING ',' IDENT ')' flags  */
#line 747 "griby.y"
                                                                      { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-8].str),0,(grib_yyvsp[-4].str),(grib_yyvsp[-10].str),(grib_yyvsp[-6].str),(grib_yyvsp[-2].str),0,0,(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-2].str)); free((grib_yyvsp[-10].str));}
#line 3437 "y.tab.c"
    break;

  case 186: /* concept_block: CONCEPT IDENT '.' IDENT '{' concept_list '}' flags  */
#line 748 "griby.y"
                                                        { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-4].str),(grib_yyvsp[-2].concept_value),0,(grib_yyvsp[-6].str),0,0,0,0,(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-6].str));free((grib_yyvsp[-4].str)); }
#line 3443 "y.tab.c"
    break;

  case 187: /* concept_block: CONCEPT IDENT '.' IDENT '(' IDENT ')' '{' concept_list '}' flags  */
#line 749 "griby.y"
                                                                      { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-7].str),(grib_yyvsp[-2].concept_value),0,(grib_yyvsp[-9].str),(grib_yyvsp[-5].str),0,0,0,(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-9].str));free((grib_yyvsp[-7].str));free((grib_yyvsp[-5].str)); }
#line 3449 "y.tab.c"
    break;

  case 188: /* concept_block: CONCEPT_NOFAIL IDENT '{' concept_list '}' flags  */
#line 751 "griby.y"
                                                     { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-4].str),(grib_yyvsp[-2].concept_value),0,0,0,0,0,0,(grib_yyvsp[0].lval),1);  free((grib_yyvsp[-4].str)); }
#line 3455 "y.tab.c"
    break;

  case 189: /* concept_block: CONCEPT_NOFAIL IDENT '(' IDENT ')' '{' concept_list '}' flags  */
#line 752 "griby.y"
                                                                   { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-7].str),(grib_yyvsp[-2].concept_value),0,0,(grib_yyvsp[-5].str),0,0,0,(grib_yyvsp[0].lval),1);  free((grib_yyvsp[-7].str));free((grib_yyvsp[-5].str)); }
#line 3461 "y.tab.c"
    break;

  case 190: /* concept_block: CONCEPT_NOFAIL IDENT '(' IDENT ',' STRING ',' IDENT ',' IDENT ')' flags  */
#line 753 "griby.y"
                                                                             { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-10].str),0,(grib_yyvsp[-6].str),0,(grib_yyvsp[-8].str),(grib_yyvsp[-4].str),(grib_yyvsp[-2].str),0,(grib_yyvsp[0].lval),1);  free((grib_yyvsp[-10].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str));free((grib_yyvsp[-2].str)); }
#line 3467 "y.tab.c"
    break;

  case 191: /* concept_block: CONCEPT_NOFAIL IDENT '(' IDENT ',' STRING ',' IDENT ')' flags  */
#line 754 "griby.y"
                                                                   { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-8].str),0,(grib_yyvsp[-4].str),0,(grib_yyvsp[-6].str),(grib_yyvsp[-2].str),0,0,(grib_yyvsp[0].lval),1);  free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-2].str)); }
#line 3473 "y.tab.c"
    break;

  case 192: /* concept_block: CONCEPT_NOFAIL IDENT '.' IDENT '(' IDENT ',' STRING ',' IDENT ',' IDENT ')' flags  */
#line 755 "griby.y"
                                                                                       { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-10].str),0,(grib_yyvsp[-6].str),(grib_yyvsp[-12].str),(grib_yyvsp[-8].str),(grib_yyvsp[-4].str),(grib_yyvsp[-2].str),0,(grib_yyvsp[0].lval),1);  free((grib_yyvsp[-10].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str));free((grib_yyvsp[-2].str)); free((grib_yyvsp[-12].str));}
#line 3479 "y.tab.c"
    break;

  case 193: /* concept_block: CONCEPT_NOFAIL IDENT '.' IDENT '(' IDENT ',' STRING ',' IDENT ')' flags  */
#line 756 "griby.y"
                                                                             { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-8].str),0,(grib_yyvsp[-4].str),(grib_yyvsp[-10].str),(grib_yyvsp[-6].str),(grib_yyvsp[-2].str),0,0,(grib_yyvsp[0].lval),1);  free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-2].str)); free((grib_yyvsp[-10].str));}
#line 3485 "y.tab.c"
    break;

  case 194: /* concept_block: CONCEPT_NOFAIL IDENT '.' IDENT '{' concept_list '}' flags  */
#line 757 "griby.y"
                                                               { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-4].str),(grib_yyvsp[-2].concept_value),0,(grib_yyvsp[-6].str),0,0,0,0,(grib_yyvsp[0].lval),1);  free((grib_yyvsp[-6].str));free((grib_yyvsp[-4].str)); }
#line 3491 "y.tab.c"
    break;

  case 195: /* concept_block: CONCEPT_NOFAIL IDENT '.' IDENT '(' IDENT ')' '{' concept_list '}' flags  */
#line 758 "griby.y"
                                                                             { (grib_yyval.act) = grib_action_create_concept(grib_parser_context,(grib_yyvsp[-7].str),(grib_yyvsp[-2].concept_value),0,(grib_yyvsp[-9].str),(grib_yyvsp[-5].str),0,0,0,(grib_yyvsp[0].lval),1);  free((grib_yyvsp[-9].str));free((grib_yyvsp[-7].str));free((grib_yyvsp[-5].str)); }
#line 3497 "y.tab.c"
    break;

  case 197: /* concept_list: concept_list concept_value  */
#line 763 "griby.y"
                                          { (grib_yyval.concept_value) = (grib_yyvsp[0].concept_value); (grib_yyvsp[0].concept_value)->next = (grib_yyvsp[-1].concept_value);   }
#line 3503 "y.tab.c"
    break;

  case 199: /* hash_array_list: hash_array_list hash_array_value  */
#line 767 "griby.y"
                                                { (grib_yyval.hash_array_value) = (grib_yyvsp[0].hash_array_value); (grib_yyvsp[0].hash_array_value)->next = (grib_yyvsp[-1].hash_array_value);   }
#line 3509 "y.tab.c"
    break;

  case 200: /* hash_array_block: HASH_ARRAY IDENT '{' hash_array_list '}' flags  */
#line 770 "griby.y"
                                                                 { (grib_yyval.act) = grib_action_create_hash_array(grib_parser_context,(grib_yyvsp[-4].str),(grib_yyvsp[-2].hash_array_value),0,0,0,0,0,0,(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-4].str)); }
#line 3515 "y.tab.c"
    break;

  case 201: /* hash_array_block: HASH_ARRAY IDENT '(' IDENT ',' STRING ',' IDENT ',' IDENT ')' flags  */
#line 771 "griby.y"
                                                                         { (grib_yyval.act) = grib_action_create_hash_array(grib_parser_context,(grib_yyvsp[-10].str),0,(grib_yyvsp[-6].str),0,(grib_yyvsp[-8].str),(grib_yyvsp[-4].str),(grib_yyvsp[-2].str),0,(grib_yyvsp[0].lval),0);  free((grib_yyvsp[-10].str));free((grib_yyvsp[-6].str));free((grib_yyvsp[-8].str));free((grib_yyvsp[-4].str));free((grib_yyvsp[-2].str)); }
#line 3521 "y.tab.c"
    break;

  case 203: /* case_list: case_list case_value  */
#line 775 "griby.y"
                                    { (grib_yyval.case_value) = (grib_yyvsp[0].case_value); (grib_yyvsp[0].case_value)->next = (grib_yyvsp[-1].case_value);   }
#line 3527 "y.tab.c"
    break;

  case 204: /* case_value: CASE arguments ':' instructions  */
#line 778 "griby.y"
                                              { (grib_yyval.case_value) = grib_case_new(grib_parser_context,(grib_yyvsp[-2].explist),(grib_yyvsp[0].act));  }
#line 3533 "y.tab.c"
    break;

  case 205: /* switch_block: SWITCH '(' argument_list ')' '{' case_list DEFAULT ':' instructions '}'  */
#line 782 "griby.y"
                                                                           { (grib_yyval.act) = grib_action_create_switch(grib_parser_context,(grib_yyvsp[-7].explist),(grib_yyvsp[-4].case_value),(grib_yyvsp[-1].act)); }
#line 3539 "y.tab.c"
    break;

  case 206: /* switch_block: SWITCH '(' argument_list ')' '{' case_list DEFAULT ':' '}'  */
#line 783 "griby.y"
                                                               { (grib_yyval.act) = grib_action_create_switch(grib_parser_context,(grib_yyvsp[-6].explist),(grib_yyvsp[-3].case_value),grib_action_create_noop(grib_parser_context,"continue")); }
#line 3545 "y.tab.c"
    break;

  case 207: /* switch_block: SWITCH '(' argument_list ')' '{' case_list '}'  */
#line 784 "griby.y"
                                                   { (grib_yyval.act) = grib_action_create_switch(grib_parser_context,(grib_yyvsp[-4].explist),(grib_yyvsp[-1].case_value),0); }
#line 3551 "y.tab.c"
    break;

  case 208: /* concept_value: STRING '=' '{' concept_conditions '}'  */
#line 787 "griby.y"
                                                      {
	  				(grib_yyval.concept_value) = grib_concept_value_new(grib_parser_context,(grib_yyvsp[-4].str),(grib_yyvsp[-1].concept_condition)); free((grib_yyvsp[-4].str));}
#line 3558 "y.tab.c"
    break;

  case 209: /* concept_value: IDENT '=' '{' concept_conditions '}'  */
#line 789 "griby.y"
                                                                       {
	  				(grib_yyval.concept_value) = grib_concept_value_new(grib_parser_context,(grib_yyvsp[-4].str),(grib_yyvsp[-1].concept_condition)); free((grib_yyvsp[-4].str));}
#line 3565 "y.tab.c"
    break;

  case 210: /* concept_value: INTEGER '=' '{' concept_conditions '}'  */
#line 791 "griby.y"
                                                                         {
					char buf[80]; snprintf(buf, sizeof(buf), "%ld",(long)(grib_yyvsp[-4].lval)); (grib_yyval.concept_value) = grib_concept_value_new(grib_parser_context,buf,(grib_yyvsp[-1].concept_condition));}
#line 3572 "y.tab.c"
    break;

  case 211: /* concept_value: FLOAT '=' '{' concept_conditions '}'  */
#line 793 "griby.y"
                                                                       {
					char buf[80]; snprintf(buf, sizeof(buf), "%g", (double)(grib_yyvsp[-4].dval)); (grib_yyval.concept_value) = grib_concept_value_new(grib_parser_context,buf,(grib_yyvsp[-1].concept_condition));}
#line 3579 "y.tab.c"
    break;

  case 213: /* concept_conditions: concept_condition concept_conditions  */
#line 798 "griby.y"
                                                       { (grib_yyvsp[-1].concept_condition)->next = (grib_yyvsp[0].concept_condition); (grib_yyval.concept_condition) = (grib_yyvsp[-1].concept_condition); }
#line 3585 "y.tab.c"
    break;

  case 214: /* concept_condition: IDENT '=' expression ';'  */
#line 801 "griby.y"
                                            { (grib_yyval.concept_condition) = grib_concept_condition_new(grib_parser_context,(grib_yyvsp[-3].str),(grib_yyvsp[-1].exp),0); free((grib_yyvsp[-3].str)); }
#line 3591 "y.tab.c"
    break;

  case 215: /* concept_condition: IDENT '=' '[' integer_array ']' ';'  */
#line 802 "griby.y"
                                               { (grib_yyval.concept_condition) = grib_concept_condition_new(grib_parser_context,(grib_yyvsp[-5].str),0,(grib_yyvsp[-2].ivalue)); free((grib_yyvsp[-5].str)); }
#line 3597 "y.tab.c"
    break;

  case 216: /* hash_array_value: STRING '=' '[' integer_array ']'  */
#line 806 "griby.y"
                                                    {
	  				(grib_yyval.hash_array_value) = grib_integer_hash_array_value_new((grib_yyvsp[-4].str),(grib_yyvsp[-1].ivalue)); free((grib_yyvsp[-4].str));}
#line 3604 "y.tab.c"
    break;

  case 217: /* hash_array_value: IDENT '=' '[' integer_array ']'  */
#line 808 "griby.y"
                                                                  {
	  				(grib_yyval.hash_array_value) = grib_integer_hash_array_value_new((grib_yyvsp[-4].str),(grib_yyvsp[-1].ivalue)); free((grib_yyvsp[-4].str));}
#line 3611 "y.tab.c"
    break;

  case 218: /* string_or_ident: SUBSTR '(' IDENT ',' INTEGER ',' INTEGER ')'  */
#line 812 "griby.y"
                                                              { (grib_yyval.exp) = new_accessor_expression(grib_parser_context,(grib_yyvsp[-5].str),(grib_yyvsp[-3].lval),(grib_yyvsp[-1].lval)); free((grib_yyvsp[-5].str)); }
#line 3617 "y.tab.c"
    break;

  case 219: /* string_or_ident: IDENT  */
#line 813 "griby.y"
                                          { (grib_yyval.exp) = new_accessor_expression(grib_parser_context,(grib_yyvsp[0].str),0,0); free((grib_yyvsp[0].str)); }
#line 3623 "y.tab.c"
    break;

  case 220: /* string_or_ident: SUBSTR '(' STRING ',' INTEGER ',' INTEGER ')'  */
#line 814 "griby.y"
                                                                { (grib_yyval.exp) = new_sub_string_expression(grib_parser_context,(grib_yyvsp[-5].str),(grib_yyvsp[-3].lval),(grib_yyvsp[-1].lval)); free((grib_yyvsp[-5].str)); }
#line 3629 "y.tab.c"
    break;

  case 221: /* string_or_ident: STRING  */
#line 815 "griby.y"
                          { (grib_yyval.exp) = new_string_expression(grib_parser_context,(grib_yyvsp[0].str));  free((grib_yyvsp[0].str)); }
#line 3635 "y.tab.c"
    break;

  case 223: /* atom: INTEGER  */
#line 819 "griby.y"
                { (grib_yyval.exp) = new_long_expression(grib_parser_context,(grib_yyvsp[0].lval));  }
#line 3641 "y.tab.c"
    break;

  case 224: /* atom: FLOAT  */
#line 820 "griby.y"
              { (grib_yyval.exp) = new_double_expression(grib_parser_context,(grib_yyvsp[0].dval));  /* TODO: change to new_float_expression*/}
#line 3647 "y.tab.c"
    break;

  case 225: /* atom: NIL  */
#line 822 "griby.y"
              { (grib_yyval.exp) = NULL; }
#line 3653 "y.tab.c"
    break;

  case 226: /* atom: DUMMY  */
#line 823 "griby.y"
                    { (grib_yyval.exp) = new_true_expression(grib_parser_context); }
#line 3659 "y.tab.c"
    break;

  case 227: /* atom: '(' expression ')'  */
#line 824 "griby.y"
                           { (grib_yyval.exp) = (grib_yyvsp[-1].exp); }
#line 3665 "y.tab.c"
    break;

  case 228: /* atom: '-' atom  */
#line 825 "griby.y"
                 { (grib_yyval.exp) = new_unop_expression(grib_parser_context,&grib_op_neg,&grib_op_neg_d,(grib_yyvsp[0].exp)); }
#line 3671 "y.tab.c"
    break;

  case 229: /* atom: IDENT '(' ')'  */
#line 826 "griby.y"
                    { (grib_yyval.exp) = new_func_expression(grib_parser_context,(grib_yyvsp[-2].str),NULL); free((grib_yyvsp[-2].str));}
#line 3677 "y.tab.c"
    break;

  case 230: /* atom: IDENT '(' argument_list ')'  */
#line 827 "griby.y"
                                  { (grib_yyval.exp) = new_func_expression(grib_parser_context,(grib_yyvsp[-3].str),(grib_yyvsp[-1].explist)); free((grib_yyvsp[-3].str));}
#line 3683 "y.tab.c"
    break;

  case 231: /* power: atom '^' power  */
#line 831 "griby.y"
                          { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_pow,NULL,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3689 "y.tab.c"
    break;

  case 233: /* factor: factor '*' power  */
#line 835 "griby.y"
                            { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_mul,&grib_op_mul_d,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3695 "y.tab.c"
    break;

  case 234: /* factor: factor '/' power  */
#line 836 "griby.y"
                                     { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_div,&grib_op_div_d,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3701 "y.tab.c"
    break;

  case 235: /* factor: factor '%' power  */
#line 837 "griby.y"
                                     { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_modulo,NULL,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3707 "y.tab.c"
    break;

  case 236: /* factor: factor BIT power  */
#line 838 "griby.y"
                                  { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_bit,NULL,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3713 "y.tab.c"
    break;

  case 237: /* factor: factor BITOFF power  */
#line 839 "griby.y"
                                  { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_bitoff,NULL,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3719 "y.tab.c"
    break;

  case 239: /* factor: LENGTH '(' IDENT ')'  */
#line 841 "griby.y"
                                   { (grib_yyval.exp) = new_length_expression(grib_parser_context,(grib_yyvsp[-1].str)); free((grib_yyvsp[-1].str));}
#line 3725 "y.tab.c"
    break;

  case 240: /* factor: IS_IN_LIST '(' IDENT ',' STRING ')'  */
#line 842 "griby.y"
                                                  { (grib_yyval.exp) = new_is_in_list_expression(grib_parser_context,(grib_yyvsp[-3].str),(grib_yyvsp[-1].str)); free((grib_yyvsp[-3].str));free((grib_yyvsp[-1].str));}
#line 3731 "y.tab.c"
    break;

  case 241: /* factor: IS_IN_DICT '(' IDENT ',' STRING ')'  */
#line 843 "griby.y"
                                                  { (grib_yyval.exp) = new_is_in_dict_expression(grib_parser_context,(grib_yyvsp[-3].str),(grib_yyvsp[-1].str)); free((grib_yyvsp[-3].str));free((grib_yyvsp[-1].str));}
#line 3737 "y.tab.c"
    break;

  case 242: /* factor: IS_INTEGER '(' IDENT ',' INTEGER ')'  */
#line 844 "griby.y"
                                                   { (grib_yyval.exp) = new_is_integer_expression(grib_parser_context,(grib_yyvsp[-3].str),(grib_yyvsp[-1].lval),0); free((grib_yyvsp[-3].str));}
#line 3743 "y.tab.c"
    break;

  case 243: /* factor: IS_INTEGER '(' IDENT ',' INTEGER ',' INTEGER ')'  */
#line 845 "griby.y"
                                                               { (grib_yyval.exp) = new_is_integer_expression(grib_parser_context,(grib_yyvsp[-5].str),(grib_yyvsp[-3].lval),(grib_yyvsp[-1].lval)); free((grib_yyvsp[-5].str));}
#line 3749 "y.tab.c"
    break;

  case 244: /* factor: IS_INTEGER '(' IDENT ')'  */
#line 846 "griby.y"
                                       { (grib_yyval.exp) = new_is_integer_expression(grib_parser_context,(grib_yyvsp[-1].str),0,0); free((grib_yyvsp[-1].str));}
#line 3755 "y.tab.c"
    break;

  case 245: /* term: term '+' factor  */
#line 849 "griby.y"
                         { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_add,&grib_op_add_d,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3761 "y.tab.c"
    break;

  case 246: /* term: term '-' factor  */
#line 850 "griby.y"
                                    { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_sub,&grib_op_sub_d,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3767 "y.tab.c"
    break;

  case 248: /* condition: condition GT term  */
#line 854 "griby.y"
                                { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_gt,&grib_op_gt_d,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3773 "y.tab.c"
    break;

  case 249: /* condition: condition EQ term  */
#line 856 "griby.y"
                                     { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_eq,&grib_op_eq_d,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3779 "y.tab.c"
    break;

  case 250: /* condition: condition LT term  */
#line 857 "griby.y"
                                     { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_lt,&grib_op_lt_d,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3785 "y.tab.c"
    break;

  case 251: /* condition: condition GE term  */
#line 858 "griby.y"
                                     { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_ge,&grib_op_ge_d,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3791 "y.tab.c"
    break;

  case 252: /* condition: condition LE term  */
#line 859 "griby.y"
                                     { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_le,&grib_op_le_d,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3797 "y.tab.c"
    break;

  case 253: /* condition: condition NE term  */
#line 860 "griby.y"
                                     { (grib_yyval.exp) = new_binop_expression(grib_parser_context,&grib_op_ne,&grib_op_ne_d,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3803 "y.tab.c"
    break;

  case 254: /* condition: string_or_ident IS string_or_ident  */
#line 861 "griby.y"
                                                  { (grib_yyval.exp) = new_string_compare_expression(grib_parser_context,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp),1); }
#line 3809 "y.tab.c"
    break;

  case 255: /* condition: string_or_ident ISNOT string_or_ident  */
#line 862 "griby.y"
                                                     { (grib_yyval.exp) = new_string_compare_expression(grib_parser_context,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp),0); }
#line 3815 "y.tab.c"
    break;

  case 256: /* condition: NOT condition  */
#line 867 "griby.y"
                                      { (grib_yyval.exp) = new_unop_expression(grib_parser_context,&grib_op_not,NULL,(grib_yyvsp[0].exp)); }
#line 3821 "y.tab.c"
    break;

  case 258: /* conjunction: conjunction AND condition  */
#line 871 "griby.y"
                                       { (grib_yyval.exp) = new_logical_and_expression(grib_parser_context,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp)); }
#line 3827 "y.tab.c"
    break;

  case 260: /* disjunction: disjunction OR conjunction  */
#line 875 "griby.y"
                                        { (grib_yyval.exp) = new_logical_or_expression(grib_parser_context,(grib_yyvsp[-2].exp),(grib_yyvsp[0].exp));}
#line 3833 "y.tab.c"
    break;

  case 265: /* rule_entry: IDENT '=' expression ';'  */
#line 889 "griby.y"
                                      { (grib_yyval.rule_entry) = grib_new_rule_entry(grib_parser_context,(grib_yyvsp[-3].str),(grib_yyvsp[-1].exp)); free((grib_yyvsp[-3].str)); }
#line 3839 "y.tab.c"
    break;

  case 266: /* rule_entry: SKIP ';'  */
#line 890 "griby.y"
                       { (grib_yyval.rule_entry) = grib_new_rule_entry(grib_parser_context,"skip",0);}
#line 3845 "y.tab.c"
    break;

  case 268: /* rule_entries: rule_entry rule_entries  */
#line 894 "griby.y"
                                       { (grib_yyvsp[-1].rule_entry)->next = (grib_yyvsp[0].rule_entry); (grib_yyval.rule_entry) = (grib_yyvsp[-1].rule_entry); }
#line 3851 "y.tab.c"
    break;

  case 269: /* fact: rule_entry  */
#line 897 "griby.y"
                  { (grib_yyval.rules) = grib_new_rule(grib_parser_context,NULL,(grib_yyvsp[0].rule_entry)); }
#line 3857 "y.tab.c"
    break;

  case 270: /* conditional_rule: IF '(' expression ')' '{' rule_entries '}'  */
#line 901 "griby.y"
                                                             { (grib_yyval.rules) = grib_new_rule(grib_parser_context,(grib_yyvsp[-4].exp),(grib_yyvsp[-1].rule_entry)); }
#line 3863 "y.tab.c"
    break;

  case 272: /* rules: rule rules  */
#line 905 "griby.y"
                   { (grib_yyvsp[-1].rules)->next = (grib_yyvsp[0].rules); (grib_yyval.rules) = (grib_yyvsp[-1].rules); }
#line 3869 "y.tab.c"
    break;


#line 3873 "y.tab.c"

      default: break;
    }
  /* User semantic actions sometimes alter grib_yychar, and that requires
     that grib_yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of grib_yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering grib_yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (grib_yysymbol_kind_t, grib_yyr1[grib_yyn]), &grib_yyval, &grib_yyloc);

  YYPOPSTACK (grib_yylen);
  grib_yylen = 0;

  *++grib_yyvsp = grib_yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int grib_yylhs = grib_yyr1[grib_yyn] - YYNTOKENS;
    const int grib_yyi = grib_yypgoto[grib_yylhs] + *grib_yyssp;
    grib_yystate = (0 <= grib_yyi && grib_yyi <= YYLAST && grib_yycheck[grib_yyi] == *grib_yyssp
               ? grib_yytable[grib_yyi]
               : grib_yydefgoto[grib_yylhs]);
  }

  goto grib_yynewstate;


/*--------------------------------------.
| grib_yyerrlab -- here on detecting error.  |
`--------------------------------------*/
grib_yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  grib_yytoken = grib_yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (grib_yychar);
  /* If not already recovering from an error, report this error.  */
  if (!grib_yyerrstatus)
    {
      ++grib_yynerrs;
      grib_yyerror (YY_("syntax error"));
    }

  if (grib_yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (grib_yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (grib_yychar == YYEOF)
            YYABORT;
        }
      else
        {
          grib_yydestruct ("Error: discarding",
                      grib_yytoken, &grib_yylval);
          grib_yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto grib_yyerrlab1;


/*---------------------------------------------------.
| grib_yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
grib_yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label grib_yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++grib_yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (grib_yylen);
  grib_yylen = 0;
  YY_STACK_PRINT (grib_yyss, grib_yyssp);
  grib_yystate = *grib_yyssp;
  goto grib_yyerrlab1;


/*-------------------------------------------------------------.
| grib_yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
grib_yyerrlab1:
  grib_yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      grib_yyn = grib_yypact[grib_yystate];
      if (!grib_yypact_value_is_default (grib_yyn))
        {
          grib_yyn += YYSYMBOL_YYerror;
          if (0 <= grib_yyn && grib_yyn <= YYLAST && grib_yycheck[grib_yyn] == YYSYMBOL_YYerror)
            {
              grib_yyn = grib_yytable[grib_yyn];
              if (0 < grib_yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (grib_yyssp == grib_yyss)
        YYABORT;


      grib_yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (grib_yystate), grib_yyvsp);
      YYPOPSTACK (1);
      grib_yystate = *grib_yyssp;
      YY_STACK_PRINT (grib_yyss, grib_yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++grib_yyvsp = grib_yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (grib_yyn), grib_yyvsp, grib_yylsp);

  grib_yystate = grib_yyn;
  goto grib_yynewstate;


/*-------------------------------------.
| grib_yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
grib_yyacceptlab:
  grib_yyresult = 0;
  goto grib_yyreturnlab;


/*-----------------------------------.
| grib_yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
grib_yyabortlab:
  grib_yyresult = 1;
  goto grib_yyreturnlab;


/*-----------------------------------------------------------.
| grib_yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
grib_yyexhaustedlab:
  grib_yyerror (YY_("memory exhausted"));
  grib_yyresult = 2;
  goto grib_yyreturnlab;


/*----------------------------------------------------------.
| grib_yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
grib_yyreturnlab:
  if (grib_yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      grib_yytoken = YYTRANSLATE (grib_yychar);
      grib_yydestruct ("Cleanup: discarding lookahead",
                  grib_yytoken, &grib_yylval);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (grib_yylen);
  YY_STACK_PRINT (grib_yyss, grib_yyssp);
  while (grib_yyssp != grib_yyss)
    {
      grib_yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*grib_yyssp), grib_yyvsp);
      YYPOPSTACK (1);
    }
#ifndef grib_yyoverflow
  if (grib_yyss != grib_yyssa)
    YYSTACK_FREE (grib_yyss);
#endif

  return grib_yyresult;
}

#line 909 "griby.y"


static grib_concept_value *_reverse_concept(grib_concept_value *r,grib_concept_value *s)
{
    grib_concept_value *v;

    if(r == NULL) return s;

    v         = r->next;
    r->next   = s;
    return _reverse_concept(v,r);
}

static grib_concept_value* reverse_concept(grib_concept_value* r)
{
    return _reverse_concept(r,NULL);
}

static grib_hash_array_value *_reverse_hash_array(grib_hash_array_value *r,grib_hash_array_value *s)
{
    grib_hash_array_value *v;

    if(r == NULL) return s;

    v         = r->next;
    r->next   = s;
    return _reverse_hash_array(v,r);
}

static grib_hash_array_value* reverse_hash_array(grib_hash_array_value* r)
{
    return _reverse_hash_array(r,NULL);
}
