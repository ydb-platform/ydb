/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

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

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_DAP_DAP_TAB_H_INCLUDED
# define YY_DAP_DAP_TAB_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 1
#endif
#if YYDEBUG
extern int dapdebug;
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
    SCAN_ALIAS = 258,              /* SCAN_ALIAS  */
    SCAN_ARRAY = 259,              /* SCAN_ARRAY  */
    SCAN_ATTR = 260,               /* SCAN_ATTR  */
    SCAN_BYTE = 261,               /* SCAN_BYTE  */
    SCAN_CODE = 262,               /* SCAN_CODE  */
    SCAN_DATASET = 263,            /* SCAN_DATASET  */
    SCAN_DATA = 264,               /* SCAN_DATA  */
    SCAN_ERROR = 265,              /* SCAN_ERROR  */
    SCAN_FLOAT32 = 266,            /* SCAN_FLOAT32  */
    SCAN_FLOAT64 = 267,            /* SCAN_FLOAT64  */
    SCAN_GRID = 268,               /* SCAN_GRID  */
    SCAN_INT16 = 269,              /* SCAN_INT16  */
    SCAN_INT32 = 270,              /* SCAN_INT32  */
    SCAN_MAPS = 271,               /* SCAN_MAPS  */
    SCAN_MESSAGE = 272,            /* SCAN_MESSAGE  */
    SCAN_SEQUENCE = 273,           /* SCAN_SEQUENCE  */
    SCAN_STRING = 274,             /* SCAN_STRING  */
    SCAN_STRUCTURE = 275,          /* SCAN_STRUCTURE  */
    SCAN_UINT16 = 276,             /* SCAN_UINT16  */
    SCAN_UINT32 = 277,             /* SCAN_UINT32  */
    SCAN_URL = 278,                /* SCAN_URL  */
    SCAN_PTYPE = 279,              /* SCAN_PTYPE  */
    SCAN_PROG = 280,               /* SCAN_PROG  */
    WORD_WORD = 281,               /* WORD_WORD  */
    WORD_STRING = 282              /* WORD_STRING  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef int YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif




int dapparse (DAPparsestate* parsestate);


#endif /* !YY_DAP_DAP_TAB_H_INCLUDED  */
