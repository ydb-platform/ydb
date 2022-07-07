/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

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

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     CHAR = 258,
     NUMBER = 259,
     SECTEND = 260,
     SCDECL = 261,
     XSCDECL = 262,
     NAME = 263,
     PREVCCL = 264,
     EOF_OP = 265,
     OPTION_OP = 266,
     OPT_OUTFILE = 267,
     OPT_PREFIX = 268,
     OPT_YYCLASS = 269,
     CCE_ALNUM = 270,
     CCE_ALPHA = 271,
     CCE_BLANK = 272,
     CCE_CNTRL = 273,
     CCE_DIGIT = 274,
     CCE_GRAPH = 275,
     CCE_LOWER = 276,
     CCE_PRINT = 277,
     CCE_PUNCT = 278,
     CCE_SPACE = 279,
     CCE_UPPER = 280,
     CCE_XDIGIT = 281
   };
#endif
/* Tokens.  */
#define CHAR 258
#define NUMBER 259
#define SECTEND 260
#define SCDECL 261
#define XSCDECL 262
#define NAME 263
#define PREVCCL 264
#define EOF_OP 265
#define OPTION_OP 266
#define OPT_OUTFILE 267
#define OPT_PREFIX 268
#define OPT_YYCLASS 269
#define CCE_ALNUM 270
#define CCE_ALPHA 271
#define CCE_BLANK 272
#define CCE_CNTRL 273
#define CCE_DIGIT 274
#define CCE_GRAPH 275
#define CCE_LOWER 276
#define CCE_PRINT 277
#define CCE_PUNCT 278
#define CCE_SPACE 279
#define CCE_UPPER 280
#define CCE_XDIGIT 281




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef int YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif

extern YYSTYPE yylval;
