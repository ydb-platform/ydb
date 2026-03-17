/* A Bison parser, made by GNU Bison 3.5.1.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2020 Free Software Foundation,
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

/* Undocumented macros, especially those whose name start with YY_,
   are private implementation details.  Do not rely on them.  */

#ifndef YY_PJ_WKT1_WKT1_GENERATED_PARSER_H_INCLUDED
# define YY_PJ_WKT1_WKT1_GENERATED_PARSER_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int pj_wkt1_debug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    END = 0,
    T_PARAM_MT = 258,
    T_CONCAT_MT = 259,
    T_INVERSE_MT = 260,
    T_PASSTHROUGH_MT = 261,
    T_PROJCS = 262,
    T_PROJECTION = 263,
    T_GEOGCS = 264,
    T_DATUM = 265,
    T_SPHEROID = 266,
    T_PRIMEM = 267,
    T_UNIT = 268,
    T_LINUNIT = 269,
    T_GEOCCS = 270,
    T_AUTHORITY = 271,
    T_VERT_CS = 272,
    T_VERTCS = 273,
    T_VERT_DATUM = 274,
    T_VDATUM = 275,
    T_COMPD_CS = 276,
    T_AXIS = 277,
    T_TOWGS84 = 278,
    T_FITTED_CS = 279,
    T_LOCAL_CS = 280,
    T_LOCAL_DATUM = 281,
    T_PARAMETER = 282,
    T_EXTENSION = 283,
    T_STRING = 284,
    T_NUMBER = 285,
    T_IDENTIFIER = 286
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef int YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int pj_wkt1_parse (pj_wkt1_parse_context *context);

#endif /* !YY_PJ_WKT1_WKT1_GENERATED_PARSER_H_INCLUDED  */
