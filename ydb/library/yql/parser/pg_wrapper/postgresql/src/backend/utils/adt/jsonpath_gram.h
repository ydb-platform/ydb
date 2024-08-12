/* A Bison parser, made by GNU Bison 3.7.5.  */

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

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_JSONPATH_YY_JSONPATH_GRAM_H_INCLUDED
# define YY_JSONPATH_YY_JSONPATH_GRAM_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int jsonpath_yydebug;
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
    TO_P = 258,                    /* TO_P  */
    NULL_P = 259,                  /* NULL_P  */
    TRUE_P = 260,                  /* TRUE_P  */
    FALSE_P = 261,                 /* FALSE_P  */
    IS_P = 262,                    /* IS_P  */
    UNKNOWN_P = 263,               /* UNKNOWN_P  */
    EXISTS_P = 264,                /* EXISTS_P  */
    IDENT_P = 265,                 /* IDENT_P  */
    STRING_P = 266,                /* STRING_P  */
    NUMERIC_P = 267,               /* NUMERIC_P  */
    INT_P = 268,                   /* INT_P  */
    VARIABLE_P = 269,              /* VARIABLE_P  */
    OR_P = 270,                    /* OR_P  */
    AND_P = 271,                   /* AND_P  */
    NOT_P = 272,                   /* NOT_P  */
    LESS_P = 273,                  /* LESS_P  */
    LESSEQUAL_P = 274,             /* LESSEQUAL_P  */
    EQUAL_P = 275,                 /* EQUAL_P  */
    NOTEQUAL_P = 276,              /* NOTEQUAL_P  */
    GREATEREQUAL_P = 277,          /* GREATEREQUAL_P  */
    GREATER_P = 278,               /* GREATER_P  */
    ANY_P = 279,                   /* ANY_P  */
    STRICT_P = 280,                /* STRICT_P  */
    LAX_P = 281,                   /* LAX_P  */
    LAST_P = 282,                  /* LAST_P  */
    STARTS_P = 283,                /* STARTS_P  */
    WITH_P = 284,                  /* WITH_P  */
    LIKE_REGEX_P = 285,            /* LIKE_REGEX_P  */
    FLAG_P = 286,                  /* FLAG_P  */
    ABS_P = 287,                   /* ABS_P  */
    SIZE_P = 288,                  /* SIZE_P  */
    TYPE_P = 289,                  /* TYPE_P  */
    FLOOR_P = 290,                 /* FLOOR_P  */
    DOUBLE_P = 291,                /* DOUBLE_P  */
    CEILING_P = 292,               /* CEILING_P  */
    KEYVALUE_P = 293,              /* KEYVALUE_P  */
    DATETIME_P = 294,              /* DATETIME_P  */
    UMINUS = 295                   /* UMINUS  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 67 "jsonpath_gram.y"

	JsonPathString		str;
	List			   *elems;	/* list of JsonPathParseItem */
	List			   *indexs;	/* list of integers */
	JsonPathParseItem  *value;
	JsonPathParseResult *result;
	JsonPathItemType	optype;
	bool				boolean;
	int					integer;

#line 115 "jsonpath_gram.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif



int jsonpath_yyparse (JsonPathParseResult **result, struct Node *escontext);

#endif /* !YY_JSONPATH_YY_JSONPATH_GRAM_H_INCLUDED  */
