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

#ifndef YY_BOOT_YY_BOOTPARSE_H_INCLUDED
# define YY_BOOT_YY_BOOTPARSE_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int boot_yydebug;
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
    ID = 258,                      /* ID  */
    COMMA = 259,                   /* COMMA  */
    EQUALS = 260,                  /* EQUALS  */
    LPAREN = 261,                  /* LPAREN  */
    RPAREN = 262,                  /* RPAREN  */
    NULLVAL = 263,                 /* NULLVAL  */
    OPEN = 264,                    /* OPEN  */
    XCLOSE = 265,                  /* XCLOSE  */
    XCREATE = 266,                 /* XCREATE  */
    INSERT_TUPLE = 267,            /* INSERT_TUPLE  */
    XDECLARE = 268,                /* XDECLARE  */
    INDEX = 269,                   /* INDEX  */
    ON = 270,                      /* ON  */
    USING = 271,                   /* USING  */
    XBUILD = 272,                  /* XBUILD  */
    INDICES = 273,                 /* INDICES  */
    UNIQUE = 274,                  /* UNIQUE  */
    XTOAST = 275,                  /* XTOAST  */
    OBJ_ID = 276,                  /* OBJ_ID  */
    XBOOTSTRAP = 277,              /* XBOOTSTRAP  */
    XSHARED_RELATION = 278,        /* XSHARED_RELATION  */
    XROWTYPE_OID = 279,            /* XROWTYPE_OID  */
    XFORCE = 280,                  /* XFORCE  */
    XNOT = 281,                    /* XNOT  */
    XNULL = 282                    /* XNULL  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 82 "bootparse.y"

	List		*list;
	IndexElem	*ielem;
	char		*str;
	const char	*kw;
	int			ival;
	Oid			oidval;

#line 100 "bootparse.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern __thread YYSTYPE boot_yylval;

int boot_yyparse (void);

#endif /* !YY_BOOT_YY_BOOTPARSE_H_INCLUDED  */
