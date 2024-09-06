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

#ifndef YY_REPLICATION_YY_REPL_GRAM_H_INCLUDED
# define YY_REPLICATION_YY_REPL_GRAM_H_INCLUDED
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
    K_READ_REPLICATION_SLOT = 264, /* K_READ_REPLICATION_SLOT  */
    K_SHOW = 265,                  /* K_SHOW  */
    K_START_REPLICATION = 266,     /* K_START_REPLICATION  */
    K_CREATE_REPLICATION_SLOT = 267, /* K_CREATE_REPLICATION_SLOT  */
    K_DROP_REPLICATION_SLOT = 268, /* K_DROP_REPLICATION_SLOT  */
    K_TIMELINE_HISTORY = 269,      /* K_TIMELINE_HISTORY  */
    K_WAIT = 270,                  /* K_WAIT  */
    K_TIMELINE = 271,              /* K_TIMELINE  */
    K_PHYSICAL = 272,              /* K_PHYSICAL  */
    K_LOGICAL = 273,               /* K_LOGICAL  */
    K_SLOT = 274,                  /* K_SLOT  */
    K_RESERVE_WAL = 275,           /* K_RESERVE_WAL  */
    K_TEMPORARY = 276,             /* K_TEMPORARY  */
    K_TWO_PHASE = 277,             /* K_TWO_PHASE  */
    K_EXPORT_SNAPSHOT = 278,       /* K_EXPORT_SNAPSHOT  */
    K_NOEXPORT_SNAPSHOT = 279,     /* K_NOEXPORT_SNAPSHOT  */
    K_USE_SNAPSHOT = 280           /* K_USE_SNAPSHOT  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 44 "repl_gram.y"

	char	   *str;
	bool		boolval;
	uint32		uintval;
	XLogRecPtr	recptr;
	Node	   *node;
	List	   *list;
	DefElem	   *defelt;

#line 99 "repl_gram.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern __thread YYSTYPE replication_yylval;

int replication_yyparse (void);

#endif /* !YY_REPLICATION_YY_REPL_GRAM_H_INCLUDED  */
