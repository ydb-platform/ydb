/* A Bison parser, made by GNU Bison 3.3.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2019 Free Software Foundation,
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

#ifndef YY_BASE_YY_GRAM_H_INCLUDED
# define YY_BASE_YY_GRAM_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
#endif

/* Token type.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    IDENT = 258,
    UIDENT = 259,
    FCONST = 260,
    SCONST = 261,
    USCONST = 262,
    BCONST = 263,
    XCONST = 264,
    Op = 265,
    ICONST = 266,
    PARAM = 267,
    TYPECAST = 268,
    DOT_DOT = 269,
    COLON_EQUALS = 270,
    EQUALS_GREATER = 271,
    LESS_EQUALS = 272,
    GREATER_EQUALS = 273,
    NOT_EQUALS = 274,
    ABORT_P = 275,
    ABSOLUTE_P = 276,
    ACCESS = 277,
    ACTION = 278,
    ADD_P = 279,
    ADMIN = 280,
    AFTER = 281,
    AGGREGATE = 282,
    ALL = 283,
    ALSO = 284,
    ALTER = 285,
    ALWAYS = 286,
    ANALYSE = 287,
    ANALYZE = 288,
    AND = 289,
    ANY = 290,
    ARRAY = 291,
    AS = 292,
    ASC = 293,
    ASSERTION = 294,
    ASSIGNMENT = 295,
    ASYMMETRIC = 296,
    AT = 297,
    ATTACH = 298,
    ATTRIBUTE = 299,
    AUTHORIZATION = 300,
    BACKWARD = 301,
    BEFORE = 302,
    BEGIN_P = 303,
    BETWEEN = 304,
    BIGINT = 305,
    BINARY = 306,
    BIT = 307,
    BOOLEAN_P = 308,
    BOTH = 309,
    BY = 310,
    CACHE = 311,
    CALL = 312,
    CALLED = 313,
    CASCADE = 314,
    CASCADED = 315,
    CASE = 316,
    CAST = 317,
    CATALOG_P = 318,
    CHAIN = 319,
    CHAR_P = 320,
    CHARACTER = 321,
    CHARACTERISTICS = 322,
    CHECK = 323,
    CHECKPOINT = 324,
    CLASS = 325,
    CLOSE = 326,
    CLUSTER = 327,
    COALESCE = 328,
    COLLATE = 329,
    COLLATION = 330,
    COLUMN = 331,
    COLUMNS = 332,
    COMMENT = 333,
    COMMENTS = 334,
    COMMIT = 335,
    COMMITTED = 336,
    CONCURRENTLY = 337,
    CONFIGURATION = 338,
    CONFLICT = 339,
    CONNECTION = 340,
    CONSTRAINT = 341,
    CONSTRAINTS = 342,
    CONTENT_P = 343,
    CONTINUE_P = 344,
    CONVERSION_P = 345,
    COPY = 346,
    COST = 347,
    CREATE = 348,
    CROSS = 349,
    CSV = 350,
    CUBE = 351,
    CURRENT_P = 352,
    CURRENT_CATALOG = 353,
    CURRENT_DATE = 354,
    CURRENT_ROLE = 355,
    CURRENT_SCHEMA = 356,
    CURRENT_TIME = 357,
    CURRENT_TIMESTAMP = 358,
    CURRENT_USER = 359,
    CURSOR = 360,
    CYCLE = 361,
    DATA_P = 362,
    DATABASE = 363,
    DAY_P = 364,
    DEALLOCATE = 365,
    DEC = 366,
    DECIMAL_P = 367,
    DECLARE = 368,
    DEFAULT = 369,
    DEFAULTS = 370,
    DEFERRABLE = 371,
    DEFERRED = 372,
    DEFINER = 373,
    DELETE_P = 374,
    DELIMITER = 375,
    DELIMITERS = 376,
    DEPENDS = 377,
    DESC = 378,
    DETACH = 379,
    DICTIONARY = 380,
    DISABLE_P = 381,
    DISCARD = 382,
    DISTINCT = 383,
    DO = 384,
    DOCUMENT_P = 385,
    DOMAIN_P = 386,
    DOUBLE_P = 387,
    DROP = 388,
    EACH = 389,
    ELSE = 390,
    ENABLE_P = 391,
    ENCODING = 392,
    ENCRYPTED = 393,
    END_P = 394,
    ENUM_P = 395,
    ESCAPE = 396,
    EVENT = 397,
    EXCEPT = 398,
    EXCLUDE = 399,
    EXCLUDING = 400,
    EXCLUSIVE = 401,
    EXECUTE = 402,
    EXISTS = 403,
    EXPLAIN = 404,
    EXPRESSION = 405,
    EXTENSION = 406,
    EXTERNAL = 407,
    EXTRACT = 408,
    FALSE_P = 409,
    FAMILY = 410,
    FETCH = 411,
    FILTER = 412,
    FIRST_P = 413,
    FLOAT_P = 414,
    FOLLOWING = 415,
    FOR = 416,
    FORCE = 417,
    FOREIGN = 418,
    FORWARD = 419,
    FREEZE = 420,
    FROM = 421,
    FULL = 422,
    FUNCTION = 423,
    FUNCTIONS = 424,
    GENERATED = 425,
    GLOBAL = 426,
    GRANT = 427,
    GRANTED = 428,
    GREATEST = 429,
    GROUP_P = 430,
    GROUPING = 431,
    GROUPS = 432,
    HANDLER = 433,
    HAVING = 434,
    HEADER_P = 435,
    HOLD = 436,
    HOUR_P = 437,
    IDENTITY_P = 438,
    IF_P = 439,
    ILIKE = 440,
    IMMEDIATE = 441,
    IMMUTABLE = 442,
    IMPLICIT_P = 443,
    IMPORT_P = 444,
    IN_P = 445,
    INCLUDE = 446,
    INCLUDING = 447,
    INCREMENT = 448,
    INDEX = 449,
    INDEXES = 450,
    INHERIT = 451,
    INHERITS = 452,
    INITIALLY = 453,
    INLINE_P = 454,
    INNER_P = 455,
    INOUT = 456,
    INPUT_P = 457,
    INSENSITIVE = 458,
    INSERT = 459,
    INSTEAD = 460,
    INT_P = 461,
    INTEGER = 462,
    INTERSECT = 463,
    INTERVAL = 464,
    INTO = 465,
    INVOKER = 466,
    IS = 467,
    ISNULL = 468,
    ISOLATION = 469,
    JOIN = 470,
    KEY = 471,
    LABEL = 472,
    LANGUAGE = 473,
    LARGE_P = 474,
    LAST_P = 475,
    LATERAL_P = 476,
    LEADING = 477,
    LEAKPROOF = 478,
    LEAST = 479,
    LEFT = 480,
    LEVEL = 481,
    LIKE = 482,
    LIMIT = 483,
    LISTEN = 484,
    LOAD = 485,
    LOCAL = 486,
    LOCALTIME = 487,
    LOCALTIMESTAMP = 488,
    LOCATION = 489,
    LOCK_P = 490,
    LOCKED = 491,
    LOGGED = 492,
    MAPPING = 493,
    MATCH = 494,
    MATERIALIZED = 495,
    MAXVALUE = 496,
    METHOD = 497,
    MINUTE_P = 498,
    MINVALUE = 499,
    MODE = 500,
    MONTH_P = 501,
    MOVE = 502,
    NAME_P = 503,
    NAMES = 504,
    NATIONAL = 505,
    NATURAL = 506,
    NCHAR = 507,
    NEW = 508,
    NEXT = 509,
    NFC = 510,
    NFD = 511,
    NFKC = 512,
    NFKD = 513,
    NO = 514,
    NONE = 515,
    NORMALIZE = 516,
    NORMALIZED = 517,
    NOT = 518,
    NOTHING = 519,
    NOTIFY = 520,
    NOTNULL = 521,
    NOWAIT = 522,
    NULL_P = 523,
    NULLIF = 524,
    NULLS_P = 525,
    NUMERIC = 526,
    OBJECT_P = 527,
    OF = 528,
    OFF = 529,
    OFFSET = 530,
    OIDS = 531,
    OLD = 532,
    ON = 533,
    ONLY = 534,
    OPERATOR = 535,
    OPTION = 536,
    OPTIONS = 537,
    OR = 538,
    ORDER = 539,
    ORDINALITY = 540,
    OTHERS = 541,
    OUT_P = 542,
    OUTER_P = 543,
    OVER = 544,
    OVERLAPS = 545,
    OVERLAY = 546,
    OVERRIDING = 547,
    OWNED = 548,
    OWNER = 549,
    PARALLEL = 550,
    PARSER = 551,
    PARTIAL = 552,
    PARTITION = 553,
    PASSING = 554,
    PASSWORD = 555,
    PLACING = 556,
    PLANS = 557,
    POLICY = 558,
    POSITION = 559,
    PRECEDING = 560,
    PRECISION = 561,
    PRESERVE = 562,
    PREPARE = 563,
    PREPARED = 564,
    PRIMARY = 565,
    PRIOR = 566,
    PRIVILEGES = 567,
    PROCEDURAL = 568,
    PROCEDURE = 569,
    PROCEDURES = 570,
    PROGRAM = 571,
    PUBLICATION = 572,
    QUOTE = 573,
    RANGE = 574,
    READ = 575,
    REAL = 576,
    REASSIGN = 577,
    RECHECK = 578,
    RECURSIVE = 579,
    REF = 580,
    REFERENCES = 581,
    REFERENCING = 582,
    REFRESH = 583,
    REINDEX = 584,
    RELATIVE_P = 585,
    RELEASE = 586,
    RENAME = 587,
    REPEATABLE = 588,
    REPLACE = 589,
    REPLICA = 590,
    RESET = 591,
    RESTART = 592,
    RESTRICT = 593,
    RETURNING = 594,
    RETURNS = 595,
    REVOKE = 596,
    RIGHT = 597,
    ROLE = 598,
    ROLLBACK = 599,
    ROLLUP = 600,
    ROUTINE = 601,
    ROUTINES = 602,
    ROW = 603,
    ROWS = 604,
    RULE = 605,
    SAVEPOINT = 606,
    SCHEMA = 607,
    SCHEMAS = 608,
    SCROLL = 609,
    SEARCH = 610,
    SECOND_P = 611,
    SECURITY = 612,
    SELECT = 613,
    SEQUENCE = 614,
    SEQUENCES = 615,
    SERIALIZABLE = 616,
    SERVER = 617,
    SESSION = 618,
    SESSION_USER = 619,
    SET = 620,
    SETS = 621,
    SETOF = 622,
    SHARE = 623,
    SHOW = 624,
    SIMILAR = 625,
    SIMPLE = 626,
    SKIP = 627,
    SMALLINT = 628,
    SNAPSHOT = 629,
    SOME = 630,
    SQL_P = 631,
    STABLE = 632,
    STANDALONE_P = 633,
    START = 634,
    STATEMENT = 635,
    STATISTICS = 636,
    STDIN = 637,
    STDOUT = 638,
    STORAGE = 639,
    STORED = 640,
    STRICT_P = 641,
    STRIP_P = 642,
    SUBSCRIPTION = 643,
    SUBSTRING = 644,
    SUPPORT = 645,
    SYMMETRIC = 646,
    SYSID = 647,
    SYSTEM_P = 648,
    TABLE = 649,
    TABLES = 650,
    TABLESAMPLE = 651,
    TABLESPACE = 652,
    TEMP = 653,
    TEMPLATE = 654,
    TEMPORARY = 655,
    TEXT_P = 656,
    THEN = 657,
    TIES = 658,
    TIME = 659,
    TIMESTAMP = 660,
    TO = 661,
    TRAILING = 662,
    TRANSACTION = 663,
    TRANSFORM = 664,
    TREAT = 665,
    TRIGGER = 666,
    TRIM = 667,
    TRUE_P = 668,
    TRUNCATE = 669,
    TRUSTED = 670,
    TYPE_P = 671,
    TYPES_P = 672,
    UESCAPE = 673,
    UNBOUNDED = 674,
    UNCOMMITTED = 675,
    UNENCRYPTED = 676,
    UNION = 677,
    UNIQUE = 678,
    UNKNOWN = 679,
    UNLISTEN = 680,
    UNLOGGED = 681,
    UNTIL = 682,
    UPDATE = 683,
    USER = 684,
    USING = 685,
    VACUUM = 686,
    VALID = 687,
    VALIDATE = 688,
    VALIDATOR = 689,
    VALUE_P = 690,
    VALUES = 691,
    VARCHAR = 692,
    VARIADIC = 693,
    VARYING = 694,
    VERBOSE = 695,
    VERSION_P = 696,
    VIEW = 697,
    VIEWS = 698,
    VOLATILE = 699,
    WHEN = 700,
    WHERE = 701,
    WHITESPACE_P = 702,
    WINDOW = 703,
    WITH = 704,
    WITHIN = 705,
    WITHOUT = 706,
    WORK = 707,
    WRAPPER = 708,
    WRITE = 709,
    XML_P = 710,
    XMLATTRIBUTES = 711,
    XMLCONCAT = 712,
    XMLELEMENT = 713,
    XMLEXISTS = 714,
    XMLFOREST = 715,
    XMLNAMESPACES = 716,
    XMLPARSE = 717,
    XMLPI = 718,
    XMLROOT = 719,
    XMLSERIALIZE = 720,
    XMLTABLE = 721,
    YEAR_P = 722,
    YES_P = 723,
    ZONE = 724,
    NOT_LA = 725,
    NULLS_LA = 726,
    WITH_LA = 727,
    POSTFIXOP = 728,
    UMINUS = 729
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 211 "gram.y" /* yacc.c:1921  */

	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	ObjectWithArgs		*objwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	WindowDef			*windef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	InferClause			*infer;
	OnConflictClause	*onconflict;
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	struct ImportQual	*importqual;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;
	PartitionElem		*partelem;
	PartitionSpec		*partspec;
	PartitionBoundSpec	*partboundspec;
	RoleSpec			*rolespec;
	struct SelectLimit	*selectlimit;

#line 578 "gram.h" /* yacc.c:1921  */
};

typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
};
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif



int base_yyparse (core_yyscan_t yyscanner);

#endif /* !YY_BASE_YY_GRAM_H_INCLUDED  */
