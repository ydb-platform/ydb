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
    ASENSITIVE = 294,
    ASSERTION = 295,
    ASSIGNMENT = 296,
    ASYMMETRIC = 297,
    ATOMIC = 298,
    AT = 299,
    ATTACH = 300,
    ATTRIBUTE = 301,
    AUTHORIZATION = 302,
    BACKWARD = 303,
    BEFORE = 304,
    BEGIN_P = 305,
    BETWEEN = 306,
    BIGINT = 307,
    BINARY = 308,
    BIT = 309,
    BOOLEAN_P = 310,
    BOTH = 311,
    BREADTH = 312,
    BY = 313,
    CACHE = 314,
    CALL = 315,
    CALLED = 316,
    CASCADE = 317,
    CASCADED = 318,
    CASE = 319,
    CAST = 320,
    CATALOG_P = 321,
    CHAIN = 322,
    CHAR_P = 323,
    CHARACTER = 324,
    CHARACTERISTICS = 325,
    CHECK = 326,
    CHECKPOINT = 327,
    CLASS = 328,
    CLOSE = 329,
    CLUSTER = 330,
    COALESCE = 331,
    COLLATE = 332,
    COLLATION = 333,
    COLUMN = 334,
    COLUMNS = 335,
    COMMENT = 336,
    COMMENTS = 337,
    COMMIT = 338,
    COMMITTED = 339,
    COMPRESSION = 340,
    CONCURRENTLY = 341,
    CONFIGURATION = 342,
    CONFLICT = 343,
    CONNECTION = 344,
    CONSTRAINT = 345,
    CONSTRAINTS = 346,
    CONTENT_P = 347,
    CONTINUE_P = 348,
    CONVERSION_P = 349,
    COPY = 350,
    COST = 351,
    CREATE = 352,
    CROSS = 353,
    CSV = 354,
    CUBE = 355,
    CURRENT_P = 356,
    CURRENT_CATALOG = 357,
    CURRENT_DATE = 358,
    CURRENT_ROLE = 359,
    CURRENT_SCHEMA = 360,
    CURRENT_TIME = 361,
    CURRENT_TIMESTAMP = 362,
    CURRENT_USER = 363,
    CURSOR = 364,
    CYCLE = 365,
    DATA_P = 366,
    DATABASE = 367,
    DAY_P = 368,
    DEALLOCATE = 369,
    DEC = 370,
    DECIMAL_P = 371,
    DECLARE = 372,
    DEFAULT = 373,
    DEFAULTS = 374,
    DEFERRABLE = 375,
    DEFERRED = 376,
    DEFINER = 377,
    DELETE_P = 378,
    DELIMITER = 379,
    DELIMITERS = 380,
    DEPENDS = 381,
    DEPTH = 382,
    DESC = 383,
    DETACH = 384,
    DICTIONARY = 385,
    DISABLE_P = 386,
    DISCARD = 387,
    DISTINCT = 388,
    DO = 389,
    DOCUMENT_P = 390,
    DOMAIN_P = 391,
    DOUBLE_P = 392,
    DROP = 393,
    EACH = 394,
    ELSE = 395,
    ENABLE_P = 396,
    ENCODING = 397,
    ENCRYPTED = 398,
    END_P = 399,
    ENUM_P = 400,
    ESCAPE = 401,
    EVENT = 402,
    EXCEPT = 403,
    EXCLUDE = 404,
    EXCLUDING = 405,
    EXCLUSIVE = 406,
    EXECUTE = 407,
    EXISTS = 408,
    EXPLAIN = 409,
    EXPRESSION = 410,
    EXTENSION = 411,
    EXTERNAL = 412,
    EXTRACT = 413,
    FALSE_P = 414,
    FAMILY = 415,
    FETCH = 416,
    FILTER = 417,
    FINALIZE = 418,
    FIRST_P = 419,
    FLOAT_P = 420,
    FOLLOWING = 421,
    FOR = 422,
    FORCE = 423,
    FOREIGN = 424,
    FORWARD = 425,
    FREEZE = 426,
    FROM = 427,
    FULL = 428,
    FUNCTION = 429,
    FUNCTIONS = 430,
    GENERATED = 431,
    GLOBAL = 432,
    GRANT = 433,
    GRANTED = 434,
    GREATEST = 435,
    GROUP_P = 436,
    GROUPING = 437,
    GROUPS = 438,
    HANDLER = 439,
    HAVING = 440,
    HEADER_P = 441,
    HOLD = 442,
    HOUR_P = 443,
    IDENTITY_P = 444,
    IF_P = 445,
    ILIKE = 446,
    IMMEDIATE = 447,
    IMMUTABLE = 448,
    IMPLICIT_P = 449,
    IMPORT_P = 450,
    IN_P = 451,
    INCLUDE = 452,
    INCLUDING = 453,
    INCREMENT = 454,
    INDEX = 455,
    INDEXES = 456,
    INHERIT = 457,
    INHERITS = 458,
    INITIALLY = 459,
    INLINE_P = 460,
    INNER_P = 461,
    INOUT = 462,
    INPUT_P = 463,
    INSENSITIVE = 464,
    INSERT = 465,
    INSTEAD = 466,
    INT_P = 467,
    INTEGER = 468,
    INTERSECT = 469,
    INTERVAL = 470,
    INTO = 471,
    INVOKER = 472,
    IS = 473,
    ISNULL = 474,
    ISOLATION = 475,
    JOIN = 476,
    KEY = 477,
    LABEL = 478,
    LANGUAGE = 479,
    LARGE_P = 480,
    LAST_P = 481,
    LATERAL_P = 482,
    LEADING = 483,
    LEAKPROOF = 484,
    LEAST = 485,
    LEFT = 486,
    LEVEL = 487,
    LIKE = 488,
    LIMIT = 489,
    LISTEN = 490,
    LOAD = 491,
    LOCAL = 492,
    LOCALTIME = 493,
    LOCALTIMESTAMP = 494,
    LOCATION = 495,
    LOCK_P = 496,
    LOCKED = 497,
    LOGGED = 498,
    MAPPING = 499,
    MATCH = 500,
    MATERIALIZED = 501,
    MAXVALUE = 502,
    METHOD = 503,
    MINUTE_P = 504,
    MINVALUE = 505,
    MODE = 506,
    MONTH_P = 507,
    MOVE = 508,
    NAME_P = 509,
    NAMES = 510,
    NATIONAL = 511,
    NATURAL = 512,
    NCHAR = 513,
    NEW = 514,
    NEXT = 515,
    NFC = 516,
    NFD = 517,
    NFKC = 518,
    NFKD = 519,
    NO = 520,
    NONE = 521,
    NORMALIZE = 522,
    NORMALIZED = 523,
    NOT = 524,
    NOTHING = 525,
    NOTIFY = 526,
    NOTNULL = 527,
    NOWAIT = 528,
    NULL_P = 529,
    NULLIF = 530,
    NULLS_P = 531,
    NUMERIC = 532,
    OBJECT_P = 533,
    OF = 534,
    OFF = 535,
    OFFSET = 536,
    OIDS = 537,
    OLD = 538,
    ON = 539,
    ONLY = 540,
    OPERATOR = 541,
    OPTION = 542,
    OPTIONS = 543,
    OR = 544,
    ORDER = 545,
    ORDINALITY = 546,
    OTHERS = 547,
    OUT_P = 548,
    OUTER_P = 549,
    OVER = 550,
    OVERLAPS = 551,
    OVERLAY = 552,
    OVERRIDING = 553,
    OWNED = 554,
    OWNER = 555,
    PARALLEL = 556,
    PARSER = 557,
    PARTIAL = 558,
    PARTITION = 559,
    PASSING = 560,
    PASSWORD = 561,
    PLACING = 562,
    PLANS = 563,
    POLICY = 564,
    POSITION = 565,
    PRECEDING = 566,
    PRECISION = 567,
    PRESERVE = 568,
    PREPARE = 569,
    PREPARED = 570,
    PRIMARY = 571,
    PRIOR = 572,
    PRIVILEGES = 573,
    PROCEDURAL = 574,
    PROCEDURE = 575,
    PROCEDURES = 576,
    PROGRAM = 577,
    PUBLICATION = 578,
    QUOTE = 579,
    RANGE = 580,
    READ = 581,
    REAL = 582,
    REASSIGN = 583,
    RECHECK = 584,
    RECURSIVE = 585,
    REF = 586,
    REFERENCES = 587,
    REFERENCING = 588,
    REFRESH = 589,
    REINDEX = 590,
    RELATIVE_P = 591,
    RELEASE = 592,
    RENAME = 593,
    REPEATABLE = 594,
    REPLACE = 595,
    REPLICA = 596,
    RESET = 597,
    RESTART = 598,
    RESTRICT = 599,
    RETURN = 600,
    RETURNING = 601,
    RETURNS = 602,
    REVOKE = 603,
    RIGHT = 604,
    ROLE = 605,
    ROLLBACK = 606,
    ROLLUP = 607,
    ROUTINE = 608,
    ROUTINES = 609,
    ROW = 610,
    ROWS = 611,
    RULE = 612,
    SAVEPOINT = 613,
    SCHEMA = 614,
    SCHEMAS = 615,
    SCROLL = 616,
    SEARCH = 617,
    SECOND_P = 618,
    SECURITY = 619,
    SELECT = 620,
    SEQUENCE = 621,
    SEQUENCES = 622,
    SERIALIZABLE = 623,
    SERVER = 624,
    SESSION = 625,
    SESSION_USER = 626,
    SET = 627,
    SETS = 628,
    SETOF = 629,
    SHARE = 630,
    SHOW = 631,
    SIMILAR = 632,
    SIMPLE = 633,
    SKIP = 634,
    SMALLINT = 635,
    SNAPSHOT = 636,
    SOME = 637,
    SQL_P = 638,
    STABLE = 639,
    STANDALONE_P = 640,
    START = 641,
    STATEMENT = 642,
    STATISTICS = 643,
    STDIN = 644,
    STDOUT = 645,
    STORAGE = 646,
    STORED = 647,
    STRICT_P = 648,
    STRIP_P = 649,
    SUBSCRIPTION = 650,
    SUBSTRING = 651,
    SUPPORT = 652,
    SYMMETRIC = 653,
    SYSID = 654,
    SYSTEM_P = 655,
    TABLE = 656,
    TABLES = 657,
    TABLESAMPLE = 658,
    TABLESPACE = 659,
    TEMP = 660,
    TEMPLATE = 661,
    TEMPORARY = 662,
    TEXT_P = 663,
    THEN = 664,
    TIES = 665,
    TIME = 666,
    TIMESTAMP = 667,
    TO = 668,
    TRAILING = 669,
    TRANSACTION = 670,
    TRANSFORM = 671,
    TREAT = 672,
    TRIGGER = 673,
    TRIM = 674,
    TRUE_P = 675,
    TRUNCATE = 676,
    TRUSTED = 677,
    TYPE_P = 678,
    TYPES_P = 679,
    UESCAPE = 680,
    UNBOUNDED = 681,
    UNCOMMITTED = 682,
    UNENCRYPTED = 683,
    UNION = 684,
    UNIQUE = 685,
    UNKNOWN = 686,
    UNLISTEN = 687,
    UNLOGGED = 688,
    UNTIL = 689,
    UPDATE = 690,
    USER = 691,
    USING = 692,
    VACUUM = 693,
    VALID = 694,
    VALIDATE = 695,
    VALIDATOR = 696,
    VALUE_P = 697,
    VALUES = 698,
    VARCHAR = 699,
    VARIADIC = 700,
    VARYING = 701,
    VERBOSE = 702,
    VERSION_P = 703,
    VIEW = 704,
    VIEWS = 705,
    VOLATILE = 706,
    WHEN = 707,
    WHERE = 708,
    WHITESPACE_P = 709,
    WINDOW = 710,
    WITH = 711,
    WITHIN = 712,
    WITHOUT = 713,
    WORK = 714,
    WRAPPER = 715,
    WRITE = 716,
    XML_P = 717,
    XMLATTRIBUTES = 718,
    XMLCONCAT = 719,
    XMLELEMENT = 720,
    XMLEXISTS = 721,
    XMLFOREST = 722,
    XMLNAMESPACES = 723,
    XMLPARSE = 724,
    XMLPI = 725,
    XMLROOT = 726,
    XMLSERIALIZE = 727,
    XMLTABLE = 728,
    YEAR_P = 729,
    YES_P = 730,
    ZONE = 731,
    NOT_LA = 732,
    NULLS_LA = 733,
    WITH_LA = 734,
    MODE_TYPE_NAME = 735,
    MODE_PLPGSQL_EXPR = 736,
    MODE_PLPGSQL_ASSIGN1 = 737,
    MODE_PLPGSQL_ASSIGN2 = 738,
    MODE_PLPGSQL_ASSIGN3 = 739,
    UMINUS = 740
  };
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED

union YYSTYPE
{
#line 217 "gram.y" /* yacc.c:1921  */

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
	StatsElem			*selem;
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
	SetQuantifier	 setquantifier;
	struct GroupClause  *groupclause;

#line 592 "gram.h" /* yacc.c:1921  */
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
