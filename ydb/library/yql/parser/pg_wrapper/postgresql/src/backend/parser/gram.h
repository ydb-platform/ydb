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

#ifndef YY_BASE_YY_GRAM_H_INCLUDED
# define YY_BASE_YY_GRAM_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
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
    IDENT = 258,                   /* IDENT  */
    UIDENT = 259,                  /* UIDENT  */
    FCONST = 260,                  /* FCONST  */
    SCONST = 261,                  /* SCONST  */
    USCONST = 262,                 /* USCONST  */
    BCONST = 263,                  /* BCONST  */
    XCONST = 264,                  /* XCONST  */
    Op = 265,                      /* Op  */
    ICONST = 266,                  /* ICONST  */
    PARAM = 267,                   /* PARAM  */
    TYPECAST = 268,                /* TYPECAST  */
    DOT_DOT = 269,                 /* DOT_DOT  */
    COLON_EQUALS = 270,            /* COLON_EQUALS  */
    EQUALS_GREATER = 271,          /* EQUALS_GREATER  */
    LESS_EQUALS = 272,             /* LESS_EQUALS  */
    GREATER_EQUALS = 273,          /* GREATER_EQUALS  */
    NOT_EQUALS = 274,              /* NOT_EQUALS  */
    ABORT_P = 275,                 /* ABORT_P  */
    ABSOLUTE_P = 276,              /* ABSOLUTE_P  */
    ACCESS = 277,                  /* ACCESS  */
    ACTION = 278,                  /* ACTION  */
    ADD_P = 279,                   /* ADD_P  */
    ADMIN = 280,                   /* ADMIN  */
    AFTER = 281,                   /* AFTER  */
    AGGREGATE = 282,               /* AGGREGATE  */
    ALL = 283,                     /* ALL  */
    ALSO = 284,                    /* ALSO  */
    ALTER = 285,                   /* ALTER  */
    ALWAYS = 286,                  /* ALWAYS  */
    ANALYSE = 287,                 /* ANALYSE  */
    ANALYZE = 288,                 /* ANALYZE  */
    AND = 289,                     /* AND  */
    ANY = 290,                     /* ANY  */
    ARRAY = 291,                   /* ARRAY  */
    AS = 292,                      /* AS  */
    ASC = 293,                     /* ASC  */
    ASENSITIVE = 294,              /* ASENSITIVE  */
    ASSERTION = 295,               /* ASSERTION  */
    ASSIGNMENT = 296,              /* ASSIGNMENT  */
    ASYMMETRIC = 297,              /* ASYMMETRIC  */
    ATOMIC = 298,                  /* ATOMIC  */
    AT = 299,                      /* AT  */
    ATTACH = 300,                  /* ATTACH  */
    ATTRIBUTE = 301,               /* ATTRIBUTE  */
    AUTHORIZATION = 302,           /* AUTHORIZATION  */
    BACKWARD = 303,                /* BACKWARD  */
    BEFORE = 304,                  /* BEFORE  */
    BEGIN_P = 305,                 /* BEGIN_P  */
    BETWEEN = 306,                 /* BETWEEN  */
    BIGINT = 307,                  /* BIGINT  */
    BINARY = 308,                  /* BINARY  */
    BIT = 309,                     /* BIT  */
    BOOLEAN_P = 310,               /* BOOLEAN_P  */
    BOTH = 311,                    /* BOTH  */
    BREADTH = 312,                 /* BREADTH  */
    BY = 313,                      /* BY  */
    CACHE = 314,                   /* CACHE  */
    CALL = 315,                    /* CALL  */
    CALLED = 316,                  /* CALLED  */
    CASCADE = 317,                 /* CASCADE  */
    CASCADED = 318,                /* CASCADED  */
    CASE = 319,                    /* CASE  */
    CAST = 320,                    /* CAST  */
    CATALOG_P = 321,               /* CATALOG_P  */
    CHAIN = 322,                   /* CHAIN  */
    CHAR_P = 323,                  /* CHAR_P  */
    CHARACTER = 324,               /* CHARACTER  */
    CHARACTERISTICS = 325,         /* CHARACTERISTICS  */
    CHECK = 326,                   /* CHECK  */
    CHECKPOINT = 327,              /* CHECKPOINT  */
    CLASS = 328,                   /* CLASS  */
    CLOSE = 329,                   /* CLOSE  */
    CLUSTER = 330,                 /* CLUSTER  */
    COALESCE = 331,                /* COALESCE  */
    COLLATE = 332,                 /* COLLATE  */
    COLLATION = 333,               /* COLLATION  */
    COLUMN = 334,                  /* COLUMN  */
    COLUMNS = 335,                 /* COLUMNS  */
    COMMENT = 336,                 /* COMMENT  */
    COMMENTS = 337,                /* COMMENTS  */
    COMMIT = 338,                  /* COMMIT  */
    COMMITTED = 339,               /* COMMITTED  */
    COMPRESSION = 340,             /* COMPRESSION  */
    CONCURRENTLY = 341,            /* CONCURRENTLY  */
    CONFIGURATION = 342,           /* CONFIGURATION  */
    CONFLICT = 343,                /* CONFLICT  */
    CONNECTION = 344,              /* CONNECTION  */
    CONSTRAINT = 345,              /* CONSTRAINT  */
    CONSTRAINTS = 346,             /* CONSTRAINTS  */
    CONTENT_P = 347,               /* CONTENT_P  */
    CONTINUE_P = 348,              /* CONTINUE_P  */
    CONVERSION_P = 349,            /* CONVERSION_P  */
    COPY = 350,                    /* COPY  */
    COST = 351,                    /* COST  */
    CREATE = 352,                  /* CREATE  */
    CROSS = 353,                   /* CROSS  */
    CSV = 354,                     /* CSV  */
    CUBE = 355,                    /* CUBE  */
    CURRENT_P = 356,               /* CURRENT_P  */
    CURRENT_CATALOG = 357,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 358,            /* CURRENT_DATE  */
    CURRENT_ROLE = 359,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 360,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 361,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 362,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 363,            /* CURRENT_USER  */
    CURSOR = 364,                  /* CURSOR  */
    CYCLE = 365,                   /* CYCLE  */
    DATA_P = 366,                  /* DATA_P  */
    DATABASE = 367,                /* DATABASE  */
    DAY_P = 368,                   /* DAY_P  */
    DEALLOCATE = 369,              /* DEALLOCATE  */
    DEC = 370,                     /* DEC  */
    DECIMAL_P = 371,               /* DECIMAL_P  */
    DECLARE = 372,                 /* DECLARE  */
    DEFAULT = 373,                 /* DEFAULT  */
    DEFAULTS = 374,                /* DEFAULTS  */
    DEFERRABLE = 375,              /* DEFERRABLE  */
    DEFERRED = 376,                /* DEFERRED  */
    DEFINER = 377,                 /* DEFINER  */
    DELETE_P = 378,                /* DELETE_P  */
    DELIMITER = 379,               /* DELIMITER  */
    DELIMITERS = 380,              /* DELIMITERS  */
    DEPENDS = 381,                 /* DEPENDS  */
    DEPTH = 382,                   /* DEPTH  */
    DESC = 383,                    /* DESC  */
    DETACH = 384,                  /* DETACH  */
    DICTIONARY = 385,              /* DICTIONARY  */
    DISABLE_P = 386,               /* DISABLE_P  */
    DISCARD = 387,                 /* DISCARD  */
    DISTINCT = 388,                /* DISTINCT  */
    DO = 389,                      /* DO  */
    DOCUMENT_P = 390,              /* DOCUMENT_P  */
    DOMAIN_P = 391,                /* DOMAIN_P  */
    DOUBLE_P = 392,                /* DOUBLE_P  */
    DROP = 393,                    /* DROP  */
    EACH = 394,                    /* EACH  */
    ELSE = 395,                    /* ELSE  */
    ENABLE_P = 396,                /* ENABLE_P  */
    ENCODING = 397,                /* ENCODING  */
    ENCRYPTED = 398,               /* ENCRYPTED  */
    END_P = 399,                   /* END_P  */
    ENUM_P = 400,                  /* ENUM_P  */
    ESCAPE = 401,                  /* ESCAPE  */
    EVENT = 402,                   /* EVENT  */
    EXCEPT = 403,                  /* EXCEPT  */
    EXCLUDE = 404,                 /* EXCLUDE  */
    EXCLUDING = 405,               /* EXCLUDING  */
    EXCLUSIVE = 406,               /* EXCLUSIVE  */
    EXECUTE = 407,                 /* EXECUTE  */
    EXISTS = 408,                  /* EXISTS  */
    EXPLAIN = 409,                 /* EXPLAIN  */
    EXPRESSION = 410,              /* EXPRESSION  */
    EXTENSION = 411,               /* EXTENSION  */
    EXTERNAL = 412,                /* EXTERNAL  */
    EXTRACT = 413,                 /* EXTRACT  */
    FALSE_P = 414,                 /* FALSE_P  */
    FAMILY = 415,                  /* FAMILY  */
    FETCH = 416,                   /* FETCH  */
    FILTER = 417,                  /* FILTER  */
    FINALIZE = 418,                /* FINALIZE  */
    FIRST_P = 419,                 /* FIRST_P  */
    FLOAT_P = 420,                 /* FLOAT_P  */
    FOLLOWING = 421,               /* FOLLOWING  */
    FOR = 422,                     /* FOR  */
    FORCE = 423,                   /* FORCE  */
    FOREIGN = 424,                 /* FOREIGN  */
    FORWARD = 425,                 /* FORWARD  */
    FREEZE = 426,                  /* FREEZE  */
    FROM = 427,                    /* FROM  */
    FULL = 428,                    /* FULL  */
    FUNCTION = 429,                /* FUNCTION  */
    FUNCTIONS = 430,               /* FUNCTIONS  */
    GENERATED = 431,               /* GENERATED  */
    GLOBAL = 432,                  /* GLOBAL  */
    GRANT = 433,                   /* GRANT  */
    GRANTED = 434,                 /* GRANTED  */
    GREATEST = 435,                /* GREATEST  */
    GROUP_P = 436,                 /* GROUP_P  */
    GROUPING = 437,                /* GROUPING  */
    GROUPS = 438,                  /* GROUPS  */
    HANDLER = 439,                 /* HANDLER  */
    HAVING = 440,                  /* HAVING  */
    HEADER_P = 441,                /* HEADER_P  */
    HOLD = 442,                    /* HOLD  */
    HOUR_P = 443,                  /* HOUR_P  */
    IDENTITY_P = 444,              /* IDENTITY_P  */
    IF_P = 445,                    /* IF_P  */
    ILIKE = 446,                   /* ILIKE  */
    IMMEDIATE = 447,               /* IMMEDIATE  */
    IMMUTABLE = 448,               /* IMMUTABLE  */
    IMPLICIT_P = 449,              /* IMPLICIT_P  */
    IMPORT_P = 450,                /* IMPORT_P  */
    IN_P = 451,                    /* IN_P  */
    INCLUDE = 452,                 /* INCLUDE  */
    INCLUDING = 453,               /* INCLUDING  */
    INCREMENT = 454,               /* INCREMENT  */
    INDEX = 455,                   /* INDEX  */
    INDEXES = 456,                 /* INDEXES  */
    INHERIT = 457,                 /* INHERIT  */
    INHERITS = 458,                /* INHERITS  */
    INITIALLY = 459,               /* INITIALLY  */
    INLINE_P = 460,                /* INLINE_P  */
    INNER_P = 461,                 /* INNER_P  */
    INOUT = 462,                   /* INOUT  */
    INPUT_P = 463,                 /* INPUT_P  */
    INSENSITIVE = 464,             /* INSENSITIVE  */
    INSERT = 465,                  /* INSERT  */
    INSTEAD = 466,                 /* INSTEAD  */
    INT_P = 467,                   /* INT_P  */
    INTEGER = 468,                 /* INTEGER  */
    INTERSECT = 469,               /* INTERSECT  */
    INTERVAL = 470,                /* INTERVAL  */
    INTO = 471,                    /* INTO  */
    INVOKER = 472,                 /* INVOKER  */
    IS = 473,                      /* IS  */
    ISNULL = 474,                  /* ISNULL  */
    ISOLATION = 475,               /* ISOLATION  */
    JOIN = 476,                    /* JOIN  */
    KEY = 477,                     /* KEY  */
    LABEL = 478,                   /* LABEL  */
    LANGUAGE = 479,                /* LANGUAGE  */
    LARGE_P = 480,                 /* LARGE_P  */
    LAST_P = 481,                  /* LAST_P  */
    LATERAL_P = 482,               /* LATERAL_P  */
    LEADING = 483,                 /* LEADING  */
    LEAKPROOF = 484,               /* LEAKPROOF  */
    LEAST = 485,                   /* LEAST  */
    LEFT = 486,                    /* LEFT  */
    LEVEL = 487,                   /* LEVEL  */
    LIKE = 488,                    /* LIKE  */
    LIMIT = 489,                   /* LIMIT  */
    LISTEN = 490,                  /* LISTEN  */
    LOAD = 491,                    /* LOAD  */
    LOCAL = 492,                   /* LOCAL  */
    LOCALTIME = 493,               /* LOCALTIME  */
    LOCALTIMESTAMP = 494,          /* LOCALTIMESTAMP  */
    LOCATION = 495,                /* LOCATION  */
    LOCK_P = 496,                  /* LOCK_P  */
    LOCKED = 497,                  /* LOCKED  */
    LOGGED = 498,                  /* LOGGED  */
    MAPPING = 499,                 /* MAPPING  */
    MATCH = 500,                   /* MATCH  */
    MATERIALIZED = 501,            /* MATERIALIZED  */
    MAXVALUE = 502,                /* MAXVALUE  */
    METHOD = 503,                  /* METHOD  */
    MINUTE_P = 504,                /* MINUTE_P  */
    MINVALUE = 505,                /* MINVALUE  */
    MODE = 506,                    /* MODE  */
    MONTH_P = 507,                 /* MONTH_P  */
    MOVE = 508,                    /* MOVE  */
    NAME_P = 509,                  /* NAME_P  */
    NAMES = 510,                   /* NAMES  */
    NATIONAL = 511,                /* NATIONAL  */
    NATURAL = 512,                 /* NATURAL  */
    NCHAR = 513,                   /* NCHAR  */
    NEW = 514,                     /* NEW  */
    NEXT = 515,                    /* NEXT  */
    NFC = 516,                     /* NFC  */
    NFD = 517,                     /* NFD  */
    NFKC = 518,                    /* NFKC  */
    NFKD = 519,                    /* NFKD  */
    NO = 520,                      /* NO  */
    NONE = 521,                    /* NONE  */
    NORMALIZE = 522,               /* NORMALIZE  */
    NORMALIZED = 523,              /* NORMALIZED  */
    NOT = 524,                     /* NOT  */
    NOTHING = 525,                 /* NOTHING  */
    NOTIFY = 526,                  /* NOTIFY  */
    NOTNULL = 527,                 /* NOTNULL  */
    NOWAIT = 528,                  /* NOWAIT  */
    NULL_P = 529,                  /* NULL_P  */
    NULLIF = 530,                  /* NULLIF  */
    NULLS_P = 531,                 /* NULLS_P  */
    NUMERIC = 532,                 /* NUMERIC  */
    OBJECT_P = 533,                /* OBJECT_P  */
    OF = 534,                      /* OF  */
    OFF = 535,                     /* OFF  */
    OFFSET = 536,                  /* OFFSET  */
    OIDS = 537,                    /* OIDS  */
    OLD = 538,                     /* OLD  */
    ON = 539,                      /* ON  */
    ONLY = 540,                    /* ONLY  */
    OPERATOR = 541,                /* OPERATOR  */
    OPTION = 542,                  /* OPTION  */
    OPTIONS = 543,                 /* OPTIONS  */
    OR = 544,                      /* OR  */
    ORDER = 545,                   /* ORDER  */
    ORDINALITY = 546,              /* ORDINALITY  */
    OTHERS = 547,                  /* OTHERS  */
    OUT_P = 548,                   /* OUT_P  */
    OUTER_P = 549,                 /* OUTER_P  */
    OVER = 550,                    /* OVER  */
    OVERLAPS = 551,                /* OVERLAPS  */
    OVERLAY = 552,                 /* OVERLAY  */
    OVERRIDING = 553,              /* OVERRIDING  */
    OWNED = 554,                   /* OWNED  */
    OWNER = 555,                   /* OWNER  */
    PARALLEL = 556,                /* PARALLEL  */
    PARSER = 557,                  /* PARSER  */
    PARTIAL = 558,                 /* PARTIAL  */
    PARTITION = 559,               /* PARTITION  */
    PASSING = 560,                 /* PASSING  */
    PASSWORD = 561,                /* PASSWORD  */
    PLACING = 562,                 /* PLACING  */
    PLANS = 563,                   /* PLANS  */
    POLICY = 564,                  /* POLICY  */
    POSITION = 565,                /* POSITION  */
    PRECEDING = 566,               /* PRECEDING  */
    PRECISION = 567,               /* PRECISION  */
    PRESERVE = 568,                /* PRESERVE  */
    PREPARE = 569,                 /* PREPARE  */
    PREPARED = 570,                /* PREPARED  */
    PRIMARY = 571,                 /* PRIMARY  */
    PRIOR = 572,                   /* PRIOR  */
    PRIVILEGES = 573,              /* PRIVILEGES  */
    PROCEDURAL = 574,              /* PROCEDURAL  */
    PROCEDURE = 575,               /* PROCEDURE  */
    PROCEDURES = 576,              /* PROCEDURES  */
    PROGRAM = 577,                 /* PROGRAM  */
    PUBLICATION = 578,             /* PUBLICATION  */
    QUOTE = 579,                   /* QUOTE  */
    RANGE = 580,                   /* RANGE  */
    READ = 581,                    /* READ  */
    REAL = 582,                    /* REAL  */
    REASSIGN = 583,                /* REASSIGN  */
    RECHECK = 584,                 /* RECHECK  */
    RECURSIVE = 585,               /* RECURSIVE  */
    REF = 586,                     /* REF  */
    REFERENCES = 587,              /* REFERENCES  */
    REFERENCING = 588,             /* REFERENCING  */
    REFRESH = 589,                 /* REFRESH  */
    REINDEX = 590,                 /* REINDEX  */
    RELATIVE_P = 591,              /* RELATIVE_P  */
    RELEASE = 592,                 /* RELEASE  */
    RENAME = 593,                  /* RENAME  */
    REPEATABLE = 594,              /* REPEATABLE  */
    REPLACE = 595,                 /* REPLACE  */
    REPLICA = 596,                 /* REPLICA  */
    RESET = 597,                   /* RESET  */
    RESTART = 598,                 /* RESTART  */
    RESTRICT = 599,                /* RESTRICT  */
    RETURN = 600,                  /* RETURN  */
    RETURNING = 601,               /* RETURNING  */
    RETURNS = 602,                 /* RETURNS  */
    REVOKE = 603,                  /* REVOKE  */
    RIGHT = 604,                   /* RIGHT  */
    ROLE = 605,                    /* ROLE  */
    ROLLBACK = 606,                /* ROLLBACK  */
    ROLLUP = 607,                  /* ROLLUP  */
    ROUTINE = 608,                 /* ROUTINE  */
    ROUTINES = 609,                /* ROUTINES  */
    ROW = 610,                     /* ROW  */
    ROWS = 611,                    /* ROWS  */
    RULE = 612,                    /* RULE  */
    SAVEPOINT = 613,               /* SAVEPOINT  */
    SCHEMA = 614,                  /* SCHEMA  */
    SCHEMAS = 615,                 /* SCHEMAS  */
    SCROLL = 616,                  /* SCROLL  */
    SEARCH = 617,                  /* SEARCH  */
    SECOND_P = 618,                /* SECOND_P  */
    SECURITY = 619,                /* SECURITY  */
    SELECT = 620,                  /* SELECT  */
    SEQUENCE = 621,                /* SEQUENCE  */
    SEQUENCES = 622,               /* SEQUENCES  */
    SERIALIZABLE = 623,            /* SERIALIZABLE  */
    SERVER = 624,                  /* SERVER  */
    SESSION = 625,                 /* SESSION  */
    SESSION_USER = 626,            /* SESSION_USER  */
    SET = 627,                     /* SET  */
    SETS = 628,                    /* SETS  */
    SETOF = 629,                   /* SETOF  */
    SHARE = 630,                   /* SHARE  */
    SHOW = 631,                    /* SHOW  */
    SIMILAR = 632,                 /* SIMILAR  */
    SIMPLE = 633,                  /* SIMPLE  */
    SKIP = 634,                    /* SKIP  */
    SMALLINT = 635,                /* SMALLINT  */
    SNAPSHOT = 636,                /* SNAPSHOT  */
    SOME = 637,                    /* SOME  */
    SQL_P = 638,                   /* SQL_P  */
    STABLE = 639,                  /* STABLE  */
    STANDALONE_P = 640,            /* STANDALONE_P  */
    START = 641,                   /* START  */
    STATEMENT = 642,               /* STATEMENT  */
    STATISTICS = 643,              /* STATISTICS  */
    STDIN = 644,                   /* STDIN  */
    STDOUT = 645,                  /* STDOUT  */
    STORAGE = 646,                 /* STORAGE  */
    STORED = 647,                  /* STORED  */
    STRICT_P = 648,                /* STRICT_P  */
    STRIP_P = 649,                 /* STRIP_P  */
    SUBSCRIPTION = 650,            /* SUBSCRIPTION  */
    SUBSTRING = 651,               /* SUBSTRING  */
    SUPPORT = 652,                 /* SUPPORT  */
    SYMMETRIC = 653,               /* SYMMETRIC  */
    SYSID = 654,                   /* SYSID  */
    SYSTEM_P = 655,                /* SYSTEM_P  */
    TABLE = 656,                   /* TABLE  */
    TABLES = 657,                  /* TABLES  */
    TABLESAMPLE = 658,             /* TABLESAMPLE  */
    TABLESPACE = 659,              /* TABLESPACE  */
    TEMP = 660,                    /* TEMP  */
    TEMPLATE = 661,                /* TEMPLATE  */
    TEMPORARY = 662,               /* TEMPORARY  */
    TEXT_P = 663,                  /* TEXT_P  */
    THEN = 664,                    /* THEN  */
    TIES = 665,                    /* TIES  */
    TIME = 666,                    /* TIME  */
    TIMESTAMP = 667,               /* TIMESTAMP  */
    TO = 668,                      /* TO  */
    TRAILING = 669,                /* TRAILING  */
    TRANSACTION = 670,             /* TRANSACTION  */
    TRANSFORM = 671,               /* TRANSFORM  */
    TREAT = 672,                   /* TREAT  */
    TRIGGER = 673,                 /* TRIGGER  */
    TRIM = 674,                    /* TRIM  */
    TRUE_P = 675,                  /* TRUE_P  */
    TRUNCATE = 676,                /* TRUNCATE  */
    TRUSTED = 677,                 /* TRUSTED  */
    TYPE_P = 678,                  /* TYPE_P  */
    TYPES_P = 679,                 /* TYPES_P  */
    UESCAPE = 680,                 /* UESCAPE  */
    UNBOUNDED = 681,               /* UNBOUNDED  */
    UNCOMMITTED = 682,             /* UNCOMMITTED  */
    UNENCRYPTED = 683,             /* UNENCRYPTED  */
    UNION = 684,                   /* UNION  */
    UNIQUE = 685,                  /* UNIQUE  */
    UNKNOWN = 686,                 /* UNKNOWN  */
    UNLISTEN = 687,                /* UNLISTEN  */
    UNLOGGED = 688,                /* UNLOGGED  */
    UNTIL = 689,                   /* UNTIL  */
    UPDATE = 690,                  /* UPDATE  */
    USER = 691,                    /* USER  */
    USING = 692,                   /* USING  */
    VACUUM = 693,                  /* VACUUM  */
    VALID = 694,                   /* VALID  */
    VALIDATE = 695,                /* VALIDATE  */
    VALIDATOR = 696,               /* VALIDATOR  */
    VALUE_P = 697,                 /* VALUE_P  */
    VALUES = 698,                  /* VALUES  */
    VARCHAR = 699,                 /* VARCHAR  */
    VARIADIC = 700,                /* VARIADIC  */
    VARYING = 701,                 /* VARYING  */
    VERBOSE = 702,                 /* VERBOSE  */
    VERSION_P = 703,               /* VERSION_P  */
    VIEW = 704,                    /* VIEW  */
    VIEWS = 705,                   /* VIEWS  */
    VOLATILE = 706,                /* VOLATILE  */
    WHEN = 707,                    /* WHEN  */
    WHERE = 708,                   /* WHERE  */
    WHITESPACE_P = 709,            /* WHITESPACE_P  */
    WINDOW = 710,                  /* WINDOW  */
    WITH = 711,                    /* WITH  */
    WITHIN = 712,                  /* WITHIN  */
    WITHOUT = 713,                 /* WITHOUT  */
    WORK = 714,                    /* WORK  */
    WRAPPER = 715,                 /* WRAPPER  */
    WRITE = 716,                   /* WRITE  */
    XML_P = 717,                   /* XML_P  */
    XMLATTRIBUTES = 718,           /* XMLATTRIBUTES  */
    XMLCONCAT = 719,               /* XMLCONCAT  */
    XMLELEMENT = 720,              /* XMLELEMENT  */
    XMLEXISTS = 721,               /* XMLEXISTS  */
    XMLFOREST = 722,               /* XMLFOREST  */
    XMLNAMESPACES = 723,           /* XMLNAMESPACES  */
    XMLPARSE = 724,                /* XMLPARSE  */
    XMLPI = 725,                   /* XMLPI  */
    XMLROOT = 726,                 /* XMLROOT  */
    XMLSERIALIZE = 727,            /* XMLSERIALIZE  */
    XMLTABLE = 728,                /* XMLTABLE  */
    YEAR_P = 729,                  /* YEAR_P  */
    YES_P = 730,                   /* YES_P  */
    ZONE = 731,                    /* ZONE  */
    NOT_LA = 732,                  /* NOT_LA  */
    NULLS_LA = 733,                /* NULLS_LA  */
    WITH_LA = 734,                 /* WITH_LA  */
    MODE_TYPE_NAME = 735,          /* MODE_TYPE_NAME  */
    MODE_PLPGSQL_EXPR = 736,       /* MODE_PLPGSQL_EXPR  */
    MODE_PLPGSQL_ASSIGN1 = 737,    /* MODE_PLPGSQL_ASSIGN1  */
    MODE_PLPGSQL_ASSIGN2 = 738,    /* MODE_PLPGSQL_ASSIGN2  */
    MODE_PLPGSQL_ASSIGN3 = 739,    /* MODE_PLPGSQL_ASSIGN3  */
    UMINUS = 740                   /* UMINUS  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 217 "gram.y"

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

#line 597 "gram.h"

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
