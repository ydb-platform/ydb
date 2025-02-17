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
    ABSENT = 276,                  /* ABSENT  */
    ABSOLUTE_P = 277,              /* ABSOLUTE_P  */
    ACCESS = 278,                  /* ACCESS  */
    ACTION = 279,                  /* ACTION  */
    ADD_P = 280,                   /* ADD_P  */
    ADMIN = 281,                   /* ADMIN  */
    AFTER = 282,                   /* AFTER  */
    AGGREGATE = 283,               /* AGGREGATE  */
    ALL = 284,                     /* ALL  */
    ALSO = 285,                    /* ALSO  */
    ALTER = 286,                   /* ALTER  */
    ALWAYS = 287,                  /* ALWAYS  */
    ANALYSE = 288,                 /* ANALYSE  */
    ANALYZE = 289,                 /* ANALYZE  */
    AND = 290,                     /* AND  */
    ANY = 291,                     /* ANY  */
    ARRAY = 292,                   /* ARRAY  */
    AS = 293,                      /* AS  */
    ASC = 294,                     /* ASC  */
    ASENSITIVE = 295,              /* ASENSITIVE  */
    ASSERTION = 296,               /* ASSERTION  */
    ASSIGNMENT = 297,              /* ASSIGNMENT  */
    ASYMMETRIC = 298,              /* ASYMMETRIC  */
    ATOMIC = 299,                  /* ATOMIC  */
    AT = 300,                      /* AT  */
    ATTACH = 301,                  /* ATTACH  */
    ATTRIBUTE = 302,               /* ATTRIBUTE  */
    AUTHORIZATION = 303,           /* AUTHORIZATION  */
    BACKWARD = 304,                /* BACKWARD  */
    BEFORE = 305,                  /* BEFORE  */
    BEGIN_P = 306,                 /* BEGIN_P  */
    BETWEEN = 307,                 /* BETWEEN  */
    BIGINT = 308,                  /* BIGINT  */
    BINARY = 309,                  /* BINARY  */
    BIT = 310,                     /* BIT  */
    BOOLEAN_P = 311,               /* BOOLEAN_P  */
    BOTH = 312,                    /* BOTH  */
    BREADTH = 313,                 /* BREADTH  */
    BY = 314,                      /* BY  */
    CACHE = 315,                   /* CACHE  */
    CALL = 316,                    /* CALL  */
    CALLED = 317,                  /* CALLED  */
    CASCADE = 318,                 /* CASCADE  */
    CASCADED = 319,                /* CASCADED  */
    CASE = 320,                    /* CASE  */
    CAST = 321,                    /* CAST  */
    CATALOG_P = 322,               /* CATALOG_P  */
    CHAIN = 323,                   /* CHAIN  */
    CHAR_P = 324,                  /* CHAR_P  */
    CHARACTER = 325,               /* CHARACTER  */
    CHARACTERISTICS = 326,         /* CHARACTERISTICS  */
    CHECK = 327,                   /* CHECK  */
    CHECKPOINT = 328,              /* CHECKPOINT  */
    CLASS = 329,                   /* CLASS  */
    CLOSE = 330,                   /* CLOSE  */
    CLUSTER = 331,                 /* CLUSTER  */
    COALESCE = 332,                /* COALESCE  */
    COLLATE = 333,                 /* COLLATE  */
    COLLATION = 334,               /* COLLATION  */
    COLUMN = 335,                  /* COLUMN  */
    COLUMNS = 336,                 /* COLUMNS  */
    COMMENT = 337,                 /* COMMENT  */
    COMMENTS = 338,                /* COMMENTS  */
    COMMIT = 339,                  /* COMMIT  */
    COMMITTED = 340,               /* COMMITTED  */
    COMPRESSION = 341,             /* COMPRESSION  */
    CONCURRENTLY = 342,            /* CONCURRENTLY  */
    CONFIGURATION = 343,           /* CONFIGURATION  */
    CONFLICT = 344,                /* CONFLICT  */
    CONNECTION = 345,              /* CONNECTION  */
    CONSTRAINT = 346,              /* CONSTRAINT  */
    CONSTRAINTS = 347,             /* CONSTRAINTS  */
    CONTENT_P = 348,               /* CONTENT_P  */
    CONTINUE_P = 349,              /* CONTINUE_P  */
    CONVERSION_P = 350,            /* CONVERSION_P  */
    COPY = 351,                    /* COPY  */
    COST = 352,                    /* COST  */
    CREATE = 353,                  /* CREATE  */
    CROSS = 354,                   /* CROSS  */
    CSV = 355,                     /* CSV  */
    CUBE = 356,                    /* CUBE  */
    CURRENT_P = 357,               /* CURRENT_P  */
    CURRENT_CATALOG = 358,         /* CURRENT_CATALOG  */
    CURRENT_DATE = 359,            /* CURRENT_DATE  */
    CURRENT_ROLE = 360,            /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 361,          /* CURRENT_SCHEMA  */
    CURRENT_TIME = 362,            /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 363,       /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 364,            /* CURRENT_USER  */
    CURSOR = 365,                  /* CURSOR  */
    CYCLE = 366,                   /* CYCLE  */
    DATA_P = 367,                  /* DATA_P  */
    DATABASE = 368,                /* DATABASE  */
    DAY_P = 369,                   /* DAY_P  */
    DEALLOCATE = 370,              /* DEALLOCATE  */
    DEC = 371,                     /* DEC  */
    DECIMAL_P = 372,               /* DECIMAL_P  */
    DECLARE = 373,                 /* DECLARE  */
    DEFAULT = 374,                 /* DEFAULT  */
    DEFAULTS = 375,                /* DEFAULTS  */
    DEFERRABLE = 376,              /* DEFERRABLE  */
    DEFERRED = 377,                /* DEFERRED  */
    DEFINER = 378,                 /* DEFINER  */
    DELETE_P = 379,                /* DELETE_P  */
    DELIMITER = 380,               /* DELIMITER  */
    DELIMITERS = 381,              /* DELIMITERS  */
    DEPENDS = 382,                 /* DEPENDS  */
    DEPTH = 383,                   /* DEPTH  */
    DESC = 384,                    /* DESC  */
    DETACH = 385,                  /* DETACH  */
    DICTIONARY = 386,              /* DICTIONARY  */
    DISABLE_P = 387,               /* DISABLE_P  */
    DISCARD = 388,                 /* DISCARD  */
    DISTINCT = 389,                /* DISTINCT  */
    DO = 390,                      /* DO  */
    DOCUMENT_P = 391,              /* DOCUMENT_P  */
    DOMAIN_P = 392,                /* DOMAIN_P  */
    DOUBLE_P = 393,                /* DOUBLE_P  */
    DROP = 394,                    /* DROP  */
    EACH = 395,                    /* EACH  */
    ELSE = 396,                    /* ELSE  */
    ENABLE_P = 397,                /* ENABLE_P  */
    ENCODING = 398,                /* ENCODING  */
    ENCRYPTED = 399,               /* ENCRYPTED  */
    END_P = 400,                   /* END_P  */
    ENUM_P = 401,                  /* ENUM_P  */
    ESCAPE = 402,                  /* ESCAPE  */
    EVENT = 403,                   /* EVENT  */
    EXCEPT = 404,                  /* EXCEPT  */
    EXCLUDE = 405,                 /* EXCLUDE  */
    EXCLUDING = 406,               /* EXCLUDING  */
    EXCLUSIVE = 407,               /* EXCLUSIVE  */
    EXECUTE = 408,                 /* EXECUTE  */
    EXISTS = 409,                  /* EXISTS  */
    EXPLAIN = 410,                 /* EXPLAIN  */
    EXPRESSION = 411,              /* EXPRESSION  */
    EXTENSION = 412,               /* EXTENSION  */
    EXTERNAL = 413,                /* EXTERNAL  */
    EXTRACT = 414,                 /* EXTRACT  */
    FALSE_P = 415,                 /* FALSE_P  */
    FAMILY = 416,                  /* FAMILY  */
    FETCH = 417,                   /* FETCH  */
    FILTER = 418,                  /* FILTER  */
    FINALIZE = 419,                /* FINALIZE  */
    FIRST_P = 420,                 /* FIRST_P  */
    FLOAT_P = 421,                 /* FLOAT_P  */
    FOLLOWING = 422,               /* FOLLOWING  */
    FOR = 423,                     /* FOR  */
    FORCE = 424,                   /* FORCE  */
    FOREIGN = 425,                 /* FOREIGN  */
    FORMAT = 426,                  /* FORMAT  */
    FORWARD = 427,                 /* FORWARD  */
    FREEZE = 428,                  /* FREEZE  */
    FROM = 429,                    /* FROM  */
    FULL = 430,                    /* FULL  */
    FUNCTION = 431,                /* FUNCTION  */
    FUNCTIONS = 432,               /* FUNCTIONS  */
    GENERATED = 433,               /* GENERATED  */
    GLOBAL = 434,                  /* GLOBAL  */
    GRANT = 435,                   /* GRANT  */
    GRANTED = 436,                 /* GRANTED  */
    GREATEST = 437,                /* GREATEST  */
    GROUP_P = 438,                 /* GROUP_P  */
    GROUPING = 439,                /* GROUPING  */
    GROUPS = 440,                  /* GROUPS  */
    HANDLER = 441,                 /* HANDLER  */
    HAVING = 442,                  /* HAVING  */
    HEADER_P = 443,                /* HEADER_P  */
    HOLD = 444,                    /* HOLD  */
    HOUR_P = 445,                  /* HOUR_P  */
    IDENTITY_P = 446,              /* IDENTITY_P  */
    IF_P = 447,                    /* IF_P  */
    ILIKE = 448,                   /* ILIKE  */
    IMMEDIATE = 449,               /* IMMEDIATE  */
    IMMUTABLE = 450,               /* IMMUTABLE  */
    IMPLICIT_P = 451,              /* IMPLICIT_P  */
    IMPORT_P = 452,                /* IMPORT_P  */
    IN_P = 453,                    /* IN_P  */
    INCLUDE = 454,                 /* INCLUDE  */
    INCLUDING = 455,               /* INCLUDING  */
    INCREMENT = 456,               /* INCREMENT  */
    INDENT = 457,                  /* INDENT  */
    INDEX = 458,                   /* INDEX  */
    INDEXES = 459,                 /* INDEXES  */
    INHERIT = 460,                 /* INHERIT  */
    INHERITS = 461,                /* INHERITS  */
    INITIALLY = 462,               /* INITIALLY  */
    INLINE_P = 463,                /* INLINE_P  */
    INNER_P = 464,                 /* INNER_P  */
    INOUT = 465,                   /* INOUT  */
    INPUT_P = 466,                 /* INPUT_P  */
    INSENSITIVE = 467,             /* INSENSITIVE  */
    INSERT = 468,                  /* INSERT  */
    INSTEAD = 469,                 /* INSTEAD  */
    INT_P = 470,                   /* INT_P  */
    INTEGER = 471,                 /* INTEGER  */
    INTERSECT = 472,               /* INTERSECT  */
    INTERVAL = 473,                /* INTERVAL  */
    INTO = 474,                    /* INTO  */
    INVOKER = 475,                 /* INVOKER  */
    IS = 476,                      /* IS  */
    ISNULL = 477,                  /* ISNULL  */
    ISOLATION = 478,               /* ISOLATION  */
    JOIN = 479,                    /* JOIN  */
    JSON = 480,                    /* JSON  */
    JSON_ARRAY = 481,              /* JSON_ARRAY  */
    JSON_ARRAYAGG = 482,           /* JSON_ARRAYAGG  */
    JSON_OBJECT = 483,             /* JSON_OBJECT  */
    JSON_OBJECTAGG = 484,          /* JSON_OBJECTAGG  */
    KEY = 485,                     /* KEY  */
    KEYS = 486,                    /* KEYS  */
    LABEL = 487,                   /* LABEL  */
    LANGUAGE = 488,                /* LANGUAGE  */
    LARGE_P = 489,                 /* LARGE_P  */
    LAST_P = 490,                  /* LAST_P  */
    LATERAL_P = 491,               /* LATERAL_P  */
    LEADING = 492,                 /* LEADING  */
    LEAKPROOF = 493,               /* LEAKPROOF  */
    LEAST = 494,                   /* LEAST  */
    LEFT = 495,                    /* LEFT  */
    LEVEL = 496,                   /* LEVEL  */
    LIKE = 497,                    /* LIKE  */
    LIMIT = 498,                   /* LIMIT  */
    LISTEN = 499,                  /* LISTEN  */
    LOAD = 500,                    /* LOAD  */
    LOCAL = 501,                   /* LOCAL  */
    LOCALTIME = 502,               /* LOCALTIME  */
    LOCALTIMESTAMP = 503,          /* LOCALTIMESTAMP  */
    LOCATION = 504,                /* LOCATION  */
    LOCK_P = 505,                  /* LOCK_P  */
    LOCKED = 506,                  /* LOCKED  */
    LOGGED = 507,                  /* LOGGED  */
    MAPPING = 508,                 /* MAPPING  */
    MATCH = 509,                   /* MATCH  */
    MATCHED = 510,                 /* MATCHED  */
    MATERIALIZED = 511,            /* MATERIALIZED  */
    MAXVALUE = 512,                /* MAXVALUE  */
    MERGE = 513,                   /* MERGE  */
    METHOD = 514,                  /* METHOD  */
    MINUTE_P = 515,                /* MINUTE_P  */
    MINVALUE = 516,                /* MINVALUE  */
    MODE = 517,                    /* MODE  */
    MONTH_P = 518,                 /* MONTH_P  */
    MOVE = 519,                    /* MOVE  */
    NAME_P = 520,                  /* NAME_P  */
    NAMES = 521,                   /* NAMES  */
    NATIONAL = 522,                /* NATIONAL  */
    NATURAL = 523,                 /* NATURAL  */
    NCHAR = 524,                   /* NCHAR  */
    NEW = 525,                     /* NEW  */
    NEXT = 526,                    /* NEXT  */
    NFC = 527,                     /* NFC  */
    NFD = 528,                     /* NFD  */
    NFKC = 529,                    /* NFKC  */
    NFKD = 530,                    /* NFKD  */
    NO = 531,                      /* NO  */
    NONE = 532,                    /* NONE  */
    NORMALIZE = 533,               /* NORMALIZE  */
    NORMALIZED = 534,              /* NORMALIZED  */
    NOT = 535,                     /* NOT  */
    NOTHING = 536,                 /* NOTHING  */
    NOTIFY = 537,                  /* NOTIFY  */
    NOTNULL = 538,                 /* NOTNULL  */
    NOWAIT = 539,                  /* NOWAIT  */
    NULL_P = 540,                  /* NULL_P  */
    NULLIF = 541,                  /* NULLIF  */
    NULLS_P = 542,                 /* NULLS_P  */
    NUMERIC = 543,                 /* NUMERIC  */
    OBJECT_P = 544,                /* OBJECT_P  */
    OF = 545,                      /* OF  */
    OFF = 546,                     /* OFF  */
    OFFSET = 547,                  /* OFFSET  */
    OIDS = 548,                    /* OIDS  */
    OLD = 549,                     /* OLD  */
    ON = 550,                      /* ON  */
    ONLY = 551,                    /* ONLY  */
    OPERATOR = 552,                /* OPERATOR  */
    OPTION = 553,                  /* OPTION  */
    OPTIONS = 554,                 /* OPTIONS  */
    OR = 555,                      /* OR  */
    ORDER = 556,                   /* ORDER  */
    ORDINALITY = 557,              /* ORDINALITY  */
    OTHERS = 558,                  /* OTHERS  */
    OUT_P = 559,                   /* OUT_P  */
    OUTER_P = 560,                 /* OUTER_P  */
    OVER = 561,                    /* OVER  */
    OVERLAPS = 562,                /* OVERLAPS  */
    OVERLAY = 563,                 /* OVERLAY  */
    OVERRIDING = 564,              /* OVERRIDING  */
    OWNED = 565,                   /* OWNED  */
    OWNER = 566,                   /* OWNER  */
    PARALLEL = 567,                /* PARALLEL  */
    PARAMETER = 568,               /* PARAMETER  */
    PARSER = 569,                  /* PARSER  */
    PARTIAL = 570,                 /* PARTIAL  */
    PARTITION = 571,               /* PARTITION  */
    PASSING = 572,                 /* PASSING  */
    PASSWORD = 573,                /* PASSWORD  */
    PLACING = 574,                 /* PLACING  */
    PLANS = 575,                   /* PLANS  */
    POLICY = 576,                  /* POLICY  */
    POSITION = 577,                /* POSITION  */
    PRECEDING = 578,               /* PRECEDING  */
    PRECISION = 579,               /* PRECISION  */
    PRESERVE = 580,                /* PRESERVE  */
    PREPARE = 581,                 /* PREPARE  */
    PREPARED = 582,                /* PREPARED  */
    PRIMARY = 583,                 /* PRIMARY  */
    PRIOR = 584,                   /* PRIOR  */
    PRIVILEGES = 585,              /* PRIVILEGES  */
    PROCEDURAL = 586,              /* PROCEDURAL  */
    PROCEDURE = 587,               /* PROCEDURE  */
    PROCEDURES = 588,              /* PROCEDURES  */
    PROGRAM = 589,                 /* PROGRAM  */
    PUBLICATION = 590,             /* PUBLICATION  */
    QUOTE = 591,                   /* QUOTE  */
    RANGE = 592,                   /* RANGE  */
    READ = 593,                    /* READ  */
    REAL = 594,                    /* REAL  */
    REASSIGN = 595,                /* REASSIGN  */
    RECHECK = 596,                 /* RECHECK  */
    RECURSIVE = 597,               /* RECURSIVE  */
    REF_P = 598,                   /* REF_P  */
    REFERENCES = 599,              /* REFERENCES  */
    REFERENCING = 600,             /* REFERENCING  */
    REFRESH = 601,                 /* REFRESH  */
    REINDEX = 602,                 /* REINDEX  */
    RELATIVE_P = 603,              /* RELATIVE_P  */
    RELEASE = 604,                 /* RELEASE  */
    RENAME = 605,                  /* RENAME  */
    REPEATABLE = 606,              /* REPEATABLE  */
    REPLACE = 607,                 /* REPLACE  */
    REPLICA = 608,                 /* REPLICA  */
    RESET = 609,                   /* RESET  */
    RESTART = 610,                 /* RESTART  */
    RESTRICT = 611,                /* RESTRICT  */
    RETURN = 612,                  /* RETURN  */
    RETURNING = 613,               /* RETURNING  */
    RETURNS = 614,                 /* RETURNS  */
    REVOKE = 615,                  /* REVOKE  */
    RIGHT = 616,                   /* RIGHT  */
    ROLE = 617,                    /* ROLE  */
    ROLLBACK = 618,                /* ROLLBACK  */
    ROLLUP = 619,                  /* ROLLUP  */
    ROUTINE = 620,                 /* ROUTINE  */
    ROUTINES = 621,                /* ROUTINES  */
    ROW = 622,                     /* ROW  */
    ROWS = 623,                    /* ROWS  */
    RULE = 624,                    /* RULE  */
    SAVEPOINT = 625,               /* SAVEPOINT  */
    SCALAR = 626,                  /* SCALAR  */
    SCHEMA = 627,                  /* SCHEMA  */
    SCHEMAS = 628,                 /* SCHEMAS  */
    SCROLL = 629,                  /* SCROLL  */
    SEARCH = 630,                  /* SEARCH  */
    SECOND_P = 631,                /* SECOND_P  */
    SECURITY = 632,                /* SECURITY  */
    SELECT = 633,                  /* SELECT  */
    SEQUENCE = 634,                /* SEQUENCE  */
    SEQUENCES = 635,               /* SEQUENCES  */
    SERIALIZABLE = 636,            /* SERIALIZABLE  */
    SERVER = 637,                  /* SERVER  */
    SESSION = 638,                 /* SESSION  */
    SESSION_USER = 639,            /* SESSION_USER  */
    SET = 640,                     /* SET  */
    SETS = 641,                    /* SETS  */
    SETOF = 642,                   /* SETOF  */
    SHARE = 643,                   /* SHARE  */
    SHOW = 644,                    /* SHOW  */
    SIMILAR = 645,                 /* SIMILAR  */
    SIMPLE = 646,                  /* SIMPLE  */
    SKIP = 647,                    /* SKIP  */
    SMALLINT = 648,                /* SMALLINT  */
    SNAPSHOT = 649,                /* SNAPSHOT  */
    SOME = 650,                    /* SOME  */
    SQL_P = 651,                   /* SQL_P  */
    STABLE = 652,                  /* STABLE  */
    STANDALONE_P = 653,            /* STANDALONE_P  */
    START = 654,                   /* START  */
    STATEMENT = 655,               /* STATEMENT  */
    STATISTICS = 656,              /* STATISTICS  */
    STDIN = 657,                   /* STDIN  */
    STDOUT = 658,                  /* STDOUT  */
    STORAGE = 659,                 /* STORAGE  */
    STORED = 660,                  /* STORED  */
    STRICT_P = 661,                /* STRICT_P  */
    STRIP_P = 662,                 /* STRIP_P  */
    SUBSCRIPTION = 663,            /* SUBSCRIPTION  */
    SUBSTRING = 664,               /* SUBSTRING  */
    SUPPORT = 665,                 /* SUPPORT  */
    SYMMETRIC = 666,               /* SYMMETRIC  */
    SYSID = 667,                   /* SYSID  */
    SYSTEM_P = 668,                /* SYSTEM_P  */
    SYSTEM_USER = 669,             /* SYSTEM_USER  */
    TABLE = 670,                   /* TABLE  */
    TABLES = 671,                  /* TABLES  */
    TABLESAMPLE = 672,             /* TABLESAMPLE  */
    TABLESPACE = 673,              /* TABLESPACE  */
    TEMP = 674,                    /* TEMP  */
    TEMPLATE = 675,                /* TEMPLATE  */
    TEMPORARY = 676,               /* TEMPORARY  */
    TEXT_P = 677,                  /* TEXT_P  */
    THEN = 678,                    /* THEN  */
    TIES = 679,                    /* TIES  */
    TIME = 680,                    /* TIME  */
    TIMESTAMP = 681,               /* TIMESTAMP  */
    TO = 682,                      /* TO  */
    TRAILING = 683,                /* TRAILING  */
    TRANSACTION = 684,             /* TRANSACTION  */
    TRANSFORM = 685,               /* TRANSFORM  */
    TREAT = 686,                   /* TREAT  */
    TRIGGER = 687,                 /* TRIGGER  */
    TRIM = 688,                    /* TRIM  */
    TRUE_P = 689,                  /* TRUE_P  */
    TRUNCATE = 690,                /* TRUNCATE  */
    TRUSTED = 691,                 /* TRUSTED  */
    TYPE_P = 692,                  /* TYPE_P  */
    TYPES_P = 693,                 /* TYPES_P  */
    UESCAPE = 694,                 /* UESCAPE  */
    UNBOUNDED = 695,               /* UNBOUNDED  */
    UNCOMMITTED = 696,             /* UNCOMMITTED  */
    UNENCRYPTED = 697,             /* UNENCRYPTED  */
    UNION = 698,                   /* UNION  */
    UNIQUE = 699,                  /* UNIQUE  */
    UNKNOWN = 700,                 /* UNKNOWN  */
    UNLISTEN = 701,                /* UNLISTEN  */
    UNLOGGED = 702,                /* UNLOGGED  */
    UNTIL = 703,                   /* UNTIL  */
    UPDATE = 704,                  /* UPDATE  */
    USER = 705,                    /* USER  */
    USING = 706,                   /* USING  */
    VACUUM = 707,                  /* VACUUM  */
    VALID = 708,                   /* VALID  */
    VALIDATE = 709,                /* VALIDATE  */
    VALIDATOR = 710,               /* VALIDATOR  */
    VALUE_P = 711,                 /* VALUE_P  */
    VALUES = 712,                  /* VALUES  */
    VARCHAR = 713,                 /* VARCHAR  */
    VARIADIC = 714,                /* VARIADIC  */
    VARYING = 715,                 /* VARYING  */
    VERBOSE = 716,                 /* VERBOSE  */
    VERSION_P = 717,               /* VERSION_P  */
    VIEW = 718,                    /* VIEW  */
    VIEWS = 719,                   /* VIEWS  */
    VOLATILE = 720,                /* VOLATILE  */
    WHEN = 721,                    /* WHEN  */
    WHERE = 722,                   /* WHERE  */
    WHITESPACE_P = 723,            /* WHITESPACE_P  */
    WINDOW = 724,                  /* WINDOW  */
    WITH = 725,                    /* WITH  */
    WITHIN = 726,                  /* WITHIN  */
    WITHOUT = 727,                 /* WITHOUT  */
    WORK = 728,                    /* WORK  */
    WRAPPER = 729,                 /* WRAPPER  */
    WRITE = 730,                   /* WRITE  */
    XML_P = 731,                   /* XML_P  */
    XMLATTRIBUTES = 732,           /* XMLATTRIBUTES  */
    XMLCONCAT = 733,               /* XMLCONCAT  */
    XMLELEMENT = 734,              /* XMLELEMENT  */
    XMLEXISTS = 735,               /* XMLEXISTS  */
    XMLFOREST = 736,               /* XMLFOREST  */
    XMLNAMESPACES = 737,           /* XMLNAMESPACES  */
    XMLPARSE = 738,                /* XMLPARSE  */
    XMLPI = 739,                   /* XMLPI  */
    XMLROOT = 740,                 /* XMLROOT  */
    XMLSERIALIZE = 741,            /* XMLSERIALIZE  */
    XMLTABLE = 742,                /* XMLTABLE  */
    YEAR_P = 743,                  /* YEAR_P  */
    YES_P = 744,                   /* YES_P  */
    ZONE = 745,                    /* ZONE  */
    FORMAT_LA = 746,               /* FORMAT_LA  */
    NOT_LA = 747,                  /* NOT_LA  */
    NULLS_LA = 748,                /* NULLS_LA  */
    WITH_LA = 749,                 /* WITH_LA  */
    WITHOUT_LA = 750,              /* WITHOUT_LA  */
    MODE_TYPE_NAME = 751,          /* MODE_TYPE_NAME  */
    MODE_PLPGSQL_EXPR = 752,       /* MODE_PLPGSQL_EXPR  */
    MODE_PLPGSQL_ASSIGN1 = 753,    /* MODE_PLPGSQL_ASSIGN1  */
    MODE_PLPGSQL_ASSIGN2 = 754,    /* MODE_PLPGSQL_ASSIGN2  */
    MODE_PLPGSQL_ASSIGN3 = 755,    /* MODE_PLPGSQL_ASSIGN3  */
    UMINUS = 756                   /* UMINUS  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 232 "gram.y"

	core_YYSTYPE core_yystype;
	/* these fields must match core_YYSTYPE: */
	int			ival;
	char	   *str;
	const char *keyword;

	char		chr;
	bool		boolean;
	JoinType	jtype;
	DropBehavior dbehavior;
	OnCommitAction oncommit;
	List	   *list;
	Node	   *node;
	ObjectType	objtype;
	TypeName   *typnam;
	FunctionParameter *fun_param;
	FunctionParameterMode fun_param_mode;
	ObjectWithArgs *objwithargs;
	DefElem	   *defelt;
	SortBy	   *sortby;
	WindowDef  *windef;
	JoinExpr   *jexpr;
	IndexElem  *ielem;
	StatsElem  *selem;
	Alias	   *alias;
	RangeVar   *range;
	IntoClause *into;
	WithClause *with;
	InferClause	*infer;
	OnConflictClause *onconflict;
	A_Indices  *aind;
	ResTarget  *target;
	struct PrivTarget *privtarget;
	AccessPriv *accesspriv;
	struct ImportQual *importqual;
	InsertStmt *istmt;
	VariableSetStmt *vsetstmt;
	PartitionElem *partelem;
	PartitionSpec *partspec;
	PartitionBoundSpec *partboundspec;
	RoleSpec   *rolespec;
	PublicationObjSpec *publicationobjectspec;
	struct SelectLimit *selectlimit;
	SetQuantifier setquantifier;
	struct GroupClause *groupclause;
	MergeWhenClause *mergewhen;
	struct KeyActions *keyactions;
	struct KeyAction *keyaction;

#line 616 "gram.h"

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
