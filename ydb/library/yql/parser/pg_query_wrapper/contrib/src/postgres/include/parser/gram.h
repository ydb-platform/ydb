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
     SQL_COMMENT = 275,
     C_COMMENT = 276,
     ABORT_P = 277,
     ABSOLUTE_P = 278,
     ACCESS = 279,
     ACTION = 280,
     ADD_P = 281,
     ADMIN = 282,
     AFTER = 283,
     AGGREGATE = 284,
     ALL = 285,
     ALSO = 286,
     ALTER = 287,
     ALWAYS = 288,
     ANALYSE = 289,
     ANALYZE = 290,
     AND = 291,
     ANY = 292,
     ARRAY = 293,
     AS = 294,
     ASC = 295,
     ASSERTION = 296,
     ASSIGNMENT = 297,
     ASYMMETRIC = 298,
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
     BY = 312,
     CACHE = 313,
     CALL = 314,
     CALLED = 315,
     CASCADE = 316,
     CASCADED = 317,
     CASE = 318,
     CAST = 319,
     CATALOG_P = 320,
     CHAIN = 321,
     CHAR_P = 322,
     CHARACTER = 323,
     CHARACTERISTICS = 324,
     CHECK = 325,
     CHECKPOINT = 326,
     CLASS = 327,
     CLOSE = 328,
     CLUSTER = 329,
     COALESCE = 330,
     COLLATE = 331,
     COLLATION = 332,
     COLUMN = 333,
     COLUMNS = 334,
     COMMENT = 335,
     COMMENTS = 336,
     COMMIT = 337,
     COMMITTED = 338,
     CONCURRENTLY = 339,
     CONFIGURATION = 340,
     CONFLICT = 341,
     CONNECTION = 342,
     CONSTRAINT = 343,
     CONSTRAINTS = 344,
     CONTENT_P = 345,
     CONTINUE_P = 346,
     CONVERSION_P = 347,
     COPY = 348,
     COST = 349,
     CREATE = 350,
     CROSS = 351,
     CSV = 352,
     CUBE = 353,
     CURRENT_P = 354,
     CURRENT_CATALOG = 355,
     CURRENT_DATE = 356,
     CURRENT_ROLE = 357,
     CURRENT_SCHEMA = 358,
     CURRENT_TIME = 359,
     CURRENT_TIMESTAMP = 360,
     CURRENT_USER = 361,
     CURSOR = 362,
     CYCLE = 363,
     DATA_P = 364,
     DATABASE = 365,
     DAY_P = 366,
     DEALLOCATE = 367,
     DEC = 368,
     DECIMAL_P = 369,
     DECLARE = 370,
     DEFAULT = 371,
     DEFAULTS = 372,
     DEFERRABLE = 373,
     DEFERRED = 374,
     DEFINER = 375,
     DELETE_P = 376,
     DELIMITER = 377,
     DELIMITERS = 378,
     DEPENDS = 379,
     DESC = 380,
     DETACH = 381,
     DICTIONARY = 382,
     DISABLE_P = 383,
     DISCARD = 384,
     DISTINCT = 385,
     DO = 386,
     DOCUMENT_P = 387,
     DOMAIN_P = 388,
     DOUBLE_P = 389,
     DROP = 390,
     EACH = 391,
     ELSE = 392,
     ENABLE_P = 393,
     ENCODING = 394,
     ENCRYPTED = 395,
     END_P = 396,
     ENUM_P = 397,
     ESCAPE = 398,
     EVENT = 399,
     EXCEPT = 400,
     EXCLUDE = 401,
     EXCLUDING = 402,
     EXCLUSIVE = 403,
     EXECUTE = 404,
     EXISTS = 405,
     EXPLAIN = 406,
     EXPRESSION = 407,
     EXTENSION = 408,
     EXTERNAL = 409,
     EXTRACT = 410,
     FALSE_P = 411,
     FAMILY = 412,
     FETCH = 413,
     FILTER = 414,
     FIRST_P = 415,
     FLOAT_P = 416,
     FOLLOWING = 417,
     FOR = 418,
     FORCE = 419,
     FOREIGN = 420,
     FORWARD = 421,
     FREEZE = 422,
     FROM = 423,
     FULL = 424,
     FUNCTION = 425,
     FUNCTIONS = 426,
     GENERATED = 427,
     GLOBAL = 428,
     GRANT = 429,
     GRANTED = 430,
     GREATEST = 431,
     GROUP_P = 432,
     GROUPING = 433,
     GROUPS = 434,
     HANDLER = 435,
     HAVING = 436,
     HEADER_P = 437,
     HOLD = 438,
     HOUR_P = 439,
     IDENTITY_P = 440,
     IF_P = 441,
     ILIKE = 442,
     IMMEDIATE = 443,
     IMMUTABLE = 444,
     IMPLICIT_P = 445,
     IMPORT_P = 446,
     IN_P = 447,
     INCLUDE = 448,
     INCLUDING = 449,
     INCREMENT = 450,
     INDEX = 451,
     INDEXES = 452,
     INHERIT = 453,
     INHERITS = 454,
     INITIALLY = 455,
     INLINE_P = 456,
     INNER_P = 457,
     INOUT = 458,
     INPUT_P = 459,
     INSENSITIVE = 460,
     INSERT = 461,
     INSTEAD = 462,
     INT_P = 463,
     INTEGER = 464,
     INTERSECT = 465,
     INTERVAL = 466,
     INTO = 467,
     INVOKER = 468,
     IS = 469,
     ISNULL = 470,
     ISOLATION = 471,
     JOIN = 472,
     KEY = 473,
     LABEL = 474,
     LANGUAGE = 475,
     LARGE_P = 476,
     LAST_P = 477,
     LATERAL_P = 478,
     LEADING = 479,
     LEAKPROOF = 480,
     LEAST = 481,
     LEFT = 482,
     LEVEL = 483,
     LIKE = 484,
     LIMIT = 485,
     LISTEN = 486,
     LOAD = 487,
     LOCAL = 488,
     LOCALTIME = 489,
     LOCALTIMESTAMP = 490,
     LOCATION = 491,
     LOCK_P = 492,
     LOCKED = 493,
     LOGGED = 494,
     MAPPING = 495,
     MATCH = 496,
     MATERIALIZED = 497,
     MAXVALUE = 498,
     METHOD = 499,
     MINUTE_P = 500,
     MINVALUE = 501,
     MODE = 502,
     MONTH_P = 503,
     MOVE = 504,
     NAME_P = 505,
     NAMES = 506,
     NATIONAL = 507,
     NATURAL = 508,
     NCHAR = 509,
     NEW = 510,
     NEXT = 511,
     NFC = 512,
     NFD = 513,
     NFKC = 514,
     NFKD = 515,
     NO = 516,
     NONE = 517,
     NORMALIZE = 518,
     NORMALIZED = 519,
     NOT = 520,
     NOTHING = 521,
     NOTIFY = 522,
     NOTNULL = 523,
     NOWAIT = 524,
     NULL_P = 525,
     NULLIF = 526,
     NULLS_P = 527,
     NUMERIC = 528,
     OBJECT_P = 529,
     OF = 530,
     OFF = 531,
     OFFSET = 532,
     OIDS = 533,
     OLD = 534,
     ON = 535,
     ONLY = 536,
     OPERATOR = 537,
     OPTION = 538,
     OPTIONS = 539,
     OR = 540,
     ORDER = 541,
     ORDINALITY = 542,
     OTHERS = 543,
     OUT_P = 544,
     OUTER_P = 545,
     OVER = 546,
     OVERLAPS = 547,
     OVERLAY = 548,
     OVERRIDING = 549,
     OWNED = 550,
     OWNER = 551,
     PARALLEL = 552,
     PARSER = 553,
     PARTIAL = 554,
     PARTITION = 555,
     PASSING = 556,
     PASSWORD = 557,
     PLACING = 558,
     PLANS = 559,
     POLICY = 560,
     POSITION = 561,
     PRECEDING = 562,
     PRECISION = 563,
     PRESERVE = 564,
     PREPARE = 565,
     PREPARED = 566,
     PRIMARY = 567,
     PRIOR = 568,
     PRIVILEGES = 569,
     PROCEDURAL = 570,
     PROCEDURE = 571,
     PROCEDURES = 572,
     PROGRAM = 573,
     PUBLICATION = 574,
     QUOTE = 575,
     RANGE = 576,
     READ = 577,
     REAL = 578,
     REASSIGN = 579,
     RECHECK = 580,
     RECURSIVE = 581,
     REF = 582,
     REFERENCES = 583,
     REFERENCING = 584,
     REFRESH = 585,
     REINDEX = 586,
     RELATIVE_P = 587,
     RELEASE = 588,
     RENAME = 589,
     REPEATABLE = 590,
     REPLACE = 591,
     REPLICA = 592,
     RESET = 593,
     RESTART = 594,
     RESTRICT = 595,
     RETURNING = 596,
     RETURNS = 597,
     REVOKE = 598,
     RIGHT = 599,
     ROLE = 600,
     ROLLBACK = 601,
     ROLLUP = 602,
     ROUTINE = 603,
     ROUTINES = 604,
     ROW = 605,
     ROWS = 606,
     RULE = 607,
     SAVEPOINT = 608,
     SCHEMA = 609,
     SCHEMAS = 610,
     SCROLL = 611,
     SEARCH = 612,
     SECOND_P = 613,
     SECURITY = 614,
     SELECT = 615,
     SEQUENCE = 616,
     SEQUENCES = 617,
     SERIALIZABLE = 618,
     SERVER = 619,
     SESSION = 620,
     SESSION_USER = 621,
     SET = 622,
     SETS = 623,
     SETOF = 624,
     SHARE = 625,
     SHOW = 626,
     SIMILAR = 627,
     SIMPLE = 628,
     SKIP = 629,
     SMALLINT = 630,
     SNAPSHOT = 631,
     SOME = 632,
     SQL_P = 633,
     STABLE = 634,
     STANDALONE_P = 635,
     START = 636,
     STATEMENT = 637,
     STATISTICS = 638,
     STDIN = 639,
     STDOUT = 640,
     STORAGE = 641,
     STORED = 642,
     STRICT_P = 643,
     STRIP_P = 644,
     SUBSCRIPTION = 645,
     SUBSTRING = 646,
     SUPPORT = 647,
     SYMMETRIC = 648,
     SYSID = 649,
     SYSTEM_P = 650,
     TABLE = 651,
     TABLES = 652,
     TABLESAMPLE = 653,
     TABLESPACE = 654,
     TEMP = 655,
     TEMPLATE = 656,
     TEMPORARY = 657,
     TEXT_P = 658,
     THEN = 659,
     TIES = 660,
     TIME = 661,
     TIMESTAMP = 662,
     TO = 663,
     TRAILING = 664,
     TRANSACTION = 665,
     TRANSFORM = 666,
     TREAT = 667,
     TRIGGER = 668,
     TRIM = 669,
     TRUE_P = 670,
     TRUNCATE = 671,
     TRUSTED = 672,
     TYPE_P = 673,
     TYPES_P = 674,
     UESCAPE = 675,
     UNBOUNDED = 676,
     UNCOMMITTED = 677,
     UNENCRYPTED = 678,
     UNION = 679,
     UNIQUE = 680,
     UNKNOWN = 681,
     UNLISTEN = 682,
     UNLOGGED = 683,
     UNTIL = 684,
     UPDATE = 685,
     USER = 686,
     USING = 687,
     VACUUM = 688,
     VALID = 689,
     VALIDATE = 690,
     VALIDATOR = 691,
     VALUE_P = 692,
     VALUES = 693,
     VARCHAR = 694,
     VARIADIC = 695,
     VARYING = 696,
     VERBOSE = 697,
     VERSION_P = 698,
     VIEW = 699,
     VIEWS = 700,
     VOLATILE = 701,
     WHEN = 702,
     WHERE = 703,
     WHITESPACE_P = 704,
     WINDOW = 705,
     WITH = 706,
     WITHIN = 707,
     WITHOUT = 708,
     WORK = 709,
     WRAPPER = 710,
     WRITE = 711,
     XML_P = 712,
     XMLATTRIBUTES = 713,
     XMLCONCAT = 714,
     XMLELEMENT = 715,
     XMLEXISTS = 716,
     XMLFOREST = 717,
     XMLNAMESPACES = 718,
     XMLPARSE = 719,
     XMLPI = 720,
     XMLROOT = 721,
     XMLSERIALIZE = 722,
     XMLTABLE = 723,
     YEAR_P = 724,
     YES_P = 725,
     ZONE = 726,
     NOT_LA = 727,
     NULLS_LA = 728,
     WITH_LA = 729,
     POSTFIXOP = 730,
     UMINUS = 731
   };
#endif
/* Tokens.  */
#define IDENT 258
#define UIDENT 259
#define FCONST 260
#define SCONST 261
#define USCONST 262
#define BCONST 263
#define XCONST 264
#define Op 265
#define ICONST 266
#define PARAM 267
#define TYPECAST 268
#define DOT_DOT 269
#define COLON_EQUALS 270
#define EQUALS_GREATER 271
#define LESS_EQUALS 272
#define GREATER_EQUALS 273
#define NOT_EQUALS 274
#define SQL_COMMENT 275
#define C_COMMENT 276
#define ABORT_P 277
#define ABSOLUTE_P 278
#define ACCESS 279
#define ACTION 280
#define ADD_P 281
#define ADMIN 282
#define AFTER 283
#define AGGREGATE 284
#define ALL 285
#define ALSO 286
#define ALTER 287
#define ALWAYS 288
#define ANALYSE 289
#define ANALYZE 290
#define AND 291
#define ANY 292
#define ARRAY 293
#define AS 294
#define ASC 295
#define ASSERTION 296
#define ASSIGNMENT 297
#define ASYMMETRIC 298
#define AT 299
#define ATTACH 300
#define ATTRIBUTE 301
#define AUTHORIZATION 302
#define BACKWARD 303
#define BEFORE 304
#define BEGIN_P 305
#define BETWEEN 306
#define BIGINT 307
#define BINARY 308
#define BIT 309
#define BOOLEAN_P 310
#define BOTH 311
#define BY 312
#define CACHE 313
#define CALL 314
#define CALLED 315
#define CASCADE 316
#define CASCADED 317
#define CASE 318
#define CAST 319
#define CATALOG_P 320
#define CHAIN 321
#define CHAR_P 322
#define CHARACTER 323
#define CHARACTERISTICS 324
#define CHECK 325
#define CHECKPOINT 326
#define CLASS 327
#define CLOSE 328
#define CLUSTER 329
#define COALESCE 330
#define COLLATE 331
#define COLLATION 332
#define COLUMN 333
#define COLUMNS 334
#define COMMENT 335
#define COMMENTS 336
#define COMMIT 337
#define COMMITTED 338
#define CONCURRENTLY 339
#define CONFIGURATION 340
#define CONFLICT 341
#define CONNECTION 342
#define CONSTRAINT 343
#define CONSTRAINTS 344
#define CONTENT_P 345
#define CONTINUE_P 346
#define CONVERSION_P 347
#define COPY 348
#define COST 349
#define CREATE 350
#define CROSS 351
#define CSV 352
#define CUBE 353
#define CURRENT_P 354
#define CURRENT_CATALOG 355
#define CURRENT_DATE 356
#define CURRENT_ROLE 357
#define CURRENT_SCHEMA 358
#define CURRENT_TIME 359
#define CURRENT_TIMESTAMP 360
#define CURRENT_USER 361
#define CURSOR 362
#define CYCLE 363
#define DATA_P 364
#define DATABASE 365
#define DAY_P 366
#define DEALLOCATE 367
#define DEC 368
#define DECIMAL_P 369
#define DECLARE 370
#define DEFAULT 371
#define DEFAULTS 372
#define DEFERRABLE 373
#define DEFERRED 374
#define DEFINER 375
#define DELETE_P 376
#define DELIMITER 377
#define DELIMITERS 378
#define DEPENDS 379
#define DESC 380
#define DETACH 381
#define DICTIONARY 382
#define DISABLE_P 383
#define DISCARD 384
#define DISTINCT 385
#define DO 386
#define DOCUMENT_P 387
#define DOMAIN_P 388
#define DOUBLE_P 389
#define DROP 390
#define EACH 391
#define ELSE 392
#define ENABLE_P 393
#define ENCODING 394
#define ENCRYPTED 395
#define END_P 396
#define ENUM_P 397
#define ESCAPE 398
#define EVENT 399
#define EXCEPT 400
#define EXCLUDE 401
#define EXCLUDING 402
#define EXCLUSIVE 403
#define EXECUTE 404
#define EXISTS 405
#define EXPLAIN 406
#define EXPRESSION 407
#define EXTENSION 408
#define EXTERNAL 409
#define EXTRACT 410
#define FALSE_P 411
#define FAMILY 412
#define FETCH 413
#define FILTER 414
#define FIRST_P 415
#define FLOAT_P 416
#define FOLLOWING 417
#define FOR 418
#define FORCE 419
#define FOREIGN 420
#define FORWARD 421
#define FREEZE 422
#define FROM 423
#define FULL 424
#define FUNCTION 425
#define FUNCTIONS 426
#define GENERATED 427
#define GLOBAL 428
#define GRANT 429
#define GRANTED 430
#define GREATEST 431
#define GROUP_P 432
#define GROUPING 433
#define GROUPS 434
#define HANDLER 435
#define HAVING 436
#define HEADER_P 437
#define HOLD 438
#define HOUR_P 439
#define IDENTITY_P 440
#define IF_P 441
#define ILIKE 442
#define IMMEDIATE 443
#define IMMUTABLE 444
#define IMPLICIT_P 445
#define IMPORT_P 446
#define IN_P 447
#define INCLUDE 448
#define INCLUDING 449
#define INCREMENT 450
#define INDEX 451
#define INDEXES 452
#define INHERIT 453
#define INHERITS 454
#define INITIALLY 455
#define INLINE_P 456
#define INNER_P 457
#define INOUT 458
#define INPUT_P 459
#define INSENSITIVE 460
#define INSERT 461
#define INSTEAD 462
#define INT_P 463
#define INTEGER 464
#define INTERSECT 465
#define INTERVAL 466
#define INTO 467
#define INVOKER 468
#define IS 469
#define ISNULL 470
#define ISOLATION 471
#define JOIN 472
#define KEY 473
#define LABEL 474
#define LANGUAGE 475
#define LARGE_P 476
#define LAST_P 477
#define LATERAL_P 478
#define LEADING 479
#define LEAKPROOF 480
#define LEAST 481
#define LEFT 482
#define LEVEL 483
#define LIKE 484
#define LIMIT 485
#define LISTEN 486
#define LOAD 487
#define LOCAL 488
#define LOCALTIME 489
#define LOCALTIMESTAMP 490
#define LOCATION 491
#define LOCK_P 492
#define LOCKED 493
#define LOGGED 494
#define MAPPING 495
#define MATCH 496
#define MATERIALIZED 497
#define MAXVALUE 498
#define METHOD 499
#define MINUTE_P 500
#define MINVALUE 501
#define MODE 502
#define MONTH_P 503
#define MOVE 504
#define NAME_P 505
#define NAMES 506
#define NATIONAL 507
#define NATURAL 508
#define NCHAR 509
#define NEW 510
#define NEXT 511
#define NFC 512
#define NFD 513
#define NFKC 514
#define NFKD 515
#define NO 516
#define NONE 517
#define NORMALIZE 518
#define NORMALIZED 519
#define NOT 520
#define NOTHING 521
#define NOTIFY 522
#define NOTNULL 523
#define NOWAIT 524
#define NULL_P 525
#define NULLIF 526
#define NULLS_P 527
#define NUMERIC 528
#define OBJECT_P 529
#define OF 530
#define OFF 531
#define OFFSET 532
#define OIDS 533
#define OLD 534
#define ON 535
#define ONLY 536
#define OPERATOR 537
#define OPTION 538
#define OPTIONS 539
#define OR 540
#define ORDER 541
#define ORDINALITY 542
#define OTHERS 543
#define OUT_P 544
#define OUTER_P 545
#define OVER 546
#define OVERLAPS 547
#define OVERLAY 548
#define OVERRIDING 549
#define OWNED 550
#define OWNER 551
#define PARALLEL 552
#define PARSER 553
#define PARTIAL 554
#define PARTITION 555
#define PASSING 556
#define PASSWORD 557
#define PLACING 558
#define PLANS 559
#define POLICY 560
#define POSITION 561
#define PRECEDING 562
#define PRECISION 563
#define PRESERVE 564
#define PREPARE 565
#define PREPARED 566
#define PRIMARY 567
#define PRIOR 568
#define PRIVILEGES 569
#define PROCEDURAL 570
#define PROCEDURE 571
#define PROCEDURES 572
#define PROGRAM 573
#define PUBLICATION 574
#define QUOTE 575
#define RANGE 576
#define READ 577
#define REAL 578
#define REASSIGN 579
#define RECHECK 580
#define RECURSIVE 581
#define REF 582
#define REFERENCES 583
#define REFERENCING 584
#define REFRESH 585
#define REINDEX 586
#define RELATIVE_P 587
#define RELEASE 588
#define RENAME 589
#define REPEATABLE 590
#define REPLACE 591
#define REPLICA 592
#define RESET 593
#define RESTART 594
#define RESTRICT 595
#define RETURNING 596
#define RETURNS 597
#define REVOKE 598
#define RIGHT 599
#define ROLE 600
#define ROLLBACK 601
#define ROLLUP 602
#define ROUTINE 603
#define ROUTINES 604
#define ROW 605
#define ROWS 606
#define RULE 607
#define SAVEPOINT 608
#define SCHEMA 609
#define SCHEMAS 610
#define SCROLL 611
#define SEARCH 612
#define SECOND_P 613
#define SECURITY 614
#define SELECT 615
#define SEQUENCE 616
#define SEQUENCES 617
#define SERIALIZABLE 618
#define SERVER 619
#define SESSION 620
#define SESSION_USER 621
#define SET 622
#define SETS 623
#define SETOF 624
#define SHARE 625
#define SHOW 626
#define SIMILAR 627
#define SIMPLE 628
#define SKIP 629
#define SMALLINT 630
#define SNAPSHOT 631
#define SOME 632
#define SQL_P 633
#define STABLE 634
#define STANDALONE_P 635
#define START 636
#define STATEMENT 637
#define STATISTICS 638
#define STDIN 639
#define STDOUT 640
#define STORAGE 641
#define STORED 642
#define STRICT_P 643
#define STRIP_P 644
#define SUBSCRIPTION 645
#define SUBSTRING 646
#define SUPPORT 647
#define SYMMETRIC 648
#define SYSID 649
#define SYSTEM_P 650
#define TABLE 651
#define TABLES 652
#define TABLESAMPLE 653
#define TABLESPACE 654
#define TEMP 655
#define TEMPLATE 656
#define TEMPORARY 657
#define TEXT_P 658
#define THEN 659
#define TIES 660
#define TIME 661
#define TIMESTAMP 662
#define TO 663
#define TRAILING 664
#define TRANSACTION 665
#define TRANSFORM 666
#define TREAT 667
#define TRIGGER 668
#define TRIM 669
#define TRUE_P 670
#define TRUNCATE 671
#define TRUSTED 672
#define TYPE_P 673
#define TYPES_P 674
#define UESCAPE 675
#define UNBOUNDED 676
#define UNCOMMITTED 677
#define UNENCRYPTED 678
#define UNION 679
#define UNIQUE 680
#define UNKNOWN 681
#define UNLISTEN 682
#define UNLOGGED 683
#define UNTIL 684
#define UPDATE 685
#define USER 686
#define USING 687
#define VACUUM 688
#define VALID 689
#define VALIDATE 690
#define VALIDATOR 691
#define VALUE_P 692
#define VALUES 693
#define VARCHAR 694
#define VARIADIC 695
#define VARYING 696
#define VERBOSE 697
#define VERSION_P 698
#define VIEW 699
#define VIEWS 700
#define VOLATILE 701
#define WHEN 702
#define WHERE 703
#define WHITESPACE_P 704
#define WINDOW 705
#define WITH 706
#define WITHIN 707
#define WITHOUT 708
#define WORK 709
#define WRAPPER 710
#define WRITE 711
#define XML_P 712
#define XMLATTRIBUTES 713
#define XMLCONCAT 714
#define XMLELEMENT 715
#define XMLEXISTS 716
#define XMLFOREST 717
#define XMLNAMESPACES 718
#define XMLPARSE 719
#define XMLPI 720
#define XMLROOT 721
#define XMLSERIALIZE 722
#define XMLTABLE 723
#define YEAR_P 724
#define YES_P 725
#define ZONE 726
#define NOT_LA 727
#define NULLS_LA 728
#define WITH_LA 729
#define POSTFIXOP 730
#define UMINUS 731




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 214 "gram.y"
{
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
}
/* Line 1529 of yacc.c.  */
#line 1046 "gram.h"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


