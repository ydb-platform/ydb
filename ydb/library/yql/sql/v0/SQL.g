grammar SQL;

options {
    language = Cpp;
    k = 4;
    memoize=true;
}

// Input is a list of statements.
sql_stmt_list: sql_stmt (SEMI sql_stmt)* SEMI? EOF;

lambda_body: (lambda_stmt SEMI)* RETURN expr SEMI?;
lambda_stmt:
    named_nodes_stmt
  | import_stmt
;

sql_stmt: (EXPLAIN (Q U E R Y PLAN)?)? sql_stmt_core;

sql_stmt_core:
    pragma_stmt
  | select_stmt
  | named_nodes_stmt
  | create_table_stmt
  | drop_table_stmt
  | use_stmt
  | into_table_stmt
  | commit_stmt
  | update_stmt
  | delete_stmt
  | rollback_stmt
  | declare_stmt
  | import_stmt
  | export_stmt
  | alter_table_stmt
  | do_stmt
  | define_action_or_subquery_stmt
  | evaluate_if_stmt
  | evaluate_for_stmt
;

expr: or_subexpr (OR or_subexpr)*;

or_subexpr: and_subexpr (AND and_subexpr)*;

and_subexpr: xor_subexpr (XOR xor_subexpr)*;

xor_subexpr: eq_subexpr cond_expr?;

cond_expr:
    NOT? match_op eq_subexpr (ESCAPE eq_subexpr)?
  | NOT? IN COMPACT? in_expr
  | (ISNULL | NOTNULL | IS NULL | (IS)? NOT NULL)
  | NOT? BETWEEN eq_subexpr AND eq_subexpr
  | ((EQUALS | EQUALS2 | NOT_EQUALS | NOT_EQUALS2) eq_subexpr)+ /* order of the eq subexpressions is reversed! */
;

match_op: LIKE | ILIKE | GLOB | REGEXP | RLIKE | MATCH;

eq_subexpr: neq_subexpr ((LESS | LESS_OR_EQ | GREATER | GREATER_OR_EQ) neq_subexpr)*;

neq_subexpr: bit_subexpr ((SHIFT_LEFT | SHIFT_RIGHT | ROT_LEFT | ROT_RIGHT | AMPERSAND | PIPE | CARET) bit_subexpr)* (DOUBLE_QUESTION neq_subexpr)?;

bit_subexpr: add_subexpr ((PLUS | MINUS) add_subexpr)*;

add_subexpr: mul_subexpr ((ASTERISK | SLASH | PERCENT) mul_subexpr)*;

mul_subexpr: con_subexpr (DOUBLE_PIPE con_subexpr)*;

con_subexpr: unary_subexpr | unary_op unary_subexpr;

unary_op: PLUS | MINUS | TILDA | NOT;

unary_subexpr:    (id_expr    | atom_expr   ) key_expr* (DOT (bind_parameter | DIGITS | id_or_string) key_expr*)* (COLLATE id)?;

in_unary_subexpr: (in_id_expr | in_atom_expr) key_expr* (DOT (bind_parameter | DIGITS | id_or_string) key_expr*)* (COLLATE id)?;

atom_expr:
    literal_value
  | bind_parameter
  | window_function
  | lambda
  | cast_expr
  | exists_expr
  | case_expr
  | id_or_string NAMESPACE id_or_string
  | bitcast_expr
;

in_atom_expr:
    literal_value
  | bind_parameter
  | in_window_function
  | smart_parenthesis
  | cast_expr
  | case_expr
  | LPAREN select_stmt RPAREN
  | bitcast_expr
;

cast_expr: CAST LPAREN expr AS type_name RPAREN;

bitcast_expr: BITCAST LPAREN expr AS type_name RPAREN;

exists_expr: EXISTS LPAREN select_stmt RPAREN;

case_expr: CASE expr? when_expr+ (ELSE expr)? END;

lambda: smart_parenthesis (ARROW LBRACE_CURLY lambda_body RBRACE_CURLY)?;

in_expr: in_unary_subexpr;

// struct, tuple or named list
smart_parenthesis: LPAREN named_expr_list? COMMA? RPAREN;

expr_list: expr (COMMA expr)*;

pure_column_list: LPAREN id_or_string (COMMA id_or_string)* RPAREN;

pure_column_or_named: bind_parameter | id_or_string;
pure_column_or_named_list: LPAREN pure_column_or_named (COMMA pure_column_or_named)* RPAREN;

column_name: opt_id_prefix id_or_string;

column_list: column_name (COMMA column_name)*;

named_expr: expr (AS id_or_string)?;

named_expr_list: named_expr (COMMA named_expr)*;

named_column: column_name (AS id_or_string)?;

named_column_list: named_column (COMMA named_column)*;

call_expr: ((id_or_string NAMESPACE id_or_string) | id_expr | bind_parameter) LPAREN (opt_set_quantifier named_expr_list COMMA? | ASTERISK)? RPAREN;

in_call_expr: ((id_or_string NAMESPACE id_or_string) | in_id_expr | bind_parameter) LPAREN (opt_set_quantifier named_expr_list COMMA? | ASTERISK)? RPAREN;

key_expr: LBRACE_CURLY expr RBRACE_CURLY;

when_expr: WHEN expr THEN expr;

literal_value:
    integer
  | real
  | STRING
  | BLOB // it's unused right now
  | NULL
  | CURRENT_TIME // it's unused right now
  | CURRENT_DATE // it's unused right now
  | CURRENT_TIMESTAMP // it's unused right now
  | bool_value
  | EMPTY_ACTION
;

bind_parameter: DOLLAR id;

bind_parameter_list: bind_parameter (COMMA bind_parameter)*;
named_bind_parameter: bind_parameter (AS bind_parameter)?;
named_bind_parameter_list: named_bind_parameter (COMMA named_bind_parameter)*;

unsigned_number: integer | real;
signed_number: (PLUS | MINUS)? (integer | real);
type_name: id (LPAREN integer (COMMA integer)? RPAREN)?;

flex_type: STRING | type_name;

declare_stmt: DECLARE bind_parameter AS flex_type (EQUALS literal_value)?;

module_path: DOT? id (DOT id)*;
import_stmt: IMPORT module_path SYMBOLS named_bind_parameter_list;
export_stmt: EXPORT bind_parameter_list;

do_stmt: DO (bind_parameter | EMPTY_ACTION) LPAREN expr_list? RPAREN;
pragma_stmt: PRAGMA opt_id_prefix id_or_string (EQUALS pragma_value | LPAREN pragma_value (COMMA pragma_value)* RPAREN)?;

pragma_value:
    signed_number
  | id
  | STRING
  | bool_value
  | bind_parameter
;

/// TODO: NULLS FIRST\LAST?
sort_specification: expr (ASC | DESC)?;

sort_specification_list: sort_specification (COMMA sort_specification)*;

select_stmt: select_kind_parenthesis (select_op select_kind_parenthesis)*;

select_kind_parenthesis: select_kind_partial | LPAREN select_kind_partial RPAREN;

select_op: UNION (ALL)? | INTERSECT | EXCEPT;

select_kind_partial: select_kind
  (LIMIT expr ((OFFSET | COMMA) expr)?)?
  ;

select_kind: (DISCARD)? (process_core | reduce_core | select_core) (INTO RESULT pure_column_or_named)?;

process_core:
  PROCESS STREAM? named_single_source (COMMA named_single_source)* (USING call_expr (AS id_or_string)?
  (WHERE expr)? (HAVING expr)?)?
;

reduce_core:
  REDUCE named_single_source (COMMA named_single_source)* (PRESORT sort_specification_list)?
  ON column_list USING ALL? call_expr (AS id_or_string)?
  (WHERE expr)? (HAVING expr)?
;

opt_set_quantifier: (ALL | DISTINCT)?;

select_core:
  (FROM join_source)? SELECT STREAM? opt_set_quantifier result_column (COMMA result_column)* (WITHOUT column_list)? (FROM join_source)? (WHERE expr)?
  group_by_clause? (HAVING expr)? window_clause? order_by_clause?
;

order_by_clause: ORDER BY sort_specification_list;

group_by_clause: GROUP BY opt_set_quantifier grouping_element_list;

grouping_element_list: grouping_element (COMMA grouping_element)*;

grouping_element:
    ordinary_grouping_set
  | rollup_list
  | cube_list
  | grouping_sets_specification
//empty_grouping_set inside smart_parenthesis
  | hopping_window_specification
;

/// expect column (named column), or parenthesis list columns, or expression (named expression), or list expression
ordinary_grouping_set: named_expr;
ordinary_grouping_set_list: ordinary_grouping_set (COMMA ordinary_grouping_set)*;

rollup_list: ROLLUP LPAREN ordinary_grouping_set_list RPAREN;
cube_list: CUBE LPAREN ordinary_grouping_set_list RPAREN;

/// SQL2003 grouping_set_list == grouping_element_list
grouping_sets_specification: GROUPING SETS LPAREN grouping_element_list RPAREN;

hopping_window_specification: HOP LPAREN expr COMMA expr COMMA expr COMMA expr RPAREN;

result_column:
    opt_id_prefix ASTERISK
  | expr (AS id_or_string)?
;

join_source: flatten_source (join_op flatten_source join_constraint?)*;

ordinary_named_column_list:
    named_column
  | LPAREN named_column_list RPAREN
;

flatten_source: named_single_source (FLATTEN ((OPTIONAL|LIST|DICT)? BY ordinary_named_column_list | COLUMNS))?;

named_single_source: single_source (AS id_or_string)? (sample_clause | tablesample_clause)?;

single_source:
    table_ref
  | LPAREN select_stmt RPAREN
  | AT? bind_parameter (LPAREN expr_list? RPAREN)?
;

sample_clause: SAMPLE expr;

tablesample_clause: TABLESAMPLE sampling_mode LPAREN expr RPAREN repeatable_clause?;

sampling_mode: (BERNOULLI | SYSTEM);

repeatable_clause: REPEATABLE LPAREN expr RPAREN;

join_op:
    COMMA
  | (NATURAL)? ((LEFT (ONLY | SEMI_JOIN)? | RIGHT (ONLY | SEMI_JOIN)? | EXCLUSION | FULL)? (OUTER)? | INNER | CROSS) JOIN
;

join_constraint:
    ON expr
  | USING pure_column_or_named_list
;

into_table_stmt: (INSERT | INSERT OR ABORT | INSERT OR REVERT | INSERT OR IGNORE | UPSERT | REPLACE) INTO into_simple_table_ref into_values_source;

into_values_source:
    pure_column_list? values_source
  | DEFAULT VALUES
;

values_source: VALUES values_source_row_list | select_stmt;
values_source_row_list: values_source_row (COMMA values_source_row)*;
values_source_row: LPAREN expr_list RPAREN;

simple_values_source: expr_list | select_stmt;

create_table_stmt: CREATE TABLE simple_table_ref LPAREN create_table_entry (COMMA create_table_entry)* RPAREN;
create_table_entry: column_schema | table_constraint;

alter_table_stmt: ALTER TABLE simple_table_ref alter_table_action;
alter_table_action: alter_table_add_column | alter_table_drop_column;
alter_table_add_column: ADD COLUMN? column_schema (COMMA ADD COLUMN? column_schema)*;
alter_table_drop_column: DROP COLUMN? id;

column_schema: id_schema flex_type (NOT? NULL)?;
column_order_by_specification: id (ASC | DESC)?;

table_constraint:
    PRIMARY KEY LPAREN id (COMMA id)* RPAREN
  | PARTITION BY LPAREN id (COMMA id)* RPAREN
  | ORDER BY LPAREN column_order_by_specification (COMMA column_order_by_specification)* RPAREN
;

drop_table_stmt: DROP TABLE (IF EXISTS)? simple_table_ref;
define_action_or_subquery_stmt: DEFINE (ACTION|SUBQUERY) bind_parameter LPAREN bind_parameter_list? RPAREN AS define_action_or_subquery_body END DEFINE;
define_action_or_subquery_body: (sql_stmt_core SEMI)* SEMI?;

evaluate_if_stmt: EVALUATE IF expr do_stmt (ELSE do_stmt)?;
evaluate_for_stmt: EVALUATE FOR bind_parameter IN expr do_stmt (ELSE do_stmt)?;

table_ref: opt_id_prefix (table_key | id_expr LPAREN table_arg (COMMA table_arg)* RPAREN) table_hints?;
table_key: id_table_or_at (COLON id_or_string)?;
table_arg: AT? expr (COLON id_or_string)?;
table_hints: WITH (id_or_string | pure_column_list);

simple_table_ref: ((opt_id_prefix id_or_at) | AT? bind_parameter) table_hints?;
into_simple_table_ref: simple_table_ref (ERASE BY pure_column_list)?;

delete_stmt: DELETE FROM simple_table_ref (WHERE expr | ON into_values_source)?;
update_stmt: UPDATE simple_table_ref (SET set_clause_choice (WHERE expr)? | ON into_values_source);

/// out of 2003 standart
set_clause_choice: set_clause_list | multiple_column_assignment;

set_clause_list: set_clause (COMMA set_clause)*;
set_clause: set_target EQUALS expr;
set_target: column_name;
multiple_column_assignment: set_target_list EQUALS LPAREN simple_values_source RPAREN;
set_target_list: LPAREN set_target (COMMA set_target)* RPAREN;

/// window function supp
// differ from 2003 for resolve conflict
window_function: call_expr (null_treatment? OVER window_name_or_specification)?;

in_window_function: in_call_expr (null_treatment? OVER window_name_or_specification)?;

null_treatment: RESPECT NULLS | IGNORE NULLS;

window_name_or_specification: window_name | in_line_window_specification;

in_line_window_specification: window_specification;

window_name: id;

window_clause: WINDOW window_definition_list;

window_definition_list: window_definition (COMMA window_definition)*;

window_definition: new_window_name AS window_specification;

new_window_name: window_name;

window_specification: LPAREN window_specification_details RPAREN;

window_specification_details:
    existing_window_name?
    window_partition_clause?
    window_order_clause?
    window_frame_clause?
;

existing_window_name: window_name;
window_partition_clause: PARTITION BY named_expr_list;
window_order_clause: order_by_clause;

window_frame_clause: window_frame_units window_frame_extent window_frame_exclusion?;
window_frame_units: ROWS | RANGE;

window_frame_extent: window_frame_start | window_frame_between;

window_frame_start:
    UNBOUNDED PRECEDING
  | window_frame_preceding
  | CURRENT ROW
;

window_frame_preceding: unsigned_number PRECEDING;
window_frame_following: unsigned_number FOLLOWING;
window_frame_between: BETWEEN window_frame_bound AND window_frame_bound;
window_frame_bound:
    window_frame_start
  | UNBOUNDED FOLLOWING
  | window_frame_following
;

window_frame_exclusion: EXCLUDE CURRENT ROW | EXCLUDE GROUP | EXCLUDE TIES | EXCLUDE NO OTHERS;

// EXTRAS
use_stmt: USE id_or_string;

named_nodes_stmt: bind_parameter_list EQUALS (expr | LPAREN select_stmt RPAREN);

commit_stmt: COMMIT;

rollback_stmt: ROLLBACK;

// Special rules that allow to use certain keywords as identifiers.

id_or_string: IDENTIFIER | STRING | keyword;
id: IDENTIFIER | keyword;
id_schema: IDENTIFIER | keyword_restricted;
id_expr: IDENTIFIER | keyword_compat | keyword_alter_uncompat | keyword_in_uncompat;
in_id_expr: IDENTIFIER | keyword_compat | keyword_alter_uncompat;
id_table: IDENTIFIER | keyword_restricted;
id_table_or_at: AT? id_table;
id_or_at: AT? id;

opt_id_prefix: (id_or_string DOT)?;

keyword: keyword_restricted | keyword_alter_uncompat | keyword_table_uncompat;
keyword_restricted: keyword_compat | keyword_expr_uncompat | keyword_select_uncompat | keyword_in_uncompat;

keyword_expr_uncompat:
    BITCAST
  | CASE
  | CAST
  | CUBE
  | CURRENT_TIME
  | CURRENT_DATE
  | CURRENT_TIMESTAMP
  | EMPTY_ACTION
  | EXISTS
  | FROM
  | FULL
  | HOP
  | NOT
  | NULL
  | PROCESS
  | REDUCE
  | RETURN
  | ROLLUP
  | SELECT
  | WHEN
  | WHERE
;

keyword_table_uncompat:
    ERASE
  | STREAM
;

keyword_select_uncompat:
    ALL
  | AS
  | DISTINCT
  | HAVING
;

keyword_alter_uncompat:
    COLUMN
;

keyword_in_uncompat:
    COMPACT
;

keyword_compat: (
    ABORT
  | ACTION
  | ADD
  | AFTER
  | ALTER
  | ANALYZE
  | AND
  | ASC
  | ATTACH
  | AUTOINCREMENT
  | BEFORE
  | BEGIN
  | BERNOULLI
  | BETWEEN
  | BY
  | CASCADE
  | CHECK
  | COLLATE
  | COLUMNS
  | COMMIT
  | CONFLICT
  | CONSTRAINT
  | CREATE
  | CROSS
  | CURRENT
  | DATABASE
  | DECLARE
  | DEFAULT
  | DEFERRABLE
  | DEFERRED
  | DEFINE
  | DELETE
  | DESC
  | DETACH
  | DICT
  | DISCARD
  | DO
  | DROP
  | EACH
  | ELSE
  | END
  | ESCAPE
  | EVALUATE
  | EXCEPT
  | EXCLUDE
  | EXCLUSIVE
  | EXCLUSION
  | EXPLAIN
  | EXPORT
  | FAIL
  | FLATTEN
  | FOLLOWING
  | FOR
  | FOREIGN
  | GLOB
  | GROUP
  | GROUPING
  | IF
  | IGNORE
  | ILIKE
  | IMMEDIATE
  | IMPORT
  | IN
  | INDEX
  | INDEXED
  | INITIALLY
  | INNER
  | INSERT
  | INSTEAD
  | INTERSECT
  | INTO
  | IS
  | ISNULL
  | JOIN
  | KEY
  | LEFT
  | LIKE
  | LIMIT
  | LIST
  | MATCH
  | NATURAL
  | NULLS
  | NO
  | NOTNULL
  | OF
  | OFFSET
  | ON
  | ONLY
  | OPTIONAL
  | OR
  | ORDER
  | OTHERS
  | OUTER
  | OVER
  | PLAN
  | PARTITION
  | PRAGMA
  | PRECEDING
  | PRESORT
  | PRIMARY
//  | QUERY
  | RAISE
  | RANGE
  | REFERENCES
  | REGEXP
  | REINDEX
  | RELEASE
  | RENAME
  | REPLACE
  | RESPECT
  | RESTRICT
  | RESULT
  | REVERT
  | RIGHT
  | RLIKE
  | ROLLBACK
  | ROW
  | ROWS
  | SAMPLE
  | SAVEPOINT
  | SEMI_JOIN
  | SET
  | SETS
  | SUBQUERY
  | SYMBOLS
  | SYSTEM
  | TABLE
  | TABLESAMPLE
  | TEMPORARY
  | TIES
  | THEN
  | TO
  | TRANSACTION
  | TRIGGER
  | UNBOUNDED
  | UNION
  | UNIQUE
  | UPDATE
  | UPSERT
  | USING
  | VACUUM
  | VALUES
  | VIEW
  | VIRTUAL
  | WINDOW
  | WITH
  | WITHOUT
  | XOR
  );

bool_value: (TRUE | FALSE);

//
// Lexer
//

EQUALS:        '=';
EQUALS2:       '==';
NOT_EQUALS:    '!=';
NOT_EQUALS2:   '<>';
LESS:          '<';
LESS_OR_EQ:    '<=';
GREATER:       '>';
GREATER_OR_EQ: '>=';
SHIFT_LEFT:    '<<';
SHIFT_RIGHT:   '>>';
ROT_LEFT:      '|<<';
ROT_RIGHT:     '>>|';
AMPERSAND:     '&';
PIPE:          '|';
DOUBLE_PIPE:   '||';
PLUS:          '+';
MINUS:         '-';
TILDA:         '~';
ASTERISK:      '*';
SLASH:         '/';
BACKSLASH:     '\\';
PERCENT:       '%';
SEMI:          ';';
DOT:           '.';
COMMA:         ',';
LPAREN:        '(';
RPAREN:        ')';
QUESTION:      '?';
DOUBLE_QUESTION: '??';
COLON:         ':';
AT:            '@';
DOUBLE_AT:     '@@';
DOLLAR:        '$';
QUOTE_DOUBLE:  '"'; // This comment for fix syntax highlighting "
QUOTE_SINGLE:  '\'';
APOSTROPHE:    '`';
LBRACE_CURLY:  '{';
RBRACE_CURLY:  '}';
UNDERSCORE:    '_';
CARET:         '^';
NAMESPACE:     '::';
ARROW:         '->';
RBRACE_SQUARE: ']';
LBRACE_SQUARE: '['; // pair ]

// http://www.antlr.org/wiki/pages/viewpage.action?pageId=1782
fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');

ABORT: A B O R T;
ACTION: A C T I O N;
ADD: A D D;
AFTER: A F T E R;
ALL: A L L;
ALTER: A L T E R;
ANALYZE: A N A L Y Z E;
AND: A N D;
AS: A S;
ASC: A S C;
ATTACH: A T T A C H;
AUTOINCREMENT: A U T O I N C R E M E N T;
BEFORE: B E F O R E;
BEGIN: B E G I N;
BERNOULLI: B E R N O U L L I;
BETWEEN: B E T W E E N;
BITCAST: B I T C A S T;
BY: B Y;
CASCADE: C A S C A D E;
CASE: C A S E;
CAST: C A S T;
CHECK: C H E C K;
COLLATE: C O L L A T E;
COLUMN: C O L U M N;
COLUMNS: C O L U M N S;
COMMIT: C O M M I T;
COMPACT: C O M P A C T;
CONFLICT: C O N F L I C T;
CONSTRAINT: C O N S T R A I N T;
CREATE: C R E A T E;
CROSS: C R O S S;
CUBE: C U B E;
CURRENT: C U R R E N T;
CURRENT_TIME: C U R R E N T '_' T I M E;
CURRENT_DATE: C U R R E N T '_' D A T E;
CURRENT_TIMESTAMP: C U R R E N T '_' T I M E S T A M P;
DATABASE: D A T A B A S E;
DECLARE: D E C L A R E;
DEFAULT: D E F A U L T;
DEFERRABLE: D E F E R R A B L E;
DEFERRED: D E F E R R E D;
DEFINE: D E F I N E;
DELETE: D E L E T E;
DESC: D E S C;
DETACH: D E T A C H;
DICT: D I C T;
DISCARD: D I S C A R D;
DISTINCT: D I S T I N C T;
DO: D O;
DROP: D R O P;
EACH: E A C H;
ELSE: E L S E;
EMPTY_ACTION: E M P T Y '_' A C T I O N;
END: E N D;
ERASE: E R A S E;
ESCAPE: E S C A P E;
EVALUATE: E V A L U A T E;
EXCEPT: E X C E P T;
EXCLUDE: E X C L U D E;
EXCLUSIVE: E X C L U S I V E;
EXCLUSION: E X C L U S I O N;
EXISTS: E X I S T S;
EXPLAIN: E X P L A I N;
EXPORT: E X P O R T;
FAIL: F A I L;
FLATTEN: F L A T T E N;
FOLLOWING: F O L L O W I N G;
FOR: F O R;
FOREIGN: F O R E I G N;
FROM: F R O M;
FULL: F U L L;
GLOB: G L O B;
GROUP: G R O U P;
GROUPING: G R O U P I N G;
HAVING: H A V I N G;
HOP: H O P;
IF: I F;
IGNORE: I G N O R E;
ILIKE: I L I K E;
IMMEDIATE: I M M E D I A T E;
IMPORT: I M P O R T;
IN: I N;
INDEX: I N D E X;
INDEXED: I N D E X E D;
INITIALLY: I N I T I A L L Y;
INNER: I N N E R;
INSERT: I N S E R T;
INSTEAD: I N S T E A D;
INTERSECT: I N T E R S E C T;
INTO: I N T O;
IS: I S;
ISNULL: I S N U L L;
JOIN: J O I N;
KEY: K E Y;
LEFT: L E F T;
LIKE: L I K E;
LIMIT: L I M I T;
LIST: L I S T;
MATCH: M A T C H;
NATURAL: N A T U R A L;
NO: N O;
NOT: N O T;
NOTNULL: N O T N U L L;
NULL: N U L L;
NULLS: N U L L S;
OF: O F;
OFFSET: O F F S E T;
ON: O N;
ONLY: O N L Y;
OPTIONAL: O P T I O N A L;
OR: O R;
ORDER: O R D E R;
OTHERS: O T H E R S;
OUTER: O U T E R;
OVER: O V E R;
PARTITION: P A R T I T I O N;
PLAN: P L A N;
PRAGMA: P R A G M A;
PRECEDING: P R E C E D I N G;
PRESORT: P R E S O R T;
PRIMARY: P R I M A R Y;
PROCESS: P R O C E S S;
//QUERY: Q U E R Y;
RAISE: R A I S E;
RANGE: R A N G E;
REDUCE: R E D U C E;
REFERENCES: R E F E R E N C E S;
REGEXP: R E G E X P;
REINDEX: R E I N D E X;
RELEASE: R E L E A S E;
RENAME: R E N A M E;
REPEATABLE: R E P E A T A B L E;
REPLACE: R E P L A C E;
RESPECT: R E S P E C T;
RESTRICT: R E S T R I C T;
RESULT: R E S U L T;
RETURN: R E T U R N;
REVERT: R E V E R T;
RIGHT: R I G H T;
RLIKE: R L I K E;
ROLLBACK: R O L L B A C K;
ROLLUP: R O L L U P;
ROW: R O W;
ROWS: R O W S;
SAMPLE: S A M P L E;
SAVEPOINT: S A V E P O I N T;
SELECT: S E L E C T;
SEMI_JOIN: S E M I;
SET: S E T;
SETS: S E T S;
SUBQUERY: S U B Q U E R Y;
STREAM: S T R E A M;
SYMBOLS: S Y M B O L S;
SYSTEM: S Y S T E M;
TABLE: T A B L E;
TABLESAMPLE: T A B L E S A M P L E;
TEMPORARY: T E M P ( O R A R Y )?;
THEN: T H E N;
TIES: T I E S;
TO: T O;
TRANSACTION: T R A N S A C T I O N;
TRIGGER: T R I G G E R;
UNBOUNDED: U N B O U N D E D;
UNION: U N I O N;
UNIQUE: U N I Q U E;
UPDATE: U P D A T E;
UPSERT: U P S E R T;
USE: U S E;
USING: U S I N G;
VACUUM: V A C U U M;
VALUES: V A L U E S;
VIEW: V I E W;
VIRTUAL: V I R T U A L;
WHEN: W H E N;
WHERE: W H E R E;
WINDOW: W I N D O W;
WITH: W I T H;
WITHOUT: W I T H O U T;
XOR: X O R;
TRUE: T R U E;
FALSE: F A L S E;

fragment STRING_CORE_SINGLE: ( ~(QUOTE_SINGLE | BACKSLASH) | (BACKSLASH .) )*;
fragment STRING_CORE_DOUBLE: ( ~(QUOTE_DOUBLE | BACKSLASH) | (BACKSLASH .) )*;
fragment STRING_SINGLE: (QUOTE_SINGLE STRING_CORE_SINGLE QUOTE_SINGLE);
fragment STRING_DOUBLE: (QUOTE_DOUBLE STRING_CORE_DOUBLE QUOTE_DOUBLE);
fragment STRING_MULTILINE: (DOUBLE_AT .* DOUBLE_AT)+ AT?;

STRING: (STRING_SINGLE | STRING_DOUBLE | STRING_MULTILINE);

fragment ID_START: ('a'..'z' | 'A'..'Z' | UNDERSCORE);
fragment ID_CORE: (ID_START | DIGIT | DOLLAR);
fragment ID_PLAIN: ID_START (ID_CORE)*;


fragment ID_QUOTED_ESCAPE_APOSTROPHE: BACKSLASH APOSTROPHE;
fragment ID_QUOTED_ESCAPE_SQUARE: BACKSLASH (LBRACE_SQUARE | RBRACE_SQUARE);
fragment ID_QUOTED_CORE_SQUARE: (~(LBRACE_SQUARE | RBRACE_SQUARE) | ID_QUOTED_ESCAPE_SQUARE)*;
fragment ID_QUOTED_CORE_APOSTROPHE: (~APOSTROPHE | ID_QUOTED_ESCAPE_APOSTROPHE)*;
fragment ID_QUOTED_SQUARE: LBRACE_SQUARE ID_QUOTED_CORE_SQUARE RBRACE_SQUARE;
fragment ID_QUOTED_APOSTROPHE: APOSTROPHE ID_QUOTED_CORE_APOSTROPHE APOSTROPHE;
fragment ID_QUOTED: ID_QUOTED_SQUARE | ID_QUOTED_APOSTROPHE;

IDENTIFIER: ID_PLAIN | ID_QUOTED;

fragment DIGIT: '0'..'9';
fragment HEXDIGIT: '0'..'9' | 'a'..'f' | 'A'..'F';
fragment HEXDIGITS: '0' X HEXDIGIT+;
fragment OCTDIGITS: '0' O ('0'..'8')+;
fragment BINDIGITS: '0' B ('0' | '1')+;
fragment DECDIGITS: DIGIT+;
DIGITS: DECDIGITS | HEXDIGITS | OCTDIGITS | BINDIGITS;

INTEGER: DIGITS (U? (L | S | T)?);
LEGACY_TINY_INTEGER: DIGITS (U? B?);
integer: DIGITS | INTEGER | LEGACY_TINY_INTEGER;

fragment FLOAT_EXP : E (PLUS | MINUS)? DECDIGITS ;
REAL:
      DECDIGITS DOT DIGIT* FLOAT_EXP? F?
    |   DECDIGITS FLOAT_EXP F?
//    |   DOT DECDIGITS FLOAT_EXP?    // Conflicts with tuple element access through DOT
    ;

real: REAL;

BLOB: X QUOTE_SINGLE HEXDIGIT+ QUOTE_SINGLE;

fragment MULTILINE_COMMENT: '/*' ( options {greedy=false;} : . )* '*/';
fragment LINE_COMMENT: '--' ~('\n'|'\r')* ('\n' | '\r' | EOF);
WS: (' '|'\r'|'\t'|'\u000C'|'\n') {$channel=HIDDEN;};
COMMENT: (MULTILINE_COMMENT|LINE_COMMENT) {$channel=HIDDEN;};
