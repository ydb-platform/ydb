%{
/* 
 * Legal Notice 
 * 
 * This document and associated source code (the "Work") is a part of a 
 * benchmark specification maintained by the TPC. 
 * 
 * The TPC reserves all right, title, and interest to the Work as provided 
 * under U.S. and international laws, including without limitation all patent 
 * and trademark rights therein. 
 * 
 * No Warranty 
 * 
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
 *     WITH REGARD TO THE WORK. 
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
 * 
 * Contributors:
 * Gradient Systems
 */
#include "config.h"
#include "porting.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef WIN32
#include <io.h>
#pragma warning(disable: 4305)
#endif

#include "StringBuffer.h"
#include "expr.h"
#include "grammar_support.h"
#include "keywords.h"
#include "substitution.h"
#include "error_msg.h"
#include "tables.h"
#include "qgen_params.h"
#include "tdefs.h"
#include "query_handler.h"
#include "list.h"
#include "dist.h"
#include "scaling.h"


#define DEBUG0(l, fmt)                 if (l <= verbose) printf(fmt)
#define DEBUG1(l, fmt, arg)            if (l <= verbose) printf(fmt, arg)
#define DEBUG2(l, fmt, arg, arg2)    if (l <= verbose) printf(fmt, arg, arg2)

extern file_ref_t file_stack[];

int yylex(void);

#ifdef WIN32
int yyparse(void);
#endif

/* GLOBAL VARIABLES */
int verbose = 0,
    j,
    nRetCode;
ds_key_t i;
char tmpstr[128];
segment_t *pSegment;
substitution_t *pSub;
%}

%union {
    int        intval;
    char    *strval;
    expr_t    *exprval;
    list_t  *list;
    }

%token <intval> TOK_INT
%token <strval> TOK_LITERAL
%token <strval> TOK_PATH
%token <intval> TOK_DECIMAL
%token <strval> TOK_ID
%token <strval> TOK_SQL
%token KW_DEFINE
%token KW_RANDOM
%token KW_UNIFORM
%token KW_RANGE
%token KW_DATE
%token KW_INCLUDE
%token KW_TEXT
%token KW_DIST
%token KW_LIST
%token KW_ROWCOUNT
%token KW_BEGIN
%token KW_END
%token KW_SALES
%token KW_RETURNS
%token KW_DISTMEMBER
%token KW_DISTWEIGHT
%token KW_QUERY
%token KW_STREAM
%token KW_TEMPLATE
%token KW_SEED
%token KW_SCALE
%token KW_SCALE_STEP
%token KW_SET
%token KW_ADD
%token KW_NAMES
%token KW_TYPES
%token KW_WEIGHTS
%token KW_INT
%token KW_VARCHAR
%token KW_DECIMAL
%token KW_LIMIT
%token KW_LIMITA
%token KW_LIMITB
%token KW_LIMITC
%token KW_ULIST
%type <list> comma_expr_list
%type <list> dist_expr_list
%type <exprval> expr
%type <exprval> function_call
%type <exprval> arithmetic_expr
%type <intval> dist_function_name
%type <intval> function_name
%type <intval> keyword_expr
%type <intval> keyword_value
%type <strval> path
%type <intval> opt_substitution_suffix
%type <exprval> replacement
%type <list> replacement_list
%type <intval> statement

%left WITH
%left '+' '-' '%'
%left '*' '/' '^'
%right '['
%nonassoc UMINUS
%left TCAST

%%

workload_spec:        statement_list    
                        {
                            AddQuerySegment(pCurrentQuery, "\n");
                            AddQuerySubstitution(pCurrentQuery, "_END", 0, 0);
                            AddQuerySegment(pCurrentQuery, "\n");
                        }
    ;

statement_list:    statement
    |    statement_list  statement
                        {
                            if (is_set("DEBUG"))
                            {
                                if ($2 != KW_DEFINE)
                                {
                                    printf("STATUS: parsed %s statement at line %d\n", 
                                        ($2 == TOK_SQL)?"SQL":KeywordText($2), pCurrentFile->line_number);
                                }
                            }
                        }
    ;

statement:    include_statement    {$$ = KW_INCLUDE; }
    |        define_statement    {$$ = KW_DEFINE; }
    |        query_statement        {$$ = TOK_SQL; }
    |        dist_statement        {$$ = KW_DIST; }
    ;
    

/*=====================================================================
/* generic include syntax, should be identical to standard UNIX rules. */
/* this will need to be revisited to port the product to PC platforms */
/**/
include_statement:    '#' KW_INCLUDE path
                    {
                    if (include_file($3, pCurrentQuery) < 0)
                        yyerror("File include failed");
                    }
    |    '#' KW_INCLUDE '<' path '>'
                    {
                    yywarn("System include not supported; using relative pathing");
                    if (include_file($4, pCurrentQuery) < 0)
                        yyerror("File include failed");
                    }
    ;
/*======================================================================*/

/*=====================================================================*/
/* dist statement: create a new distribution  */
dist_statement:        KW_DIST TOK_ID ';' dist_argument_list dist_members_list
    ;

dist_argument_list:    dist_argument
    |                dist_argument_list dist_argument
    ;

dist_argument:        KW_SET KW_WEIGHTS '=' TOK_INT ';'
    |                KW_SET KW_TYPES '=' '(' dist_type_list ')' ';'
    |                KW_SET KW_NAMES '=' '(' dist_name_list ':' dist_name_list ')' ';'
    ;

dist_type:            KW_INT
    |                KW_VARCHAR
    |                KW_DECIMAL
    ;

dist_type_list:
    |                dist_type
    |                dist_type_list ',' dist_type
    ;

dist_name_list:        TOK_ID
    |                dist_name_list ',' TOK_ID
    ;

dist_members_list:    dist_member
    |                dist_members_list dist_member
    ;

dist_member:        KW_ADD '(' dist_value_list ':' dist_weight_list ')' ';'
    ;

dist_value:            TOK_INT
    |                TOK_LITERAL
    |                TOK_DECIMAL
    ;

dist_value_list:    dist_value
    |                dist_value_list ',' dist_value
    ;

dist_weight_list:    TOK_INT
    |                dist_value_list ',' TOK_INT
    ;
/*======================================================================*/


/*=====================================================================*/
/* define statement: define a substitiution type                                */
define_statement:    KW_DEFINE TOK_ID '=' expr ';'
                        {
                        defineSubstitution(pCurrentQuery, $2, $4);
                        if (is_set("DEBUG"))
                            printf("STATUS: DEFINED %s\n", $2);
                        }
    |                KW_DEFINE KW_LIMIT '=' TOK_INT ';'
                        {
                        pSub = findSubstitution(pCurrentQuery, "_LIMIT", 0);
                        sprintf(tmpstr, "%d", $4);
                        ResetBuffer(pSub->pAssignment->Value.pBuf);
                        AddBuffer(pSub->pAssignment->Value.pBuf, tmpstr);
                        if (is_set("DEBUG"))
                            printf("STATUS: SET LIMIT\n");
                        }
    ;


comma_expr_list:    expr
                        {
                        $$ = makeList(L_FL_TAIL, NULL);
                        addList($$, $1);
                        }            
    |                comma_expr_list ',' expr
                        {
                        addList($1, $3);
                        $$ = $1;
                        }
    ;

expr:                TOK_LITERAL    
                        {
                        $$ = MakeStringConstant($1);
                        }
    |                TOK_INT
                        {
                        $$ = MakeIntConstant($1);
                        }
    |                function_call
    |                keyword_expr
                        {
                        $$ = MakeIntConstant($1);
                        $$->nFlags |= EXPR_FL_KEYWORD;
                        }
    |                '[' keyword_value ']'
                        {
                        $$ = getKeywordValue($2);
                        }
    |                '[' TOK_ID TOK_INT opt_substitution_suffix ']'
                        {
                        $$ = MakeVariableReference($2, $3);
                        }
    |                '[' TOK_ID opt_substitution_suffix ']'
                        {
                        $$ = MakeVariableReference($2, $3);
                        }
    |                KW_SCALE
                        {
                        $$ = MakeIntConstant(get_int("SCALE"));
                        }
    |                arithmetic_expr
    ;

function_call:        function_name '(' comma_expr_list ')'
                        {
                        $$ = MakeFunctionCall($1, $3);
                        }
    |                dist_function_name '(' dist_expr_list ')'
                        {
                        $$ = MakeFunctionCall($1, $3);
                        }
    |                KW_TEXT '(' replacement_list ')'
                        {
                        $$ = MakeFunctionCall(KW_TEXT, $3);
                        }
    |                KW_ROWCOUNT '(' TOK_LITERAL ')'
                        {
                        i = GetTableNumber($3);
                        if (i == -1)
                        {
                            i = distsize($3);
                            if (i == -1)
                                ReportError(QERR_BAD_NAME, $3, 1);
                        }
                        else
                            i = getIDCount(i);
                        $$ = MakeIntConstant(i);
                        }
    |                KW_ROWCOUNT '(' TOK_LITERAL ',' TOK_LITERAL ')'
                        {
                        /* TODO: Need to convert this to DSS_HUGE */
                        i = GetTableNumber($3);
                        if (i == -1)
                        {
                            i = distsize($3);
                            if (i == -1)
                                ReportError(QERR_BAD_NAME, $3, 1);
                        }
                        j = GetTableNumber($5);
                        if (i == -1)
                            ReportError(QERR_BAD_NAME, $5, 1);
                        i = (int)getIDCount(i);
                        j = (int)getIDCount(j);
                        $$ = MakeIntConstant((i>j)?j:i);
                        }
    |                KW_SCALE_STEP '(' ')'
                        {
                        $$ = MakeIntConstant(getScaleSlot(get_int("SCALE")) + 1);
                        }
    |                KW_ULIST '(' expr ',' TOK_INT ')'
                        {
                        $$ = MakeListExpr(KW_ULIST, $3, $5);
                        }
    |                KW_LIST '(' expr ',' TOK_INT ')'
                        {
                        $$ = MakeListExpr(KW_LIST, $3, $5);
                        }
    |                KW_RANGE '(' expr ',' TOK_INT ')'
                        {
                        $$ = MakeListExpr(KW_RANGE, $3, $5);
                        }
    ;

arithmetic_expr:    expr '+' expr
                        {
                        $$ = makeArithmeticExpr(OP_ADD, $1, $3);
                        }
    |                expr '-' expr
                        {
                        $$ = makeArithmeticExpr(OP_SUBTRACT, $1, $3);
                        }
    |                expr '*' expr
                        {
                        $$ = makeArithmeticExpr(OP_MULTIPLY, $1, $3);
                        }
    |                expr '/' expr
                        {
                        $$ = makeArithmeticExpr(OP_DIVIDE, $1, $3);
                        }
    ;
dist_expr_list:        expr        
                        {
                        $$ = makeList(L_FL_TAIL, NULL);
                        addList($$, $1);
                        }    
    |                TOK_ID    
                        {
                        $$ = makeList(L_FL_TAIL, NULL);
                        addList($$, MakeStringConstant($1));
                        }    
    |                dist_expr_list ',' expr
                        {
                        addList($1, $3);
                        $$ = $1;
                        }
    |                dist_expr_list ',' TOK_ID
                        {
                        addList($1, MakeStringConstant($3));
                        $$ = $1;
                        }    
    ;

function_name:        KW_DATE        {$$ = KW_DATE;}
    |                        KW_RANDOM    {$$ = KW_RANDOM;}
    ;

dist_function_name:    KW_DIST        {$$ = KW_DIST;}
    |                 KW_DISTMEMBER        {$$ = KW_DISTMEMBER;}
    |                 KW_DISTWEIGHT        {$$ = KW_DISTWEIGHT;}
    ;

keyword_expr:     KW_UNIFORM        {$$ = KW_UNIFORM;}
    |                        KW_SALES        {$$ = KW_SALES;}
    |                        KW_RETURNS        {$$ = KW_RETURNS;}
    ;

keyword_value:                KW_QUERY        {$$ = KW_QUERY;}
    |                   KW_TEMPLATE        {$$ = KW_TEMPLATE;}
    |                   KW_STREAM        {$$ = KW_STREAM;}
    |                   KW_SEED        {$$ = KW_SEED;}
    ;

replacement_list:    replacement    
                        {
                        $$ = makeList(L_FL_TAIL, NULL);
                        addList($$, $1);
                        }
    |                replacement_list ',' replacement
                        {
                        addList($$, $3);
                        $$ = $1;
                        }
    ;

replacement:        '{' TOK_LITERAL ',' TOK_INT '}'
                        {
                        $$ = MakeReplacement($2, $4);
                        }
    ;

/*======================================================================*/


/*=====================================================================
 * query statement: a query statement is the template into which defined
 *        substitutions are placed to make valid SQL syntax
 */
query_statement:        query_component_list ';'
                            {
                            pSegment = getTail(pCurrentQuery->SegmentList);
                            pSegment->flags |= QS_EOS;
                            }

    ;

query_component_list:    substitution
    |                    TOK_SQL
                            {
                            if ((nRetCode = AddQuerySegment(pCurrentQuery, $1)) != 0)
                                yyerror("SQL parse failed");
                            }

    |                    query_component_list substitution
    |                    query_component_list TOK_SQL
                            {
                            if ((nRetCode = AddQuerySegment(pCurrentQuery, $2)) != 0)
                                yyerror("SQL parse failed");
                            }
    ;

substitution:        '[' TOK_ID opt_substitution_suffix ']'
                        {
                            if ((nRetCode = AddQuerySubstitution(pCurrentQuery, $2, 0, $3)) < 0)
                                {
                                sprintf(tmpstr, "Substitution match failed on %s", $2);
                                yyerror(tmpstr);
                                }
                        }
    |                '[' TOK_ID TOK_INT opt_substitution_suffix ']'
                        {
                            if ((nRetCode = AddQuerySubstitution(pCurrentQuery, $2, $3, $4)) < 0)
                                {
                                sprintf(tmpstr, "Substitution match failed on %s", $2);
                                yyerror(tmpstr);
                                }
                        }
    |                '[' KW_QUERY ']'
                        {
                            if ((nRetCode = AddQuerySubstitution(pCurrentQuery, "_QUERY", 0, 0)) < 0)
                                {
                                yyerror("Lookup of predefined constant failed");
                                }
                        }
    |                '[' KW_STREAM ']'
                        {
                            if ((nRetCode = AddQuerySubstitution(pCurrentQuery, "_STREAM", 0, 0)) < 0)
                                {
                                yyerror("Lookup of predefined constant failed");
                                }
                        }
    |                '[' KW_TEMPLATE ']'
                        {
                            if ((nRetCode = AddQuerySubstitution(pCurrentQuery, "_TEMPLATE", 0, 0)) < 0)
                                {
                                yyerror("Lookup of predefined constant failed");
                                }
                        }
    |                '[' KW_SEED ']'
                        {
                            if ((nRetCode = AddQuerySubstitution(pCurrentQuery, "_SEED", 0, 0)) < 0)
                                {
                                yyerror("Lookup of predefined constant failed");
                                }
                        }
    |                '[' KW_LIMITA ']'
                        {
                            if ((nRetCode = AddQuerySubstitution(pCurrentQuery, "_LIMITA", 0, 0)) < 0)
                                {
                                yyerror("Lookup of predefined constant failed");
                                }
                        }
    |                '[' KW_LIMITB ']'
                        {
                            if ((nRetCode = AddQuerySubstitution(pCurrentQuery, "_LIMITB", 0, 0)) < 0)
                                {
                                yyerror("Lookup of predefined constant failed");
                                }
                        }
    |                '[' KW_LIMITC ']'
                        {
                            if ((nRetCode = AddQuerySubstitution(pCurrentQuery, "_LIMITC", 0, 0)) < 0)
                                {
                                yyerror("Lookup of predefined constant failed");
                                }
                        }
    ;

opt_substitution_suffix:        /* */        {$$ = 0;}
    |                        '.' KW_BEGIN    {$$ = 0;}
    |                        '.' KW_END        {$$ = 1;}
    |                        '.' TOK_INT        {$$ = $2;}
    ;

/*======================================================================*/

/*=====================================================================
 * GENERAL ELEMENTS: things used in multiple statement types
 */
path:        TOK_LITERAL    { $$ = $1; }
    ;
/*======================================================================*/

%%


