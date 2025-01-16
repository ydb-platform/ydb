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
/*****
* NOTE: added #ifndef WIN32 around generated include of unistd.h
******/
#ifdef SCAN_DEBUG
#define DECLARER
#endif /* SCAN_DEBUG */
#include "config.h"
#include "porting.h"
#ifdef USE_STRING_H
#include <string.h>
#else
#include <strings.h>
#endif
#include <malloc.h>
#include "keywords.h"
#include "StringBuffer.h"
#include "expr.h"
#include "y.tab.h"
#include "qgen_params.h"
#include "substitution.h"
#include "grammar_support.h"

#ifdef SCAN_DEBUG
#define RETURN(type)    DisplayAction(type)
#include "qgen_params.h"
extern file_ref_t *pCurrentFile = &CurrentFile;
int LN;
workload_t CurrentWorkload;
template_t CurrentQuery;
workload_t *pCurrentWorkload = &CurrentWorkload;
template_t *pCurrentQuery = &CurrentQuery;
YYSTYPE yylval;
#else
#define RETURN(type)    return(type)
#endif

/* need to keep track of state from one call to the next */
/* #define STATE(type)    printf("<STATE %d>\n", type);nState = type */
#ifndef STATE
#define STATE(type)    nState = type
#endif
static int nState = -1;

int nCharCount;
int bFirstWord = 1;
int nKeyword;
static int nOldState;

int yywrap(void);
int yylook(void);

%}

%{ 
/* macro definitions go here */ 
%}
INT    -?[0-9]+
DECIMAL   -?[0-9]*\.[0-9]+
WS    [ \t]+
ID    [_a-zA-Z][a-zA-Z0-9_]*
SUB_ID    [a-zA-Z_]*
%s SQLTEXT SUBST NORMAL
%%
%{
    switch(nState)
    {
    case NORMAL:    BEGIN(NORMAL);break;
    case SUBST:        BEGIN(SUBST);break;
    case SQLTEXT:    BEGIN(SQLTEXT);break;
    default:            BEGIN(NORMAL);break;
    }
%}
<NORMAL>{WS}            nCharCount += strlen(yytext); /* ignore whitespace */
<NORMAL>--[^\n]*        nCharCount += strlen(yytext); /* ignore comments */
<NORMAL>{DECIMAL}    { 
                nCharCount += strlen(yytext); 
                bFirstWord = 0; 
                RETURN(TOK_DECIMAL); 
                }
<NORMAL>{INT}            { 
                nCharCount += strlen(yytext); 
                bFirstWord = 0; 
                yylval.intval = atoi(yytext);
                RETURN(TOK_INT); 
                }
<NORMAL>{ID}            { 
                if ((nKeyword = FindKeyword(yytext)) >= 0)
                    {
                    bFirstWord = 0;
                    RETURN(nKeyword);
                    }
                else
                    if (bFirstWord)
                        {
                        STATE(SQLTEXT);
                        BEGIN(SQLTEXT);
                        yylval.strval = strdup(yytext);
                        return(TOK_SQL);
                        }
                    else
                        {
                        yylval.strval = strdup(yytext);
                        RETURN(TOK_ID); 
                        }
                }
<NORMAL>\"[^\n\"]*\" { 
                nCharCount += strlen(yytext); 
                bFirstWord = 0;
                yytext[yyleng - 1] = '\0';
                yylval.strval = strdup(yytext+1);
                RETURN(TOK_LITERAL); 
                }
<NORMAL>\n            { nCharCount = 1; pCurrentFile->line_number += 1; bFirstWord = 1; }
<NORMAL>"["    {
            nOldState = NORMAL;
                    nCharCount += 1; 
                STATE(SUBST); 
                BEGIN(SUBST); 
                RETURN(yytext[0]); 
            }
<NORMAL>.            { nCharCount += strlen(yytext); 
                bFirstWord = 0; 
                RETURN(yytext[0]); 
            }

<SUBST>{SUB_ID}        {
                if ((nKeyword = FindKeyword(yytext)) >= 0)
                    {
                    RETURN(nKeyword);
                    }
                else
                        {
                        yylval.strval = strdup(yytext);
                        RETURN(TOK_ID); 
                        }
                }

<SUBST>{INT}        {
                    nCharCount += strlen(yytext);
                    yylval.intval = atoi(yytext);
                    return(TOK_INT);
                    }
<SUBST>"."            {RETURN('.');}
<SUBST>"]"            {
                    STATE(nOldState);
                    BEGIN(nOldState);
                    RETURN(yytext[0]);
                    }
<SQLTEXT>[^;\[\]\n]*\n    {
                        nCharCount += strlen(yytext); 
                        yylval.strval = strdup(yytext);
                        nCharCount = 1; pCurrentFile->line_number += 1;
                        if (strlen(yylval.strval))
                            RETURN(TOK_SQL); 
                        }
<SQLTEXT>[^;\[\]\n]*    {
                        nCharCount += strlen(yytext); 
                        yylval.strval = strdup(yytext);
                        RETURN(TOK_SQL); 
                        }
<SQLTEXT>"["        {
                    nOldState = SQLTEXT;
                    nCharCount += 1; 
                    STATE(SUBST); 
                    BEGIN(SUBST); 
                    RETURN(yytext[0]); 
                }
<SQLTEXT>;        { STATE(NORMAL);BEGIN(NORMAL);nCharCount += 1;  RETURN(yytext[0]); }
<SQLTEXT>.        { nCharCount += 1;  RETURN(yytext[0]); }
%%
extern template_t *pCurrentQuery;

/*
 * Routine: yywrap()
 * Purpose: manage the hand off between multiple input files, include files, 
 *            etc.
 * Algorithm:
 *         MKS lex has special buffer handlers that aren't exposed, so this needs to be included in the LEX source
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By: 
 * Calls: 
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int
yywrap(void)
{
    file_ref_t *pNext;


    if (is_set("DEBUG"))
        printf("STATUS: CLOSE(%s)\n", pCurrentFile->name);
    fclose(pCurrentFile->file);

    if (pCurrentFile->pNext) 
        {
#ifdef MKS
        yyRestoreScan(pCurrentFile->pLexState);
#endif
#ifdef FLEX
        yy_switch_to_buffer(pCurrentFile->pNext->pLexState);
        yy_delete_buffer(pCurrentFile->pLexState);
#endif
        pNext = pCurrentFile->pNext;
#if !defined(MKS) && !defined(FLEX)
        yyin = pNext->file;
#endif
        free(pCurrentFile);
        pCurrentFile = pNext;
        if (is_set("DEBUG"))
            printf("STATUS: REOPEN(%s)\n", pCurrentFile->name);
        return(0);
        }

    return(1);
}
#ifdef SCAN_DEBUG
DisplayAction(int d)
{
if (d >= ID)
    printf("<%s: '%s'>\n", KeywordText(d), yytext);
else
    printf ("%s\n", yytext);

return;
}

main(int ac, char **av)
{
LN=1;
nCharCount=1;
InitKeywords();
yylex();
exit(0);
}
#endif
