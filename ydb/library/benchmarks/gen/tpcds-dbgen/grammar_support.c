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
#include <stdio.h>
#include <stdlib.h>
#ifdef USE_STRINGS_H
#include <strings.h>
#else
#include <string.h>
#endif

#include "StringBuffer.h"
#include "expr.h"
#include "grammar_support.h"
#include "keywords.h"
#include "error_msg.h"
#include "qgen_params.h"
#include "substitution.h"

extern char yytext[];
extern FILE *yyin;
file_ref_t *pCurrentFile = NULL;
file_ref_t CurrentFile;

extern template_t *pCurrentQuery;
static int nErrcnt = 0;
static int nWarncnt = 0;
/* 
 * different scanner handle yywrap differently
 */

#ifdef MKS
void *yySaveScan(FILE *fp);
int yyRestoreScan(void *pState);
#endif
#ifdef FLEX
/* flex has a slightly different way of handling yywrap. See O'Reilly, p. 155 */
void *yy_create_buffer( FILE *file, int size );
void yy_switch_to_buffer( void *new_buffer );
void yy_delete_buffer( void *b );
#ifndef YY_BUF_SIZE
#define YY_BUF_SIZE 16384
#endif
#endif

/*
 * Routine:  include_file()
 * Purpose: handle #include<> statements
 * Algorithm:
 * Data Structures:
 *
 * Params: char *path, int switch_to (unused?)
 * Returns: 0 on success, -1 on open failure, -2 on malloc failure
 * Called By: 
 * Calls: 
 * Assumptions:
 * Side Effects:
 * TODO: 
 */
int 
include_file(char *fn, void *pC)
{
    FILE *fp;
    file_ref_t *pFileRef;
    template_t *pContext = (template_t *)pC;

    if (fn == NULL)
        return(0);

    if ((fp = fopen(fn, "r")) == NULL)
        return(-1);
    
    pFileRef = (file_ref_t *)malloc(sizeof(struct FILE_REF_T));
    MALLOC_CHECK(pFileRef);
    if (pFileRef == NULL)
        ReportError(QERR_NO_MEMORY, "include_file()", 1);
    memset(pFileRef, 0, sizeof(struct FILE_REF_T));
    
    pFileRef->name = strdup(fn);
    pFileRef->file = fp;
    pFileRef->line_number = 1;
    pFileRef->pContext = pContext;
    if (pContext)
        pContext->name = strdup(fn);
    pFileRef->pNext = pCurrentFile;
    pCurrentFile = pFileRef;
    SetErrorGlobals(fn, &pFileRef->line_number);

#ifdef MKS
    pCurrentFile->pLexState = (void *)yySaveScan(pCurrentFile->file);
#endif
#ifdef FLEX
    pCurrentFile->pLexState = yy_create_buffer(pFileRef->file, YY_BUF_SIZE);
    yy_switch_to_buffer(pCurrentFile->pLexState);
#endif
#if !defined(MKS) && !defined(FLEX)
    yyin = pCurrentFile->file;
#endif

    if (is_set("DEBUG"))
        printf("STATUS: INCLUDE(%s)\n", fn);

    return(0);
}

/*
 * Routine: yyerror()
 * Purpose: standard error message interface
 * Algorithm:
 * Data Structures:
 *
 * Params: (char *) error message
 * Returns: 0 until 10 errors encountered, then exits
 * Called By: grammar, assorted support routines
 * Calls: None
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
void 
yyerror(char *msg, ...)
{

    printf("ERROR: %s at line %d in file %s\n",
        msg, pCurrentFile->line_number, pCurrentFile->name);
    if (++nErrcnt == 10)
        {
        printf("Too many errors. Exiting.\n");
        exit(1);
        }
    return;
}    


/*
 * Routine: yywarn()
 * Purpose: standard error message interface
 * Algorithm:
 * Data Structures:
 *
 * Params: (char *) error message
 * Returns: 0 until 10 errors encountered, then exits
 * Called By: grammar, assorted support routines
 * Calls: None
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
#ifndef SCANNER_TEST
int 
yywarn(char *str)
{
    fprintf(stderr, "warning: %s in line %d of file %s\n", 
        str, pCurrentFile->line_number, pCurrentFile->name);
    nWarncnt += 1;

    return(0);
}
#endif

/*
* Routine: 
* Purpose: 
* Algorithm:
* Data Structures:
*
* Params:
* Returns:
* Called By: 
* Calls: 
* Assumptions:
* Side Effects:
* TODO: 
*/
void 
GetErrorCounts(int *nError, int *nWarning)
{
    *nError = nErrcnt;
    *nWarning = nWarncnt;

    return;
}
