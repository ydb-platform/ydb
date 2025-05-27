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
#ifdef USE_STRING_H
#include <string.h>
#else
#include <strings.h>
#endif

/*
* Routine: mkheader() 
* Purpose: keep columns.h and streams.h in sync
* Algorithm: this should really be a simple shell script, but need to assure that it will 
* port to non-unix (i.e., windows)
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
main(int ac, char **av)
{
    FILE *pInputFile,
        *pColumnsFile,
        *pStreamsFile,
        *pTablesFile;
    char szColumn[80],
        szTable[80],
        szLine[1024],
        szDuplicate[80],
        szLastTable[80],
        *cp;
    int nLineNumber = 0,
        nColumnCount = 1,
        bError = 0,
        nTableCount = 0,
        nRNGUsage = 1;

    if (ac != 2)
        bError = 1;

    pInputFile = fopen(av[1], "r");
    if (pInputFile == NULL)
        bError = 1;

    pColumnsFile = fopen("columns.h", "w");
    if (pColumnsFile == NULL)
        bError = 1;

    pStreamsFile = fopen("streams.h", "w");
    if (pStreamsFile == NULL)
        bError = 1;

    pTablesFile = fopen("tables.h", "w");
    if (pTablesFile == NULL)
        bError = 1;

    if (bError)
    {
        fprintf(stderr, "USAGE:\n\t%s file -- build headers based on file\n", av[0]);
        fprintf(stderr, "\tcolumns.h and streams.h produced as a result\n");
        exit(-1);
    }

    /* set up varaibles */
    szLastTable[0] = '\0';

    /* put out minimal headers for each file */
    fprintf(pColumnsFile, "/*\n * THIS IS A GENERATED FILE\n * SEE COLUMNS.LIST\n*/\n");
    fprintf(pStreamsFile, "/*\n * THIS IS A GENERATED FILE\n * SEE COLUMNS.LIST\n*/\n");
    fprintf(pTablesFile, "/*\n * THIS IS A GENERATED FILE\n * SEE COLUMNS.LIST\n*/\n");
    fprintf(pColumnsFile, "#ifndef COLUMNS_H\n#define COLUMNS_H\n");
    fprintf(pStreamsFile, "#ifndef STREAMS_H\n#define STREAMS_H\n");
    fprintf(pStreamsFile, "rng_t Streams[] = {\n{0, 0, 0, 0, 0, 0, 0},\n");
    fprintf(pTablesFile, "#ifndef TABLES_H\n#define TABLES_H\n");
    
    /* add an entry to each for each column in the list */
    while (fgets(szLine, 1024, pInputFile) != NULL)
    {
        nLineNumber += 1;
        szColumn[0] = szTable[0] = szDuplicate[0] = '\0';
        nRNGUsage = 1;
        if ((cp = strchr(szLine, '#')) != NULL)
            *cp = '\0';
        if (!strlen(szLine))
            continue;
        if (sscanf(szLine, "%s %s %d %s", szColumn, szTable, &nRNGUsage, szDuplicate) != 4)
        {
        strcpy(szDuplicate, szColumn);
        if (sscanf(szLine, "%s %s %d", szColumn, szTable, &nRNGUsage) != 3)
        {
        if (sscanf(szLine, "%s %s", szColumn, szTable) != 2)
            continue;
        }
        }

        fprintf(pStreamsFile, "{0, %d, 0, 0, %s, %s, %s},\n", nRNGUsage, szColumn, szTable, szDuplicate);
        if (strcmp(szLastTable, szTable))
            {
            if (strlen(szLastTable))
                fprintf(pColumnsFile, "#define %s_END\t%d\n", szLastTable, nColumnCount - 1);
            fprintf(pColumnsFile, "#define %s_START\t%d\n", szTable, nColumnCount);
            fprintf(pTablesFile, "#define %s\t%d\n", szTable, nTableCount++);
            strcpy(szLastTable, szTable);
            }
        fprintf(pColumnsFile, "#define %s\t%d\n", szColumn, nColumnCount++);
    }

    /* close out the files */
    fprintf(pStreamsFile, "{-1, -1, -1, -1, -1, -1, -1}\n};\n");
    fprintf(pStreamsFile, "#endif\n");
    fprintf(pColumnsFile, "#define %s_END\t%d\n", szLastTable, nColumnCount - 1);
    fprintf(pColumnsFile, "#define MAX_COLUMN\t%d\n", nColumnCount - 1);
    fprintf(pColumnsFile, "#endif\n");
    fprintf(pTablesFile, "#define PSEUDO_TABLE_START\t%d\n", nTableCount);
    fprintf(pTablesFile, "/* PSEUDO TABLES from here on; used in hierarchies */\n");
    fprintf(pTablesFile, "#define ITEM_BRAND\t%d\n", nTableCount++);
    fprintf(pTablesFile, "#define ITEM_CLASS\t%d\n", nTableCount++);
    fprintf(pTablesFile, "#define ITEM_CATEGORY\t%d\n", nTableCount++);
    fprintf(pTablesFile, "#define DIVISIONS\t%d\n", nTableCount++);
    fprintf(pTablesFile, "#define COMPANY\t%d\n", nTableCount++);
    fprintf(pTablesFile, "#define CONCURRENT_WEB_SITES\t%d\n", nTableCount++);
    fprintf(pTablesFile, "#define ACTIVE_CITIES\t%d\n", nTableCount++);
    fprintf(pTablesFile, "#define ACTIVE_COUNTIES\t%d\n", nTableCount++);
    fprintf(pTablesFile, "#define ACTIVE_STATES\t%d\n", nTableCount);
    fprintf(pTablesFile, "#define MAX_TABLE\t%d\n", nTableCount);    
    fprintf(pTablesFile, "#endif\n");

    /* close the files */
    fclose(pStreamsFile);
    fclose(pColumnsFile);
    fclose(pInputFile);
    fclose(pTablesFile);

    exit(0);
}
