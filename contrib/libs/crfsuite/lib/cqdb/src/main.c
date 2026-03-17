/*
 *      Test program for Constant Quark Database (CQDB).
 *
 * Copyright (c) 2007, Naoaki Okazaki
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Northwestern University, University of Tokyo,
 *       nor the names of its contributors may be used to endorse or promote
 *       products derived from this software without specific prior written
 *       permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/* $Id$ */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cqdb.h"

//#define    TEST_WRITE    1
#define    CHECK_VALIDITY

#define    DBNAME        "test.cqdb"
#define    NUMELEMS    1000000

#ifdef    TEST_WRITE

int main(int argc, char *argv[])
{
    int i, ret;
    char str[10];
    FILE *fp = NULL;
    cqdb_writer_t* dbw = NULL;

    // Open a file for writing.
    fp = fopen(DBNAME, "wb");
    if (fp == NULL) {
        fprintf(stderr, "ERROR: failed to open the file.\n");
        return 1;
    }

    // Create a CQDB on the file stream.
    dbw = cqdb_writer(fp, 0);
    if (dbw == NULL) {
        fprintf(stderr, "ERROR: failed to create a CQDB on the file.\n");
        goto error_exit;
    }

    // Put string/integer associations, "00000001"/1, ..., "01000000"/1000000.
    for (i = 0;i < NUMELEMS;++i) {
        sprintf(str, "%08d", i);
        if (ret = cqdb_writer_put(dbw, str, i)) {
            fprintf(stderr, "ERROR: failed to put a pair '%s'/%d.\n", str, i);
            goto error_exit;    
        }
    }

    // Close the CQDB.
    if (ret = cqdb_writer_close(dbw)) {
        fprintf(stderr, "ERROR: failed to close the CQDB.\n");        
        goto error_exit;
    }

    // Close the file.
    fclose(fp);
    return 0;

error_exit:
    if (dbw != NULL) cqdb_writer_close(dbw);
    if (fp != NULL) fclose(fp);
    return 1;
}

#else /*TEST_WRITE*/

int main(int argc, char *argv[])
{
    int i, j, ret;
    long size = 0;
    const char *value = NULL;
    char str[10], *buffer = NULL;
    FILE *fp = NULL;
    cqdb_t* db = NULL;

    // Open the database.
    fp = fopen(DBNAME, "rb");
    if (fp == NULL) {
        fprintf(stderr, "ERROR: failed to open the file\n");
        return 1;
    }

    // Obtain the file size at one time.
    fseek(fp, 0, SEEK_END);
    size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    // Read the content of the file.
    buffer = (char *)malloc(size);
    if (buffer == NULL) {
        fprintf(stderr, "ERROR: out of memory.\n");
        goto error_exit;
    }
    fread(buffer, 1, size, fp);
    fclose(fp);
    fp = NULL;

    // Open the database on the memory.
    db = cqdb_reader(buffer, size);
    if (db == NULL) {
        fprintf(stderr, "ERROR: failed to open a CQDB on the file.\n");
        goto error_exit;
    }

    // Forward lookups: strings to integer identifiers.
    for (i = 0;i < NUMELEMS;++i) {
        sprintf(str, "%08d", i);
        j = cqdb_to_id(db, str);
#ifdef    CHECK_VALIDITY
        if (i != j) {
            fprintf(stderr, "ERROR: inconsistency error '%s'/%d.\n", str, i);
            goto error_exit;    
        }
#endif/*CHECK_VALIDITY*/
    }

    // Backward lookups: integer identifiers to strings.
    for (i = 0;i < NUMELEMS;++i) {
        sprintf(str, "%08d", i);
        value = cqdb_to_string(db, i);
#ifdef    CHECK_VALIDITY
        if (strcmp(str, value) != 0) {
            fprintf(stderr, "ERROR: inconsistency error '%s'/%d.\n", str, i);
            goto error_exit;    
        }
#endif/*CHECK_VALIDITY*/
    }

    cqdb_delete(db);
    free(buffer);

    return 0;

error_exit:
    if (fp != NULL) fclose(fp);
    if (buffer != NULL) free(buffer);
    return 1;
}

#endif/*TEST_WRITE*/
