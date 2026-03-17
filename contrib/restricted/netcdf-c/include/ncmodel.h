/* Copyright 2018-2018 University Corporation for Atmospheric
   Research/Unidata. */

/**
 * Functions for inferring dataset model
 * @author Dennis Heimbigner
 */

#ifndef NCINFERMODEL_H
#define NCINFERMODEL_H

/* Track the information hat will help us
   infer how to access the data defined by
   path + omode + (sometimes) file content.
*/
typedef struct NCmodel {
    int impl; /* NC_FORMATX_XXX value */
    int format; /* NC_FORMAT_XXX value; Used to remember extra info; */
} NCmodel;

/* Infer model implementation */
EXTERNL int NC_infermodel(const char* path, int* omodep, int iscreate, int useparallel, void* params, NCmodel* model, char** newpathp);

#endif /*NCINFERMODEL_H*/
