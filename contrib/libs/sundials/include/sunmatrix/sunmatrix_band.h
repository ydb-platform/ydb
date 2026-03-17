/*
 * -----------------------------------------------------------------
 * Programmer(s): Daniel Reynolds @ SMU
 *                David Gardner @ LLNL
 * Based on code sundials_direct.h by: Radu Serban @ LLNL
 * -----------------------------------------------------------------
 * SUNDIALS Copyright Start
 * Copyright (c) 2002-2019, Lawrence Livermore National Security
 * and Southern Methodist University.
 * All rights reserved.
 *
 * See the top-level LICENSE and NOTICE files for details.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 * SUNDIALS Copyright End
 * -----------------------------------------------------------------
 * This is the header file for the band implementation of the 
 * SUNMATRIX module, SUNMATRIX_BAND.
 *
 * Notes:
 *   - The definition of the generic SUNMatrix structure can be found
 *     in the header file sundials_matrix.h.
 *   - The definition of the type 'realtype' can be found in the
 *     header file sundials_types.h, and it may be changed (at the 
 *     configuration stage) according to the user's needs. 
 *     The sundials_types.h file also contains the definition
 *     for the type 'booleantype' and 'indextype'.
 * -----------------------------------------------------------------
 */

#ifndef _SUNMATRIX_BAND_H
#define _SUNMATRIX_BAND_H

#include <stdio.h>
#include <sundials/sundials_matrix.h>

#ifdef __cplusplus  /* wrapper to enable C++ usage */
extern "C" {
#endif

/* ---------------------------------
 * Band implementation of SUNMatrix
 * --------------------------------- */
  
struct _SUNMatrixContent_Band {
  sunindextype M;
  sunindextype N;
  sunindextype ldim;
  sunindextype mu;
  sunindextype ml;
  sunindextype s_mu;
  realtype *data;
  sunindextype ldata;
  realtype **cols;
};

typedef struct _SUNMatrixContent_Band *SUNMatrixContent_Band;

  
/* ------------------------------------
 * Macros for access to SUNMATRIX_BAND
 * ------------------------------------ */

#define SM_CONTENT_B(A)     ( (SUNMatrixContent_Band)(A->content) )

#define SM_ROWS_B(A)        ( SM_CONTENT_B(A)->M )

#define SM_COLUMNS_B(A)     ( SM_CONTENT_B(A)->N )

#define SM_LDATA_B(A)       ( SM_CONTENT_B(A)->ldata )

#define SM_UBAND_B(A)       ( SM_CONTENT_B(A)->mu )

#define SM_LBAND_B(A)       ( SM_CONTENT_B(A)->ml )

#define SM_SUBAND_B(A)      ( SM_CONTENT_B(A)->s_mu )

#define SM_LDIM_B(A)        ( SM_CONTENT_B(A)->ldim )

#define SM_DATA_B(A)        ( SM_CONTENT_B(A)->data )

#define SM_COLS_B(A)        ( SM_CONTENT_B(A)->cols )

#define SM_COLUMN_B(A,j)    ( ((SM_CONTENT_B(A)->cols)[j])+SM_SUBAND_B(A) )

#define SM_COLUMN_ELEMENT_B(col_j,i,j) (col_j[(i)-(j)])

#define SM_ELEMENT_B(A,i,j) ( (SM_CONTENT_B(A)->cols)[j][(i)-(j)+SM_SUBAND_B(A)] )


/* ----------------------------------------
 * Exported  Functions for  SUNMATRIX_BAND
 * ---------------------------------------- */

SUNDIALS_EXPORT SUNMatrix SUNBandMatrix(sunindextype N, sunindextype mu,
                                        sunindextype ml);

SUNDIALS_EXPORT SUNMatrix SUNBandMatrixStorage(sunindextype N,
                                               sunindextype mu,
                                               sunindextype ml,
                                               sunindextype smu);

SUNDIALS_EXPORT void SUNBandMatrix_Print(SUNMatrix A, FILE* outfile);

SUNDIALS_EXPORT sunindextype SUNBandMatrix_Rows(SUNMatrix A);
SUNDIALS_EXPORT sunindextype SUNBandMatrix_Columns(SUNMatrix A);
SUNDIALS_EXPORT sunindextype SUNBandMatrix_LowerBandwidth(SUNMatrix A);
SUNDIALS_EXPORT sunindextype SUNBandMatrix_UpperBandwidth(SUNMatrix A);
SUNDIALS_EXPORT sunindextype SUNBandMatrix_StoredUpperBandwidth(SUNMatrix A);
SUNDIALS_EXPORT sunindextype SUNBandMatrix_LDim(SUNMatrix A);
SUNDIALS_EXPORT realtype* SUNBandMatrix_Data(SUNMatrix A);
SUNDIALS_EXPORT realtype** SUNBandMatrix_Cols(SUNMatrix A);
SUNDIALS_EXPORT realtype* SUNBandMatrix_Column(SUNMatrix A, sunindextype j);

SUNDIALS_EXPORT SUNMatrix_ID SUNMatGetID_Band(SUNMatrix A);
SUNDIALS_EXPORT SUNMatrix SUNMatClone_Band(SUNMatrix A);
SUNDIALS_EXPORT void SUNMatDestroy_Band(SUNMatrix A);
SUNDIALS_EXPORT int SUNMatZero_Band(SUNMatrix A);
SUNDIALS_EXPORT int SUNMatCopy_Band(SUNMatrix A, SUNMatrix B);
SUNDIALS_EXPORT int SUNMatScaleAdd_Band(realtype c, SUNMatrix A, SUNMatrix B);
SUNDIALS_EXPORT int SUNMatScaleAddI_Band(realtype c, SUNMatrix A);
SUNDIALS_EXPORT int SUNMatMatvec_Band(SUNMatrix A, N_Vector x, N_Vector y);
SUNDIALS_EXPORT int SUNMatSpace_Band(SUNMatrix A, long int *lenrw, long int *leniw);
  
#ifdef __cplusplus
}
#endif

#endif
