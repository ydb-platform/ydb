#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at#
#	http://www.apache.org/licenses/LICENSE-2.0#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: Zhe Sun, Ahmet Erdem
# April 20, 2017
# Modified by: Particular Miner
# April 14, 2021

# distutils: language = c++

from libcpp.vector cimport vector
from sparse_dot_topn.array_wrappers cimport ArrayWrapper_int, ArrayWrapper_float, ArrayWrapper_double

cimport cython
cimport numpy as np
import numpy as np

np.import_array()


ctypedef fused  float_ft:
	cython.float
	cython.double


cdef extern from "sparse_dot_topn_source.h":

	cdef void sparse_dot_topn_source[T](
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		T Ax[],
		int Bp[],
		int Bj[],
		T Bx[],
		int topn,
		T lower_bound,
		int Cp[],
		int Cj[],
		T Cx[]
	) except +;

	cdef int sparse_dot_topn_extd_source[T](
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		T Ax[],
		int Bp[],
		int Bj[],
		T Bx[],
		int topn,
		T lower_bound,
		int Cp[],
		int Cj[],
		T Cx[],
		vector[int]* alt_Cj,
		vector[T]* alt_Cx,
		int nnz_max,
		int* nminmax
	) except +;

	cdef int sparse_dot_only_nnz_source[T](
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		T Ax[],
		int Bp[],
		int Bj[],
		T Bx[],
		int ntop,
		T lower_bound
	) except +;

cpdef ArrayWrapper_template(vector[float_ft] vCx):
	# raise Exception("In sparse_dot_topn.pyx")
	if float_ft is float:
		return ArrayWrapper_float(vCx)
	elif float_ft is double:
		return ArrayWrapper_double(vCx)
	else:
		raise Exception("Type not supported")

cpdef sparse_dot_topn(
	int n_row,
	int n_col,
	np.ndarray[int, ndim=1] a_indptr,
	np.ndarray[int, ndim=1] a_indices,
	np.ndarray[float_ft, ndim=1] a_data,
	np.ndarray[int, ndim=1] b_indptr,
	np.ndarray[int, ndim=1] b_indices,
	np.ndarray[float_ft, ndim=1] b_data,
	int ntop,
	float_ft lower_bound,
	np.ndarray[int, ndim=1] c_indptr,
	np.ndarray[int, ndim=1] c_indices,
	np.ndarray[float_ft, ndim=1] c_data
):
	"""
	Cython glue function to call sparse_dot_topn C++ implementation
	This function will return a matrix C in CSR format, where
	C = [sorted top n results and results > lower_bound for each row of A * B]

	Input:
		n_row: number of rows of A matrix
		n_col: number of columns of B matrix

		a_indptr, a_indices, a_data: CSR expression of A matrix
		b_indptr, b_indices, b_data: CSR expression of B matrix

		ntop: n top results
		lower_bound: a threshold that the element of A*B must greater than

	Output by reference:
		c_indptr, c_indices, c_data: CSR expression of C matrix

	N.B. A and B must be CSR format!!!
		 The type of input numpy array must be aligned with types of C++ function arguments!
	"""

	cdef int* Ap = &a_indptr[0]
	cdef int* Aj = &a_indices[0]
	cdef float_ft* Ax = &a_data[0]
	cdef int* Bp = &b_indptr[0]
	cdef int* Bj = &b_indices[0]
	cdef float_ft* Bx = &b_data[0]
	cdef int* Cp = &c_indptr[0]
	cdef int* Cj = &c_indices[0]
	cdef float_ft* Cx = &c_data[0]

	sparse_dot_topn_source(
		n_row, n_col, Ap, Aj, Ax, Bp, Bj, Bx, ntop, lower_bound, Cp, Cj, Cx
	)
	return

cpdef sparse_dot_topn_extd(
	int n_row,
	int n_col,
	np.ndarray[int, ndim=1] a_indptr,
	np.ndarray[int, ndim=1] a_indices,
	np.ndarray[float_ft, ndim=1] a_data,
	np.ndarray[int, ndim=1] b_indptr,
	np.ndarray[int, ndim=1] b_indices,
	np.ndarray[float_ft, ndim=1] b_data,
	int ntop,
	float_ft lower_bound,
	np.ndarray[int, ndim=1] c_indptr,
	np.ndarray[int, ndim=1] c_indices,
	np.ndarray[float_ft, ndim=1] c_data,
	np.ndarray[int, ndim=1] nminmax
):
	"""
	Cython glue function to call sparse_dot_topn_extd C++
	implementation.  This function will return a matrix C in CSR
	format, where
	C = [sorted top n results > lower_bound for each row of A * B]
	The maximum number nminmax of elements per row of C (assuming 
	n = number of columns of B) is also returned.

	Input:
		n_row: number of rows of A matrix
		n_col: number of columns of B matrix

		a_indptr, a_indices, a_data: CSR expression of A matrix
		b_indptr, b_indices, b_data: CSR expression of B matrix

		ntop: n, the number of topmost results > lower_bound for
			  each row of C
		lower_bound: a threshold that the element of A*B must
					 greater than

	Output by reference:
		c_indptr, c_indices, c_data: CSR expression of matrix C
		nminmax: The maximum number of elements per row of C 
				 (assuming ntop = n_col)

	Returned output:
		c_indices, c_data: CSR expression of matrix C.  These will 
						be returned instead of output by reference
						if the preset sizes of c_indices and 
						c_data are too small to hold all the 
						results.

	N.B. A and B must be CSR format!!!
		 The type of input numpy array must be aligned with types
		 of C++ function arguments!
	"""

	cdef int* Ap = &a_indptr[0]
	cdef int* Aj = &a_indices[0]
	cdef float_ft* Ax = &a_data[0]
	cdef int* Bp = &b_indptr[0]
	cdef int* Bj = &b_indices[0]
	cdef float_ft* Bx = &b_data[0]
	cdef int* Cp = &c_indptr[0]
	cdef int* Cj = &c_indices[0]
	cdef float_ft* Cx = &c_data[0]
	cdef int* n_minmax = &nminmax[0]
	
	cdef nnz_max = len(c_indices)
	
	cdef vector[int] vCj;
	cdef vector[float_ft] vCx;

	cdef int nnz_max_is_too_small = sparse_dot_topn_extd_source(
		n_row, n_col, Ap, Aj, Ax, Bp, Bj, Bx, ntop, lower_bound, Cp, Cj, Cx, &vCj, &vCx, nnz_max, n_minmax
	)
	
	if nnz_max_is_too_small:
		
		# raise Exception("In sparse_dot_topn.pyx")
		
		c_indices = np.asarray(ArrayWrapper_int(vCj)).squeeze(axis=0)
		c_data = np.asarray(ArrayWrapper_template(vCx)).squeeze(axis=0)
	
		return c_indices, c_data		
	
	else:
		
		return None, None

cpdef sparse_dot_only_nnz(
	int n_row,
	int n_col,
	np.ndarray[int, ndim=1] a_indptr,
	np.ndarray[int, ndim=1] a_indices,
	np.ndarray[float_ft, ndim=1] a_data,
	np.ndarray[int, ndim=1] b_indptr,
	np.ndarray[int, ndim=1] b_indices,
	np.ndarray[float_ft, ndim=1] b_data,
	int ntop,
	float_ft lower_bound
):
	"""
	Cython glue function to call sparse_dot_nnz_only C++ implementation
	This function will return nnz, the total number of nonzero
	matrix-components of
	C = [top n results > lower_bound for each row of A * B].

	Input:
		a_indptr, a_indices, a_data: CSR expression of A matrix
		b_indptr, b_indices, b_data: CSR expression of B matrix

		ntop: n, the number of topmost results > lower_bound for 
			  each row of C
		lower_bound: a threshold that the element of A*B must 
					 greater than

	Returned output:
		nnz: the total number of nonzero matrix-components of C

	N.B. A and B must be CSR format!!!
		 The type of input numpy array must be aligned with types of C++ function arguments!
	"""

	cdef int* Ap = &a_indptr[0]
	cdef int* Aj = &a_indices[0]
	cdef float_ft* Ax = &a_data[0]
	cdef int* Bp = &b_indptr[0]
	cdef int* Bj = &b_indices[0]
	cdef float_ft* Bx = &b_data[0]

	return sparse_dot_only_nnz_source(
		n_row, n_col, Ap, Aj, Ax, Bp, Bj, Bx, ntop, lower_bound
	)
