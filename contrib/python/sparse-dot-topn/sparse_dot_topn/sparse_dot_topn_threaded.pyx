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


cdef extern from "sparse_dot_topn_parallel.h":

	cdef void sparse_dot_topn_parallel[T](
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
		int n_jobs
	) except +;

	cdef int sparse_dot_topn_extd_parallel[T](
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
		int* n_minmax,
		int n_jobs
	) except +;

	cdef int sparse_dot_only_nnz_parallel[T](
		int n_row,
		int n_col,
		int Ap[],
		int Aj[],
		T Ax[],
		int Bp[],
		int Bj[],
		T Bx[],
		int ntop,
		T lower_bound,
		int n_jobs
	) except +;

cpdef sparse_dot_topn_threaded(
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
	int n_jobs
):

	cdef int* Ap = &a_indptr[0]
	cdef int* Aj = &a_indices[0]
	cdef float_ft* Ax = &a_data[0]
	cdef int* Bp = &b_indptr[0]
	cdef int* Bj = &b_indices[0]
	cdef float_ft* Bx = &b_data[0]
	cdef int* Cp = &c_indptr[0]
	cdef int* Cj = &c_indices[0]
	cdef float_ft* Cx = &c_data[0]

	sparse_dot_topn_parallel(
		n_row, n_col, Ap, Aj, Ax, Bp, Bj, Bx, ntop, lower_bound, Cp, Cj, Cx, n_jobs
	)
	return

cpdef ArrayWrapper_template(vector[float_ft] vCx):
	# raise Exception("In sparse_dot_topn_threaded.pyx")
	if float_ft is float:
		return ArrayWrapper_float(vCx)
	elif float_ft is double:
		return ArrayWrapper_double(vCx)
	else:
		raise Exception("Type not supported")

cpdef sparse_dot_topn_extd_threaded(
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
	np.ndarray[int, ndim=1] nminmax,
	int n_jobs
):

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

	cdef int nnz_max_is_too_small = sparse_dot_topn_extd_parallel(
		n_row, n_col, Ap, Aj, Ax, Bp, Bj, Bx, ntop, lower_bound, Cp, Cj, Cx, &vCj, &vCx, nnz_max, n_minmax, n_jobs
	)
	
	if nnz_max_is_too_small:
		
		# raise Exception("In sparse_dot_topn_threaded.pyx")
		
		c_indices = np.asarray(ArrayWrapper_int(vCj)).squeeze(axis=0)
		c_data = np.asarray(ArrayWrapper_template(vCx)).squeeze(axis=0)
	
		return c_indices, c_data
	
	else:
		
		return None, None

cpdef sparse_dot_only_nnz_threaded(
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
	int n_jobs
):

	cdef int* Ap = &a_indptr[0]
	cdef int* Aj = &a_indices[0]
	cdef float_ft* Ax = &a_data[0]
	cdef int* Bp = &b_indptr[0]
	cdef int* Bj = &b_indices[0]
	cdef float_ft* Bx = &b_data[0]

	return sparse_dot_only_nnz_parallel(
		n_row, n_col, Ap, Aj, Ax, Bp, Bj, Bx, ntop, lower_bound, n_jobs
	)
