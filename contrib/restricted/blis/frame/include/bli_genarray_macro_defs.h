/*

   BLIS
   An object-based framework for developing high-performance BLAS-like
   libraries.

   Copyright (C) 2014, The University of Texas at Austin

   Redistribution and use in source and binary forms, with or without
   modification, are permitted provided that the following conditions are
   met:
    - Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    - Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    - Neither the name(s) of the copyright holder(s) nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
   HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

#ifndef BLIS_GENARRAY_MACRO_DEFS_H
#define BLIS_GENARRAY_MACRO_DEFS_H


// -- Macros to generate function arrays ---------------------------------------

// -- "Smart" one-operand macro --

#define GENARRAY_FPA(tname,opname) \
\
static tname PASTECH(opname,_fpa)[BLIS_NUM_FP_TYPES] = \
{ \
	( tname )PASTEMAC(s,opname), \
	( tname )PASTEMAC(c,opname), \
	( tname )PASTEMAC(d,opname), \
	( tname )PASTEMAC(z,opname)  \
}

// -- "Smart" one-operand macro (with integer support) --

#define GENARRAY_FPA_I(tname,opname) \
\
static tname PASTECH(opname,_fpa)[BLIS_NUM_FP_TYPES+1] = \
{ \
	( tname )PASTEMAC(s,opname), \
	( tname )PASTEMAC(c,opname), \
	( tname )PASTEMAC(d,opname), \
	( tname )PASTEMAC(z,opname), \
	( tname )PASTEMAC(i,opname)  \
}

// -- "Smart" two-operand macro --

#define GENARRAY_FPA2(tname,op) \
\
static tname PASTECH(op,_fpa2)[BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES] = \
{ \
	{ ( tname )PASTEMAC2(s,s,op), ( tname )PASTEMAC2(s,c,op), ( tname )PASTEMAC2(s,d,op), ( tname )PASTEMAC2(s,z,op) }, \
	{ ( tname )PASTEMAC2(c,s,op), ( tname )PASTEMAC2(c,c,op), ( tname )PASTEMAC2(c,d,op), ( tname )PASTEMAC2(c,z,op) }, \
	{ ( tname )PASTEMAC2(d,s,op), ( tname )PASTEMAC2(d,c,op), ( tname )PASTEMAC2(d,d,op), ( tname )PASTEMAC2(d,z,op) }, \
	{ ( tname )PASTEMAC2(z,s,op), ( tname )PASTEMAC2(z,c,op), ( tname )PASTEMAC2(z,d,op), ( tname )PASTEMAC2(z,z,op) }  \
}

// -- "Smart" two-operand macro --

/*
#define GENARRAY2_VFP(arrayname,op) \
\
arrayname[BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES] = \
{ \
	{ PASTEMAC2(s,s,op), PASTEMAC2(s,c,op), PASTEMAC2(s,d,op), PASTEMAC2(s,z,op) }, \
	{ PASTEMAC2(c,s,op), PASTEMAC2(c,c,op), PASTEMAC2(c,d,op), PASTEMAC2(c,z,op) }, \
	{ PASTEMAC2(d,s,op), PASTEMAC2(d,c,op), PASTEMAC2(d,d,op), PASTEMAC2(d,z,op) }, \
	{ PASTEMAC2(z,s,op), PASTEMAC2(z,c,op), PASTEMAC2(z,d,op), PASTEMAC2(z,z,op) }  \
}
*/



// -- One-operand macro --

#define GENARRAY(arrayname,op) \
\
arrayname[BLIS_NUM_FP_TYPES] = \
{ \
	PASTEMAC(s,op), \
	PASTEMAC(c,op), \
	PASTEMAC(d,op), \
	PASTEMAC(z,op)  \
}

#define GENARRAY_I(arrayname,op) \
\
arrayname[BLIS_NUM_FP_TYPES+1] = \
{ \
	PASTEMAC(s,op), \
	PASTEMAC(c,op), \
	PASTEMAC(d,op), \
	PASTEMAC(z,op), \
	PASTEMAC(i,op)  \
}

/*
#define GENARRAYR(arrayname,op) \
\
arrayname[BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES] = \
{ \
	{ PASTEMAC2(s,s,op), NULL,              PASTEMAC2(s,d,op), NULL,             }, \
	{ PASTEMAC2(c,s,op), NULL,              PASTEMAC2(c,d,op), NULL,             }, \
	{ PASTEMAC2(d,s,op), NULL,              PASTEMAC2(d,d,op), NULL,             }, \
	{ PASTEMAC2(z,s,op), NULL,              PASTEMAC2(z,d,op), NULL,             }  \
}
*/



// -- Two-operand macros --


#define GENARRAY2_ALL(arrayname,op) \
\
arrayname[BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES] = \
{ \
	{ PASTEMAC2(s,s,op), PASTEMAC2(s,c,op), PASTEMAC2(s,d,op), PASTEMAC2(s,z,op) }, \
	{ PASTEMAC2(c,s,op), PASTEMAC2(c,c,op), PASTEMAC2(c,d,op), PASTEMAC2(c,z,op) }, \
	{ PASTEMAC2(d,s,op), PASTEMAC2(d,c,op), PASTEMAC2(d,d,op), PASTEMAC2(d,z,op) }, \
	{ PASTEMAC2(z,s,op), PASTEMAC2(z,c,op), PASTEMAC2(z,d,op), PASTEMAC2(z,z,op) }  \
}


#define GENARRAY2_EXT(arrayname,op) \
\
arrayname[BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES] = \
{ \
	{ PASTEMAC2(s,s,op), PASTEMAC2(s,c,op), NULL,              NULL,             }, \
	{ PASTEMAC2(c,s,op), PASTEMAC2(c,c,op), NULL,              NULL,             }, \
	{ NULL,              NULL,              PASTEMAC2(d,d,op), PASTEMAC2(d,z,op) }, \
	{ NULL,              NULL,              PASTEMAC2(z,d,op), PASTEMAC2(z,z,op) }  \
}


#define GENARRAY2_MIN(arrayname,op) \
\
arrayname[BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES] = \
{ \
	{ PASTEMAC2(s,s,op), NULL,              NULL,              NULL,             }, \
	{ NULL,              PASTEMAC2(c,c,op), NULL,              NULL,             }, \
	{ NULL,              NULL,              PASTEMAC2(d,d,op), NULL,             }, \
	{ NULL,              NULL,              NULL,              PASTEMAC2(z,z,op) }  \
}


// -- Three-operand macros --


#define GENARRAY3_ALL(arrayname,op) \
\
arrayname[BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES] = \
{ \
	{ \
	{ PASTEMAC3(s,s,s,op), PASTEMAC3(s,s,c,op), PASTEMAC3(s,s,d,op), PASTEMAC3(s,s,z,op) }, \
	{ PASTEMAC3(s,c,s,op), PASTEMAC3(s,c,c,op), PASTEMAC3(s,c,d,op), PASTEMAC3(s,c,z,op) }, \
	{ PASTEMAC3(s,d,s,op), PASTEMAC3(s,d,c,op), PASTEMAC3(s,d,d,op), PASTEMAC3(s,d,z,op) }, \
	{ PASTEMAC3(s,z,s,op), PASTEMAC3(s,z,c,op), PASTEMAC3(s,z,d,op), PASTEMAC3(s,z,z,op) }  \
	}, \
	{ \
	{ PASTEMAC3(c,s,s,op), PASTEMAC3(c,s,c,op), PASTEMAC3(c,s,d,op), PASTEMAC3(c,s,z,op) }, \
	{ PASTEMAC3(c,c,s,op), PASTEMAC3(c,c,c,op), PASTEMAC3(c,c,d,op), PASTEMAC3(c,c,z,op) }, \
	{ PASTEMAC3(c,d,s,op), PASTEMAC3(c,d,c,op), PASTEMAC3(c,d,d,op), PASTEMAC3(c,d,z,op) }, \
	{ PASTEMAC3(c,z,s,op), PASTEMAC3(c,z,c,op), PASTEMAC3(c,z,d,op), PASTEMAC3(c,z,z,op) }  \
	}, \
	{ \
	{ PASTEMAC3(d,s,s,op), PASTEMAC3(d,s,c,op), PASTEMAC3(d,s,d,op), PASTEMAC3(d,s,z,op) }, \
	{ PASTEMAC3(d,c,s,op), PASTEMAC3(d,c,c,op), PASTEMAC3(d,c,d,op), PASTEMAC3(d,c,z,op) }, \
	{ PASTEMAC3(d,d,s,op), PASTEMAC3(d,d,c,op), PASTEMAC3(d,d,d,op), PASTEMAC3(d,d,z,op) }, \
	{ PASTEMAC3(d,z,s,op), PASTEMAC3(d,z,c,op), PASTEMAC3(d,z,d,op), PASTEMAC3(d,z,z,op) }  \
	}, \
	{ \
	{ PASTEMAC3(z,s,s,op), PASTEMAC3(z,s,c,op), PASTEMAC3(z,s,d,op), PASTEMAC3(z,s,z,op) }, \
	{ PASTEMAC3(z,c,s,op), PASTEMAC3(z,c,c,op), PASTEMAC3(z,c,d,op), PASTEMAC3(z,c,z,op) }, \
	{ PASTEMAC3(z,d,s,op), PASTEMAC3(z,d,c,op), PASTEMAC3(z,d,d,op), PASTEMAC3(z,d,z,op) }, \
	{ PASTEMAC3(z,z,s,op), PASTEMAC3(z,z,c,op), PASTEMAC3(z,z,d,op), PASTEMAC3(z,z,z,op) }  \
	} \
}


#define GENARRAY3_EXT(arrayname,op) \
\
arrayname[BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES] = \
{ \
	{ \
	{ PASTEMAC3(s,s,s,op), PASTEMAC3(s,s,c,op), NULL,                NULL,               }, \
	{ PASTEMAC3(s,c,s,op), PASTEMAC3(s,c,c,op), NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }  \
	}, \
	{ \
	{ PASTEMAC3(c,s,s,op), PASTEMAC3(c,s,c,op), NULL,                NULL,               }, \
	{ PASTEMAC3(c,c,s,op), PASTEMAC3(c,c,c,op), NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }  \
	}, \
	{ \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                PASTEMAC3(d,d,d,op), PASTEMAC3(d,d,z,op) }, \
	{ NULL,                NULL,                PASTEMAC3(d,z,d,op), PASTEMAC3(d,z,z,op) }  \
	}, \
	{ \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                PASTEMAC3(z,d,d,op), PASTEMAC3(z,d,z,op) }, \
	{ NULL,                NULL,                PASTEMAC3(z,z,d,op), PASTEMAC3(z,z,z,op) }  \
	} \
}


#define GENARRAY3_MIN(arrayname,op) \
\
arrayname[BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES][BLIS_NUM_FP_TYPES] = \
{ \
	{ \
	{ PASTEMAC3(s,s,s,op), NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }  \
	}, \
	{ \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                PASTEMAC3(c,c,c,op), NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }  \
	}, \
	{ \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                PASTEMAC3(d,d,d,op), NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }  \
	}, \
	{ \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                NULL,               }, \
	{ NULL,                NULL,                NULL,                PASTEMAC3(z,z,z,op) }  \
	} \
}


#endif
