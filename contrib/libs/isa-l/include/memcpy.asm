;;
;; Copyright (c) 2023, Intel Corporation
;;
;; Redistribution and use in source and binary forms, with or without
;; modification, are permitted provided that the following conditions are met:
;;
;;     * Redistributions of source code must retain the above copyright notice,
;;       this list of conditions and the following disclaimer.
;;     * Redistributions in binary form must reproduce the above copyright
;;       notice, this list of conditions and the following disclaimer in the
;;       documentation and/or other materials provided with the distribution.
;;     * Neither the name of Intel Corporation nor the names of its contributors
;;       may be used to endorse or promote products derived from this software
;;       without specific prior written permission.
;;
;; THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
;; AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
;; IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
;; DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
;; FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
;; DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
;; SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
;; CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
;; OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
;; OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
;;

%ifndef __MEMCPY_INC__
%define __MEMCPY_INC__

%include "reg_sizes.asm"

; This section defines a series of macros to copy small to medium amounts
; of data from memory to memory, where the size is variable but limited.
;
; The macros are all called as:
; memcpy DST, SRC, SIZE, TMP0, TMP1, XTMP0, XTMP1, XTMP2, XTMP3
; with the parameters defined as:
;    DST     : register: pointer to dst (not modified)
;    SRC     : register: pointer to src (not modified)
;    SIZE    : register: length in bytes (not modified)
;    TMP0    : 64-bit temp GPR (clobbered)
;    TMP1    : 64-bit temp GPR (clobbered)
;    XTMP0   : temp XMM (clobbered)
;    XTMP1   : temp XMM (clobbered)
;    XTMP2   : temp XMM (clobbered)
;    XTMP3   : temp XMM (clobbered)
;
; The name indicates the options. The name is of the form:
; memcpy_<VEC>_<SZ><ZERO><RET>
; where:
; <VEC> is either "sse" or "avx" or "avx2"
; <SZ> is either "64" or "128" and defines largest value of SIZE
; <ZERO> is blank or "_1". If "_1" then the min SIZE is 1 (otherwise 0)
; <RET> is blank or "_ret". If blank, the code falls through. If "ret"
;                           it does a "ret" at the end
;
; For the avx2 versions, the temp XMM registers need to be YMM registers
; If the SZ is 64, then only two YMM temps are needed, i.e. it is called as:
; memcpy_avx2_64 DST, SRC, SIZE, TMP0, TMP1, YTMP0, YTMP1
; memcpy_avx2_128 DST, SRC, SIZE, TMP0, TMP1, YTMP0, YTMP1, YTMP2, YTMP3
;
; For example:
; memcpy_sse_64		: SSE,  0 <= size < 64, falls through
; memcpy_avx_64_1	: AVX1, 1 <= size < 64, falls through
; memcpy_sse_128_ret	: SSE,  0 <= size < 128, ends with ret
; mempcy_avx_128_1_ret	: AVX1, 1 <= size < 128, ends with ret
;

%macro memcpy_sse_64 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 0, 64, 0, 0
%endm

%macro memcpy_sse_64_1 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 1, 64, 0, 0
%endm

%macro memcpy_sse_128 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 0, 128, 0, 0
%endm

%macro memcpy_sse_128_1 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 1, 128, 0, 0
%endm

%macro memcpy_sse_64_ret 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 0, 64, 1, 0
%endm

%macro memcpy_sse_64_1_ret 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 1, 64, 1, 0
%endm

%macro memcpy_sse_128_ret 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 0, 128, 1, 0
%endm

%macro memcpy_sse_128_1_ret 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 1, 128, 1, 0
%endm

%macro memcpy_sse_16 5
	__memcpy_int %1,%2,%3,%4,%5,,,,, 0, 16, 0, 0
%endm

%macro memcpy_sse_16_1 5
	__memcpy_int %1,%2,%3,%4,%5,,,,, 1, 16, 0, 0
%endm

	;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

%macro memcpy_avx_64 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 0, 64, 0, 1
%endm

%macro memcpy_avx_64_1 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 1, 64, 0, 1
%endm

%macro memcpy_avx_128 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 0, 128, 0, 1
%endm

%macro memcpy_avx_128_1 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 1, 128, 0, 1
%endm

%macro memcpy_avx_64_ret 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 0, 64, 1, 1
%endm

%macro memcpy_avx_64_1_ret 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 1, 64, 1, 1
%endm

%macro memcpy_avx_128_ret 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 0, 128, 1, 1
%endm

%macro memcpy_avx_128_1_ret 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 1, 128, 1, 1
%endm

%macro memcpy_avx_16 5
	__memcpy_int %1,%2,%3,%4,%5,,,,, 0, 16, 0, 1
%endm

%macro memcpy_avx_16_1 5
	__memcpy_int %1,%2,%3,%4,%5,,,,, 1, 16, 0, 1
%endm

	;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

%macro memcpy_avx2_64 7
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,--,--, 0, 64, 0, 2
%endm

%macro memcpy_avx2_64_1 7
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,--,--, 1, 64, 0, 2
%endm

%macro memcpy_avx2_128 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7, %8, %9, 0, 128, 0, 2
%endm

%macro memcpy_avx2_128_1 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7, %8, %9, 1, 128, 0, 2
%endm

%macro memcpy_avx2_64_ret 7
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,--,--, 0, 64, 1, 2
%endm

%macro memcpy_avx2_64_1_ret 7
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,--,--, 1, 64, 1, 2
%endm

%macro memcpy_avx2_128_ret 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 0, 128, 1, 2
%endm

%macro memcpy_avx2_128_1_ret 9
	__memcpy_int %1,%2,%3,%4,%5,%6,%7,%8,%9, 1, 128, 1, 2
%endm

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

%macro __memcpy_int 13
%define %%DST     %1	; register: pointer to dst (not modified)
%define %%SRC     %2	; register: pointer to src (not modified)
%define %%SIZE    %3	; register: length in bytes (not modified)
%define %%TMP0    %4	; 64-bit temp GPR (clobbered)
%define %%TMP1    %5	; 64-bit temp GPR (clobbered)
%define %%XTMP0   %6	; temp XMM (clobbered)
%define %%XTMP1   %7	; temp XMM (clobbered)
%define %%XTMP2   %8	; temp XMM (clobbered)
%define %%XTMP3   %9	; temp XMM (clobbered)
%define %%NOT0    %10	; if not 0, then assume size cannot be zero
%define %%MAXSIZE %11	; 128, 64, etc
%define %%USERET  %12   ; if not 0, use "ret" at end
%define %%USEAVX  %13   ; 0 = SSE, 1 = AVX1, 2 = AVX2

%if (%%USERET != 0)
 %define %%DONE	ret
%else
 %define %%DONE jmp %%end
%endif

%if (%%USEAVX != 0)
 %define %%MOVDQU vmovdqu
%else
 %define %%MOVDQU movdqu
%endif

%if (%%MAXSIZE >= 128)
	test	%%SIZE, 64
	jz	%%lt64
  %if (%%USEAVX >= 2)
	%%MOVDQU	%%XTMP0, [%%SRC + 0*32]
	%%MOVDQU	%%XTMP1, [%%SRC + 1*32]
	%%MOVDQU	%%XTMP2, [%%SRC + %%SIZE - 2*32]
	%%MOVDQU	%%XTMP3, [%%SRC + %%SIZE - 1*32]

	%%MOVDQU	[%%DST + 0*32], %%XTMP0
	%%MOVDQU	[%%DST + 1*32], %%XTMP1
	%%MOVDQU	[%%DST + %%SIZE - 2*32], %%XTMP2
	%%MOVDQU	[%%DST + %%SIZE - 1*32], %%XTMP3
  %else
	%%MOVDQU	%%XTMP0, [%%SRC + 0*16]
	%%MOVDQU	%%XTMP1, [%%SRC + 1*16]
	%%MOVDQU	%%XTMP2, [%%SRC + 2*16]
	%%MOVDQU	%%XTMP3, [%%SRC + 3*16]
	%%MOVDQU	[%%DST + 0*16], %%XTMP0
	%%MOVDQU	[%%DST + 1*16], %%XTMP1
	%%MOVDQU	[%%DST + 2*16], %%XTMP2
	%%MOVDQU	[%%DST + 3*16], %%XTMP3

	%%MOVDQU	%%XTMP0, [%%SRC + %%SIZE - 4*16]
	%%MOVDQU	%%XTMP1, [%%SRC + %%SIZE - 3*16]
	%%MOVDQU	%%XTMP2, [%%SRC + %%SIZE - 2*16]
	%%MOVDQU	%%XTMP3, [%%SRC + %%SIZE - 1*16]
	%%MOVDQU	[%%DST + %%SIZE - 4*16], %%XTMP0
	%%MOVDQU	[%%DST + %%SIZE - 3*16], %%XTMP1
	%%MOVDQU	[%%DST + %%SIZE - 2*16], %%XTMP2
	%%MOVDQU	[%%DST + %%SIZE - 1*16], %%XTMP3
  %endif
	%%DONE
%endif

%if (%%MAXSIZE >= 64)
%%lt64:
	test	%%SIZE, 32
	jz	%%lt32
  %if (%%USEAVX >= 2)
	%%MOVDQU	%%XTMP0, [%%SRC + 0*32]
	%%MOVDQU	%%XTMP1, [%%SRC + %%SIZE - 1*32]
	%%MOVDQU	[%%DST + 0*32], %%XTMP0
	%%MOVDQU	[%%DST + %%SIZE - 1*32], %%XTMP1
  %else
	%%MOVDQU	%%XTMP0, [%%SRC + 0*16]
	%%MOVDQU	%%XTMP1, [%%SRC + 1*16]
	%%MOVDQU	%%XTMP2, [%%SRC + %%SIZE - 2*16]
	%%MOVDQU	%%XTMP3, [%%SRC + %%SIZE - 1*16]
	%%MOVDQU	[%%DST + 0*16], %%XTMP0
	%%MOVDQU	[%%DST + 1*16], %%XTMP1
	%%MOVDQU	[%%DST + %%SIZE - 2*16], %%XTMP2
	%%MOVDQU	[%%DST + %%SIZE - 1*16], %%XTMP3
  %endif
	%%DONE
%endif

%if (%%MAXSIZE >= 32)
%%lt32:
	test	%%SIZE, 16
	jz	%%lt16
  %if (%%USEAVX >= 2)
	%%MOVDQU	XWORD(%%XTMP0), [%%SRC + 0*16]
	%%MOVDQU	XWORD(%%XTMP1), [%%SRC + %%SIZE - 1*16]
	%%MOVDQU	[%%DST + 0*16], XWORD(%%XTMP0)
	%%MOVDQU	[%%DST + %%SIZE - 1*16], XWORD(%%XTMP1)
  %else
	%%MOVDQU	%%XTMP0, [%%SRC + 0*16]
	%%MOVDQU	%%XTMP1, [%%SRC + %%SIZE - 1*16]
	%%MOVDQU	[%%DST + 0*16], %%XTMP0
	%%MOVDQU	[%%DST + %%SIZE - 1*16], %%XTMP1
  %endif
	%%DONE
%endif

%if (%%MAXSIZE >= 16)
        test    %%SIZE, 16
        jz      %%lt16
        mov	%%TMP0, [%%SRC]
        mov	%%TMP1, [%%SRC + 8]
        mov	[%%DST], %%TMP0
        mov	[%%DST + 8], %%TMP1
%%lt16:
	test	%%SIZE, 8
	jz	%%lt8
	mov	%%TMP0, [%%SRC]
	mov	%%TMP1, [%%SRC + %%SIZE - 8]
	mov	[%%DST], %%TMP0
	mov	[%%DST + %%SIZE - 8], %%TMP1
	%%DONE
%endif

%if (%%MAXSIZE >= 8)
%%lt8:
	test	%%SIZE, 4
	jz	%%lt4
	mov	DWORD(%%TMP0), [%%SRC]
	mov	DWORD(%%TMP1), [%%SRC + %%SIZE - 4]
	mov	[%%DST], DWORD(%%TMP0)
	mov	[%%DST + %%SIZE - 4], DWORD(%%TMP1)
	%%DONE
%endif

%if (%%MAXSIZE >= 4)
%%lt4:
	test	%%SIZE, 2
	jz	%%lt2
	movzx	DWORD(%%TMP0), word [%%SRC]
	movzx	DWORD(%%TMP1), byte [%%SRC + %%SIZE - 1]
	mov	[%%DST], WORD(%%TMP0)
	mov	[%%DST + %%SIZE - 1], BYTE(%%TMP1)
	%%DONE
%endif

%%lt2:
%if (%%NOT0 == 0)
	 test	 %%SIZE, 1
	 jz	 %%end
%endif
	movzx	DWORD(%%TMP0), byte [%%SRC]
	mov	[%%DST], BYTE(%%TMP0)
%%end:
%if (%%USERET != 0)
	ret
%endif
%endm

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Utility macro to assist with SIMD shifting
%macro _PSRLDQ 3
%define %%VEC   %1
%define %%REG   %2
%define %%IMM   %3

%ifidn %%VEC, SSE
        psrldq  %%REG, %%IMM
%else
        vpsrldq %%REG, %%REG, %%IMM
%endif
%endm

        ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

; This section defines a series of macros to store small to medium amounts
; of data from SIMD registers to memory, where the size is variable but limited.
;
; The macros are all called as:
; memcpy DST, SRC, SIZE, TMP, IDX
; with the parameters defined as:
;    DST     : register: pointer to dst (not modified)
;    SRC     : register: src data (clobbered)
;    SIZE    : register: length in bytes (not modified)
;    TMP     : 64-bit temp GPR (clobbered)
;    IDX     : 64-bit GPR to store dst index/offset (clobbered)
;    OFFSET  ; Offset to be applied to destination pointer (optional)
;
; The name indicates the options. The name is of the form:
; simd_store_<VEC>
; where <VEC> is the SIMD instruction type e.g. "sse" or "avx"

%macro simd_store_sse 5-6
%if %0 == 6
        __simd_store %1,%2,%3,%4,%5,SSE,16,%6
%else
        __simd_store %1,%2,%3,%4,%5,SSE,16
%endif
%endm

%macro simd_store_avx 5-6
%if %0 == 6
        __simd_store %1,%2,%3,%4,%5,AVX,16,%6
%else
        __simd_store %1,%2,%3,%4,%5,AVX,16
%endif
%endm

%macro simd_store_sse_15 5-6
%if %0 == 6
        __simd_store %1,%2,%3,%4,%5,SSE,15,%6
%else
        __simd_store %1,%2,%3,%4,%5,SSE,15
%endif
%endm

%macro simd_store_avx_15 5-6
%if %0 == 6
        __simd_store %1,%2,%3,%4,%5,AVX,15,%6
%else
        __simd_store %1,%2,%3,%4,%5,AVX,15
%endif
%endm

%macro __simd_store 7-8
%define %%DST      %1    ; register: pointer to dst (not modified)
%define %%SRC      %2    ; register: src data (clobbered)
%define %%SIZE     %3    ; register: length in bytes (not modified)
%define %%TMP      %4    ; 64-bit temp GPR (clobbered)
%define %%IDX      %5    ; 64-bit temp GPR to store dst idx (clobbered)
%define %%SIMDTYPE %6    ; "SSE" or "AVX"
%define %%MAX_LEN  %7    ; maximum length to be stored
%define %%OFFSET   %8    ; offset to be applied to destination pointer

%define %%PSRLDQ _PSRLDQ %%SIMDTYPE,

%ifidn %%SIMDTYPE, SSE
 %define %%MOVDQU movdqu
 %define %%MOVQ movq
%else
 %define %%MOVDQU vmovdqu
 %define %%MOVQ vmovq
%endif

;; determine max byte size for store operation
%assign max_length_to_store %%MAX_LEN

%if max_length_to_store > 16
%error "__simd_store macro invoked with MAX_LEN bigger than 16!"
%endif

%if %0 == 8
        mov     %%IDX, %%OFFSET
%else
        xor     %%IDX, %%IDX        ; zero idx
%endif

%if max_length_to_store == 16
        test    %%SIZE, 16
        jz      %%lt16
        %%MOVDQU [%%DST + %%IDX], %%SRC
        jmp     %%end
%%lt16:
%endif

%if max_length_to_store >= 8
        test    %%SIZE, 8
        jz      %%lt8
        %%MOVQ  [%%DST + %%IDX], %%SRC
        %%PSRLDQ %%SRC, 8
        add     %%IDX, 8
%%lt8:
%endif

        %%MOVQ %%TMP, %%SRC     ; use GPR from now on

%if max_length_to_store >= 4
        test    %%SIZE, 4
        jz      %%lt4
        mov     [%%DST + %%IDX], DWORD(%%TMP)
        shr     %%TMP, 32
        add     %%IDX, 4
%%lt4:
%endif

        test    %%SIZE, 2
        jz      %%lt2
        mov     [%%DST + %%IDX], WORD(%%TMP)
        shr     %%TMP, 16
        add     %%IDX, 2
%%lt2:
        test    %%SIZE, 1
        jz      %%end
        mov     [%%DST + %%IDX], BYTE(%%TMP)
%%end:
%endm

; This section defines a series of macros to load small to medium amounts
; (from 0 to 16 bytes) of data from memory to SIMD registers,
; where the size is variable but limited.
;
; The macros are all called as:
; simd_load DST, SRC, SIZE
; with the parameters defined as:
;    DST     : register: destination XMM register
;    SRC     : register: pointer to src data (not modified)
;    SIZE    : register: length in bytes (not modified)
;
; The name indicates the options. The name is of the form:
; simd_load_<VEC>_<SZ><ZERO>
; where:
; <VEC> is either "sse" or "avx"
; <SZ> is either "15" or "16" and defines largest value of SIZE
; <ZERO> is blank or "_1". If "_1" then the min SIZE is 1 (otherwise 0)
;
; For example:
; simd_load_sse_16		: SSE, 0 <= size <= 16
; simd_load_avx_15_1	        : AVX, 1 <= size <= 15

%macro simd_load_sse_15_1 3
        __simd_load %1,%2,%3,0,0,SSE
%endm
%macro simd_load_sse_15 3
        __simd_load %1,%2,%3,1,0,SSE
%endm
%macro simd_load_sse_16_1 3
        __simd_load %1,%2,%3,0,1,SSE
%endm
%macro simd_load_sse_16 3
        __simd_load %1,%2,%3,1,1,SSE
%endm

%macro simd_load_avx_15_1 3
        __simd_load %1,%2,%3,0,0,AVX
%endm
%macro simd_load_avx_15 3
        __simd_load %1,%2,%3,1,0,AVX
%endm
%macro simd_load_avx_16_1 3
        __simd_load %1,%2,%3,0,1,AVX
%endm
%macro simd_load_avx_16 3
        __simd_load %1,%2,%3,1,1,AVX
%endm

%macro __simd_load 6
%define %%DST       %1    ; [out] destination XMM register
%define %%SRC       %2    ; [in] pointer to src data
%define %%SIZE      %3    ; [in] length in bytes (0-16 bytes)
%define %%ACCEPT_0  %4    ; 0 = min length = 1, 1 = min length = 0
%define %%ACCEPT_16 %5    ; 0 = max length = 15 , 1 = max length = 16
%define %%SIMDTYPE  %6    ; "SSE" or "AVX"

%ifidn %%SIMDTYPE, SSE
 %define %%MOVDQU movdqu
 %define %%PINSRB pinsrb
 %define %%PINSRQ pinsrq
 %define %%PXOR   pxor
%else
 %define %%MOVDQU vmovdqu
 %define %%PINSRB vpinsrb
 %define %%PINSRQ vpinsrq
 %define %%PXOR   vpxor
%endif

%if (%%ACCEPT_16 != 0)
        test    %%SIZE, 16
        jz      %%_skip_16
        %%MOVDQU %%DST, [%%SRC]
        jmp     %%end_load

%%_skip_16:
%endif
        %%PXOR  %%DST, %%DST ; clear XMM register
%if (%%ACCEPT_0 != 0)
        or      %%SIZE, %%SIZE
        je      %%end_load
%endif
        cmp     %%SIZE, 2
        jb      %%_size_1
        je      %%_size_2
        cmp     %%SIZE, 4
        jb      %%_size_3
        je      %%_size_4
        cmp     %%SIZE, 6
        jb      %%_size_5
        je      %%_size_6
        cmp     %%SIZE, 8
        jb      %%_size_7
        je      %%_size_8
        cmp     %%SIZE, 10
        jb      %%_size_9
        je      %%_size_10
        cmp     %%SIZE, 12
        jb      %%_size_11
        je      %%_size_12
        cmp     %%SIZE, 14
        jb      %%_size_13
        je      %%_size_14

%%_size_15:
        %%PINSRB %%DST, [%%SRC + 14], 14
%%_size_14:
        %%PINSRB %%DST, [%%SRC + 13], 13
%%_size_13:
        %%PINSRB %%DST, [%%SRC + 12], 12
%%_size_12:
        %%PINSRB %%DST, [%%SRC + 11], 11
%%_size_11:
        %%PINSRB %%DST, [%%SRC + 10], 10
%%_size_10:
        %%PINSRB %%DST, [%%SRC + 9], 9
%%_size_9:
        %%PINSRB %%DST, [%%SRC + 8], 8
%%_size_8:
        %%PINSRQ %%DST, [%%SRC], 0
        jmp    %%end_load
%%_size_7:
        %%PINSRB %%DST, [%%SRC + 6], 6
%%_size_6:
        %%PINSRB %%DST, [%%SRC + 5], 5
%%_size_5:
        %%PINSRB %%DST, [%%SRC + 4], 4
%%_size_4:
        %%PINSRB %%DST, [%%SRC + 3], 3
%%_size_3:
        %%PINSRB %%DST, [%%SRC + 2], 2
%%_size_2:
        %%PINSRB %%DST, [%%SRC + 1], 1
%%_size_1:
        %%PINSRB %%DST, [%%SRC + 0], 0
%%end_load:
%endm

%macro simd_load_avx2 5
%define %%DST       %1    ; [out] destination YMM register
%define %%SRC       %2    ; [in] pointer to src data
%define %%SIZE      %3    ; [in] length in bytes (0-32 bytes)
%define %%IDX       %4    ; [clobbered] Temp GP register to store src idx
%define %%TMP       %5    ; [clobbered] Temp GP register

        test    %%SIZE, 32
        jz      %%_skip_32
        vmovdqu %%DST, [%%SRC]
        jmp     %%end_load

%%_skip_32:
        vpxor   %%DST, %%DST ; clear YMM register
        or      %%SIZE, %%SIZE
        je      %%end_load

        lea     %%IDX, [%%SRC]
        mov     %%TMP, %%SIZE
        cmp     %%SIZE, 16
        jle     %%_check_size

        add     %%IDX, 16
        sub     %%TMP, 16

%%_check_size:
        cmp     %%TMP, 2
        jb      %%_size_1
        je      %%_size_2
        cmp     %%TMP, 4
        jb      %%_size_3
        je      %%_size_4
        cmp     %%TMP, 6
        jb      %%_size_5
        je      %%_size_6
        cmp     %%TMP, 8
        jb      %%_size_7
        je      %%_size_8
        cmp     %%TMP, 10
        jb      %%_size_9
        je      %%_size_10
        cmp     %%TMP, 12
        jb      %%_size_11
        je      %%_size_12
        cmp     %%TMP, 14
        jb      %%_size_13
        je      %%_size_14
        cmp     %%TMP, 15
        je      %%_size_15

%%_size_16:
        vmovdqu XWORD(%%DST), [%%IDX]
        jmp    %%end_load
%%_size_15:
        vpinsrb XWORD(%%DST), [%%IDX + 14], 14
%%_size_14:
        vpinsrb XWORD(%%DST), [%%IDX + 13], 13
%%_size_13:
        vpinsrb XWORD(%%DST), [%%IDX + 12], 12
%%_size_12:
        vpinsrb XWORD(%%DST), [%%IDX + 11], 11
%%_size_11:
        vpinsrb XWORD(%%DST), [%%IDX + 10], 10
%%_size_10:
        vpinsrb XWORD(%%DST), [%%IDX + 9], 9
%%_size_9:
        vpinsrb XWORD(%%DST), [%%IDX + 8], 8
%%_size_8:
        vpinsrq XWORD(%%DST), [%%IDX], 0
        jmp    %%_check_higher_16
%%_size_7:
        vpinsrb XWORD(%%DST), [%%IDX + 6], 6
%%_size_6:
        vpinsrb XWORD(%%DST), [%%IDX + 5], 5
%%_size_5:
        vpinsrb XWORD(%%DST), [%%IDX + 4], 4
%%_size_4:
        vpinsrb XWORD(%%DST), [%%IDX + 3], 3
%%_size_3:
        vpinsrb XWORD(%%DST), [%%IDX + 2], 2
%%_size_2:
        vpinsrb XWORD(%%DST), [%%IDX + 1], 1
%%_size_1:
        vpinsrb XWORD(%%DST), [%%IDX + 0], 0
%%_check_higher_16:
        test    %%SIZE, 16
        jz      %%end_load

        ; Move last bytes loaded to upper half and load 16 bytes in lower half
        vinserti128 %%DST, XWORD(%%DST), 1
        vinserti128 %%DST, [%%SRC], 0
%%end_load:
%endm

%macro simd_store_avx2 5
%define %%DST      %1    ; register: pointer to dst (not modified)
%define %%SRC      %2    ; register: src data (clobbered)
%define %%SIZE     %3    ; register: length in bytes (not modified)
%define %%TMP      %4    ; 64-bit temp GPR (clobbered)
%define %%IDX      %5    ; 64-bit temp GPR to store dst idx (clobbered)

        xor %%IDX, %%IDX        ; zero idx

        test    %%SIZE, 32
        jz      %%lt32
        vmovdqu [%%DST], %%SRC
        jmp     %%end
%%lt32:

        test    %%SIZE, 16
        jz      %%lt16
        vmovdqu [%%DST], XWORD(%%SRC)
        ; Move upper half to lower half for further stores
        vperm2i128 %%SRC, %%SRC, %%SRC, 0x81
        add     %%IDX, 16
%%lt16:

        test    %%SIZE, 8
        jz      %%lt8
        vmovq  [%%DST + %%IDX], XWORD(%%SRC)
        vpsrldq XWORD(%%SRC), 8
        add     %%IDX, 8
%%lt8:

        vmovq %%TMP, XWORD(%%SRC)     ; use GPR from now on

        test    %%SIZE, 4
        jz      %%lt4
        mov     [%%DST + %%IDX], DWORD(%%TMP)
        shr     %%TMP, 32
        add     %%IDX, 4
%%lt4:

        test    %%SIZE, 2
        jz      %%lt2
        mov     [%%DST + %%IDX], WORD(%%TMP)
        shr     %%TMP, 16
        add     %%IDX, 2
%%lt2:
        test    %%SIZE, 1
        jz      %%end
        mov     [%%DST + %%IDX], BYTE(%%TMP)
%%end:
%endm

%endif ; ifndef __MEMCPY_INC__
