;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;  Copyright(c) 2023 Intel Corporation All rights reserved.
;
;  Redistribution and use in source and binary forms, with or without
;  modification, are permitted provided that the following conditions
;  are met:
;    * Redistributions of source code must retain the above copyright
;      notice, this list of conditions and the following disclaimer.
;    * Redistributions in binary form must reproduce the above copyright
;      notice, this list of conditions and the following disclaimer in
;      the documentation and/or other materials provided with the
;      distribution.
;    * Neither the name of Intel Corporation nor the names of its
;      contributors may be used to endorse or promote products derived
;      from this software without specific prior written permission.
;
;  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
;  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
;  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
;  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
;  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
;  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
;  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
;  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
;  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
;  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
;  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;
; Multiply 1 source register to up to 6 different GF table registers
; and XOR the results to partial registers
;
%macro GF_MUL_XOR 5-20
%define %%ENCODING %1
%define %%SRC      %2
%define %%GFTABLE1 %3
%define %%TMP1     %4
%define %%PARTIAL1 %5
%define %%GFTABLE2 %6
%define %%TMP2     %7
%define %%PARTIAL2 %8
%define %%GFTABLE3 %9
%define %%TMP3     %10
%define %%PARTIAL3 %11
%define %%GFTABLE4 %12
%define %%TMP4     %13
%define %%PARTIAL4 %14
%define %%GFTABLE5 %15
%define %%TMP5     %16
%define %%PARTIAL5 %17
%define %%GFTABLE6 %18
%define %%TMP6     %19
%define %%PARTIAL6 %20

%define %%N_BLOCKS ((%0 - 2) / 3)

%assign %%I 1
%rep %%N_BLOCKS
        vgf2p8affineqb  %%TMP %+ %%I, %%SRC, %%GFTABLE %+ %%I, 0x00
%assign %%I (%%I + 1)
%endrep
%assign %%I 1
%rep %%N_BLOCKS
%ifidn %%ENCODING, VEX
        vpxor           %%PARTIAL %+ %%I, %%TMP %+ %%I
%else
        vpxorq          %%PARTIAL %+ %%I, %%TMP %+ %%I
%endif
%assign %%I (%%I + 1)
%endrep
%endmacro
