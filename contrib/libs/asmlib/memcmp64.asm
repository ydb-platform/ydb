%include "defs.asm"

;*************************  memcmp64.asm  *************************************
; Author:           Agner Fog
; Date created:     2013-10-03
; Last modified:    2013-10-03
; Description:
; Faster version of the standard memcmp function:
;
; int A_memcmp (const void * ptr1, const void * ptr2, size_t count);
;
; Compares two memory blocks of size num.
; The return value is zero if the two memory blocks ptr1 and ptr2 are equal
; The return value is positive if the first differing byte of ptr1 is bigger 
; than ptr2 when compared as unsigned bytes.
; The return value is negative if the first differing byte of ptr1 is smaller 
; than ptr2 when compared as unsigned bytes.
;
; Overriding standard function memcmp:
; The alias ?OVR_memcmp is changed to _memcmp in the object file if
; it is desired to override the standard library function memcmp.
;
; Optimization:
; Uses XMM registers if SSE2 is available, uses YMM registers if AVX2.
;
; The latest version of this file is available at:
; www.agner.org/optimize/asmexamples.zip
; Copyright (c) 2013 GNU General Public License www.gnu.org/licenses
;******************************************************************************

global A_memcmp: function              ; Function memcmp
global EXP(memcmp): function           ; ?OVR_ removed if standard function memcmp overridden
; Direct entries to CPU-specific versions
global memcmpSSE2: function            ; SSE2 version
global memcmpAVX2: function            ; AVX2 version

; Imported from instrset64.asm
extern InstructionSet                 ; Instruction set for CPU dispatcher

default rel

; define registers used for parameters
%IFDEF  WINDOWS
%define par1   rcx                     ; function parameter 1
%define par2   rdx                     ; function parameter 2
%define par3   r8                      ; function parameter 3
%define par4   r9                      ; scratch register
%define par4d  r9d                     ; scratch register
%ENDIF
%IFDEF  UNIX
%define par1   rdi                     ; function parameter 1
%define par2   rsi                     ; function parameter 2
%define par3   rdx                     ; function parameter 3
%define par4   rcx                     ; scratch register
%define par4d  ecx                     ; scratch register
%ENDIF



SECTION .text  align=16

; extern "C" int A_memcmp (const void * ptr1, const void * ptr2, size_t count);
; Function entry:
A_memcmp:
EXP(memcmp):
        jmp     qword [memcmpDispatch] ; Go to appropriate version, depending on instruction set


align 16
memcmpAVX2:    ; AVX2 version. Use ymm register
memcmpAVX2@:   ; internal reference

        add     par1, par3                       ; use negative index from end of memory block
        add     par2, par3
        neg     par3
        jz      A900
        mov     par4d, 0FFFFH 
        cmp     par3, -32
        ja      A100
        
A000:   ; loop comparing 32 bytes
        vmovdqu   ymm1, [par1+par3]
        vpcmpeqb  ymm0, ymm1, [par2+par3]        ; compare 32 bytes
        vpmovmskb eax, ymm0                      ; get byte mask
        xor     eax, -1                          ; not eax would not set flags
        jnz     A700                             ; difference found
        add     par3, 32
        jz      A900                             ; finished, equal
        cmp     par3, -32
        jna     A000                             ; next 32 bytes
        vzeroupper                               ; end ymm state
        
A100:   ; less than 32 bytes left
        cmp     par3, -16
        ja      A200
        movdqu  xmm1, [par1+par3]
        movdqu  xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 16 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d                       ; invert lower 16 bits
        jnz     A701                             ; difference found
        add     par3, 16
        jz      A901                             ; finished, equal
        
A200:   ; less than 16 bytes left
        cmp     par3, -8
        ja      A300
        ; compare 8 bytes
        movq    xmm1, [par1+par3]
        movq    xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 8 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d
        jnz     A701                             ; difference found
        add     par3, 8
        jz      A901 
        
A300:   ; less than 8 bytes left
        cmp     par3, -4
        ja      A400
        ; compare 4 bytes
        movd    xmm1, [par1+par3]
        movd    xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 4 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d                         ; not ax
        jnz     A701                             ; difference found
        add     par3, 4
        jz      A901 

A400:   ; less than 4 bytes left
        cmp     par3, -2
        ja      A500
        movzx   eax, word [par1+par3]
        movzx   par4d, word [par2+par3]
        sub     eax, par4d
        jnz     A800                             ; difference in byte 0 or 1
        add     par3, 2
        jz      A901 
        
A500:   ; less than 2 bytes left
        test    par3, par3
        jz      A901                             ; no bytes left
        
A600:   ; one byte left
        movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

A700:   ; difference found. find position
        vzeroupper
A701:   
        bsf     eax, eax
        add     par3, rax
        movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

A800:   ; difference in byte 0 or 1
        neg     al
        sbb     par3, -1                           ; add 1 to par3 if al == 0
        movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

A900:   ; equal
        vzeroupper
A901:   xor     eax, eax        
        ret
        

memcmpSSE2:    ; SSE2 version. Use xmm register
memcmpSSE2@:   ; internal reference

        add     par1, par3                         ; use negative index from end of memory block
        add     par2, par3
        neg     par3
        jz      S900 
        mov     par4d, 0FFFFH
        cmp     par3, -16
        ja      S200
        
S100:   ; loop comparing 16 bytes
        movdqu  xmm1, [par1+par3]
        movdqu  xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 16 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d                         ; not ax
        jnz     S700                             ; difference found
        add     par3, 16
        jz      S900                             ; finished, equal
        cmp     par3, -16
        jna     S100                             ; next 16 bytes
        
S200:   ; less than 16 bytes left
        cmp     par3, -8
        ja      S300
        ; compare 8 bytes
        movq    xmm1, [par1+par3]
        movq    xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 8 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d                         ; not ax
        jnz     S700                             ; difference found
        add     par3, 8
        jz      S900 
        
S300:   ; less than 8 bytes left
        cmp     par3, -4
        ja      S400
        ; compare 4 bytes
        movd    xmm1, [par1+par3]
        movd    xmm2, [par2+par3]
        pcmpeqb xmm1, xmm2                       ; compare 4 bytes
        pmovmskb eax, xmm1                       ; get byte mask
        xor     eax, par4d                         ; not ax
        jnz     S700                             ; difference found
        add     par3, 4
        jz      S900 

S400:   ; less than 4 bytes left
        cmp     par3, -2
        ja      S500
        movzx   eax, word [par1+par3]
        movzx   par4d, word [par2+par3]
        sub     eax, par4d
        jnz     S800                             ; difference in byte 0 or 1
        add     par3, 2
        jz      S900 
        
S500:   ; less than 2 bytes left
        test    par3, par3
        jz      S900                             ; no bytes left
        
        ; one byte left
        movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

S700:   ; difference found. find position
        bsf     eax, eax
        add     par3, rax
        movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

S800:   ; difference in byte 0 or 1
        neg     al
        sbb     par3, -1                          ; add 1 to par3 if al == 0
S820:   movzx   eax, byte [par1+par3]
        movzx   par4d, byte [par2+par3]
        sub     eax, par4d                         ; return result
        ret

S900:   ; equal
        xor     eax, eax        
        ret

        
; CPU dispatching for memcmp. This is executed only once
memcmpCPUDispatch:
        push    par1
        push    par2
        push    par3        
        call    InstructionSet                         ; get supported instruction set
        ; SSE2 always supported
        lea     par4, [memcmpSSE2@]
        cmp     eax, 13                ; check AVX2
        jb      Q100
        ; AVX2 supported
        lea     par4, [memcmpAVX2@]        
Q100:   ; save pointer
        mov     qword [memcmpDispatch], par4
; Continue in appropriate version of memcmp
        pop     par3
        pop     par2
        pop     par1
        jmp     par4


SECTION .data
align 16


; Pointer to appropriate version.
; This initially points to memcmpCPUDispatch. memcmpCPUDispatch will
; change this to the appropriate version of memcmp, so that
; memcmpCPUDispatch is only executed once:
memcmpDispatch DQ memcmpCPUDispatch

