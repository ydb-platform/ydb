%include "defs.asm"

;*************************  memset64.asm  *************************************
; Author:           Agner Fog
; Date created:     2008-07-19
; Last modified:    2016-11-12 (patched version with AVX512 support removed)
; Description:
; Faster version of the standard memset function:
; void * A_memset(void * dest, int c, size_t count);
; Sets 'count' bytes from 'dest' to the 8-bit value 'c'
;
; Overriding standard function memset:
; The alias ?OVR_memset is changed to _memset in the object file if
; it is desired to override the standard library function memset.
;
; extern "C" size_t GetMemsetCacheLimit(); // Data blocks bigger than this will be stored uncached by memset
; extern "C" void   SetMemsetCacheLimit(); // Change limit in GetMemsetCacheLimit
;
; Optimization:
; Uses XMM registers to set 16 bytes at a time, aligned.
;
; The latest version of this file is available at:
; www.agner.org/optimize/asmexamples.zip
; Copyright (c) 2008-2013 GNU General Public License www.gnu.org/licenses
;******************************************************************************

default rel

global A_memset: function              ; Function memset
global EXP(memset): function           ; ?OVR removed if standard function memset overridden
global memsetSSE2: function            ; SSE2 version
global memsetAVX: function             ; version for CPUs with fast 256-bit store
global GetMemsetCacheLimit: function   ; Data blocks bigger than this will be stored uncached by memset
global SetMemsetCacheLimit: function   ; Change limit in GetMemsetCacheLimit

; Imported from cachesize64.asm:
extern DataCacheSize                   ; Get size of data cache

; Imported from unalignedisfaster64.asm:
extern Store256BitIsFaster             ; Tells if a 256 bit store is faster than two 128 bit stores

; Define prolog for this function
%MACRO  PROLOGM  0
%IFDEF  WINDOWS
%define Rdest   rcx                    ; dest
        movzx   eax, dl                ; c
        mov     rdx, r8                ; count
%define Rcount  rdx                    ; count
%define Rdest2  r9                     ; copy of dest
%define Rcount2 r8                     ; copy of count

%ELSE   ; Unix
%define Rdest   rdi                    ; dest
        movzx   eax, sil               ; c
%define Rcount  rdx                    ; count
%define Rdest2  rcx                    ; copy of dest
%define Rcount2 rsi                    ; copy of count
        mov     Rcount2, Rcount        ; copy count
%ENDIF
%ENDMACRO


SECTION .text  align=16

; extern "C" void * memset(void * dest, int c, size_t count);
; Function entry:
A_memset:
EXP(memset):
        jmp     [memsetDispatch]       ; CPU dispatch table
        
memsetAVX:  ; AVX version. Use ymm register
memsetAVX@: ; local label
        PROLOGM
        imul    eax, 01010101H         ; Broadcast c into all bytes of eax
        mov     Rdest2, Rdest          ; save dest
        cmp     Rcount, 16
        ja      B100
B050:   lea     r10, [MemsetJTab]      ; SSE2 version comes in here
        jmp     qword [r10+Rcount*8]   ; jump table for small counts
        
; Separate code for each count from 0 to 16:
M16:    mov     [Rdest+12], eax
M12:    mov     [Rdest+8],  eax
M08:    mov     [Rdest+4],  eax
M04:    mov     [Rdest],    eax
M00:    mov     rax, Rdest2            ; return dest
        ret

M15:    mov     [Rdest+11], eax
M11:    mov     [Rdest+7],  eax
M07:    mov     [Rdest+3],  eax
M03:    mov     [Rdest+1],  ax
M01:    mov     [Rdest],    al
        mov     rax, Rdest2            ; return dest
        ret
       
M14:    mov     [Rdest+10], eax
M10:    mov     [Rdest+6],  eax
M06:    mov     [Rdest+2],  eax
M02:    mov     [Rdest],    ax
        mov     rax, Rdest2            ; return dest
        ret

M13:    mov     [Rdest+9],  eax
M09:    mov     [Rdest+5],  eax
M05:    mov     [Rdest+1],  eax
        mov     [Rdest],    al
        mov     rax, Rdest2            ; return dest
        ret
        
B100:   ; AVX version, Rcount > 16
        movd    xmm0, eax
        pshufd  xmm0, xmm0, 0          ; Broadcast c into all bytes of xmm0
        
        lea     rax, [Rdest+Rcount]    ; point to end
        
        cmp     Rcount, 20H
        jbe     K600                   ; faster to use xmm registers if small
        
        ; Store the first possibly unaligned 16 bytes
        ; It is faster to always write 16 bytes, possibly overlapping
        ; with the subsequent regular part, than to make possibly mispredicted
        ; branches depending on the size of the first part.
        movups  oword [Rdest], xmm0
        
        ; store another 16 bytes, aligned        
        add     Rdest, 10H
        and     Rdest, -10H
        movaps  oword [Rdest], xmm0
        
        ; go to next 32 bytes boundary
        add     Rdest, 10H
        and     Rdest, -20H
        
        ; Check if count very big
        cmp     Rcount, [MemsetCacheLimit]        
        ja      K300                   ; Use non-temporal store if count > MemsetCacheLimit
        
        ; find last 32 bytes boundary
        mov     Rcount, rax
        and     Rcount, -20H
        
        ; - size of 32-bytes blocks
        sub     Rdest, Rcount
        jnb     K200                   ; Jump if not negative
        
        ; extend value to 256 bits
        vinsertf128 ymm0,ymm0,xmm0,1
        
align   16        
K100:   ; Loop through 32-bytes blocks. Register use is swapped
        ; Rcount = end of 32-bytes blocks part
        ; Rdest = negative index from the end, counting up to zero
        vmovaps [Rcount+Rdest], ymm0
        add     Rdest, 20H
        jnz     K100
        vzeroupper
        
K200:   ; the last part from Rcount to rax is < 32 bytes. write last 32 bytes with overlap
        movups  [rax-20H], xmm0
        movups  [rax-10H], xmm0
        mov     rax, Rdest2            ; return dest
        ret
        
K300:   ; Use non-temporal moves, same code as above:

        ; find last 32 bytes boundary
        mov     Rcount, rax
        and     Rcount, -20H
        
        ; - size of 32-bytes blocks
        sub     Rdest, Rcount
        jnb     K500                   ; Jump if not negative
        
        ; extend value to 256 bits
        vinsertf128 ymm0,ymm0,xmm0,1
        
align   16        
K400:   ; Loop through 32-bytes blocks. Register use is swapped
        ; Rcount = end of 32-bytes blocks part
        ; Rdest = negative index from the end, counting up to zero
        vmovntps [Rcount+Rdest], ymm0
        add     Rdest, 20H
        jnz     K400
        sfence
        vzeroupper
        
K500:   ; the last part from Rcount to rax is < 32 bytes. write last 32 bytes with overlap
        movups  [rax-20H], xmm0
        movups  [rax-10H], xmm0
        mov     rax, Rdest2            ; return dest
        ret
        
K600:   ; 16 < count <= 32
        movups [Rdest], xmm0
        movups [rax-10H], xmm0
        mov     rax, Rdest2            ; return dest
        ret
        

memsetSSE2:  ; count > 16. Use SSE2 instruction set
memsetSSE2@: ; local label
        PROLOGM
        imul    eax, 01010101H         ; Broadcast c into all bytes of eax
        mov     Rdest2, Rdest          ; save dest
        cmp     Rcount, 16
        jna     B050

        movd    xmm0, eax
        pshufd  xmm0, xmm0, 0          ; Broadcast c into all bytes of xmm0
        
        ; Store the first unaligned part.
        ; The size of this part is 1 - 16 bytes.
        ; It is faster to always write 16 bytes, possibly overlapping
        ; with the subsequent regular part, than to make possibly mispredicted
        ; branches depending on the size of the first part.
        movq    qword [Rdest],   xmm0
        movq    qword [Rdest+8], xmm0
        
        ; Check if count very big
M150:   mov     rax, [MemsetCacheLimit]        
        cmp     Rcount, rax
        ja      M500                   ; Use non-temporal store if count > MemsetCacheLimit
        
        ; Point to end of regular part:
        ; Round down dest+count to nearest preceding 16-bytes boundary
        lea     Rcount, [Rdest+Rcount-1]
        and     Rcount, -10H
        
        ; Point to start of regular part:
        ; Round up dest to next 16-bytes boundary
        add     Rdest, 10H
        and     Rdest, -10H
        
        ; -(size of regular part)
        sub     Rdest, Rcount
        jnb     M300                   ; Jump if not negative
        
align 16
M200:   ; Loop through regular part
        ; Rcount = end of regular part
        ; Rdest = negative index from the end, counting up to zero
        movdqa  [Rcount+Rdest], xmm0
        add     Rdest, 10H
        jnz     M200
        
M300:   ; Do the last irregular part
        ; The size of this part is 1 - 16 bytes.
        ; It is faster to always write 16 bytes, possibly overlapping
        ; with the preceding regular part, than to make possibly mispredicted
        ; branches depending on the size of the last part.
        mov     rax, Rdest2                          ; dest
        movq    qword [rax+Rcount2-10H], xmm0
        movq    qword [rax+Rcount2-8], xmm0
        ret

        
M500:   ; Use non-temporal moves, same code as above:
        ; End of regular part:
        ; Round down dest+count to nearest preceding 16-bytes boundary
        lea     Rcount, [Rdest+Rcount-1]
        and     Rcount, -10H
        
        ; Start of regular part:
        ; Round up dest to next 16-bytes boundary
        add     Rdest, 10H
        and     Rdest, -10H
        
        ; -(size of regular part)
        sub     Rdest, Rcount
        jnb     M700                   ; Jump if not negative

align 16        
M600:   ; Loop through regular part
        ; Rcount = end of regular part
        ; Rdest = negative index from the end, counting up to zero
        movntdq [Rcount+Rdest], xmm0
        add     Rdest, 10H
        jnz     M600
        sfence

M700:   ; Do the last irregular part
        ; The size of this part is 1 - 16 bytes.
        ; It is faster to always write 16 bytes, possibly overlapping
        ; with the preceding regular part, than to make possibly mispredicted
        ; branches depending on the size of the last part.
        mov     rax, Rdest2            ; dest
        movq    qword [rax+Rcount2-10H], xmm0
        movq    qword [rax+Rcount2-8], xmm0
        ret
        
        
memsetCPUDispatch:    ; CPU dispatcher, check for instruction sets and which method is fastest        
        ; This part is executed only once
        push    rbx
        push    rcx
        push    rdx
        push    rsi
        push    rdi
        push    r8
        ; set CacheBypassLimit to half the size of the largest level cache
        call    GetMemsetCacheLimit@
        lea     rbx, [memsetSSE2@]
        call    Store256BitIsFaster    ; Test if 256-bit read/write is available and faster than 128-bit read/write
        test    eax, eax
        jz      Q100
        lea     rbx, [memsetAVX@]
Q100:
        ; Insert appropriate pointer
        mov     [memsetDispatch], rbx
        mov     rax, rbx
        pop     r8
        pop     rdi
        pop     rsi
        pop     rdx
        pop     rcx
        pop     rbx
        ; Jump according to the replaced function pointer
        jmp     rax

        
; extern "C" size_t GetMemsetCacheLimit(); // Data blocks bigger than this will be stored uncached by memset
GetMemsetCacheLimit:
GetMemsetCacheLimit@:
        mov     rax, [MemsetCacheLimit]
        test    rax, rax
        jnz     U200
        ; Get half the size of the largest level cache
%ifdef  WINDOWS
        xor     ecx, ecx               ; 0 means largest level cache
%else
        xor     edi, edi               ; 0 means largest level cache
%endif
        call    DataCacheSize          ; get cache size
        shr     eax, 1                 ; half the size
        jnz     U100
        mov     eax, 400000H           ; cannot determine cache size. use 4 Mbytes
U100:   mov     [MemsetCacheLimit], eax
U200:   ret

; extern "C" void   SetMemsetCacheLimit(); // Change limit in GetMemsetCacheLimit
SetMemsetCacheLimit:
%ifdef  WINDOWS
        mov     rax, rcx
%else
        mov     rax, rdi
%endif
        test    rax, rax
        jnz     U400
        ; zero, means default
        mov     [MemsetCacheLimit], rax
        call    GetMemsetCacheLimit@
U400:   mov     [MemsetCacheLimit], rax
        ret
        
   
SECTION .data
align 16
; Jump table for count from 0 to 16:
MemsetJTab:DQ M00, M01, M02, M03, M04, M05, M06, M07
           DQ M08, M09, M10, M11, M12, M13, M14, M15, M16
           
; Pointer to appropriate version.
; This initially points to memsetCPUDispatch. memsetCPUDispatch will
; change this to the appropriate version of memset, so that
; memsetCPUDispatch is only executed once:
memsetDispatch: DQ memsetCPUDispatch           

; Bypass cache by using non-temporal moves if count > MemsetCacheLimit
; The optimal value of MemsetCacheLimit is difficult to estimate, but
; a reasonable value is half the size of the largest cache
MemsetCacheLimit: DQ 0
