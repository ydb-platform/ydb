/* 
            Copyright Oliver Kowalke 2009. 
   Distributed under the Boost Software License, Version 1.0. 
      (See accompanying file LICENSE_1_0.txt or copy at 
            http://www.boost.org/LICENSE_1_0.txt) 
*/ 
 
/**************************************************************************************** 
 *                                                                                      * 
 *  ----------------------------------------------------------------------------------  * 
 *  |    0    |    1    |    2    |    3    |    4     |    5    |    6    |    7    |  * 
 *  ----------------------------------------------------------------------------------  * 
 *  |   0x0   |   0x4   |   0x8   |   0xc   |   0x10   |   0x14  |   0x18  |   0x1c  |  * 
 *  ----------------------------------------------------------------------------------  * 
 *  | fc_mxcsr|fc_x87_cw|       guard       |         R12        |        R13        |  *
 *  ----------------------------------------------------------------------------------  * 
 *  ----------------------------------------------------------------------------------  * 
 *  |    8    |    9    |   10    |   11    |    12    |    13   |    14   |    15   |  * 
 *  ----------------------------------------------------------------------------------  * 
 *  |   0x20  |   0x24  |   0x28  |  0x2c   |   0x30   |   0x34  |   0x38  |   0x3c  |  * 
 *  ----------------------------------------------------------------------------------  * 
 *  |        R14        |        R15        |         RBX        |        RBP        |  *
 *  ----------------------------------------------------------------------------------  * 
 *  ----------------------------------------------------------------------------------  *
 *  |   16    |   17    |   18    |   19    |    20    |    21   |    22   |    23   |  *
 *  ----------------------------------------------------------------------------------  *
 *  |   0x40  |   0x44  |                                                            |  *
 *  ----------------------------------------------------------------------------------  *
 *  |        RIP        |                                                            |  *
 *  ----------------------------------------------------------------------------------  *
 *                                                                                      * 
 ****************************************************************************************/ 
 
.text 
.globl jump_fcontext 
.type jump_fcontext,@function 
.align 16 
jump_fcontext: 
    leaq  -0x40(%rsp), %rsp /* prepare stack */
 
#if !defined(BOOST_USE_TSX) 
    stmxcsr  (%rsp)     /* save MMX control- and status-word */ 
    fnstcw   0x4(%rsp)  /* save x87 control-word */ 
#endif 
 
#if defined(TLS_STACK_PROTECTOR)
    movq  %fs:0x28, %rcx    /* read stack guard from TLS record */
    movq  %rcx, 0x8(%rsp)   /* save stack guard */
#endif
 
    movq  %r12, 0x10(%rsp)  /* save R12 */
    movq  %r13, 0x18(%rsp)  /* save R13 */
    movq  %r14, 0x20(%rsp)  /* save R14 */
    movq  %r15, 0x28(%rsp)  /* save R15 */
    movq  %rbx, 0x30(%rsp)  /* save RBX */
    movq  %rbp, 0x38(%rsp)  /* save RBP */

    /* store RSP (pointing to context-data) in RAX */ 
    movq  %rsp, %rax 
 
    /* restore RSP (pointing to context-data) from RDI */ 
    movq  %rdi, %rsp 
 
    movq  0x40(%rsp), %r8  /* restore return-address */
 
#if !defined(BOOST_USE_TSX) 
    ldmxcsr  (%rsp)     /* restore MMX control- and status-word */ 
    fldcw    0x4(%rsp)  /* restore x87 control-word */ 
#endif 
 
#if defined(TLS_STACK_PROTECTOR)
    movq  0x8(%rsp), %rdx  /* load stack guard */
    movq  %rdx, %fs:0x28   /* restore stack guard to TLS record */
#endif
 
    movq  0x10(%rsp), %r12  /* restore R12 */
    movq  0x18(%rsp), %r13  /* restore R13 */
    movq  0x20(%rsp), %r14  /* restore R14 */
    movq  0x28(%rsp), %r15  /* restore R15 */
    movq  0x30(%rsp), %rbx  /* restore RBX */
    movq  0x38(%rsp), %rbp  /* restore RBP */
 
    leaq  0x48(%rsp), %rsp /* prepare stack */

    /* return transfer_t from jump */ 
    /* RAX == fctx, RDX == data */ 
    movq  %rsi, %rdx 
    /* pass transfer_t as first arg in context function */ 
    /* RDI == fctx, RSI == data */ 
    movq  %rax, %rdi 
 
    /* indirect jump to context */ 
    jmp  *%r8 
.size jump_fcontext,.-jump_fcontext 
 
/* Mark that we don't need executable stack.  */ 
.section .note.GNU-stack,"",%progbits 
