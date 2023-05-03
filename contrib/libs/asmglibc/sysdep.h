#if defined(__APPLE__)
    #define ENTRY(X) .globl _## X; .align 1<<3; _ ## X:
    #define END(X)
    #define L(X) L ## X
#else
    #define ENTRY(X) .globl X; .type X,@function; .align 1<<4; X: .cfi_startproc;
    #define END(X) .cfi_endproc; .size X,.-X;
    #define L(X) .L ## X
#endif

#define libc_hidden_builtin_def(X)
#define strong_alias(X, Y)
