#include <contrib/libs/libunwind/include/libunwind.h>
#include <signal.h>

size_t BackTrace(void** p, size_t len, ucontext_t* signal_ucontext) {
    unw_context_t unw_context = {};
    unw_getcontext(&unw_context);
    unw_cursor_t  unw_cursor = {};
    unw_init_local(&unw_cursor, &unw_context);
    const sigcontext* signal_mcontext = (const sigcontext*)&(signal_ucontext->uc_mcontext);
    
    unw_set_reg(&unw_cursor, UNW_X86_64_RSI, signal_mcontext->rsi);
    unw_set_reg(&unw_cursor, UNW_X86_64_RDI, signal_mcontext->rdi);
    unw_set_reg(&unw_cursor, UNW_X86_64_RBP, signal_mcontext->rbp);
    unw_set_reg(&unw_cursor, UNW_X86_64_RAX, signal_mcontext->rax);
    unw_set_reg(&unw_cursor, UNW_X86_64_RBX, signal_mcontext->rbx);
    unw_set_reg(&unw_cursor, UNW_X86_64_RCX, signal_mcontext->rcx);
    unw_set_reg(&unw_cursor, UNW_X86_64_R8, signal_mcontext->r8);
    unw_set_reg(&unw_cursor, UNW_X86_64_R9, signal_mcontext->r9);
    unw_set_reg(&unw_cursor, UNW_X86_64_R10, signal_mcontext->r10);
    unw_set_reg(&unw_cursor, UNW_X86_64_R11, signal_mcontext->r11);
    unw_set_reg(&unw_cursor, UNW_X86_64_R12, signal_mcontext->r12);
    unw_set_reg(&unw_cursor, UNW_X86_64_R13, signal_mcontext->r13);
    unw_set_reg(&unw_cursor, UNW_X86_64_R14, signal_mcontext->r14);
    unw_set_reg(&unw_cursor, UNW_X86_64_R15, signal_mcontext->r15);

    unw_set_reg(&unw_cursor, UNW_REG_SP, 8 + signal_mcontext->rsp);
    unw_set_reg(&unw_cursor, UNW_REG_IP, *(size_t*)signal_mcontext->rsp);

    size_t pos = 0;
    p[pos++] = (void*)signal_mcontext->rip;
    p[pos++] = (void*)(*(size_t*)signal_mcontext->rsp);
    while (pos < len && unw_step(&unw_cursor) > 0) {
        unw_word_t ip = 0;
        unw_get_reg(&unw_cursor, UNW_REG_IP, &ip);
        p[pos++] = (void*)ip;
    }
    return pos;
}
