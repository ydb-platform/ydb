#include "backtrace_lib.h"

#include <libunwind.h>
#include <signal.h>

#include <util/system/backtrace.h>

namespace {
    size_t BackTrace(void** p, size_t len, ucontext_t* con) {
        unw_context_t context;
        unw_cursor_t cursor;
        if (unw_getcontext(&context)) {
            return 0;
        }

        if (unw_init_local(&cursor, &context)) {
            return 0;
        }
        const sigcontext* signal_mcontext = (const sigcontext*)&(con->uc_mcontext);
        unw_set_reg(&cursor, UNW_X86_64_RSI, signal_mcontext->rsi);
        unw_set_reg(&cursor, UNW_X86_64_RDI, signal_mcontext->rdi);
        unw_set_reg(&cursor, UNW_X86_64_RBP, signal_mcontext->rbp);
        unw_set_reg(&cursor, UNW_X86_64_RAX, signal_mcontext->rax);
        unw_set_reg(&cursor, UNW_X86_64_RBX, signal_mcontext->rbx);
        unw_set_reg(&cursor, UNW_X86_64_RCX, signal_mcontext->rcx);
        unw_set_reg(&cursor, UNW_X86_64_R8, signal_mcontext->r8);
        unw_set_reg(&cursor, UNW_X86_64_R9, signal_mcontext->r9);
        unw_set_reg(&cursor, UNW_X86_64_R10, signal_mcontext->r10);
        unw_set_reg(&cursor, UNW_X86_64_R11, signal_mcontext->r11);
        unw_set_reg(&cursor, UNW_X86_64_R12, signal_mcontext->r12);
        unw_set_reg(&cursor, UNW_X86_64_R13, signal_mcontext->r13);
        unw_set_reg(&cursor, UNW_X86_64_R14, signal_mcontext->r14);
        unw_set_reg(&cursor, UNW_X86_64_R15, signal_mcontext->r15);
        unw_set_reg(&cursor, UNW_X86_64_RSP, signal_mcontext->rsp);

        unw_set_reg(&cursor, UNW_REG_SP, signal_mcontext->rsp);
        unw_set_reg(&cursor, UNW_REG_IP, signal_mcontext->rip);

        size_t pos = 0;
        p[pos++] = (void*)signal_mcontext->rip;
        while (pos < len && unw_step(&cursor) > 0) {
            unw_word_t ip = 0;
            unw_get_reg(&cursor, UNW_REG_IP, &ip);
            if (unw_is_signal_frame(&cursor)) {
                continue;
            }
            p[pos++] = (void*)ip;
        }
        return pos;
    }
}

namespace NYql {
    namespace NBacktrace {
        size_t CollectBacktrace(void** addresses, size_t limit, void* data) {
            if (!data) {
                return BackTrace(addresses, limit);
            }
            return BackTrace(addresses, limit, reinterpret_cast<ucontext_t*>(data));
        }
    }
}