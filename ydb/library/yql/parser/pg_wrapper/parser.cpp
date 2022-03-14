#include "parser.h"

#include <util/generic/scope.h>
#include <util/memory/segmented_string_pool.h>
#include <fcntl.h>
#include <stdint.h>

#ifndef WIN32
#include <pthread.h>
#endif
#include <signal.h>

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
#include "mb/pg_wchar.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "parser/parser.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/resowner.h"
#include "port/pg_bitutils.h"
#include "port/pg_crc32c.h"
#include "thread_inits.h"
#undef Abs
#undef Min
#undef Max
#undef TypeName
#undef SortBy
#undef LOG
#undef INFO
#undef NOTICE
#undef WARNING
#undef ERROR
#undef FATAL
#undef PANIC
#undef open
#undef fopen
#undef bind
#undef locale_t
}

extern "C" {

extern void destroy_timezone_hashtable();
const char *progname;

#define STDERR_BUFFER_LEN 4096
#define DEBUG

typedef struct {
    char* message; // exception message
    char* funcname; // source function of exception (e.g. SearchSysCache)
    char* filename; // source of exception (e.g. parse.l)
    int lineno; // source of exception (e.g. 104)
    int cursorpos; // char in query at which exception occurred
    char* context; // additional context (optional, can be NULL)
} PgQueryError;

typedef struct {
    List *tree;
    char* stderr_buffer;
    PgQueryError* error;
} PgQueryInternalParsetreeAndError;

PgQueryInternalParsetreeAndError pg_query_raw_parse(const char* input) {
    PgQueryInternalParsetreeAndError result = { 0 };

    char stderr_buffer[STDERR_BUFFER_LEN + 1] = { 0 };
#ifndef DEBUG
    int stderr_global;
    int stderr_pipe[2];
#endif

#ifndef DEBUG
    // Setup pipe for stderr redirection
    if (pipe(stderr_pipe) != 0) {
        PgQueryError* error = (PgQueryError*)malloc(sizeof(PgQueryError));

        error->message = strdup("Failed to open pipe, too many open file descriptors");

        result.error = error;

        return result;
    }

    fcntl(stderr_pipe[0], F_SETFL, fcntl(stderr_pipe[0], F_GETFL) | O_NONBLOCK);

    // Redirect stderr to the pipe
    stderr_global = dup(STDERR_FILENO);
    dup2(stderr_pipe[1], STDERR_FILENO);
    close(stderr_pipe[1]);
#endif

    PG_TRY();
    {
        result.tree = raw_parser(input, RAW_PARSE_DEFAULT);

#ifndef DEBUG
        // Save stderr for result
        read(stderr_pipe[0], stderr_buffer, STDERR_BUFFER_LEN);
#endif

        result.stderr_buffer = strdup(stderr_buffer);
    }
    PG_CATCH();
    {
        ErrorData* error_data;
        PgQueryError* error;

        error_data = CopyErrorData();

        // Note: This is intentionally malloc so exiting the memory context doesn't free this
        error = (PgQueryError*)malloc(sizeof(PgQueryError));
        error->message = strdup(error_data->message);
        error->filename = strdup(error_data->filename);
        error->funcname = strdup(error_data->funcname);
        error->context = NULL;
        error->lineno = error_data->lineno;
        error->cursorpos = error_data->cursorpos;

        result.error = error;
        FlushErrorState();
    }
    PG_END_TRY();

#ifndef DEBUG
    // Restore stderr, close pipe
    dup2(stderr_global, STDERR_FILENO);
    close(stderr_pipe[0]);
    close(stderr_global);
#endif

    return result;
}

void pg_query_free_error(PgQueryError *error) {
    free(error->message);
    free(error->funcname);
    free(error->filename);

    if (error->context) {
        free(error->context);
    }

    free(error);
}

struct TAlloc {
    segmented_string_pool Pool;
};

__thread TAlloc* CurrentAlloc;

void *MyAllocSetAlloc(MemoryContext context, Size size) {
    auto fullSize = size + MAXIMUM_ALIGNOF - 1 + sizeof(void*);
    auto ptr = CurrentAlloc->Pool.Allocate(fullSize);
    auto aligned = (void*)MAXALIGN(ptr + sizeof(void*));
    *(MemoryContext *)(((char *)aligned) - sizeof(void *)) = context;
    return aligned;
}

void MyAllocSetFree(MemoryContext context, void* pointer) {
}

void* MyAllocSetRealloc(MemoryContext context, void* pointer, Size size) {
    if (!size) {
        return nullptr;
    }

    void* ret = MyAllocSetAlloc(context, size);
    if (pointer) {
        memmove(ret, pointer, size);
    }

    return ret;
}

void MyAllocSetReset(MemoryContext context) {
}

void MyAllocSetDelete(MemoryContext context) {
}

Size MyAllocSetGetChunkSpace(MemoryContext context, void* pointer) {
    return 0;
}

bool MyAllocSetIsEmpty(MemoryContext context) {
    return false;
}

void MyAllocSetStats(MemoryContext context,
    MemoryStatsPrintFunc printfunc, void *passthru,
    MemoryContextCounters *totals,
    bool print_to_stderr) {
}

void MyAllocSetCheck(MemoryContext context) {
}

const MemoryContextMethods MyMethods = {
    MyAllocSetAlloc,
    MyAllocSetFree,
    MyAllocSetRealloc,
    MyAllocSetReset,
    MyAllocSetDelete,
    MyAllocSetGetChunkSpace,
    MyAllocSetIsEmpty,
    MyAllocSetStats
#ifdef MEMORY_CONTEXT_CHECKING
    ,MyAllocSetCheck
#endif
};

}

namespace NYql {

static struct TGlobalInit {
    TGlobalInit() {
        pg_popcount32(0);
        pg_popcount64(0);
        pg_comp_crc32c(0,"",0);
    }
} GlobalInit;

void PGParse(const TString& input, IPGParseEvents& events) {
    pg_thread_init();

    MemoryContext ctx = NULL;
    PgQueryInternalParsetreeAndError parsetree_and_error;

    SetDatabaseEncoding(PG_UTF8);

    CurrentMemoryContext = (MemoryContext)malloc(sizeof(MemoryContextData));
    MemoryContextCreate(CurrentMemoryContext,
        T_AllocSetContext,
        &MyMethods,
        nullptr,
        "yql");
    ErrorContext = CurrentMemoryContext;

    Y_DEFER {
        free(CurrentMemoryContext);
        CurrentMemoryContext = nullptr;
        ErrorContext = nullptr;
    };

    TAlloc alloc;
    CurrentAlloc = &alloc;
    Y_DEFER {
        CurrentAlloc = nullptr;
    };

    parsetree_and_error = pg_query_raw_parse(input.c_str());
    Y_DEFER {
        if (parsetree_and_error.error) {
            pg_query_free_error(parsetree_and_error.error);
        }

        free(parsetree_and_error.stderr_buffer);
    };

    if (parsetree_and_error.error) {
        TPosition position(1, 1);
        TTextWalker walker(position);
        size_t distance = Min(size_t(parsetree_and_error.error->cursorpos), input.Size());
        for (size_t i = 0; i < distance; ++i) {
            walker.Advance(input[i]);
        }

        events.OnError(TIssue(position, TString(parsetree_and_error.error->message)));
    } else {
        events.OnResult(parsetree_and_error.tree);
    }
}

TString PrintPGTree(const List* raw) {
    auto str = nodeToString(raw);
    Y_DEFER {
       pfree(str);
    };

    return TString(str);
}

}

extern "C" void setup_pg_thread_cleanup() {
    struct TThreadCleanup {
        ~TThreadCleanup() {
            destroy_timezone_hashtable();
            ResourceOwnerDelete(CurrentResourceOwner);
            MemoryContextDelete(TopMemoryContext);
        }
    };

    static thread_local TThreadCleanup ThreadCleanup;
    MemoryContextInit();
    auto owner = ResourceOwnerCreate(NULL, "TopTransaction");
    TopTransactionResourceOwner = owner;
    CurTransactionResourceOwner = owner;
    CurrentResourceOwner = owner;
};
