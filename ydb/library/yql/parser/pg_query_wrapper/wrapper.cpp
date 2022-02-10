#include "wrapper.h"

#include <util/generic/scope.h>
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
#undef Min
#undef Max
#undef TypeName
#undef SortBy
}

extern "C" {
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
    MemoryContext parse_context = CurrentMemoryContext;

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
        result.tree = raw_parser(input);

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

        MemoryContextSwitchTo(parse_context);
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

void pg_query_free_top_memory_context(MemoryContext context)
{
    AssertArg(MemoryContextIsValid(context));

    /*
     * After this, no memory contexts are valid anymore, so ensure that
     * the current context is the top-level context.
     */
    Assert(TopMemoryContext == CurrentMemoryContext);

    MemoryContextDeleteChildren(context);

    /* Clean up the aset.c freelist, to leave no unused context behind */
    AllocSetDeleteFreeList(context);

    context->methods->delete_context(context);

    VALGRIND_DESTROY_MEMPOOL(context);

    /* Without this, Valgrind will complain */
    free(context);

    /* Reset pointers */
    TopMemoryContext = NULL;
    CurrentMemoryContext = NULL;
    ErrorContext = NULL;
}

__thread volatile sig_atomic_t pg_query_initialized = 0;

#ifndef WIN32
static pthread_key_t pg_query_thread_exit_key;
static void pg_query_thread_exit(void *key);
#endif

#ifndef WIN32
static void pg_query_thread_exit(void *key)
{
    MemoryContext context = (MemoryContext)key;
    pg_query_free_top_memory_context(context);
}
#endif

void pg_query_init(void)
{
    if (pg_query_initialized != 0) return;
    pg_query_initialized = 1;

    MemoryContextInit();
    SetDatabaseEncoding(PG_UTF8);

#ifndef WIN32
    pthread_key_create(&pg_query_thread_exit_key, pg_query_thread_exit);
    pthread_setspecific(pg_query_thread_exit_key, TopMemoryContext);
#endif
}

MemoryContext pg_query_enter_memory_context() {
    MemoryContext ctx = NULL;

    pg_query_init();

    Assert(CurrentMemoryContext == TopMemoryContext);
    ctx = AllocSetContextCreate(TopMemoryContext,
        "pg_query",
        ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(ctx);

    return ctx;
}

void pg_query_exit_memory_context(MemoryContext ctx) {
    // Return to previous PostgreSQL memory context
    MemoryContextSwitchTo(TopMemoryContext);

    MemoryContextDelete(ctx);
    ctx = NULL;
}

}

namespace NYql {

void PGParse(const TString& input, IPGParseEvents& events) {
    MemoryContext ctx = NULL;
    PgQueryInternalParsetreeAndError parsetree_and_error;

    ctx = pg_query_enter_memory_context();
    Y_DEFER {
        pg_query_exit_memory_context(ctx);
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

