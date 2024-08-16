#include <ydb/library/yql/parser/pg_wrapper/interface/raw_parser.h>

#include "arena_ctx.h"

#include <util/generic/scope.h>
#include <fcntl.h>
#include <stdint.h>

#ifndef WIN32
#include <pthread.h>
#endif
#include <signal.h>

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#define Sort PG_Sort
#define Unique PG_Unique
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
#include "access/session.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "mb/pg_wchar.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "parser/parser.h"
#include "utils/guc.h"
#include "utils/palloc.h"
#include "utils/memutils.h"
#include "utils/memdebug.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/guc_hooks.h"
#include "port/pg_bitutils.h"
#include "port/pg_crc32c.h"
#include "postmaster/postmaster.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
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

extern __thread Latch LocalLatchData;
extern void destroy_timezone_hashtable();
extern void destroy_typecache_hashtable();
extern void free_current_locale_conv();
extern void RE_cleanup_cache();
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

}

namespace NYql {

static struct TGlobalInit {
    TGlobalInit() {
        pg_crc32c crc = 0;
        pg_popcount32(0);
        pg_popcount64(0);
        COMP_CRC32C(crc,"",0);
    }
} GlobalInit;

void PGParse(const TString& input, IPGParseEvents& events) {
    pg_thread_init();

    PgQueryInternalParsetreeAndError parsetree_and_error;

    TArenaMemoryContext arena;
    auto prevErrorContext = ErrorContext;
    ErrorContext = CurrentMemoryContext;

    Y_DEFER {
        ErrorContext = prevErrorContext;
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

        events.OnError(TIssue(position, "ERROR:  " + TString(parsetree_and_error.error->message) + "\n"));
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

TString GetCommandName(Node* node) {
    return CreateCommandName(node);
}

}

extern "C" void setup_pg_thread_cleanup() {
    struct TThreadCleanup {
        ~TThreadCleanup() {
            destroy_timezone_hashtable();
            destroy_typecache_hashtable();
            RE_cleanup_cache();

            free_current_locale_conv();
            ResourceOwnerDelete(CurrentResourceOwner);
            MemoryContextDelete(TopMemoryContext);
            free(MyProc);
        }
    };

    static thread_local TThreadCleanup ThreadCleanup;
    Log_error_verbosity = PGERROR_DEFAULT;
    SetDatabaseEncoding(PG_UTF8);
    SetClientEncoding(PG_UTF8);
    InitializeClientEncoding();
    MemoryContextInit();
    auto owner = ResourceOwnerCreate(NULL, "TopTransaction");
    TopTransactionResourceOwner = owner;
    CurTransactionResourceOwner = owner;
    CurrentResourceOwner = owner;

    MyProcPid = getpid();
    MyStartTimestamp = GetCurrentTimestamp();
    MyStartTime = timestamptz_to_time_t(MyStartTimestamp);

    InitializeLatchSupport();
    MyLatch = &LocalLatchData;
    InitLatch(MyLatch);
    InitializeLatchWaitSet();

    MyProc = (PGPROC*)malloc(sizeof(PGPROC));
    Zero(*MyProc);
    StartTransactionCommand();

    InitializeSession();
    work_mem = MAX_KILOBYTES; // a way to postpone spilling for tuple stores
    assign_max_stack_depth(1024, nullptr);
    MyDatabaseId = 3; // from catalog.pg_database
    namespace_search_path = pstrdup("public");
    InitializeSessionUserId(nullptr, 1);
};
