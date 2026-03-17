
#include "pyodbc.h"
#include "dbspecific.h"

void PrintBytes(void* p, size_t len)
{
    unsigned char* pch = (unsigned char*)p;
    for (size_t i = 0; i < len; i++)
        printf("%02x ", (int)pch[i]);
    printf("\n");
}

#define _MAKESTR(n) case n: return #n
const char* SqlTypeName(SQLSMALLINT n)
{
    switch (n)
    {
        _MAKESTR(SQL_UNKNOWN_TYPE);
        _MAKESTR(SQL_CHAR);
        _MAKESTR(SQL_VARCHAR);
        _MAKESTR(SQL_LONGVARCHAR);
        _MAKESTR(SQL_NUMERIC);
        _MAKESTR(SQL_DECIMAL);
        _MAKESTR(SQL_INTEGER);
        _MAKESTR(SQL_SMALLINT);
        _MAKESTR(SQL_FLOAT);
        _MAKESTR(SQL_REAL);
        _MAKESTR(SQL_DOUBLE);
        _MAKESTR(SQL_DATETIME);
        _MAKESTR(SQL_WCHAR);
        _MAKESTR(SQL_WVARCHAR);
        _MAKESTR(SQL_WLONGVARCHAR);
        _MAKESTR(SQL_TYPE_DATE);
        _MAKESTR(SQL_TYPE_TIME);
        _MAKESTR(SQL_TYPE_TIMESTAMP);
        _MAKESTR(SQL_SS_TIME2);
        _MAKESTR(SQL_SS_XML);
        _MAKESTR(SQL_BINARY);
        _MAKESTR(SQL_VARBINARY);
        _MAKESTR(SQL_LONGVARBINARY);
    }
    return "unknown";
}

const char* CTypeName(SQLSMALLINT n)
{
    switch (n)
    {
        _MAKESTR(SQL_C_CHAR);
        _MAKESTR(SQL_C_WCHAR);
        _MAKESTR(SQL_C_LONG);
        _MAKESTR(SQL_C_SHORT);
        _MAKESTR(SQL_C_FLOAT);
        _MAKESTR(SQL_C_DOUBLE);
        _MAKESTR(SQL_C_NUMERIC);
        _MAKESTR(SQL_C_DEFAULT);
        _MAKESTR(SQL_C_DATE);
        _MAKESTR(SQL_C_TIME);
        _MAKESTR(SQL_C_TIMESTAMP);
        _MAKESTR(SQL_C_TYPE_DATE);
        _MAKESTR(SQL_C_TYPE_TIME);
        _MAKESTR(SQL_C_TYPE_TIMESTAMP);
        _MAKESTR(SQL_C_INTERVAL_YEAR);
        _MAKESTR(SQL_C_INTERVAL_MONTH);
        _MAKESTR(SQL_C_INTERVAL_DAY);
        _MAKESTR(SQL_C_INTERVAL_HOUR);
        _MAKESTR(SQL_C_INTERVAL_MINUTE);
        _MAKESTR(SQL_C_INTERVAL_SECOND);
        _MAKESTR(SQL_C_INTERVAL_YEAR_TO_MONTH);
        _MAKESTR(SQL_C_INTERVAL_DAY_TO_HOUR);
        _MAKESTR(SQL_C_INTERVAL_DAY_TO_MINUTE);
        _MAKESTR(SQL_C_INTERVAL_DAY_TO_SECOND);
        _MAKESTR(SQL_C_INTERVAL_HOUR_TO_MINUTE);
        _MAKESTR(SQL_C_INTERVAL_HOUR_TO_SECOND);
        _MAKESTR(SQL_C_INTERVAL_MINUTE_TO_SECOND);
        _MAKESTR(SQL_C_BINARY);
        _MAKESTR(SQL_C_BIT);
        _MAKESTR(SQL_C_SBIGINT);
        _MAKESTR(SQL_C_UBIGINT);
        _MAKESTR(SQL_C_TINYINT);
        _MAKESTR(SQL_C_SLONG);
        _MAKESTR(SQL_C_SSHORT);
        _MAKESTR(SQL_C_STINYINT);
        _MAKESTR(SQL_C_ULONG);
        _MAKESTR(SQL_C_USHORT);
        _MAKESTR(SQL_C_UTINYINT);
        _MAKESTR(SQL_C_GUID);
    }
    return "unknown";
}


#ifdef PYODBC_TRACE
void DebugTrace(const char* szFmt, ...)
{
    va_list marker;
    va_start(marker, szFmt);
    vprintf(szFmt, marker);
    va_end(marker);
}
#endif

#ifdef PYODBC_LEAK_CHECK

// THIS IS NOT THREAD SAFE: This is only designed for the single-threaded unit tests!

struct Allocation
{
    const char* filename;
    int lineno;
    size_t len;
    void* pointer;
    int counter;
};

static Allocation* allocs = 0;
static int bufsize = 0;
static int count = 0;
static int allocCounter = 0;

void* _pyodbc_malloc(const char* filename, int lineno, size_t len)
{
    void* p = malloc(len);
    if (p == 0)
        return 0;

    if (count == bufsize)
    {
        allocs = (Allocation*)realloc(allocs, (bufsize + 20) * sizeof(Allocation));
        if (allocs == 0)
        {
            // Yes we just lost the original pointer, but we don't care since everything is about to fail.  This is a
            // debug leak check, not a production malloc that needs to be robust in low memory.
            bufsize = 0;
            count   = 0;
            return 0;
        }
        bufsize += 20;
    }

    allocs[count].filename = filename;
    allocs[count].lineno   = lineno;
    allocs[count].len      = len;
    allocs[count].pointer  = p;
    allocs[count].counter  = allocCounter++;

    printf("malloc(%d): %s(%d) %d %p\n", allocs[count].counter, filename, lineno, (int)len, p);

    count += 1;

    return p;
}

void pyodbc_free(void* p)
{
    if (p == 0)
        return;

    for (int i = 0; i < count; i++)
    {
        if (allocs[i].pointer == p)
        {
            printf("free(%d): %s(%d) %d %p i=%d\n", allocs[i].counter, allocs[i].filename, allocs[i].lineno, (int)allocs[i].len, allocs[i].pointer, i);
            memmove(&allocs[i], &allocs[i + 1], sizeof(Allocation) * (count - i - 1));
            count -= 1;
            free(p);
            return;
        }
    }

    printf("FREE FAILED: %p\n", p);
    free(p);
}

void pyodbc_leak_check()
{
    if (count == 0)
    {
        printf("NO LEAKS\n");
    }
    else
    {
        printf("********************************************************************************\n");
        printf("%d leaks\n", count);
        for (int i = 0; i < count; i++)
            printf("LEAK: %d %s(%d) len=%d\n", allocs[i].counter, allocs[i].filename, allocs[i].lineno, allocs[i].len);
    }
}

#endif
