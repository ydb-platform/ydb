/*
  A prepared statement cache for SQLite

  See the accompanying LICENSE file.
*/

/* sqlite3_prepare takes quite a while to run, and is often run on the
   same query over and over.  This statement cache uses extra memory
   saving previous prepares in order to save the cpu of repreparing.

   The implementation used for the first 18 years of APSW used a
   dictionary for the cache.  The key was the query string and the
   value was the prepared statement but also with a LRU linked list
   amongst the values.

   The biggest problem was that the same query submitted (but not
   completed) in overlapped usage would result in only the first one
   coming from the cache.  This could be fixed by having a list of
   values per key instead, but that ratchets up the complexity even
   further.  The values also had to be valid Python objects because
   Python's dictionary implementation was used, resulting in Python
   object overhead such as ref counts, (de)allocation/gc etc.

   This second implementation is simpler and allows having multiple
   entries for the same query.  The primary data structure is an array
   of hash values.  Finding an entry is a linear search (fast on
   modern cpus).  Entries are removed while in use. When finished they
   are placed back in a circular order, which then evicts the oldest
   entry.

   A copy of the query has to be kept around for doing equality
   comparisons when looking in the cache.  But sqlite also keeps a
   copy of the query, so we try to use that if possible.
*/

typedef struct APSWStatementOptions
{
  int can_cache;     /* are we allowed to cache this statement */
  int prepare_flags; /* sqlite3_prepare_v3 flags */
} APSWStatementOptions;

typedef struct APSWStatement
{
  sqlite3_stmt *vdbestatement; /* the sqlite level vdbe code */
  PyObject *query;             /* a PyUnicode object - source of the utf8 */
  const char *utf8;            /* pointer to the utf8 */
  Py_ssize_t utf8_size;        /* length of the utf8 in bytes */
  Py_ssize_t query_size;       /* how many bytes of utf8 constitute the first query
                                  (the utf8 could have more than one query) */
  Py_hash_t hash;              /* hash of all of utf8 */
  APSWStatementOptions options;
  unsigned uses; /* how many times the prepared statement has been (re)used */
} APSWStatement;

typedef struct StatementCache
{
  Py_hash_t *hashes;      /* array of hash values */
  APSWStatement **caches; /* corresponding statements */
  sqlite3 *db;            /* db to work against */
  unsigned highest_used;  /* largest entry we have used - no point scanning beyond */
  unsigned maxentries;    /* maximum number of entries */
  unsigned next_eviction; /* which entry is evicted next */
  /* stats tracking */
  unsigned evictions; /* how many there have been */
  unsigned no_cache;  /* can cache was false */
  unsigned hits;      /* found in cache */
  unsigned misses;    /* not found in cache */
  unsigned no_vdbe;   /* no bytecode emitted */
  unsigned too_big;   /* query was bigger than SC_MAX_ITEM_SIZE */
} StatementCache;

/* we don't bother caching larger than this many bytes */
#define SC_MAX_ITEM_SIZE 16384

/* the hash value we use for unoccupied */
#define SC_SENTINEL_HASH (-1)

/* recycle bin for APSWStatements to avoid repeated malloc/free calls */
#define SC_STATEMENT_RECYCLE_BIN_ENTRIES 32

#if SC_STATEMENT_RECYCLE_BIN_ENTRIES > 0
static APSWStatement *apsw_sc_recycle_bin[SC_STATEMENT_RECYCLE_BIN_ENTRIES];
static unsigned apsw_sc_recycle_bin_next = 0;
#endif

static int
statementcache_free_statement(StatementCache *sc, APSWStatement *s)
{
  int res;

  Py_CLEAR(s->query);

  PYSQLITE_SC_CALL(res = sqlite3_finalize(s->vdbestatement));

#if SC_STATEMENT_RECYCLE_BIN_ENTRIES > 0
  if (apsw_sc_recycle_bin_next + 1 < SC_STATEMENT_RECYCLE_BIN_ENTRIES)
    apsw_sc_recycle_bin[apsw_sc_recycle_bin_next++] = s;
  else
#endif

    PyMem_Free(s);

  return res;
}

static int
statementcache_hasmore(APSWStatement *statement)
{
  return statement ? (statement->query_size != statement->utf8_size) : 0;
}

/* completely done with this statement */
static int
statementcache_finalize(StatementCache *sc, APSWStatement *statement)
{
  int res = SQLITE_OK;
  if (!statement)
    return res;

  if (statement->hash != SC_SENTINEL_HASH)
  {
    APSWStatement *evictee = NULL;

    PYSQLITE_SC_CALL(res = sqlite3_reset(statement->vdbestatement));

    /*
      https://sqlite.org/forum/forumpost/d72cba6ff7

      window function final can be called to do cleanup during reset.  If it
      returns an error, SQLite does not pass on the error.
    */
    if (res == SQLITE_OK && PyErr_Occurred())
      res = SQLITE_ERROR;

    if (sc->caches[sc->next_eviction])
    {
      assert(sc->hashes[sc->next_eviction] != SC_SENTINEL_HASH);
      evictee = sc->caches[sc->next_eviction];
    }
    sc->hashes[sc->next_eviction] = statement->hash;
    sc->caches[sc->next_eviction] = statement;
    sc->highest_used = Py_MAX(sc->highest_used, sc->next_eviction);
    sc->next_eviction++;
    if (sc->next_eviction == sc->maxentries)
      sc->next_eviction = 0;
    if (evictee)
    {
      statementcache_free_statement(sc, evictee);
      sc->evictions++;
    }
  }
  else
  {
    /* not caching */
    res = statementcache_free_statement(sc, statement);
    if (res == SQLITE_OK && PyErr_Occurred())
      res = SQLITE_ERROR;
  }
  return res;
}

static int
statementcache_prepare_internal(StatementCache *sc, const char *utf8, Py_ssize_t utf8size, PyObject *query, APSWStatement **statement_out, APSWStatementOptions *options)
{
  Py_hash_t hash = SC_SENTINEL_HASH;
  APSWStatement *statement = NULL;
  const char *tail = NULL;
  const char *orig_tail = NULL;
  sqlite3_stmt *vdbestatement = NULL;
  int res = SQLITE_OK;

  *statement_out = NULL;
  if (sc->maxentries && utf8size < SC_MAX_ITEM_SIZE && options->can_cache)
  {
    unsigned i;
#ifdef PYPY_VERSION
    hash = utf8size;
#else
    hash = _Py_HashBytes(utf8, utf8size);
#endif
    for (i = 0; i <= sc->highest_used; i++)
    {
      if (sc->hashes[i] == hash && sc->caches[i]->utf8_size == utf8size && 0 == memcmp(utf8, sc->caches[i]->utf8, utf8size) && 0 == memcmp(&sc->caches[i]->options, options, sizeof(APSWStatementOptions)))
      {
        /* cache hit */
        sc->hashes[i] = SC_SENTINEL_HASH;
        statement = sc->caches[i];
        sc->caches[i] = NULL;
        APSW_FAULT_INJECT(SCClearBindingsFails,
                          PYSQLITE_SC_CALL(res = sqlite3_clear_bindings(statement->vdbestatement)),
                          res = SQLITE_NOMEM);
        if (res)
        {
          SET_EXC(res, sc->db);
          statementcache_finalize(sc, statement);
          return res;
        }
        *statement_out = statement;
        statement->uses++;
        sc->hits++;
        assert(res == SQLITE_OK);
        return res;
      }
    }
  }
  /* cache miss */

  /* Undocumented stuff alert:  if the size passed to sqlite3_prepare
     doesn't include the trailing null then sqlite makes a copy of the
     sql text in order to run on a buffer that does have a trailing
     null.  When using speedtest bigstmt (about 20MB of sql text)
     runtime goes from 2 seconds to 2 minutes due to that copying
     which happens on each statement as we progress through the sql.

     The utf8 we originally got from PyUnicode_AsUTF8AndSize is
     documented to always have a trailing null (not included in the
     size) so we have an assert to verify that, and add one to the
     length passed to sqlite3_prepare */

  assert(0 == utf8[utf8size]);
  /* note that prepare can return ok while a python level occurred that couldn't be reported */
  PYSQLITE_SC_CALL(res = sqlite3_prepare_v3(sc->db, utf8, utf8size + 1, options->prepare_flags, &vdbestatement, &tail));
  if (!*tail && tail - utf8 < utf8size)
    PyErr_Format(PyExc_ValueError, "null character in query");
  if (res != SQLITE_OK || PyErr_Occurred())
  {
    SET_EXC(res, sc->db);
    PYSQLITE_SC_CALL(sqlite3_finalize(vdbestatement));
    return res ? res : SQLITE_ERROR;
  }

  orig_tail = tail;

  /* skip whitespace and semis in tail */
  while (*tail && (*tail == ' ' || *tail == '\t' || *tail == ';' || *tail == '\r' || *tail == '\n'))
    tail++;

  /* comments and some pragmas result in no vdbe, which we shouldn't
     cache either */
  if (!vdbestatement)
    hash = SC_SENTINEL_HASH;

#if SC_STATEMENT_RECYCLE_BIN_ENTRIES > 0
  if (apsw_sc_recycle_bin_next)
    statement = apsw_sc_recycle_bin[--apsw_sc_recycle_bin_next];
  else
#endif
  {
    APSW_FAULT_INJECT(SCAllocFails, statement = PyMem_Malloc(sizeof(APSWStatement)), statement = NULL);
    if (!statement)
    {
      PYSQLITE_SC_CALL(sqlite3_finalize(vdbestatement));
      res = SQLITE_NOMEM;
      SET_EXC(res, sc->db);
      return res;
    }
  }

  sc->misses++;
  if (!options->can_cache)
    sc->no_cache++;
  else if (utf8size >= SC_MAX_ITEM_SIZE)
    sc->too_big++;

  statement->hash = hash;
  statement->vdbestatement = vdbestatement;
  statement->query_size = tail - utf8;
  statement->utf8_size = utf8size;
  statement->uses = 1;
  memcpy(&statement->options, options, sizeof(APSWStatementOptions));

  if (tail == orig_tail && !statementcache_hasmore(statement))
  {
    /* no subsequent queries, so use sqlite's copy of the utf8
       providing we didn't grab additional whitespace */
    statement->utf8 = sqlite3_sql(vdbestatement); /* No PYSQLITE_CALL needed as the function does not take a mutex */
    statement->query = NULL;
  }
  else
  {
    assert(query);
    statement->utf8 = utf8;
    statement->query = query;
    Py_INCREF(query);
  }
  *statement_out = statement;
  if (!vdbestatement)
    sc->no_vdbe++;
  return SQLITE_OK;
}

static APSWStatement *
statementcache_prepare(StatementCache *sc, PyObject *query, APSWStatementOptions *options)
{
  const char *utf8 = NULL;
  Py_ssize_t utf8size = 0;
  APSWStatement *statement = NULL;
  int res;

  assert(options->can_cache == 0 || options->can_cache == 1);
  assert(PyUnicode_Check(query));
  utf8 = PyUnicode_AsUTF8AndSize(query, &utf8size);
  if (!utf8)
    return NULL;

  res = statementcache_prepare_internal(sc, utf8, utf8size, query, &statement, options);
  assert((res == SQLITE_OK && statement && !PyErr_Occurred()) || (res != SQLITE_OK && !statement));
  if (res)
    SET_EXC(res, sc->db);
  return statement;
}

/* statement has more, so finalize one being pointed to and then
   replace with next statement in the query */
static int
statementcache_next(StatementCache *sc, APSWStatement **statement)
{
  APSWStatement *old = *statement, *new = NULL;
  int res, res2;

  *statement = NULL;
  assert(statementcache_hasmore(old));

  /* we have to prepare the new one ... */
  res = statementcache_prepare_internal(sc, old->utf8 + old->query_size, old->utf8_size - old->query_size, old->query, &new, &old->options);
  assert((res == SQLITE_OK && new) || (res != SQLITE_OK && !new));

  /* ... before finalizing the old */
  res2 = statementcache_finalize(sc, old);

  if (res || res2)
  {
    statementcache_finalize(sc, new);
    if (res2) /* report finalizing old error */
      return res2;
    return res;
  }
  *statement = new;
  return SQLITE_OK;
}

static void
statementcache_free(StatementCache *sc)
{
  if (sc)
  {
    PyMem_Free(sc->hashes);
    if (sc->caches)
    {
      unsigned i;
      for (i = 0; i <= sc->highest_used; i++)
        if (sc->caches[i])
        {
          statementcache_free_statement(sc, sc->caches[i]);
        }
    }
    PyMem_Free(sc->caches);
    PyMem_Free(sc);
  }
}

static StatementCache *
statementcache_init(sqlite3 *db, unsigned size)
{
  StatementCache *res;
  APSW_FAULT_INJECT(StatementCacheAllocFails, res = (StatementCache *)PyMem_Calloc(1, sizeof(StatementCache)), res = NULL);
  if (res)
  {
    res->hashes = size ? PyMem_Calloc(size, sizeof(Py_hash_t)) : 0;
    res->caches = size ? PyMem_Calloc(size, sizeof(APSWStatement *)) : 0;
    res->maxentries = size;
    res->db = db;
    if (res->hashes)
    {
      unsigned i;
      for (i = 0; i <= res->highest_used; i++)
        res->hashes[i] = SC_SENTINEL_HASH;
    }
  }
  if (!res || (size && !res->hashes) || (size && !res->caches))
  {
    statementcache_free(res);
    res = NULL;
    PyErr_NoMemory();
  }
  return res;
}

static PyObject *
statementcache_stats(StatementCache *sc, int include_entries)
{
  /* Update the table of explanations in Connection_cache_stats if you
     update this */
  PyObject *res = NULL, *entries = NULL, *entry = NULL;

  APSW_FAULT_INJECT(SCStatsBuildFail,
                    res = Py_BuildValue("{s: I, s: I, s: I, s: I, s: I, s: I, s: I, s: I, s: I}",
                                        "size", sc->maxentries,
                                        "evictions", sc->evictions,
                                        "no_cache", sc->no_cache,
                                        "hits", sc->hits,
                                        "no_vdbe", sc->no_vdbe,
                                        "misses", sc->misses,
                                        "too_big", sc->too_big,
                                        "no_cache", sc->no_cache,
                                        "max_cacheable_bytes", SC_MAX_ITEM_SIZE),
                    PyErr_NoMemory());
  if (res && include_entries)
  {
    int pycres;
    APSW_FAULT_INJECT(SCStatsListFail, entries = PyList_New(0), entries = PyErr_NoMemory());
    if (!entries)
      goto fail;

    unsigned i;
    for (i = 0; sc->hashes && i <= sc->highest_used; i++)
    {
      if (sc->hashes[i] != SC_SENTINEL_HASH)
      {
        APSWStatement *stmt = sc->caches[i];
        APSW_FAULT_INJECT(SCStatsEntryBuildFail,
                          entry = Py_BuildValue("{s: s#, s: O, s: i, s: I}",
                                                "query", stmt->utf8, stmt->query_size,
                                                "has_more", (stmt->query_size == stmt->utf8_size) ? Py_False : Py_True,
                                                "prepare_flags", stmt->options.prepare_flags,
                                                "uses", stmt->uses),
                          entry = PyErr_NoMemory());
        if (!entry)
          goto fail;
        APSW_FAULT_INJECT(SCStatsAppendFail, pycres = PyList_Append(entries, entry), (PyErr_NoMemory(), pycres = -1));
        if (pycres)
          goto fail;
        Py_CLEAR(entry);
      }
    }
    APSW_FAULT_INJECT(SCStatsEntriesSetFail, pycres = PyDict_SetItemString(res, "entries", entries), (PyErr_NoMemory(), pycres = -1));
    if (pycres)
      goto fail;
    Py_DECREF(entries);
  }
  return res;

fail:
  Py_XDECREF(entries);
  Py_XDECREF(res);
  Py_XDECREF(entry);
  return NULL;
}

#ifdef APSW_TESTFIXTURES
static void
statementcache_fini(void)
{
#if SC_STATEMENT_RECYCLE_BIN_ENTRIES > 0
  while (apsw_sc_recycle_bin_next)
    PyMem_Free(apsw_sc_recycle_bin[apsw_sc_recycle_bin_next--]);
#endif
}
#endif