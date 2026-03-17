/* Do not edit: automatically built by gen_rec.awk. */

#ifndef	__ham_AUTO_H
#define	__ham_AUTO_H
#ifdef HAVE_HASH
#include <contrib/deprecated/bdb/src/dbinc/log.h>
#define	DB___ham_insdel	21
typedef struct ___ham_insdel_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	opcode;
	int32_t	fileid;
	db_pgno_t	pgno;
	u_int32_t	ndx;
	DB_LSN	pagelsn;
	u_int32_t	keytype;
	DBT	key;
	u_int32_t	datatype;
	DBT	data;
} __ham_insdel_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_insdel_desc[];
static inline int
__ham_insdel_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    u_int32_t opcode, db_pgno_t pgno, u_int32_t ndx, DB_LSN * pagelsn,
    u_int32_t keytype, const DBT *key, u_int32_t datatype, const DBT *data)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_insdel, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(*pagelsn) + sizeof(u_int32_t) +
	    LOG_DBT_SIZE(key) + sizeof(u_int32_t) + LOG_DBT_SIZE(data),
	    __ham_insdel_desc,
	    opcode, pgno, ndx, pagelsn, keytype, key, datatype,
	    data));
}

static inline int __ham_insdel_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_insdel_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_insdel_desc, sizeof(__ham_insdel_args), (void**)arg));
}
#define	DB___ham_insdel_42	21
typedef struct ___ham_insdel_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	opcode;
	int32_t	fileid;
	db_pgno_t	pgno;
	u_int32_t	ndx;
	DB_LSN	pagelsn;
	DBT	key;
	DBT	data;
} __ham_insdel_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_insdel_42_desc[];
static inline int __ham_insdel_42_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_insdel_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_insdel_42_desc, sizeof(__ham_insdel_42_args), (void**)arg));
}
#define	DB___ham_newpage	22
typedef struct ___ham_newpage_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	opcode;
	int32_t	fileid;
	db_pgno_t	prev_pgno;
	DB_LSN	prevlsn;
	db_pgno_t	new_pgno;
	DB_LSN	pagelsn;
	db_pgno_t	next_pgno;
	DB_LSN	nextlsn;
} __ham_newpage_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_newpage_desc[];
static inline int
__ham_newpage_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    u_int32_t opcode, db_pgno_t prev_pgno, DB_LSN * prevlsn, db_pgno_t new_pgno,
    DB_LSN * pagelsn, db_pgno_t next_pgno, DB_LSN * nextlsn)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_newpage, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(*prevlsn) + sizeof(u_int32_t) + sizeof(*pagelsn) +
	    sizeof(u_int32_t) + sizeof(*nextlsn),
	    __ham_newpage_desc,
	    opcode, prev_pgno, prevlsn, new_pgno, pagelsn, next_pgno, nextlsn));
}

static inline int __ham_newpage_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_newpage_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_newpage_desc, sizeof(__ham_newpage_args), (void**)arg));
}
#define	DB___ham_splitdata	24
typedef struct ___ham_splitdata_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	u_int32_t	opcode;
	db_pgno_t	pgno;
	DBT	pageimage;
	DB_LSN	pagelsn;
} __ham_splitdata_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_splitdata_desc[];
static inline int
__ham_splitdata_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, u_int32_t opcode, db_pgno_t pgno, const DBT *pageimage, DB_LSN * pagelsn)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_splitdata, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    LOG_DBT_SIZE(pageimage) + sizeof(*pagelsn),
	    __ham_splitdata_desc, opcode, pgno, pageimage, pagelsn));
}

static inline int __ham_splitdata_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_splitdata_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_splitdata_desc, sizeof(__ham_splitdata_args), (void**)arg));
}
#define	DB___ham_replace	25
typedef struct ___ham_replace_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	u_int32_t	ndx;
	DB_LSN	pagelsn;
	int32_t	off;
	u_int32_t	oldtype;
	DBT	olditem;
	u_int32_t	newtype;
	DBT	newitem;
} __ham_replace_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_replace_desc[];
static inline int
__ham_replace_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, u_int32_t ndx, DB_LSN * pagelsn, int32_t off,
    u_int32_t oldtype, const DBT *olditem, u_int32_t newtype, const DBT *newitem)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_replace, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(*pagelsn) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    LOG_DBT_SIZE(olditem) + sizeof(u_int32_t) + LOG_DBT_SIZE(newitem),
	    __ham_replace_desc, pgno, ndx, pagelsn, off, oldtype, olditem, newtype,
	    newitem));
}

static inline int __ham_replace_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_replace_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_replace_desc, sizeof(__ham_replace_args), (void**)arg));
}
#define	DB___ham_replace_42	25
typedef struct ___ham_replace_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	u_int32_t	ndx;
	DB_LSN	pagelsn;
	int32_t	off;
	DBT	olditem;
	DBT	newitem;
	u_int32_t	makedup;
} __ham_replace_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_replace_42_desc[];
static inline int __ham_replace_42_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_replace_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_replace_42_desc, sizeof(__ham_replace_42_args), (void**)arg));
}
#define	DB___ham_copypage	28
typedef struct ___ham_copypage_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DB_LSN	pagelsn;
	db_pgno_t	next_pgno;
	DB_LSN	nextlsn;
	db_pgno_t	nnext_pgno;
	DB_LSN	nnextlsn;
	DBT	page;
} __ham_copypage_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_copypage_desc[];
static inline int
__ham_copypage_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, DB_LSN * pagelsn, db_pgno_t next_pgno, DB_LSN * nextlsn,
    db_pgno_t nnext_pgno, DB_LSN * nnextlsn, const DBT *page)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_copypage, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(*pagelsn) +
	    sizeof(u_int32_t) + sizeof(*nextlsn) + sizeof(u_int32_t) +
	    sizeof(*nnextlsn) + LOG_DBT_SIZE(page),
	    __ham_copypage_desc, pgno, pagelsn, next_pgno, nextlsn, nnext_pgno, nnextlsn, page));
}

static inline int __ham_copypage_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_copypage_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_copypage_desc, sizeof(__ham_copypage_args), (void**)arg));
}
#define	DB___ham_metagroup_42	29
typedef struct ___ham_metagroup_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	u_int32_t	bucket;
	db_pgno_t	mmpgno;
	DB_LSN	mmetalsn;
	db_pgno_t	mpgno;
	DB_LSN	metalsn;
	db_pgno_t	pgno;
	DB_LSN	pagelsn;
	u_int32_t	newalloc;
} __ham_metagroup_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_metagroup_42_desc[];
static inline int __ham_metagroup_42_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_metagroup_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_metagroup_42_desc, sizeof(__ham_metagroup_42_args), (void**)arg));
}
#define	DB___ham_metagroup	29
typedef struct ___ham_metagroup_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	u_int32_t	bucket;
	db_pgno_t	mmpgno;
	DB_LSN	mmetalsn;
	db_pgno_t	mpgno;
	DB_LSN	metalsn;
	db_pgno_t	pgno;
	DB_LSN	pagelsn;
	u_int32_t	newalloc;
	db_pgno_t	last_pgno;
} __ham_metagroup_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_metagroup_desc[];
static inline int
__ham_metagroup_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, u_int32_t bucket, db_pgno_t mmpgno, DB_LSN * mmetalsn, db_pgno_t mpgno,
    DB_LSN * metalsn, db_pgno_t pgno, DB_LSN * pagelsn, u_int32_t newalloc, db_pgno_t last_pgno)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_metagroup, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(*mmetalsn) + sizeof(u_int32_t) + sizeof(*metalsn) +
	    sizeof(u_int32_t) + sizeof(*pagelsn) + sizeof(u_int32_t) +
	    sizeof(u_int32_t),
	    __ham_metagroup_desc, bucket, mmpgno, mmetalsn, mpgno, metalsn, pgno, pagelsn,
	    newalloc, last_pgno));
}

static inline int __ham_metagroup_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_metagroup_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_metagroup_desc, sizeof(__ham_metagroup_args), (void**)arg));
}
#define	DB___ham_groupalloc_42	32
typedef struct ___ham_groupalloc_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	DB_LSN	meta_lsn;
	db_pgno_t	start_pgno;
	u_int32_t	num;
	db_pgno_t	free;
} __ham_groupalloc_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_groupalloc_42_desc[];
static inline int __ham_groupalloc_42_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_groupalloc_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_groupalloc_42_desc, sizeof(__ham_groupalloc_42_args), (void**)arg));
}
#define	DB___ham_groupalloc	32
typedef struct ___ham_groupalloc_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	DB_LSN	meta_lsn;
	db_pgno_t	start_pgno;
	u_int32_t	num;
	db_pgno_t	unused;
	db_pgno_t	last_pgno;
} __ham_groupalloc_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_groupalloc_desc[];
static inline int
__ham_groupalloc_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, DB_LSN * meta_lsn, db_pgno_t start_pgno, u_int32_t num, db_pgno_t unused,
    db_pgno_t last_pgno)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_groupalloc, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(*meta_lsn) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t),
	    __ham_groupalloc_desc, meta_lsn, start_pgno, num, unused, last_pgno));
}

static inline int __ham_groupalloc_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_groupalloc_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_groupalloc_desc, sizeof(__ham_groupalloc_args), (void**)arg));
}
#define	DB___ham_changeslot	35
typedef struct ___ham_changeslot_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	DB_LSN	meta_lsn;
	u_int32_t	slot;
	db_pgno_t	old;
	db_pgno_t	new;
} __ham_changeslot_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_changeslot_desc[];
static inline int
__ham_changeslot_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, DB_LSN * meta_lsn, u_int32_t slot, db_pgno_t old, db_pgno_t new)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_changeslot, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(*meta_lsn) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t),
	    __ham_changeslot_desc, meta_lsn, slot, old, new));
}

static inline int __ham_changeslot_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_changeslot_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_changeslot_desc, sizeof(__ham_changeslot_args), (void**)arg));
}
#define	DB___ham_contract	37
typedef struct ___ham_contract_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	meta;
	DB_LSN	meta_lsn;
	u_int32_t	bucket;
	db_pgno_t	pgno;
} __ham_contract_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_contract_desc[];
static inline int
__ham_contract_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t meta, DB_LSN * meta_lsn, u_int32_t bucket, db_pgno_t pgno)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_contract, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(*meta_lsn) +
	    sizeof(u_int32_t) + sizeof(u_int32_t),
	    __ham_contract_desc, meta, meta_lsn, bucket, pgno));
}

static inline int __ham_contract_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_contract_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_contract_desc, sizeof(__ham_contract_args), (void**)arg));
}
#define	DB___ham_curadj	33
typedef struct ___ham_curadj_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	u_int32_t	indx;
	u_int32_t	len;
	u_int32_t	dup_off;
	int	add;
	int	is_dup;
	u_int32_t	order;
} __ham_curadj_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_curadj_desc[];
static inline int
__ham_curadj_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, u_int32_t indx, u_int32_t len, u_int32_t dup_off,
    int add, int is_dup, u_int32_t order)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_curadj, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t),
	    __ham_curadj_desc, pgno, indx, len, dup_off, add, is_dup, order));
}

static inline int __ham_curadj_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_curadj_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_curadj_desc, sizeof(__ham_curadj_args), (void**)arg));
}
#define	DB___ham_chgpg	34
typedef struct ___ham_chgpg_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_ham_mode	mode;
	db_pgno_t	old_pgno;
	db_pgno_t	new_pgno;
	u_int32_t	old_indx;
	u_int32_t	new_indx;
} __ham_chgpg_args;

extern __DB_IMPORT DB_LOG_RECSPEC __ham_chgpg_desc[];
static inline int
__ham_chgpg_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_ham_mode mode, db_pgno_t old_pgno, db_pgno_t new_pgno, u_int32_t old_indx,
    u_int32_t new_indx)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___ham_chgpg, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t),
	    __ham_chgpg_desc, mode, old_pgno, new_pgno, old_indx, new_indx));
}

static inline int __ham_chgpg_read(ENV *env, 
    DB **dbpp, void *td, void *data, __ham_chgpg_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __ham_chgpg_desc, sizeof(__ham_chgpg_args), (void**)arg));
}
#endif /* HAVE_HASH */
#endif
