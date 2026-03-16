/* Do not edit: automatically built by gen_rec.awk. */

#ifndef	__bam_AUTO_H
#define	__bam_AUTO_H
#include <contrib/deprecated/bdb/src/dbinc/log.h>
#define	DB___bam_split	62
typedef struct ___bam_split_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	u_int32_t	opflags;
	db_pgno_t	left;
	DB_LSN	llsn;
	db_pgno_t	right;
	DB_LSN	rlsn;
	u_int32_t	indx;
	db_pgno_t	npgno;
	DB_LSN	nlsn;
	db_pgno_t	ppgno;
	DB_LSN	plsn;
	u_int32_t	pindx;
	DBT	pg;
	DBT	pentry;
	DBT	rentry;
} __bam_split_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_split_desc[];
static inline int
__bam_split_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, u_int32_t opflags, db_pgno_t left, DB_LSN * llsn, db_pgno_t right,
    DB_LSN * rlsn, u_int32_t indx, db_pgno_t npgno, DB_LSN * nlsn, db_pgno_t ppgno,
    DB_LSN * plsn, u_int32_t pindx, const DBT *pg, const DBT *pentry, const DBT *rentry)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___bam_split, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(*llsn) + sizeof(u_int32_t) + sizeof(*rlsn) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(*nlsn) +
	    sizeof(u_int32_t) + sizeof(*plsn) + sizeof(u_int32_t) +
	    LOG_DBT_SIZE(pg) + LOG_DBT_SIZE(pentry) + LOG_DBT_SIZE(rentry),
	    __bam_split_desc, opflags, left, llsn, right, rlsn, indx, npgno,
	    nlsn, ppgno, plsn, pindx, pg, pentry, rentry));
}

static inline int __bam_split_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_split_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_split_desc, sizeof(__bam_split_args), (void**)arg));
}
#define	DB___bam_split_48	62
typedef struct ___bam_split_48_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	left;
	DB_LSN	llsn;
	db_pgno_t	right;
	DB_LSN	rlsn;
	u_int32_t	indx;
	db_pgno_t	npgno;
	DB_LSN	nlsn;
	db_pgno_t	ppgno;
	DB_LSN	plsn;
	u_int32_t	pindx;
	DBT	pg;
	DBT	pentry;
	DBT	rentry;
	u_int32_t	opflags;
} __bam_split_48_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_split_48_desc[];
static inline int __bam_split_48_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_split_48_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_split_48_desc, sizeof(__bam_split_48_args), (void**)arg));
}
#define	DB___bam_split_42	62
typedef struct ___bam_split_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	left;
	DB_LSN	llsn;
	db_pgno_t	right;
	DB_LSN	rlsn;
	u_int32_t	indx;
	db_pgno_t	npgno;
	DB_LSN	nlsn;
	db_pgno_t	root_pgno;
	DBT	pg;
	u_int32_t	opflags;
} __bam_split_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_split_42_desc[];
static inline int __bam_split_42_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_split_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_split_42_desc, sizeof(__bam_split_42_args), (void**)arg));
}
#define	DB___bam_rsplit	63
typedef struct ___bam_rsplit_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DBT	pgdbt;
	db_pgno_t	root_pgno;
	db_pgno_t	nrec;
	DBT	rootent;
	DB_LSN	rootlsn;
} __bam_rsplit_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_rsplit_desc[];
static inline int
__bam_rsplit_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, const DBT *pgdbt, db_pgno_t root_pgno, db_pgno_t nrec,
    const DBT *rootent, DB_LSN * rootlsn)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___bam_rsplit, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + LOG_DBT_SIZE(pgdbt) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + LOG_DBT_SIZE(rootent) +
	    sizeof(*rootlsn),
	    __bam_rsplit_desc, pgno, pgdbt, root_pgno, nrec, rootent, rootlsn));
}

static inline int __bam_rsplit_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_rsplit_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_rsplit_desc, sizeof(__bam_rsplit_args), (void**)arg));
}
#define	DB___bam_adj	55
typedef struct ___bam_adj_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DB_LSN	lsn;
	u_int32_t	indx;
	u_int32_t	indx_copy;
	u_int32_t	is_insert;
} __bam_adj_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_adj_desc[];
static inline int
__bam_adj_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, DB_LSN * lsn, u_int32_t indx, u_int32_t indx_copy,
    u_int32_t is_insert)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___bam_adj, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(*lsn) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t),
	    __bam_adj_desc, pgno, lsn, indx, indx_copy, is_insert));
}

static inline int __bam_adj_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_adj_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_adj_desc, sizeof(__bam_adj_args), (void**)arg));
}
#define	DB___bam_cadjust	56
typedef struct ___bam_cadjust_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DB_LSN	lsn;
	u_int32_t	indx;
	int32_t	adjust;
	u_int32_t	opflags;
} __bam_cadjust_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_cadjust_desc[];
static inline int
__bam_cadjust_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, DB_LSN * lsn, u_int32_t indx, int32_t adjust,
    u_int32_t opflags)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___bam_cadjust, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(*lsn) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t),
	    __bam_cadjust_desc, pgno, lsn, indx, adjust, opflags));
}

static inline int __bam_cadjust_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_cadjust_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_cadjust_desc, sizeof(__bam_cadjust_args), (void**)arg));
}
#define	DB___bam_cdel	57
typedef struct ___bam_cdel_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DB_LSN	lsn;
	u_int32_t	indx;
} __bam_cdel_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_cdel_desc[];
static inline int
__bam_cdel_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, DB_LSN * lsn, u_int32_t indx)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___bam_cdel, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(*lsn) +
	    sizeof(u_int32_t),
	    __bam_cdel_desc, pgno, lsn, indx));
}

static inline int __bam_cdel_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_cdel_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_cdel_desc, sizeof(__bam_cdel_args), (void**)arg));
}
#define	DB___bam_repl	58
typedef struct ___bam_repl_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DB_LSN	lsn;
	u_int32_t	indx;
	u_int32_t	isdeleted;
	DBT	orig;
	DBT	repl;
	u_int32_t	prefix;
	u_int32_t	suffix;
} __bam_repl_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_repl_desc[];
static inline int
__bam_repl_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, DB_LSN * lsn, u_int32_t indx, u_int32_t isdeleted,
    const DBT *orig, const DBT *repl, u_int32_t prefix, u_int32_t suffix)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___bam_repl, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(*lsn) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + LOG_DBT_SIZE(orig) +
	    LOG_DBT_SIZE(repl) + sizeof(u_int32_t) + sizeof(u_int32_t),
	    __bam_repl_desc, pgno, lsn, indx, isdeleted, orig, repl, prefix,
	    suffix));
}

static inline int __bam_repl_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_repl_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_repl_desc, sizeof(__bam_repl_args), (void**)arg));
}
#define	DB___bam_irep	67
typedef struct ___bam_irep_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DB_LSN	lsn;
	u_int32_t	indx;
	u_int32_t	ptype;
	DBT	hdr;
	DBT	data;
	DBT	old;
} __bam_irep_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_irep_desc[];
static inline int
__bam_irep_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t pgno, DB_LSN * lsn, u_int32_t indx, u_int32_t ptype,
    const DBT *hdr, const DBT *data, const DBT *old)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___bam_irep, 1,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(*lsn) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + LOG_DBT_SIZE(hdr) +
	    LOG_DBT_SIZE(data) + LOG_DBT_SIZE(old),
	    __bam_irep_desc, pgno, lsn, indx, ptype, hdr, data, old));
}

static inline int __bam_irep_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_irep_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_irep_desc, sizeof(__bam_irep_args), (void**)arg));
}
#define	DB___bam_root	59
typedef struct ___bam_root_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	meta_pgno;
	db_pgno_t	root_pgno;
	DB_LSN	meta_lsn;
} __bam_root_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_root_desc[];
static inline int
__bam_root_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_pgno_t meta_pgno, db_pgno_t root_pgno, DB_LSN * meta_lsn)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___bam_root, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(*meta_lsn),
	    __bam_root_desc, meta_pgno, root_pgno, meta_lsn));
}

static inline int __bam_root_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_root_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_root_desc, sizeof(__bam_root_args), (void**)arg));
}
#define	DB___bam_curadj	64
typedef struct ___bam_curadj_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_ca_mode	mode;
	db_pgno_t	from_pgno;
	db_pgno_t	to_pgno;
	db_pgno_t	left_pgno;
	u_int32_t	first_indx;
	u_int32_t	from_indx;
	u_int32_t	to_indx;
} __bam_curadj_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_curadj_desc[];
static inline int
__bam_curadj_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, db_ca_mode mode, db_pgno_t from_pgno, db_pgno_t to_pgno, db_pgno_t left_pgno,
    u_int32_t first_indx, u_int32_t from_indx, u_int32_t to_indx)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___bam_curadj, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t),
	    __bam_curadj_desc, mode, from_pgno, to_pgno, left_pgno, first_indx, from_indx, to_indx));
}

static inline int __bam_curadj_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_curadj_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_curadj_desc, sizeof(__bam_curadj_args), (void**)arg));
}
#define	DB___bam_rcuradj	65
typedef struct ___bam_rcuradj_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	ca_recno_arg	mode;
	db_pgno_t	root;
	db_recno_t	recno;
	u_int32_t	order;
} __bam_rcuradj_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_rcuradj_desc[];
static inline int
__bam_rcuradj_log(DB *dbp, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags, ca_recno_arg mode, db_pgno_t root, db_recno_t recno, u_int32_t order)
{
	return (__log_put_record((dbp)->env, dbp, txnp, ret_lsnp,
	    flags, DB___bam_rcuradj, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t),
	    __bam_rcuradj_desc, mode, root, recno, order));
}

static inline int __bam_rcuradj_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_rcuradj_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_rcuradj_desc, sizeof(__bam_rcuradj_args), (void**)arg));
}
#define	DB___bam_relink_43	147
typedef struct ___bam_relink_43_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DB_LSN	lsn;
	db_pgno_t	prev;
	DB_LSN	lsn_prev;
	db_pgno_t	next;
	DB_LSN	lsn_next;
} __bam_relink_43_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_relink_43_desc[];
static inline int __bam_relink_43_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_relink_43_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_relink_43_desc, sizeof(__bam_relink_43_args), (void**)arg));
}
#define	DB___bam_merge_44	148
typedef struct ___bam_merge_44_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	int32_t	fileid;
	db_pgno_t	pgno;
	DB_LSN	lsn;
	db_pgno_t	npgno;
	DB_LSN	nlsn;
	DBT	hdr;
	DBT	data;
	DBT	ind;
} __bam_merge_44_args;

extern __DB_IMPORT DB_LOG_RECSPEC __bam_merge_44_desc[];
static inline int __bam_merge_44_read(ENV *env, 
    DB **dbpp, void *td, void *data, __bam_merge_44_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    dbpp, td, data, __bam_merge_44_desc, sizeof(__bam_merge_44_args), (void**)arg));
}
#endif
