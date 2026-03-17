/* Do not edit: automatically built by gen_rec.awk. */

#ifndef	__txn_AUTO_H
#define	__txn_AUTO_H
#include <contrib/deprecated/bdb/src/dbinc/log.h>
#define	DB___txn_regop_42	10
typedef struct ___txn_regop_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	opcode;
	int32_t	timestamp;
	DBT	locks;
} __txn_regop_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __txn_regop_42_desc[];
static inline int __txn_regop_42_read(ENV *env, 
    void *data, __txn_regop_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __txn_regop_42_desc, sizeof(__txn_regop_42_args), (void**)arg));
}
#define	DB___txn_regop	10
typedef struct ___txn_regop_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	opcode;
	int32_t	timestamp;
	u_int32_t	envid;
	DBT	locks;
} __txn_regop_args;

extern __DB_IMPORT DB_LOG_RECSPEC __txn_regop_desc[];
static inline int
__txn_regop_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    u_int32_t opcode, int32_t timestamp, u_int32_t envid, const DBT *locks)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___txn_regop, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(u_int32_t) +
	    LOG_DBT_SIZE(locks),
	    __txn_regop_desc,
	    opcode, timestamp, envid, locks));
}

static inline int __txn_regop_read(ENV *env, 
    void *data, __txn_regop_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __txn_regop_desc, sizeof(__txn_regop_args), (void**)arg));
}
#define	DB___txn_ckp_42	11
typedef struct ___txn_ckp_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DB_LSN	ckp_lsn;
	DB_LSN	last_ckp;
	int32_t	timestamp;
	u_int32_t	rep_gen;
} __txn_ckp_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __txn_ckp_42_desc[];
static inline int __txn_ckp_42_read(ENV *env, 
    void *data, __txn_ckp_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __txn_ckp_42_desc, sizeof(__txn_ckp_42_args), (void**)arg));
}
#define	DB___txn_ckp	11
typedef struct ___txn_ckp_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	DB_LSN	ckp_lsn;
	DB_LSN	last_ckp;
	int32_t	timestamp;
	u_int32_t	envid;
	u_int32_t	spare;
} __txn_ckp_args;

extern __DB_IMPORT DB_LOG_RECSPEC __txn_ckp_desc[];
static inline int
__txn_ckp_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    DB_LSN * ckp_lsn, DB_LSN * last_ckp, int32_t timestamp, u_int32_t envid, u_int32_t spare)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___txn_ckp, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(*ckp_lsn) + sizeof(*last_ckp) + sizeof(u_int32_t) +
	    sizeof(u_int32_t) + sizeof(u_int32_t),
	    __txn_ckp_desc,
	    ckp_lsn, last_ckp, timestamp, envid, spare));
}

static inline int __txn_ckp_read(ENV *env, 
    void *data, __txn_ckp_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __txn_ckp_desc, sizeof(__txn_ckp_args), (void**)arg));
}
#define	DB___txn_child	12
typedef struct ___txn_child_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	child;
	DB_LSN	c_lsn;
} __txn_child_args;

extern __DB_IMPORT DB_LOG_RECSPEC __txn_child_desc[];
static inline int
__txn_child_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    u_int32_t child, DB_LSN * c_lsn)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___txn_child, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(*c_lsn),
	    __txn_child_desc,
	    child, c_lsn));
}

static inline int __txn_child_read(ENV *env, 
    void *data, __txn_child_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __txn_child_desc, sizeof(__txn_child_args), (void**)arg));
}
#define	DB___txn_xa_regop_42	13
typedef struct ___txn_xa_regop_42_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	opcode;
	DBT	xid;
	int32_t	formatID;
	u_int32_t	gtrid;
	u_int32_t	bqual;
	DB_LSN	begin_lsn;
	DBT	locks;
} __txn_xa_regop_42_args;

extern __DB_IMPORT DB_LOG_RECSPEC __txn_xa_regop_42_desc[];
static inline int __txn_xa_regop_42_read(ENV *env, 
    void *data, __txn_xa_regop_42_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __txn_xa_regop_42_desc, sizeof(__txn_xa_regop_42_args), (void**)arg));
}
#define	DB___txn_prepare	13
typedef struct ___txn_prepare_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	opcode;
	DBT	gid;
	DB_LSN	begin_lsn;
	DBT	locks;
} __txn_prepare_args;

extern __DB_IMPORT DB_LOG_RECSPEC __txn_prepare_desc[];
static inline int
__txn_prepare_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    u_int32_t opcode, const DBT *gid, DB_LSN * begin_lsn, const DBT *locks)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___txn_prepare, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + LOG_DBT_SIZE(gid) + sizeof(*begin_lsn) +
	    LOG_DBT_SIZE(locks),
	    __txn_prepare_desc,
	    opcode, gid, begin_lsn, locks));
}

static inline int __txn_prepare_read(ENV *env, 
    void *data, __txn_prepare_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __txn_prepare_desc, sizeof(__txn_prepare_args), (void**)arg));
}
#define	DB___txn_recycle	14
typedef struct ___txn_recycle_args {
	u_int32_t type;
	DB_TXN *txnp;
	DB_LSN prev_lsn;
	u_int32_t	min;
	u_int32_t	max;
} __txn_recycle_args;

extern __DB_IMPORT DB_LOG_RECSPEC __txn_recycle_desc[];
static inline int
__txn_recycle_log(ENV *env, DB_TXN *txnp, DB_LSN *ret_lsnp, u_int32_t flags,
    u_int32_t min, u_int32_t max)
{
	return (__log_put_record(env, NULL, txnp, ret_lsnp,
	    flags, DB___txn_recycle, 0,
	    sizeof(u_int32_t) + sizeof(u_int32_t) + sizeof(DB_LSN) +
	    sizeof(u_int32_t) + sizeof(u_int32_t),
	    __txn_recycle_desc,
	    min, max));
}

static inline int __txn_recycle_read(ENV *env, 
    void *data, __txn_recycle_args **arg)
{
	*arg = NULL;
	return (__log_read_record(env, 
	    NULL, NULL, data, __txn_recycle_desc, sizeof(__txn_recycle_args), (void**)arg));
}
#endif
