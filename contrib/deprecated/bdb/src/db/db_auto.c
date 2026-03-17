/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/txn.h"

DB_LOG_RECSPEC __db_addrem_desc[] = {
	{LOGREC_OP, SSZ(__db_addrem_args, opcode), "opcode", "%lu"},
	{LOGREC_DB, SSZ(__db_addrem_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_addrem_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_addrem_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__db_addrem_args, nbytes), "nbytes", "%lu"},
	{LOGREC_HDR, SSZ(__db_addrem_args, hdr), "hdr", ""},
	{LOGREC_DBT, SSZ(__db_addrem_args, dbt), "dbt", ""},
	{LOGREC_POINTER, SSZ(__db_addrem_args, pagelsn), "pagelsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_addrem_42_desc[] = {
	{LOGREC_ARG, SSZ(__db_addrem_42_args, opcode), "opcode", "%lu"},
	{LOGREC_DB, SSZ(__db_addrem_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_addrem_42_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_addrem_42_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__db_addrem_42_args, nbytes), "nbytes", "%lu"},
	{LOGREC_DBT, SSZ(__db_addrem_42_args, hdr), "hdr", ""},
	{LOGREC_DBT, SSZ(__db_addrem_42_args, dbt), "dbt", ""},
	{LOGREC_POINTER, SSZ(__db_addrem_42_args, pagelsn), "pagelsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_big_desc[] = {
	{LOGREC_OP, SSZ(__db_big_args, opcode), "opcode", "%lu"},
	{LOGREC_DB, SSZ(__db_big_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_big_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_big_args, prev_pgno), "prev_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_big_args, next_pgno), "next_pgno", "%lu"},
	{LOGREC_HDR, SSZ(__db_big_args, dbt), "dbt", ""},
	{LOGREC_POINTER, SSZ(__db_big_args, pagelsn), "pagelsn", ""},
	{LOGREC_POINTER, SSZ(__db_big_args, prevlsn), "prevlsn", ""},
	{LOGREC_POINTER, SSZ(__db_big_args, nextlsn), "nextlsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_big_42_desc[] = {
	{LOGREC_ARG, SSZ(__db_big_42_args, opcode), "opcode", "%lu"},
	{LOGREC_DB, SSZ(__db_big_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_big_42_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_big_42_args, prev_pgno), "prev_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_big_42_args, next_pgno), "next_pgno", "%lu"},
	{LOGREC_DBT, SSZ(__db_big_42_args, dbt), "dbt", ""},
	{LOGREC_POINTER, SSZ(__db_big_42_args, pagelsn), "pagelsn", ""},
	{LOGREC_POINTER, SSZ(__db_big_42_args, prevlsn), "prevlsn", ""},
	{LOGREC_POINTER, SSZ(__db_big_42_args, nextlsn), "nextlsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_ovref_desc[] = {
	{LOGREC_DB, SSZ(__db_ovref_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_ovref_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_ovref_args, adjust), "adjust", "%ld"},
	{LOGREC_POINTER, SSZ(__db_ovref_args, lsn), "lsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_relink_42_desc[] = {
	{LOGREC_ARG, SSZ(__db_relink_42_args, opcode), "opcode", "%lu"},
	{LOGREC_DB, SSZ(__db_relink_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_relink_42_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_relink_42_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__db_relink_42_args, prev), "prev", "%lu"},
	{LOGREC_POINTER, SSZ(__db_relink_42_args, lsn_prev), "lsn_prev", ""},
	{LOGREC_ARG, SSZ(__db_relink_42_args, next), "next", "%lu"},
	{LOGREC_POINTER, SSZ(__db_relink_42_args, lsn_next), "lsn_next", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_debug_desc[] = {
	{LOGREC_DBT, SSZ(__db_debug_args, op), "op", ""},
	{LOGREC_ARG, SSZ(__db_debug_args, fileid), "fileid", "%ld"},
	{LOGREC_DBT, SSZ(__db_debug_args, key), "key", ""},
	{LOGREC_DBT, SSZ(__db_debug_args, data), "data", ""},
	{LOGREC_ARG, SSZ(__db_debug_args, arg_flags), "arg_flags", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_noop_desc[] = {
	{LOGREC_DB, SSZ(__db_noop_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_noop_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_noop_args, prevlsn), "prevlsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_pg_alloc_42_desc[] = {
	{LOGREC_DB, SSZ(__db_pg_alloc_42_args, fileid), "fileid", ""},
	{LOGREC_POINTER, SSZ(__db_pg_alloc_42_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_alloc_42_args, meta_pgno), "meta_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pg_alloc_42_args, page_lsn), "page_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_alloc_42_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_pg_alloc_42_args, ptype), "ptype", "%lu"},
	{LOGREC_ARG, SSZ(__db_pg_alloc_42_args, next), "next", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_pg_alloc_desc[] = {
	{LOGREC_DB, SSZ(__db_pg_alloc_args, fileid), "fileid", ""},
	{LOGREC_POINTER, SSZ(__db_pg_alloc_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_alloc_args, meta_pgno), "meta_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pg_alloc_args, page_lsn), "page_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_alloc_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_pg_alloc_args, ptype), "ptype", "%lu"},
	{LOGREC_ARG, SSZ(__db_pg_alloc_args, next), "next", "%lu"},
	{LOGREC_ARG, SSZ(__db_pg_alloc_args, last_pgno), "last_pgno", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_pg_free_42_desc[] = {
	{LOGREC_DB, SSZ(__db_pg_free_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_pg_free_42_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pg_free_42_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_free_42_args, meta_pgno), "meta_pgno", "%lu"},
	{LOGREC_PGDBT, SSZ(__db_pg_free_42_args, header), "header", ""},
	{LOGREC_ARG, SSZ(__db_pg_free_42_args, next), "next", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_pg_free_desc[] = {
	{LOGREC_DB, SSZ(__db_pg_free_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_pg_free_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pg_free_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_free_args, meta_pgno), "meta_pgno", "%lu"},
	{LOGREC_PGDBT, SSZ(__db_pg_free_args, header), "header", ""},
	{LOGREC_ARG, SSZ(__db_pg_free_args, next), "next", "%lu"},
	{LOGREC_ARG, SSZ(__db_pg_free_args, last_pgno), "last_pgno", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_cksum_desc[] = {
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_pg_freedata_42_desc[] = {
	{LOGREC_DB, SSZ(__db_pg_freedata_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_pg_freedata_42_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pg_freedata_42_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_freedata_42_args, meta_pgno), "meta_pgno", "%lu"},
	{LOGREC_PGDBT, SSZ(__db_pg_freedata_42_args, header), "header", ""},
	{LOGREC_ARG, SSZ(__db_pg_freedata_42_args, next), "next", "%lu"},
	{LOGREC_PGDDBT, SSZ(__db_pg_freedata_42_args, data), "data", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_pg_freedata_desc[] = {
	{LOGREC_DB, SSZ(__db_pg_freedata_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_pg_freedata_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pg_freedata_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_freedata_args, meta_pgno), "meta_pgno", "%lu"},
	{LOGREC_PGDBT, SSZ(__db_pg_freedata_args, header), "header", ""},
	{LOGREC_ARG, SSZ(__db_pg_freedata_args, next), "next", "%lu"},
	{LOGREC_ARG, SSZ(__db_pg_freedata_args, last_pgno), "last_pgno", "%lu"},
	{LOGREC_PGDDBT, SSZ(__db_pg_freedata_args, data), "data", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_pg_init_desc[] = {
	{LOGREC_DB, SSZ(__db_pg_init_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_pg_init_args, pgno), "pgno", "%lu"},
	{LOGREC_PGDBT, SSZ(__db_pg_init_args, header), "header", ""},
	{LOGREC_PGDDBT, SSZ(__db_pg_init_args, data), "data", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_pg_sort_44_desc[] = {
	{LOGREC_DB, SSZ(__db_pg_sort_44_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_pg_sort_44_args, meta), "meta", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pg_sort_44_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_sort_44_args, last_free), "last_free", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pg_sort_44_args, last_lsn), "last_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_sort_44_args, last_pgno), "last_pgno", "%lu"},
	{LOGREC_DBT, SSZ(__db_pg_sort_44_args, list), "list", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_pg_trunc_desc[] = {
	{LOGREC_DB, SSZ(__db_pg_trunc_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_pg_trunc_args, meta), "meta", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pg_trunc_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_trunc_args, last_free), "last_free", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pg_trunc_args, last_lsn), "last_lsn", ""},
	{LOGREC_ARG, SSZ(__db_pg_trunc_args, next_free), "next_free", "%lu"},
	{LOGREC_ARG, SSZ(__db_pg_trunc_args, last_pgno), "last_pgno", "%lu"},
	{LOGREC_PGLIST, SSZ(__db_pg_trunc_args, list), "list", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_realloc_desc[] = {
	{LOGREC_DB, SSZ(__db_realloc_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_realloc_args, prev_pgno), "prev_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_realloc_args, page_lsn), "page_lsn", ""},
	{LOGREC_ARG, SSZ(__db_realloc_args, next_free), "next_free", "%lu"},
	{LOGREC_ARG, SSZ(__db_realloc_args, ptype), "ptype", "%lu"},
	{LOGREC_PGLIST, SSZ(__db_realloc_args, list), "list", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_relink_desc[] = {
	{LOGREC_DB, SSZ(__db_relink_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_relink_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_relink_args, new_pgno), "new_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_relink_args, prev_pgno), "prev_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_relink_args, lsn_prev), "lsn_prev", ""},
	{LOGREC_ARG, SSZ(__db_relink_args, next_pgno), "next_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_relink_args, lsn_next), "lsn_next", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_merge_desc[] = {
	{LOGREC_DB, SSZ(__db_merge_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_merge_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_merge_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__db_merge_args, npgno), "npgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_merge_args, nlsn), "nlsn", ""},
	{LOGREC_PGDBT, SSZ(__db_merge_args, hdr), "hdr", ""},
	{LOGREC_PGDDBT, SSZ(__db_merge_args, data), "data", ""},
	{LOGREC_ARG, SSZ(__db_merge_args, pg_copy), "pg_copy", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __db_pgno_desc[] = {
	{LOGREC_DB, SSZ(__db_pgno_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__db_pgno_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__db_pgno_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__db_pgno_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__db_pgno_args, opgno), "opgno", "%lu"},
	{LOGREC_ARG, SSZ(__db_pgno_args, npgno), "npgno", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
/*
 * PUBLIC: int __db_init_recover __P((ENV *, DB_DISTAB *));
 */
int
__db_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_addrem_recover, DB___db_addrem)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_big_recover, DB___db_big)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_ovref_recover, DB___db_ovref)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_debug_recover, DB___db_debug)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_noop_recover, DB___db_noop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_alloc_recover, DB___db_pg_alloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_free_recover, DB___db_pg_free)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_cksum_recover, DB___db_cksum)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_freedata_recover, DB___db_pg_freedata)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_init_recover, DB___db_pg_init)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pg_trunc_recover, DB___db_pg_trunc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_realloc_recover, DB___db_realloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_relink_recover, DB___db_relink)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_merge_recover, DB___db_merge)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __db_pgno_recover, DB___db_pgno)) != 0)
		return (ret);
	return (0);
}
