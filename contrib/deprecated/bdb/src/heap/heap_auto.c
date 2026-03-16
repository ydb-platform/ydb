/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/heap.h"
#include "dbinc/txn.h"

DB_LOG_RECSPEC __heap_addrem_desc[] = {
	{LOGREC_ARG, SSZ(__heap_addrem_args, opcode), "opcode", "%lu"},
	{LOGREC_DB, SSZ(__heap_addrem_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__heap_addrem_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__heap_addrem_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__heap_addrem_args, nbytes), "nbytes", "%lu"},
	{LOGREC_DBT, SSZ(__heap_addrem_args, hdr), "hdr", ""},
	{LOGREC_DBT, SSZ(__heap_addrem_args, dbt), "dbt", ""},
	{LOGREC_POINTER, SSZ(__heap_addrem_args, pagelsn), "pagelsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __heap_pg_alloc_desc[] = {
	{LOGREC_DB, SSZ(__heap_pg_alloc_args, fileid), "fileid", ""},
	{LOGREC_POINTER, SSZ(__heap_pg_alloc_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__heap_pg_alloc_args, meta_pgno), "meta_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__heap_pg_alloc_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__heap_pg_alloc_args, ptype), "ptype", "%lu"},
	{LOGREC_ARG, SSZ(__heap_pg_alloc_args, last_pgno), "last_pgno", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __heap_trunc_meta_desc[] = {
	{LOGREC_DB, SSZ(__heap_trunc_meta_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__heap_trunc_meta_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__heap_trunc_meta_args, last_pgno), "last_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__heap_trunc_meta_args, key_count), "key_count", "%lu"},
	{LOGREC_ARG, SSZ(__heap_trunc_meta_args, record_count), "record_count", "%lu"},
	{LOGREC_ARG, SSZ(__heap_trunc_meta_args, curregion), "curregion", "%lu"},
	{LOGREC_ARG, SSZ(__heap_trunc_meta_args, nregions), "nregions", "%lu"},
	{LOGREC_POINTER, SSZ(__heap_trunc_meta_args, pagelsn), "pagelsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __heap_trunc_page_desc[] = {
	{LOGREC_DB, SSZ(__heap_trunc_page_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__heap_trunc_page_args, pgno), "pgno", "%lu"},
	{LOGREC_DBT, SSZ(__heap_trunc_page_args, old_data), "old_data", ""},
	{LOGREC_ARG, SSZ(__heap_trunc_page_args, is_region), "is_region", "%lu"},
	{LOGREC_POINTER, SSZ(__heap_trunc_page_args, pagelsn), "pagelsn", ""},
	{LOGREC_Done, 0, "", ""}
};
/*
 * PUBLIC: int __heap_init_recover __P((ENV *, DB_DISTAB *));
 */
int
__heap_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_addrem_recover, DB___heap_addrem)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_pg_alloc_recover, DB___heap_pg_alloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_trunc_meta_recover, DB___heap_trunc_meta)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __heap_trunc_page_recover, DB___heap_trunc_page)) != 0)
		return (ret);
	return (0);
}
