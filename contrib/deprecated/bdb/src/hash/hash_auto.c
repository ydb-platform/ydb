/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/hash.h"
#include "dbinc/txn.h"

DB_LOG_RECSPEC __ham_insdel_desc[] = {
	{LOGREC_ARG, SSZ(__ham_insdel_args, opcode), "opcode", "%lu"},
	{LOGREC_DB, SSZ(__ham_insdel_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_insdel_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__ham_insdel_args, ndx), "ndx", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_insdel_args, pagelsn), "pagelsn", ""},
	{LOGREC_OP, SSZ(__ham_insdel_args, keytype), "keytype", "%lu"},
	{LOGREC_HDR, SSZ(__ham_insdel_args, key), "key", ""},
	{LOGREC_OP, SSZ(__ham_insdel_args, datatype), "datatype", "%lu"},
	{LOGREC_HDR, SSZ(__ham_insdel_args, data), "data", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_insdel_42_desc[] = {
	{LOGREC_ARG, SSZ(__ham_insdel_42_args, opcode), "opcode", "%lu"},
	{LOGREC_DB, SSZ(__ham_insdel_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_insdel_42_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__ham_insdel_42_args, ndx), "ndx", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_insdel_42_args, pagelsn), "pagelsn", ""},
	{LOGREC_DBT, SSZ(__ham_insdel_42_args, key), "key", ""},
	{LOGREC_DBT, SSZ(__ham_insdel_42_args, data), "data", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_newpage_desc[] = {
	{LOGREC_ARG, SSZ(__ham_newpage_args, opcode), "opcode", "%lu"},
	{LOGREC_DB, SSZ(__ham_newpage_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_newpage_args, prev_pgno), "prev_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_newpage_args, prevlsn), "prevlsn", ""},
	{LOGREC_ARG, SSZ(__ham_newpage_args, new_pgno), "new_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_newpage_args, pagelsn), "pagelsn", ""},
	{LOGREC_ARG, SSZ(__ham_newpage_args, next_pgno), "next_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_newpage_args, nextlsn), "nextlsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_splitdata_desc[] = {
	{LOGREC_DB, SSZ(__ham_splitdata_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_splitdata_args, opcode), "opcode", "%lu"},
	{LOGREC_ARG, SSZ(__ham_splitdata_args, pgno), "pgno", "%lu"},
	{LOGREC_PGDBT, SSZ(__ham_splitdata_args, pageimage), "pageimage", ""},
	{LOGREC_POINTER, SSZ(__ham_splitdata_args, pagelsn), "pagelsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_replace_desc[] = {
	{LOGREC_DB, SSZ(__ham_replace_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_replace_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__ham_replace_args, ndx), "ndx", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_replace_args, pagelsn), "pagelsn", ""},
	{LOGREC_ARG, SSZ(__ham_replace_args, off), "off", "%ld"},
	{LOGREC_OP, SSZ(__ham_replace_args, oldtype), "oldtype", "%lu"},
	{LOGREC_HDR, SSZ(__ham_replace_args, olditem), "olditem", ""},
	{LOGREC_OP, SSZ(__ham_replace_args, newtype), "newtype", "%lu"},
	{LOGREC_HDR, SSZ(__ham_replace_args, newitem), "newitem", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_replace_42_desc[] = {
	{LOGREC_DB, SSZ(__ham_replace_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_replace_42_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__ham_replace_42_args, ndx), "ndx", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_replace_42_args, pagelsn), "pagelsn", ""},
	{LOGREC_ARG, SSZ(__ham_replace_42_args, off), "off", "%ld"},
	{LOGREC_DBT, SSZ(__ham_replace_42_args, olditem), "olditem", ""},
	{LOGREC_DBT, SSZ(__ham_replace_42_args, newitem), "newitem", ""},
	{LOGREC_ARG, SSZ(__ham_replace_42_args, makedup), "makedup", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_copypage_desc[] = {
	{LOGREC_DB, SSZ(__ham_copypage_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_copypage_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_copypage_args, pagelsn), "pagelsn", ""},
	{LOGREC_ARG, SSZ(__ham_copypage_args, next_pgno), "next_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_copypage_args, nextlsn), "nextlsn", ""},
	{LOGREC_ARG, SSZ(__ham_copypage_args, nnext_pgno), "nnext_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_copypage_args, nnextlsn), "nnextlsn", ""},
	{LOGREC_PGDBT, SSZ(__ham_copypage_args, page), "page", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_metagroup_42_desc[] = {
	{LOGREC_DB, SSZ(__ham_metagroup_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_metagroup_42_args, bucket), "bucket", "%lu"},
	{LOGREC_ARG, SSZ(__ham_metagroup_42_args, mmpgno), "mmpgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_metagroup_42_args, mmetalsn), "mmetalsn", ""},
	{LOGREC_ARG, SSZ(__ham_metagroup_42_args, mpgno), "mpgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_metagroup_42_args, metalsn), "metalsn", ""},
	{LOGREC_ARG, SSZ(__ham_metagroup_42_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_metagroup_42_args, pagelsn), "pagelsn", ""},
	{LOGREC_ARG, SSZ(__ham_metagroup_42_args, newalloc), "newalloc", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_metagroup_desc[] = {
	{LOGREC_DB, SSZ(__ham_metagroup_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_metagroup_args, bucket), "bucket", "%lu"},
	{LOGREC_ARG, SSZ(__ham_metagroup_args, mmpgno), "mmpgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_metagroup_args, mmetalsn), "mmetalsn", ""},
	{LOGREC_ARG, SSZ(__ham_metagroup_args, mpgno), "mpgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_metagroup_args, metalsn), "metalsn", ""},
	{LOGREC_ARG, SSZ(__ham_metagroup_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_metagroup_args, pagelsn), "pagelsn", ""},
	{LOGREC_ARG, SSZ(__ham_metagroup_args, newalloc), "newalloc", "%lu"},
	{LOGREC_ARG, SSZ(__ham_metagroup_args, last_pgno), "last_pgno", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_groupalloc_42_desc[] = {
	{LOGREC_DB, SSZ(__ham_groupalloc_42_args, fileid), "fileid", ""},
	{LOGREC_POINTER, SSZ(__ham_groupalloc_42_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__ham_groupalloc_42_args, start_pgno), "start_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__ham_groupalloc_42_args, num), "num", "%lu"},
	{LOGREC_ARG, SSZ(__ham_groupalloc_42_args, free), "free", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_groupalloc_desc[] = {
	{LOGREC_DB, SSZ(__ham_groupalloc_args, fileid), "fileid", ""},
	{LOGREC_POINTER, SSZ(__ham_groupalloc_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__ham_groupalloc_args, start_pgno), "start_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__ham_groupalloc_args, num), "num", "%lu"},
	{LOGREC_ARG, SSZ(__ham_groupalloc_args, unused), "unused", "%lu"},
	{LOGREC_ARG, SSZ(__ham_groupalloc_args, last_pgno), "last_pgno", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_changeslot_desc[] = {
	{LOGREC_DB, SSZ(__ham_changeslot_args, fileid), "fileid", ""},
	{LOGREC_POINTER, SSZ(__ham_changeslot_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__ham_changeslot_args, slot), "slot", "%lu"},
	{LOGREC_ARG, SSZ(__ham_changeslot_args, old), "old", "%lu"},
	{LOGREC_ARG, SSZ(__ham_changeslot_args, new), "new", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_contract_desc[] = {
	{LOGREC_DB, SSZ(__ham_contract_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_contract_args, meta), "meta", "%lu"},
	{LOGREC_POINTER, SSZ(__ham_contract_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_ARG, SSZ(__ham_contract_args, bucket), "bucket", "%lu"},
	{LOGREC_ARG, SSZ(__ham_contract_args, pgno), "pgno", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_curadj_desc[] = {
	{LOGREC_DB, SSZ(__ham_curadj_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_curadj_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__ham_curadj_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__ham_curadj_args, len), "len", "%lu"},
	{LOGREC_ARG, SSZ(__ham_curadj_args, dup_off), "dup_off", "%lu"},
	{LOGREC_ARG, SSZ(__ham_curadj_args, add), "add", "%ld"},
	{LOGREC_ARG, SSZ(__ham_curadj_args, is_dup), "is_dup", "%ld"},
	{LOGREC_ARG, SSZ(__ham_curadj_args, order), "order", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __ham_chgpg_desc[] = {
	{LOGREC_DB, SSZ(__ham_chgpg_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__ham_chgpg_args, mode), "mode", "%ld"},
	{LOGREC_ARG, SSZ(__ham_chgpg_args, old_pgno), "old_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__ham_chgpg_args, new_pgno), "new_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__ham_chgpg_args, old_indx), "old_indx", "%lu"},
	{LOGREC_ARG, SSZ(__ham_chgpg_args, new_indx), "new_indx", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
/*
 * PUBLIC: int __ham_init_recover __P((ENV *, DB_DISTAB *));
 */
int
__ham_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_insdel_recover, DB___ham_insdel)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_newpage_recover, DB___ham_newpage)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_splitdata_recover, DB___ham_splitdata)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_replace_recover, DB___ham_replace)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_copypage_recover, DB___ham_copypage)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_metagroup_recover, DB___ham_metagroup)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_groupalloc_recover, DB___ham_groupalloc)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_changeslot_recover, DB___ham_changeslot)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_contract_recover, DB___ham_contract)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_curadj_recover, DB___ham_curadj)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __ham_chgpg_recover, DB___ham_chgpg)) != 0)
		return (ret);
	return (0);
}
