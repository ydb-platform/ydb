/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc/btree.h"
#include "dbinc/txn.h"

DB_LOG_RECSPEC __bam_split_desc[] = {
	{LOGREC_DB, SSZ(__bam_split_args, fileid), "fileid", ""},
	{LOGREC_OP, SSZ(__bam_split_args, opflags), "opflags", "%lu"},
	{LOGREC_ARG, SSZ(__bam_split_args, left), "left", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_args, llsn), "llsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_args, right), "right", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_args, rlsn), "rlsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__bam_split_args, npgno), "npgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_args, nlsn), "nlsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_args, ppgno), "ppgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_args, plsn), "plsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_args, pindx), "pindx", "%lu"},
	{LOGREC_PGDBT, SSZ(__bam_split_args, pg), "pg", ""},
	{LOGREC_HDR, SSZ(__bam_split_args, pentry), "pentry", ""},
	{LOGREC_HDR, SSZ(__bam_split_args, rentry), "rentry", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_split_48_desc[] = {
	{LOGREC_DB, SSZ(__bam_split_48_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_split_48_args, left), "left", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_48_args, llsn), "llsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_48_args, right), "right", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_48_args, rlsn), "rlsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_48_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__bam_split_48_args, npgno), "npgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_48_args, nlsn), "nlsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_48_args, ppgno), "ppgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_48_args, plsn), "plsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_48_args, pindx), "pindx", "%lu"},
	{LOGREC_PGDBT, SSZ(__bam_split_48_args, pg), "pg", ""},
	{LOGREC_DBT, SSZ(__bam_split_48_args, pentry), "pentry", ""},
	{LOGREC_DBT, SSZ(__bam_split_48_args, rentry), "rentry", ""},
	{LOGREC_ARG, SSZ(__bam_split_48_args, opflags), "opflags", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_split_42_desc[] = {
	{LOGREC_DB, SSZ(__bam_split_42_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_split_42_args, left), "left", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_42_args, llsn), "llsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_42_args, right), "right", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_42_args, rlsn), "rlsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_42_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__bam_split_42_args, npgno), "npgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_split_42_args, nlsn), "nlsn", ""},
	{LOGREC_ARG, SSZ(__bam_split_42_args, root_pgno), "root_pgno", "%lu"},
	{LOGREC_PGDBT, SSZ(__bam_split_42_args, pg), "pg", ""},
	{LOGREC_ARG, SSZ(__bam_split_42_args, opflags), "opflags", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_rsplit_desc[] = {
	{LOGREC_DB, SSZ(__bam_rsplit_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_rsplit_args, pgno), "pgno", "%lu"},
	{LOGREC_PGDBT, SSZ(__bam_rsplit_args, pgdbt), "pgdbt", ""},
	{LOGREC_ARG, SSZ(__bam_rsplit_args, root_pgno), "root_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__bam_rsplit_args, nrec), "nrec", "%lu"},
	{LOGREC_DBT, SSZ(__bam_rsplit_args, rootent), "rootent", ""},
	{LOGREC_POINTER, SSZ(__bam_rsplit_args, rootlsn), "rootlsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_adj_desc[] = {
	{LOGREC_DB, SSZ(__bam_adj_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_adj_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_adj_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__bam_adj_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__bam_adj_args, indx_copy), "indx_copy", "%lu"},
	{LOGREC_ARG, SSZ(__bam_adj_args, is_insert), "is_insert", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_cadjust_desc[] = {
	{LOGREC_DB, SSZ(__bam_cadjust_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_cadjust_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_cadjust_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__bam_cadjust_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__bam_cadjust_args, adjust), "adjust", "%ld"},
	{LOGREC_ARG, SSZ(__bam_cadjust_args, opflags), "opflags", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_cdel_desc[] = {
	{LOGREC_DB, SSZ(__bam_cdel_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_cdel_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_cdel_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__bam_cdel_args, indx), "indx", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_repl_desc[] = {
	{LOGREC_DB, SSZ(__bam_repl_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_repl_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_repl_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__bam_repl_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__bam_repl_args, isdeleted), "isdeleted", "%lu"},
	{LOGREC_DBT, SSZ(__bam_repl_args, orig), "orig", ""},
	{LOGREC_DBT, SSZ(__bam_repl_args, repl), "repl", ""},
	{LOGREC_ARG, SSZ(__bam_repl_args, prefix), "prefix", "%lu"},
	{LOGREC_ARG, SSZ(__bam_repl_args, suffix), "suffix", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_irep_desc[] = {
	{LOGREC_DB, SSZ(__bam_irep_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_irep_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_irep_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__bam_irep_args, indx), "indx", "%lu"},
	{LOGREC_OP, SSZ(__bam_irep_args, ptype), "ptype", "%lu"},
	{LOGREC_HDR, SSZ(__bam_irep_args, hdr), "hdr", ""},
	{LOGREC_DATA, SSZ(__bam_irep_args, data), "data", ""},
	{LOGREC_HDR, SSZ(__bam_irep_args, old), "old", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_root_desc[] = {
	{LOGREC_DB, SSZ(__bam_root_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_root_args, meta_pgno), "meta_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__bam_root_args, root_pgno), "root_pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_root_args, meta_lsn), "meta_lsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_curadj_desc[] = {
	{LOGREC_DB, SSZ(__bam_curadj_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_curadj_args, mode), "mode", "%ld"},
	{LOGREC_ARG, SSZ(__bam_curadj_args, from_pgno), "from_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__bam_curadj_args, to_pgno), "to_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__bam_curadj_args, left_pgno), "left_pgno", "%lu"},
	{LOGREC_ARG, SSZ(__bam_curadj_args, first_indx), "first_indx", "%lu"},
	{LOGREC_ARG, SSZ(__bam_curadj_args, from_indx), "from_indx", "%lu"},
	{LOGREC_ARG, SSZ(__bam_curadj_args, to_indx), "to_indx", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_rcuradj_desc[] = {
	{LOGREC_DB, SSZ(__bam_rcuradj_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_rcuradj_args, mode), "mode", "%ld"},
	{LOGREC_ARG, SSZ(__bam_rcuradj_args, root), "root", "%ld"},
	{LOGREC_ARG, SSZ(__bam_rcuradj_args, recno), "recno", "%ld"},
	{LOGREC_ARG, SSZ(__bam_rcuradj_args, order), "order", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_relink_43_desc[] = {
	{LOGREC_DB, SSZ(__bam_relink_43_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_relink_43_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_relink_43_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__bam_relink_43_args, prev), "prev", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_relink_43_args, lsn_prev), "lsn_prev", ""},
	{LOGREC_ARG, SSZ(__bam_relink_43_args, next), "next", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_relink_43_args, lsn_next), "lsn_next", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __bam_merge_44_desc[] = {
	{LOGREC_DB, SSZ(__bam_merge_44_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__bam_merge_44_args, pgno), "pgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_merge_44_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__bam_merge_44_args, npgno), "npgno", "%lu"},
	{LOGREC_POINTER, SSZ(__bam_merge_44_args, nlsn), "nlsn", ""},
	{LOGREC_DBT, SSZ(__bam_merge_44_args, hdr), "hdr", ""},
	{LOGREC_DBT, SSZ(__bam_merge_44_args, data), "data", ""},
	{LOGREC_DBT, SSZ(__bam_merge_44_args, ind), "ind", ""},
	{LOGREC_Done, 0, "", ""}
};
/*
 * PUBLIC: int __bam_init_recover __P((ENV *, DB_DISTAB *));
 */
int
__bam_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_split_recover, DB___bam_split)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_rsplit_recover, DB___bam_rsplit)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_adj_recover, DB___bam_adj)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_cadjust_recover, DB___bam_cadjust)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_cdel_recover, DB___bam_cdel)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_repl_recover, DB___bam_repl)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_irep_recover, DB___bam_irep)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_root_recover, DB___bam_root)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_curadj_recover, DB___bam_curadj)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __bam_rcuradj_recover, DB___bam_rcuradj)) != 0)
		return (ret);
	return (0);
}
