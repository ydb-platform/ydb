/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2005, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$
 */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/btree.h"
#include "dbinc/txn.h"
#include "dbinc_auto/repmgr_auto.h"

static int dispatch_app_message __P((ENV *, REPMGR_MESSAGE *));
static int finish_gmdb_update __P((ENV *,
	DB_THREAD_INFO *, DBT *, u_int32_t, u_int32_t, __repmgr_member_args *));
static int incr_gm_version __P((ENV *, DB_THREAD_INFO *, DB_TXN *));
static void marshal_site_data __P((ENV *, u_int32_t, u_int8_t *, DBT *));
static void marshal_site_key __P((ENV *,
	repmgr_netaddr_t *, u_int8_t *, DBT *, __repmgr_member_args *));
static int message_loop __P((ENV *, REPMGR_RUNNABLE *));
static int process_message __P((ENV*, DBT*, DBT*, int));
static int reject_fwd __P((ENV *, REPMGR_CONNECTION *));
static int rescind_pending __P((ENV *,
	DB_THREAD_INFO *, int, u_int32_t, u_int32_t));
static int resolve_limbo_int __P((ENV *, DB_THREAD_INFO *));
static int resolve_limbo_wrapper __P((ENV *, DB_THREAD_INFO *));
static int send_permlsn __P((ENV *, u_int32_t, DB_LSN *));
static int send_permlsn_conn __P((ENV *,
	REPMGR_CONNECTION *, u_int32_t, DB_LSN *));
static int serve_join_request __P((ENV *,
	DB_THREAD_INFO *, REPMGR_MESSAGE *));
static int serve_remove_request __P((ENV *,
	DB_THREAD_INFO *, REPMGR_MESSAGE *));
static int serve_repmgr_request __P((ENV *, REPMGR_MESSAGE *));

/*
 * Map one of the phase-1/provisional membership status values to its
 * corresponding ultimate goal status: if "adding", the goal is to be fully
 * "present".  Otherwise ("deleting") the goal is to not even appear in the
 * database at all (0).
 */
#define	NEXT_STATUS(s) (u_int32_t)((s) == SITE_ADDING ? SITE_PRESENT : 0)

/*
 * PUBLIC: void *__repmgr_msg_thread __P((void *));
 */
void *
__repmgr_msg_thread(argsp)
	void *argsp;
{
	REPMGR_RUNNABLE *th;
	ENV *env;
	int ret;

	th = argsp;
	env = th->env;

	if ((ret = message_loop(env, th)) != 0) {
		__db_err(env, ret, "message thread failed");
		(void)__repmgr_thread_failure(env, ret);
	}
	return (NULL);
}

static int
message_loop(env, th)
	ENV *env;
	REPMGR_RUNNABLE *th;
{
	DB_REP *db_rep;
	REP *rep;
	REPMGR_MESSAGE *msg;
	REPMGR_CONNECTION *conn;
	REPMGR_SITE *site;
	__repmgr_permlsn_args permlsn;
	int incremented, ret, t_ret;
	u_int32_t membership;

	COMPQUIET(membership, 0);
	db_rep = env->rep_handle;
	rep = db_rep->region;
	LOCK_MUTEX(db_rep->mutex);
	while ((ret = __repmgr_queue_get(env, &msg, th)) == 0) {
		incremented = FALSE;
		if (IS_DEFERRABLE(msg->msg_hdr.type)) {
			/*
			 * Count threads currently processing channel requests
			 * or GMDB operations, so that we can limit the number
			 * of them, in order to avoid starving more important
			 * rep messages.
			 */
			db_rep->non_rep_th++;
			incremented = TRUE;
		}
		if (msg->msg_hdr.type == REPMGR_REP_MESSAGE) {
			DB_ASSERT(env,
			    IS_VALID_EID(msg->v.repmsg.originating_eid));
			site = SITE_FROM_EID(msg->v.repmsg.originating_eid);
			membership = site->membership;
		}
		UNLOCK_MUTEX(db_rep->mutex);

		switch (msg->msg_hdr.type) {
		case REPMGR_REP_MESSAGE:
			if (membership != SITE_PRESENT)
				break;
			while ((ret = process_message(env,
			    &msg->v.repmsg.control, &msg->v.repmsg.rec,
			    msg->v.repmsg.originating_eid)) == DB_LOCK_DEADLOCK)
				RPRINT(env, (env, DB_VERB_REPMGR_MISC,
				    "repmgr deadlock retry"));
			break;
		case REPMGR_APP_MESSAGE:
			ret = dispatch_app_message(env, msg);
			conn = msg->v.appmsg.conn;
			if (conn != NULL) {
				LOCK_MUTEX(db_rep->mutex);
				t_ret = __repmgr_decr_conn_ref(env, conn);
				UNLOCK_MUTEX(db_rep->mutex);
				if (t_ret != 0 && ret == 0)
					ret = t_ret;
			}
			break;
		case REPMGR_OWN_MSG:
			ret = serve_repmgr_request(env, msg);
			break;
		case REPMGR_HEARTBEAT:
			if ((ret = __repmgr_permlsn_unmarshal(env,
			    &permlsn, msg->v.repmsg.control.data,
			    msg->v.repmsg.control.size, NULL)) != 0)
				ret = DB_REP_UNAVAIL;
			else if (rep->master_id == db_rep->self_eid) {
				/*
				 * If a master receives a heartbeat, there
				 * may be a dupmaster.  Resend latest log
				 * message to prompt base replication to
				 * detect it without the need for application
				 * activity.
				 */
				ret = __rep_flush(env->dbenv);
			} else {
				/*
				 * Use heartbeat message to initiate rerequest
				 * processing.
				 */
				ret = __rep_check_missing(env,
				    permlsn.generation, &permlsn.lsn);
			}
			break;
		default:
			ret = __db_unknown_path(env, "message loop");
			break;
		}

		__os_free(env, msg);
		LOCK_MUTEX(db_rep->mutex);
		if (incremented)
			db_rep->non_rep_th--;
		if (ret != 0)
			goto out;
	}
	/*
	 * A return of DB_REP_UNAVAIL from __repmgr_queue_get() merely means we
	 * should finish gracefully.
	 */
	if (ret == DB_REP_UNAVAIL)
		ret = 0;
out:
	UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}

static int
dispatch_app_message(env, msg)
	ENV *env;
	REPMGR_MESSAGE *msg;
{
	DB_REP *db_rep;
	DB_CHANNEL db_channel;
	CHANNEL channel;
	__repmgr_msg_metadata_args meta;
	DBT *dbt, *segment;
	u_int32_t flags, i, size, *uiptr;
	u_int8_t *data;
	void *ptr;
	int ret;

	COMPQUIET(size, 0);

	db_rep = env->rep_handle;

	db_channel.channel = &channel;
	db_channel.send_msg = __repmgr_send_response;

	/* Supply stub functions for methods inapplicable in msg disp func. */
	db_channel.close = __repmgr_channel_close_inval;
	db_channel.send_request = __repmgr_send_request_inval;
	db_channel.set_timeout = __repmgr_channel_timeout_inval;

	channel.msg = msg;
	channel.env = env;
	channel.c.conn = msg->v.appmsg.conn;
	channel.responded = FALSE;
	channel.meta = &meta;

	/*
	 * The user data is in a form similar to that of a bulk buffer.
	 * However, there's also our meta-data tacked on to the end of it.
	 * Fortunately, the meta-data is fixed length, so it's easy to peel it
	 * off.
	 *
	 * The user data "bulk buffer" lacks the usual "-1" end-marker.  But
	 * that's OK, because we already know how many segments there are (from
	 * the message header).  Convert this information into the DBT array
	 * that we will pass to the user's function.
	 *
	 * (See the definition of DB_MULTIPLE_INIT for a reminder of the format
	 * of a bulk buffer.)
	 */
	dbt = &msg->v.appmsg.buf;
	data = dbt->data;
	dbt->size -= __REPMGR_MSG_METADATA_SIZE;
	ret = __repmgr_msg_metadata_unmarshal(env,
	    &meta, &data[dbt->size], __REPMGR_MSG_METADATA_SIZE, NULL);
	DB_ASSERT(env, ret == 0);

	dbt->ulen = dbt->size;
	DB_MULTIPLE_INIT(ptr, dbt);
	for (i = 0; i < APP_MSG_SEGMENT_COUNT(msg->msg_hdr); i++) {
		segment = &msg->v.appmsg.segments[i];
		uiptr = ptr;
		*uiptr = ntohl(*uiptr);
		uiptr[-1] = ntohl(uiptr[-1]);
		DB_MULTIPLE_NEXT(ptr, dbt, data, size);
		DB_ASSERT(env, data != NULL);
		DB_INIT_DBT(*segment, data, size);
	}

	flags = F_ISSET(&meta, REPMGR_REQUEST_MSG_TYPE) ?
	    DB_REPMGR_NEED_RESPONSE : 0;

	if (db_rep->msg_dispatch == NULL) {
		__db_errx(env, DB_STR("3670",
	    "No message dispatch call-back function has been configured"));
		if (F_ISSET(channel.meta, REPMGR_REQUEST_MSG_TYPE))
			return (__repmgr_send_err_resp(env,
			    &channel, DB_NOSERVER));
		else
			return (0);
	}

	(*db_rep->msg_dispatch)(env->dbenv,
	    &db_channel, &msg->v.appmsg.segments[0],
	    APP_MSG_SEGMENT_COUNT(msg->msg_hdr), flags);

	if (F_ISSET(channel.meta, REPMGR_REQUEST_MSG_TYPE) &&
	    !channel.responded) {
		__db_errx(env, DB_STR("3671",
		    "Application failed to provide a response"));
		return (__repmgr_send_err_resp(env, &channel, DB_KEYEMPTY));
	}

	return (0);
}

/*
 * PUBLIC: int __repmgr_send_err_resp __P((ENV *, CHANNEL *, int));
 */
int
__repmgr_send_err_resp(env, channel, err)
	ENV *env;
	CHANNEL *channel;
	int err;
{
	DB_REP *db_rep;
	REPMGR_CONNECTION *conn;
	REPMGR_IOVECS iovecs;
	__repmgr_msg_hdr_args msg_hdr;
	u_int8_t msg_hdr_buf[__REPMGR_MSG_HDR_SIZE];
	int ret;

	db_rep = env->rep_handle;
	msg_hdr.type = REPMGR_RESP_ERROR;

	/* Make it non-negative, so we can send on wire without worry. */
	DB_ASSERT(env, err < 0);
	RESP_ERROR_CODE(msg_hdr) = (u_int32_t)(-err);

	RESP_ERROR_TAG(msg_hdr) = channel->meta->tag;

	__repmgr_iovec_init(&iovecs);
	__repmgr_msg_hdr_marshal(env, &msg_hdr, msg_hdr_buf);
	__repmgr_add_buffer(&iovecs, msg_hdr_buf, __REPMGR_MSG_HDR_SIZE);

	conn = channel->c.conn;
	LOCK_MUTEX(db_rep->mutex);
	ret = __repmgr_send_many(env, conn, &iovecs, 0);
	UNLOCK_MUTEX(db_rep->mutex);

	return (ret);
}

static int
process_message(env, control, rec, eid)
	ENV *env;
	DBT *control, *rec;
	int eid;
{
	DB_LSN lsn;
	DB_REP *db_rep;
	REP *rep;
	int dirty, ret, t_ret;
	u_int32_t generation;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	/*
	 * Save initial generation number, in case it changes in a close race
	 * with a NEWMASTER.
	 */
	generation = rep->gen;

	ret = 0;
	switch (t_ret =
	    __rep_process_message_int(env, control, rec, eid, &lsn)) {
	case 0:
		if (db_rep->takeover_pending)
			ret = __repmgr_claim_victory(env);
		break;

	case DB_REP_HOLDELECTION:
		LOCK_MUTEX(db_rep->mutex);
		ret = __repmgr_init_election(env,
		    ELECT_F_IMMED | ELECT_F_INVITEE);
		UNLOCK_MUTEX(db_rep->mutex);
		break;

	case DB_REP_DUPMASTER:
		/*
		 * Initiate an election if we're configured to be using
		 * elections, but only if we're *NOT* using leases.  When using
		 * leases, there is never any uncertainty over which site is the
		 * rightful master, and only the loser gets the DUPMASTER return
		 * code.
		 */
		if ((ret = __repmgr_become_client(env)) == 0 &&
		    FLD_ISSET(rep->config, REP_C_LEASE | REP_C_ELECTIONS)
		    == REP_C_ELECTIONS) {
			LOCK_MUTEX(db_rep->mutex);
			ret = __repmgr_init_election(env, ELECT_F_IMMED);
			UNLOCK_MUTEX(db_rep->mutex);
		}
		DB_EVENT(env, DB_EVENT_REP_DUPMASTER, NULL);
		break;

	case DB_REP_ISPERM:
#ifdef	CONFIG_TEST
		if (env->test_abort == DB_TEST_REPMGR_PERM)
			VPRINT(env, (env, DB_VERB_REPMGR_MISC,
			"ISPERM: Test hook.  Skip ACK for permlsn [%lu][%lu]",
			(u_long)lsn.file, (u_long)lsn.offset));
#endif
		DB_TEST_SET(env->test_abort, DB_TEST_REPMGR_PERM);
		ret = send_permlsn(env, generation, &lsn);
DB_TEST_RECOVERY_LABEL
		break;

	case DB_LOCK_DEADLOCK:
	case DB_REP_IGNORE:
	case DB_REP_NEWSITE:
	case DB_REP_NOTPERM:
		break;

	case DB_REP_JOIN_FAILURE:
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
			"repmgr fires join failure event"));
		DB_EVENT(env, DB_EVENT_REP_JOIN_FAILURE, NULL);
		break;

	case DB_REP_WOULDROLLBACK:
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
			"repmgr fires would-rollback event"));
		DB_EVENT(env, DB_EVENT_REP_WOULD_ROLLBACK, &lsn);
		break;

	default:
		__db_err(env, t_ret, "DB_ENV->rep_process_message");
		ret = t_ret;
	}

	if (ret != 0)
		goto err;
	LOCK_MUTEX(db_rep->mutex);
	dirty = db_rep->gmdb_dirty;
	db_rep->gmdb_dirty = FALSE;
	UNLOCK_MUTEX(db_rep->mutex);
	if (dirty) {
		if ((ret = __op_rep_enter(env, FALSE, FALSE)) != 0)
			goto err;
		ret = __repmgr_reload_gmdb(env);
		t_ret = __op_rep_exit(env);
		if (ret == ENOENT)
			ret = 0;
		else if (ret == DB_DELETED)
			ret = __repmgr_bow_out(env);
		if (t_ret != 0 && ret == 0)
			ret = t_ret;
	}
err:
	return (ret);
}

/*
 * Handle replication-related events.  Returns only 0 or DB_EVENT_NOT_HANDLED;
 * no other error returns are tolerated.
 *
 * PUBLIC: int __repmgr_handle_event __P((ENV *, u_int32_t, void *));
 */
int
__repmgr_handle_event(env, event, info)
	ENV *env;
	u_int32_t event;
	void *info;
{
	DB_REP *db_rep;

	db_rep = env->rep_handle;

	if (db_rep->selector == NULL) {
		/* Repmgr is not in use, so all events go to application. */
		return (DB_EVENT_NOT_HANDLED);
	}

	switch (event) {
	case DB_EVENT_REP_ELECTED:
		DB_ASSERT(env, info == NULL);
		db_rep->takeover_pending = TRUE;

		/*
		 * The application doesn't really need to see this, because the
		 * purpose of this event is to tell the winning site that it
		 * should call rep_start(MASTER), and in repmgr we do that
		 * automatically.  Still, they could conceivably be curious, and
		 * it doesn't hurt anything to let them know.
		 */
		break;
	case DB_EVENT_REP_INIT_DONE:
		db_rep->gmdb_dirty = TRUE;
		break;
	case DB_EVENT_REP_NEWMASTER:
		DB_ASSERT(env, info != NULL);

		/* Application still needs to see this. */
		break;
	default:
		break;
	}
	return (DB_EVENT_NOT_HANDLED);
}

static int
send_permlsn(env, generation, lsn)
	ENV *env;
	u_int32_t generation;
	DB_LSN *lsn;
{
	DB_REP *db_rep;
	REP *rep;
	REPMGR_CONNECTION *conn;
	REPMGR_SITE *site;
	int ack, bcast, eid, master, policy, ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;
	ret = 0;
	master = rep->master_id;
	LOCK_MUTEX(db_rep->mutex);

	/*
	 * If the file number has changed, send it to everyone, regardless of
	 * anything else.  Otherwise, send it to the master if we know a master,
	 * and that master's ack policy requires it.
	 */
	bcast = FALSE;
	if (LOG_COMPARE(lsn, &db_rep->perm_lsn) > 0) {
		if (lsn->file > db_rep->perm_lsn.file) {
			bcast = TRUE;
			VPRINT(env, (env, DB_VERB_REPMGR_MISC,
			    "send_permlsn: broadcast [%lu][%lu]",
			    (u_long)lsn->file, (u_long)lsn->offset));
		}
		db_rep->perm_lsn = *lsn;
	}
	if (IS_KNOWN_REMOTE_SITE(master)) {
		site = SITE_FROM_EID(master);
		/*
		 * Use master's ack policy if we know it; use our own if the
		 * master is too old (down-rev) to have told us its policy.
		 */
		policy = site->ack_policy > 0 ?
		    site->ack_policy : rep->perm_policy;
		if (policy == DB_REPMGR_ACKS_NONE ||
		    (IS_PEER_POLICY(policy) && rep->priority == 0))
			ack = FALSE;
		else
			ack = TRUE;
	} else {
		site = NULL;
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
		    "dropping ack with no known master"));
		ack = FALSE;
	}

	/*
	 * Send to master first, since we need to send to all its connections.
	 */
	if (site != NULL && (bcast || ack)) {
		if (site->state == SITE_CONNECTED) {
			if ((conn = site->ref.conn.in) != NULL &&
			    conn->state == CONN_READY &&
			    (ret = send_permlsn_conn(env,
			    conn, generation, lsn)) != 0)
				goto unlock;
			if ((conn = site->ref.conn.out) != NULL &&
			    conn->state == CONN_READY &&
			    (ret = send_permlsn_conn(env,
			    conn, generation, lsn)) != 0)
				goto unlock;
		}
		TAILQ_FOREACH(conn, &site->sub_conns, entries) {
			if ((ret = send_permlsn_conn(env,
			    conn, generation, lsn)) != 0)
				goto unlock;
		}
	}
	if (bcast) {
		/*
		 * Send to everyone except the master (since we've already done
		 * that, above).
		 */
		FOR_EACH_REMOTE_SITE_INDEX(eid) {
			if (eid == master)
				continue;
			site = SITE_FROM_EID(eid);
			/*
			 * Send the ack out on primary connections only.
			 */
			if (site->state == SITE_CONNECTED) {
				if ((conn = site->ref.conn.in) != NULL &&
				    conn->state == CONN_READY &&
				    (ret = send_permlsn_conn(env,
				    conn, generation, lsn)) != 0)
					goto unlock;
				if ((conn = site->ref.conn.out) != NULL &&
				    conn->state == CONN_READY &&
				    (ret = send_permlsn_conn(env,
				    conn, generation, lsn)) != 0)
					goto unlock;
			}
		}
	}

unlock:
	UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}

/*
 * Sends a perm LSN message on one connection, if it needs it.
 *
 * !!! Called with mutex held.
 */
static int
send_permlsn_conn(env, conn, generation, lsn)
	ENV *env;
	REPMGR_CONNECTION *conn;
	u_int32_t generation;
	DB_LSN *lsn;
{
	DBT control2, rec2;
	__repmgr_permlsn_args permlsn;
	u_int8_t buf[__REPMGR_PERMLSN_SIZE];
	int ret;

	ret = 0;

	if (conn->state == CONN_READY) {
		DB_ASSERT(env, conn->version > 0);
		permlsn.generation = generation;
		memcpy(&permlsn.lsn, lsn, sizeof(DB_LSN));
		if (conn->version == 1) {
			control2.data = &permlsn;
			control2.size = sizeof(permlsn);
		} else {
			__repmgr_permlsn_marshal(env, &permlsn, buf);
			control2.data = buf;
			control2.size = __REPMGR_PERMLSN_SIZE;
		}
		rec2.size = 0;
		/*
		 * It's hard to imagine anyone would care about a lost ack if
		 * the path to the master is so congested as to need blocking;
		 * so pass "maxblock" argument as 0.
		 */
		if ((ret = __repmgr_send_one(env, conn, REPMGR_PERMLSN,
		    &control2, &rec2, 0)) == DB_REP_UNAVAIL)
			ret = __repmgr_bust_connection(env, conn);
	}
	return (ret);
}

static int
serve_repmgr_request(env, msg)
	ENV *env;
	REPMGR_MESSAGE *msg;
{
	DB_THREAD_INFO *ip;
	DBT *dbt;
	REPMGR_CONNECTION *conn;
	int ret, t_ret;

	ENV_ENTER(env, ip);
	switch (REPMGR_OWN_MSG_TYPE(msg->msg_hdr)) {
	case REPMGR_JOIN_REQUEST:
		ret = serve_join_request(env, ip, msg);
		break;
	case REPMGR_REJOIN:
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
		    "One try at rejoining group automatically"));
		if ((ret = __repmgr_join_group(env)) == DB_REP_UNAVAIL)
			ret = __repmgr_bow_out(env);
		break;
	case REPMGR_REMOVE_REQUEST:
		ret = serve_remove_request(env, ip, msg);
		break;
	case REPMGR_RESOLVE_LIMBO:
		ret = resolve_limbo_wrapper(env, ip);
		break;
	case REPMGR_SHARING:
		dbt = &msg->v.gmdb_msg.request;
		ret = __repmgr_refresh_membership(env, dbt->data, dbt->size);
		break;
	default:
		ret = __db_unknown_path(env, "serve_repmgr_request");
		break;
	}
	if ((conn = msg->v.gmdb_msg.conn) != NULL) {
		if ((t_ret = __repmgr_close_connection(env, conn)) != 0 &&
		    ret == 0)
			ret = t_ret;
		if ((t_ret = __repmgr_decr_conn_ref(env, conn)) != 0 &&
		    ret == 0)
			ret = t_ret;
	}
	ENV_LEAVE(env, ip);
	return (ret);
}

/*
 * Attempts to fulfill a remote site's request to join the replication group.
 * Only the master can grant this request, so if we've received this request
 * when we're not the master, we'll send an appropriate failure message instead.
 */
static int
serve_join_request(env, ip, msg)
	ENV *env;
	DB_THREAD_INFO *ip;
	REPMGR_MESSAGE *msg;
{
	DB_REP *db_rep;
	REPMGR_CONNECTION *conn;
	DBT *dbt;
	__repmgr_site_info_args site_info;
	u_int8_t *buf;
	char *host;
	size_t len;
	u_int32_t status;
	int eid, ret, t_ret;

	db_rep = env->rep_handle;
	COMPQUIET(status, 0);

	conn = msg->v.gmdb_msg.conn;
	dbt = &msg->v.gmdb_msg.request;
	ret = __repmgr_site_info_unmarshal(env,
	    &site_info, dbt->data, dbt->size, NULL);

	host = site_info.host.data;
	host[site_info.host.size - 1] = '\0';
	RPRINT(env, (env, DB_VERB_REPMGR_MISC,
	    "Request to join group from %s:%u", host, (u_int)site_info.port));

	if ((ret = __repmgr_hold_master_role(env, conn)) == DB_REP_UNAVAIL)
		return (0);
	if (ret != 0)
		return (ret);

	LOCK_MUTEX(db_rep->mutex);
	if ((ret = __repmgr_find_site(env, host, site_info.port, &eid)) == 0) {
		DB_ASSERT(env, eid != db_rep->self_eid);
		status = SITE_FROM_EID(eid)->membership;
	}
	UNLOCK_MUTEX(db_rep->mutex);
	if (ret != 0)
		goto err;

	switch (status) {
	case 0:
	case SITE_ADDING:
		ret = __repmgr_update_membership(env, ip, eid, SITE_ADDING);
		break;
	case SITE_PRESENT:
		/* Already in desired state. */
		break;
	case SITE_DELETING:
		ret = rescind_pending(env,
		    ip, eid, SITE_DELETING, SITE_PRESENT);
		break;
	default:
		ret = __db_unknown_path(env, "serve_join_request");
		break;
	}
	if (ret != 0)
		goto err;

	LOCK_MUTEX(db_rep->mutex);
	ret = __repmgr_marshal_member_list(env, &buf, &len);
	UNLOCK_MUTEX(db_rep->mutex);
	if (ret != 0)
		goto err;
	ret = __repmgr_send_sync_msg(env, conn, REPMGR_JOIN_SUCCESS,
	    buf, (u_int32_t)len);
	__os_free(env, buf);

err:

	if ((t_ret = __repmgr_rlse_master_role(env)) != 0 && ret == 0)
		ret = t_ret;

	if (ret == DB_REP_UNAVAIL)
		ret = __repmgr_send_sync_msg(env, conn,
		    REPMGR_GM_FAILURE, NULL, 0);

	return (ret);
}

static int
serve_remove_request(env, ip, msg)
	ENV *env;
	DB_THREAD_INFO *ip;
	REPMGR_MESSAGE *msg;
{
	DB_REP *db_rep;
	REPMGR_CONNECTION *conn;
	REPMGR_SITE *site;
	DBT *dbt;
	__repmgr_site_info_args site_info;
	char *host;
	u_int32_t status, type;
	int eid, ret, t_ret;

	COMPQUIET(status, 0);
	db_rep = env->rep_handle;

	conn = msg->v.gmdb_msg.conn;
	dbt = &msg->v.gmdb_msg.request;
	ret = __repmgr_site_info_unmarshal(env,
	    &site_info, dbt->data, dbt->size, NULL);

	host = site_info.host.data;
	host[site_info.host.size - 1] = '\0';
	RPRINT(env, (env, DB_VERB_REPMGR_MISC,
	    "Request to remove %s:%u from group", host, (u_int)site_info.port));

	if ((ret = __repmgr_hold_master_role(env, conn)) == DB_REP_UNAVAIL)
		return (0);
	if (ret != 0)
		return (ret);

	LOCK_MUTEX(db_rep->mutex);
	if ((site = __repmgr_lookup_site(env, host, site_info.port)) == NULL)
		eid = DB_EID_INVALID;
	else {
		eid = EID_FROM_SITE(site);
		status = site->membership;
	}
	UNLOCK_MUTEX(db_rep->mutex);
	if (eid == DB_EID_INVALID) {
		/* Doesn't exist: already been removed. */
		ret = 0;
		goto err;
	} else if (eid == db_rep->self_eid) {
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
			"Reject request to remove current master"));
		ret = DB_REP_UNAVAIL;
		goto err;
	}

	switch (status) {
	case 0:
		/* Already in desired state. */
		break;
	case SITE_ADDING:
		ret = rescind_pending(env, ip, eid, SITE_ADDING, 0);
		break;
	case SITE_PRESENT:
	case SITE_DELETING:
		ret = __repmgr_update_membership(env, ip, eid, SITE_DELETING);
		break;
	default:
		ret = __db_unknown_path(env, "serve_remove_request");
		break;
	}
err:
	if ((t_ret = __repmgr_rlse_master_role(env)) != 0 && ret == 0)
		ret = t_ret;
	switch (ret) {
	case 0:
		type = REPMGR_REMOVE_SUCCESS;
		break;
	case DB_REP_UNAVAIL:
		type = REPMGR_GM_FAILURE;
		break;
	default:
		return (ret);
	}
	return (__repmgr_send_sync_msg(env, conn, type, NULL, 0));
}

/*
 * Runs a limbo resolution on a message processing thread, upon request from the
 * send() function when it notices that a user transaction has gotten a perm
 * success.  (It wouldn't work for the user thread to do it in-line.)
 */
static int
resolve_limbo_wrapper(env, ip)
	ENV *env;
	DB_THREAD_INFO *ip;
{
	int do_close, ret, t_ret;

	if ((ret = __repmgr_hold_master_role(env, NULL)) == DB_REP_UNAVAIL)
		return (0);
	if (ret != 0)
		return (ret);
retry:
	if ((ret = __repmgr_setup_gmdb_op(env, ip, NULL, 0)) != 0)
		goto rlse;

	/*
	 * A limbo resolution request is merely a "best effort" attempt to
	 * shorten the duration of a pending change.  So if it fails for lack of
	 * acks again, no one really cares.
	 */
	if ((ret = resolve_limbo_int(env, ip)) == DB_REP_UNAVAIL) {
		do_close = FALSE;
		ret = 0;
	} else
		do_close = TRUE;

	if ((t_ret = __repmgr_cleanup_gmdb_op(env, do_close)) != 0 &&
	    ret == 0)
		ret = t_ret;
	if (ret == DB_LOCK_DEADLOCK || ret == DB_LOCK_NOTGRANTED)
		goto retry;
rlse:
	if ((t_ret = __repmgr_rlse_master_role(env)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * Checks for the need to resolve limbo (failure of a previous GMDB update to
 * get enough acks), and does it if nec.  No-op if none needed.
 *
 * Must be called within setup_gmdb_op/cleanup_gmdb_op context.
 */
static int
resolve_limbo_int(env, ip)
	ENV *env;
	DB_THREAD_INFO *ip;
{
	DB_REP *db_rep;
	DB_TXN *txn;
	REPMGR_SITE *site;
	DB_LSN orig_lsn;
	DBT key_dbt, data_dbt;
	__repmgr_member_args logrec;
	repmgr_netaddr_t addr;
	u_int32_t orig_status, status;
	int eid, locked, ret, t_ret;
	u_int8_t data_buf[__REPMGR_MEMBERSHIP_DATA_SIZE];
	u_int8_t key_buf[MAX_MSG_BUF];

	db_rep = env->rep_handle;
	ret = 0;

	LOCK_MUTEX(db_rep->mutex);
	locked = TRUE;

	/*
	 * Is there a previous GMDB update failure currently pending?  If not,
	 * there's nothing for us to do.
	 */
	eid = db_rep->limbo_victim;
	if (!IS_VALID_EID(eid))
		goto out;
	site = SITE_FROM_EID(eid);
	addr = site->net_addr;
	marshal_site_key(env, &addr, key_buf, &key_dbt, &logrec);
	orig_status = site->membership;
	if (orig_status == SITE_PRESENT || orig_status == 0)
		goto out;

	if (IS_ZERO_LSN(db_rep->limbo_failure))
		goto out;

	/*
	 * There are potentially two parts: the self-update of the existing
	 * limbo record, and then the finishing-off if the first is successful.
	 * We might only have to do the finishing-off, if some arbitrary random
	 * txn triggered a limbo resolution request on a msg processing thread.
	 */
	if (LOG_COMPARE(&db_rep->durable_lsn, &db_rep->limbo_failure) > 0) {
		/*
		 * Nice!  Limbo has been resolved by an arbitrary other txn
		 * succeeding subsequently.  So we don't have to do the
		 * "self-update" part.
		 */
	} else {
		/*
		 * Do a self-update, to try to trigger a "durable".  Since
		 * nothing in the database is changing, we need neither an ASL
		 * hint nor a bump in the version sequence.
		 */
		orig_lsn = db_rep->limbo_failure;
		db_rep->active_gmdb_update = gmdb_primary;
		UNLOCK_MUTEX(db_rep->mutex);
		locked = FALSE;

		if ((ret = __txn_begin(env,
		    ip, NULL, &txn, DB_IGNORE_LEASE)) != 0)
			goto out;

		marshal_site_data(env, orig_status, data_buf, &data_dbt);

		ret = __db_put(db_rep->gmdb, ip, txn, &key_dbt, &data_dbt, 0);
		if ((t_ret = __db_txn_auto_resolve(env, txn, 0, ret)) != 0 &&
		    ret == 0)
			ret = t_ret;
		if (ret != 0)
			goto out;

		/*
		 * Check to see whether we got another PERM failure.  This is
		 * quite possible in the case where a GMDB request is being
		 * retried by a requestor, but unlikely if we had a resolution
		 * via an "arbitrary" txn.
		 */
		LOCK_MUTEX(db_rep->mutex);
		locked = TRUE;
		if (LOG_COMPARE(&db_rep->limbo_failure, &orig_lsn) > 0) {
			db_rep->limbo_resolution_needed = TRUE;
			ret = DB_REP_UNAVAIL;
			goto out;
		}
	}
	DB_ASSERT(env, locked);

	/*
	 * Here, either we didn't need to do the self-update, or we did it and
	 * it succeeded.  So now we're ready to do the second phase update.
	 */
	db_rep->limbo_victim = DB_EID_INVALID;
	UNLOCK_MUTEX(db_rep->mutex);
	locked = FALSE;
	status = NEXT_STATUS(orig_status);
	if ((ret = finish_gmdb_update(env,
	    ip, &key_dbt, orig_status, status, &logrec)) != 0)
		goto out;

	/* Track modified membership status in our in-memory sites array. */
	LOCK_MUTEX(db_rep->mutex);
	locked = TRUE;
	if ((ret = __repmgr_set_membership(env,
	    addr.host, addr.port, status)) != 0)
		goto out;
	__repmgr_set_sites(env);

out:
	if (locked)
		UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}

/*
 * Update a specific record in the Group Membership database.  The record to be
 * updated is implied by "eid"; "pstatus" is the provisional status (ADDING or
 * DELETING) to be used in the first phase of the update.  The ultimate goal
 * status is inferred (ADDING -> PRESENT, or DELETING -> 0).
 *
 * PUBLIC: int __repmgr_update_membership __P((ENV *,
 * PUBLIC:     DB_THREAD_INFO *, int, u_int32_t));
 */
int
__repmgr_update_membership(env, ip, eid, pstatus)
	ENV *env;
	DB_THREAD_INFO *ip;
	int eid;
	u_int32_t pstatus;	/* Provisional status. */
{
	DB_REP *db_rep;
	REPMGR_SITE *site;
	DB_TXN *txn;
	DB_LSN lsn, orig_lsn;
	DBT key_dbt, data_dbt;
	__repmgr_member_args logrec;
	repmgr_netaddr_t addr;
	u_int32_t orig_status, ult_status;
	int do_close, locked, ret, t_ret;
	u_int8_t key_buf[MAX_MSG_BUF];
	u_int8_t status_buf[__REPMGR_MEMBERSHIP_DATA_SIZE];

	DB_ASSERT(env, pstatus == SITE_ADDING || pstatus == SITE_DELETING);

	db_rep = env->rep_handle;
	COMPQUIET(orig_status, 0);
	COMPQUIET(addr.host, NULL);
	COMPQUIET(addr.port, 0);

retry:
	txn = NULL;
	locked = FALSE;
	DB_ASSERT(env, db_rep->gmdb_busy);
	if ((ret = __repmgr_setup_gmdb_op(env, ip, NULL, 0)) != 0)
		return (ret);

	/*
	 * Usually we'll keep the GMDB closed, to conserve resources, since
	 * changes should be rare.  However, if a PERM FAIL puts us in limbo, we
	 * expect to clean that up as soon as we can; so leave it open for now
	 * in that case.
	 */
	do_close = TRUE;

	/*
	 * Before attempting any fresh updates, resolve any lingering incomplete
	 * updates from the past (i.e., those that resulted in PERM_FAIL).  If
	 * we can't, then we mustn't proceed with any more updates.  Getting an
	 * additional perm failure would increase the dissonance between the
	 * effective group size and the number of sites from which we can safely
	 * accept acks.  Besides, if we can't clear the previous failure,
	 * there's practically no hope that a new update would fare any better.
	 */
	if ((ret = resolve_limbo_int(env, ip)) != 0) {
		if (ret == DB_REP_UNAVAIL)
			do_close = FALSE;
		goto err;
	}

	/*
	 * If there was a successful limbo resolution, it could have either been
	 * for some unrelated change, or it could have been the same change our
	 * caller is now (re-)trying to perform.  In the latter case, we have
	 * nothing more to do -- resolve_limbo() has done it all for us!  To
	 * find out, compare the site's current status with the ultimate goal
	 * status associated with the provisional status that was passed to us
	 * as input.
	 */
	LOCK_MUTEX(db_rep->mutex);
	locked = TRUE;
	DB_ASSERT(env, IS_KNOWN_REMOTE_SITE(eid));
	site = SITE_FROM_EID(eid);
	if ((orig_status = site->membership) == NEXT_STATUS(pstatus))
		goto err;
	addr = site->net_addr;

	/*
	 * Anticipate modified membership status in our in-memory sites array.
	 * This forces us into an awkward rescission, below, if our transaction
	 * suffers a hard failure and must be aborted.  But it's necessary
	 * because of the requirement that, on additions, the quorum computation
	 * must be based on the incremented nsites value.  An alternative might
	 * possibly be to increment nsites separately from adding the new site
	 * to the array, or even having a special epicycle at the point where
	 * send() counts acks (we'd have to make active_gmdb_update richer), but
	 * those seem even more confusing.
	 */
	if ((ret = __repmgr_set_membership(env,
	    addr.host, addr.port, pstatus)) != 0)
		goto err;
	__repmgr_set_sites(env);

	/*
	 * Hint to our send() function that we want to know the result of ack
	 * counting.
	 */
	orig_lsn = db_rep->limbo_failure;
	db_rep->active_gmdb_update = gmdb_primary;
	UNLOCK_MUTEX(db_rep->mutex);
	locked = FALSE;

	if ((ret = __txn_begin(env, ip, NULL, &txn, DB_IGNORE_LEASE)) != 0)
		goto err;
	marshal_site_key(env, &addr, key_buf, &key_dbt, &logrec);
	marshal_site_data(env, pstatus, status_buf, &data_dbt);
	if ((ret = __db_put(db_rep->gmdb,
	    ip, txn, &key_dbt, &data_dbt, 0)) != 0)
		goto err;
	if ((ret = incr_gm_version(env, ip, txn)) != 0)
		goto err;

	/*
	 * Add some information to the log for this txn.  This is an annotation,
	 * for the sole purpose of enabling the client to notice whenever a
	 * change has occurred in this database.  It has nothing to do with
	 * local recovery.
	 */
	ZERO_LSN(lsn);
	if ((ret = __repmgr_member_log(env,
	    txn, &lsn, 0, db_rep->membership_version,
	    orig_status, pstatus, &logrec.host, logrec.port)) != 0)
		goto err;
	ret = __txn_commit(txn, 0);
	txn = NULL;
	if (ret != 0)
		goto err;

	LOCK_MUTEX(db_rep->mutex);
	locked = TRUE;

	if (LOG_COMPARE(&db_rep->limbo_failure, &orig_lsn) > 0) {
		/*
		 * Failure LSN advanced, meaning this update wasn't acked by
		 * enough clients.
		 */
		db_rep->limbo_resolution_needed = TRUE;
		db_rep->limbo_victim = eid;
		ret = DB_REP_UNAVAIL;
		do_close = FALSE;
		goto err;
	}

	/* Now we'll complete the status change. */
	ult_status = NEXT_STATUS(pstatus);
	UNLOCK_MUTEX(db_rep->mutex);
	locked = FALSE;

	if ((ret = finish_gmdb_update(env, ip,
	    &key_dbt, pstatus, ult_status, &logrec)) != 0)
		goto err;

	/* Track modified membership status in our in-memory sites array. */
	LOCK_MUTEX(db_rep->mutex);
	locked = TRUE;
	ret = __repmgr_set_membership(env, addr.host, addr.port, ult_status);
	__repmgr_set_sites(env);

err:
	if (locked)
		UNLOCK_MUTEX(db_rep->mutex);
	if (txn != NULL) {
		DB_ASSERT(env, ret != 0);
		(void)__txn_abort(txn);
		/*
		 * We've just aborted the txn which moved the site info from
		 * orig_status to something else, so restore that value now so
		 * that we keep in sync.
		 */
		(void)__repmgr_set_membership(env,
		    addr.host, addr.port, orig_status);
	}
	if ((t_ret = __repmgr_cleanup_gmdb_op(env, do_close)) != 0 &&
	    ret == 0)
		ret = t_ret;
	if (ret == DB_LOCK_DEADLOCK || ret == DB_LOCK_NOTGRANTED)
		goto retry;
	return (ret);
}

/*
 * Rescind a partially completed membership DB change, setting the new status to
 * the value given.
 */
static int
rescind_pending(env, ip, eid, cur_status, new_status)
	ENV *env;
	DB_THREAD_INFO *ip;
	int eid;
	u_int32_t cur_status, new_status;
{
	DB_REP *db_rep;
	REPMGR_SITE *site;
	DBT key_dbt;
	__repmgr_member_args logrec;
	repmgr_netaddr_t addr;
	u_int8_t key_buf[MAX_MSG_BUF];
	int ret, t_ret;

	db_rep = env->rep_handle;

retry:
	if ((ret = __repmgr_setup_gmdb_op(env, ip, NULL, 0)) != 0)
		return (ret);

	LOCK_MUTEX(db_rep->mutex);
	DB_ASSERT(env, IS_KNOWN_REMOTE_SITE(eid));
	site = SITE_FROM_EID(eid);
	addr = site->net_addr;
	UNLOCK_MUTEX(db_rep->mutex);

	marshal_site_key(env, &addr, key_buf, &key_dbt, &logrec);
	if ((ret = finish_gmdb_update(env,
	    ip, &key_dbt, cur_status, new_status, &logrec)) != 0)
		goto err;

	/* Track modified membership status in our in-memory sites array. */
	LOCK_MUTEX(db_rep->mutex);
	ret = __repmgr_set_membership(env, addr.host, addr.port, new_status);
	__repmgr_set_sites(env);
	UNLOCK_MUTEX(db_rep->mutex);

err:
	if ((t_ret = __repmgr_cleanup_gmdb_op(env, TRUE)) != 0 &&
	    ret == 0)
		ret = t_ret;
	if (ret == DB_LOCK_DEADLOCK || ret == DB_LOCK_NOTGRANTED)
		goto retry;
	return (ret);
}

/*
 * Caller must have already taken care of serializing this operation
 * (hold_master_role(), setup_gmdb_op()).
 */
static int
incr_gm_version(env, ip, txn)
	ENV *env;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
{
	DB_REP *db_rep;
	u_int32_t version;
	int ret;

	db_rep = env->rep_handle;
	version = db_rep->membership_version + 1;
	if ((ret = __repmgr_set_gm_version(env, ip, txn, version)) == 0)
		db_rep->membership_version = version;
	return (ret);
}

/*
 * PUBLIC: int __repmgr_set_gm_version __P((ENV *,
 * PUBLIC:     DB_THREAD_INFO *, DB_TXN *, u_int32_t));
 */
int
__repmgr_set_gm_version(env, ip, txn, version)
	ENV *env;
	DB_THREAD_INFO *ip;
	DB_TXN *txn;
	u_int32_t version;
{
	DB_REP *db_rep;
	DBT key_dbt, data_dbt;
	__repmgr_membership_key_args key;
	__repmgr_member_metadata_args metadata;
	u_int8_t key_buf[__REPMGR_MEMBERSHIP_KEY_SIZE + 1];
	u_int8_t metadata_buf[__REPMGR_MEMBER_METADATA_SIZE];
	size_t len;
	int ret;

	db_rep = env->rep_handle;

	metadata.format = REPMGR_GMDB_FMT_VERSION;
	metadata.version = version;
	__repmgr_member_metadata_marshal(env, &metadata, metadata_buf);
	DB_INIT_DBT(data_dbt, metadata_buf, __REPMGR_MEMBER_METADATA_SIZE);

	DB_INIT_DBT(key.host, NULL, 0);
	key.port = 0;
	ret = __repmgr_membership_key_marshal(env,
	    &key, key_buf, sizeof(key_buf), &len);
	DB_ASSERT(env, ret == 0);
	DB_INIT_DBT(key_dbt, key_buf, len);

	if ((ret = __db_put(db_rep->gmdb,
	    ip, txn, &key_dbt, &data_dbt, 0)) != 0)
		return (ret);
	return (0);
}

/*
 * Performs the second phase of a 2-phase membership DB operation: an "adding"
 * site becomes fully "present" in the group; a "deleting" site is finally
 * really deleted.
 */
static int
finish_gmdb_update(env, ip, key_dbt, prev_status, status, logrec)
	ENV *env;
	DB_THREAD_INFO *ip;
	DBT *key_dbt;
	u_int32_t prev_status, status;
	__repmgr_member_args *logrec;
{
	DB_REP *db_rep;
	DB_LSN lsn;
	DB_TXN *txn;
	DBT data_dbt;
	u_int8_t data_buf[__REPMGR_MEMBERSHIP_DATA_SIZE];
	int ret, t_ret;

	db_rep = env->rep_handle;

	db_rep->active_gmdb_update = gmdb_secondary;
	if ((ret = __txn_begin(env, ip, NULL, &txn, DB_IGNORE_LEASE)) != 0)
		return (ret);

	if (status == 0)
		ret = __db_del(db_rep->gmdb, ip, txn, key_dbt, 0);
	else {
		marshal_site_data(env, status, data_buf, &data_dbt);
		ret = __db_put(db_rep->gmdb, ip, txn, key_dbt, &data_dbt, 0);
	}
	if (ret != 0)
		goto err;

	if ((ret = incr_gm_version(env, ip, txn)) != 0)
		goto err;

	ZERO_LSN(lsn);
	if ((ret = __repmgr_member_log(env,
	    txn, &lsn, 0, db_rep->membership_version,
	    prev_status, status, &logrec->host, logrec->port)) != 0)
		goto err;

err:
	if ((t_ret = __db_txn_auto_resolve(env, txn, 0, ret)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/*
 * Set up everything we need to update the Group Membership database.  This may
 * or may not include providing a transaction in which to do the updates
 * (depending on whether the caller wants the creation of the database to be in
 * the same transaction as the updates).
 *
 * PUBLIC: int __repmgr_setup_gmdb_op __P((ENV *,
 * PUBLIC:     DB_THREAD_INFO *, DB_TXN **, u_int32_t));
 */
int
__repmgr_setup_gmdb_op(env, ip, txnp, flags)
	ENV *env;
	DB_THREAD_INFO *ip;
	DB_TXN **txnp;
	u_int32_t flags;
{
	DB_REP *db_rep;
	DB_TXN *txn;
	DB *dbp;
	int ret, was_open;

	db_rep = env->rep_handle;

	dbp = NULL;
	txn = NULL;

	/*
	 * If the caller provided a place to return a txn handle, create it and
	 * perform any open operation as part of that txn.  The caller is
	 * responsible for disposing of the txn.  Otherwise, only begin a txn if
	 * we need to do the open and in that case commit it right after the
	 * open.
	 */
	DB_ASSERT(env, db_rep->gmdb_busy);
	was_open = db_rep->gmdb != NULL;
	if ((txnp != NULL || !was_open) &&
	    (ret = __txn_begin(env, ip, NULL, &txn, DB_IGNORE_LEASE)) != 0)
		goto err;

	if (!was_open) {
		DB_ASSERT(env, txn != NULL);
		/*
		 * Opening the membership database is like a secondary GMDB
		 * operation, in the sense that we don't care how many clients
		 * ack it, yet we don't want the application to see any perm
		 * failure events.
		 */
		DB_ASSERT(env, db_rep->active_gmdb_update == none);
		db_rep->active_gmdb_update = gmdb_secondary;
		ret = __rep_open_sysdb(env,
		    ip, txn, REPMEMBERSHIP, flags, &dbp);
		if (ret == 0 && txnp == NULL) {
			/* The txn was just for the open operation. */
			ret = __txn_commit(txn, 0);
			txn = NULL;
		}
		db_rep->active_gmdb_update = none;
		if (ret != 0)
			goto err;
	}

	/*
	 * Lock out normal API operations.  Again because we need to know that
	 * if a PERM_FAIL occurs, it was associated with our txn.  Also, so that
	 * we avoid confusing the application with a PERM_FAIL event for our own
	 * txn.
	 */
	if ((ret = __rep_take_apilockout(env)) != 0)
		goto err;

	/*
	 * Here, all steps have succeeded.  Stash and/or pass back the fruits of
	 * our labor.
	 */
	if (!was_open) {
		DB_ASSERT(env, dbp != NULL);
		db_rep->gmdb = dbp;
	}
	if (txnp != NULL) {
		DB_ASSERT(env, txn != NULL);
		*txnp = txn;
	}
	/*
	 * In the successful case, a later call to cleanup_gmdb_op will
	 * ENV_LEAVE.
	 */
	return (0);

err:
	DB_ASSERT(env, ret != 0);
	if (dbp != NULL)
		(void)__db_close(dbp, txn, DB_NOSYNC);
	if (txn != NULL)
		(void)__txn_abort(txn);
	return (ret);
}

/*
 * PUBLIC: int __repmgr_cleanup_gmdb_op __P((ENV *, int));
 */
int
__repmgr_cleanup_gmdb_op(env, do_close)
	ENV *env;
	int do_close;
{
	DB_REP *db_rep;
	int ret, t_ret;

	db_rep = env->rep_handle;
	db_rep->active_gmdb_update = none;
	ret = __rep_clear_apilockout(env);

	if (do_close && db_rep->gmdb != NULL) {
		if ((t_ret = __db_close(db_rep->gmdb, NULL, DB_NOSYNC) != 0) &&
		    ret == 0)
			ret = t_ret;
		db_rep->gmdb = NULL;
	}
	return (ret);
}

/*
 * Check whether we're currently master, and if so hold that role so that we can
 * perform a Group Membership database operation.  After a successful call, the
 * caller must call rlse_master_role to release the hold.
 *
 * If we can't guarantee that we can remain master, send an appropriate failure
 * message on the given connection (unless NULL).
 *
 * We also ensure that only one GMDB operation will take place at time, for a
 * couple of reasons: if we get a PERM_FAIL it means the fate of the change is
 * indeterminate, so we have to assume the worst.  We have to assume the higher
 * value of nsites, yet we can't accept ack from the questionable site.  If we
 * allowed concurrent operations, this could lead to more than one questionable
 * site, which would be even worse.  Also, when we get a PERM_FAIL we want to
 * know which txn failed, and that would be messy if there could be several.
 *
 * Of course we can't simply take the mutex for the duration, because
 * the mutex needs to be available in order to send out the log
 * records.
 *
 * PUBLIC: int __repmgr_hold_master_role __P((ENV *, REPMGR_CONNECTION *));
 */
int
__repmgr_hold_master_role(env, conn)
	ENV *env;
	REPMGR_CONNECTION *conn;
{
	DB_REP *db_rep;
	REP *rep;
	int ret, t_ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	LOCK_MUTEX(db_rep->mutex);
	if ((ret = __repmgr_await_gmdbop(env)) == 0) {
		/*
		 * If we're currently master, but client_intent is set, it means
		 * that another thread is on the way to becoming master, so we
		 * can't promise to hold the master role for the caller: we've
		 * lost a close race.
		 */
		if (rep->master_id != db_rep->self_eid ||
		    db_rep->client_intent)
			ret = DB_REP_UNAVAIL;
		else
			db_rep->gmdb_busy = TRUE;
	}
	UNLOCK_MUTEX(db_rep->mutex);
	if (conn != NULL && ret == DB_REP_UNAVAIL &&
	    (t_ret = reject_fwd(env, conn)) != 0)
		ret = t_ret;
	return (ret);
}

/*
 * Releases the "master role" lock once we're finished performing a membership
 * DB operation.
 *
 * PUBLIC: int __repmgr_rlse_master_role __P((ENV *));
 */
int
__repmgr_rlse_master_role(env)
	ENV *env;
{
	DB_REP *db_rep;
	int ret;

	db_rep = env->rep_handle;
	LOCK_MUTEX(db_rep->mutex);
	db_rep->gmdb_busy = FALSE;
	ret = __repmgr_signal(&db_rep->gmdb_idle);
	UNLOCK_MUTEX(db_rep->mutex);
	return (ret);
}

/*
 * Responds to a membership change request in the case we're not currently
 * master.  If we know the master, responds with a "forward" message, to tell
 * the requestor who is master.  Otherwise rejects it outright.
 */
static int
reject_fwd(env, conn)
	ENV *env;
	REPMGR_CONNECTION *conn;
{
	DB_REP *db_rep;
	REP *rep;
	SITE_STRING_BUFFER site_string;
	__repmgr_gm_fwd_args fwd;
	repmgr_netaddr_t addr;
	u_int8_t buf[MAX_MSG_BUF];
	u_int32_t msg_type;
	size_t len;
	int ret;

	db_rep = env->rep_handle;
	rep = db_rep->region;

	if (IS_KNOWN_REMOTE_SITE(rep->master_id)) {
		msg_type = REPMGR_GM_FORWARD;
		LOCK_MUTEX(db_rep->mutex);
		addr = SITE_FROM_EID(rep->master_id)->net_addr;
		UNLOCK_MUTEX(db_rep->mutex);
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
		    "Forwarding request to master %s",
		    __repmgr_format_addr_loc(&addr, site_string)));
		fwd.host.data = addr.host;
		fwd.host.size = (u_int32_t)strlen(fwd.host.data) + 1;
		fwd.port = addr.port;
		fwd.gen = rep->mgen;
		ret = __repmgr_gm_fwd_marshal(env,
		    &fwd, buf, sizeof(buf), &len);
		DB_ASSERT(env, ret == 0);
	} else {
		RPRINT(env, (env, DB_VERB_REPMGR_MISC,
		    "Rejecting membership request with no known master"));
		msg_type = REPMGR_GM_FAILURE;
		len = 0;
	}

	return (__repmgr_send_sync_msg(env, conn,
	    msg_type, buf, (u_int32_t)len));
}

/*
 * The length of "buf" must be at least MAX_GMDB_KEY.
 */
static void
marshal_site_key(env, addr, buf, dbt, logrec)
	ENV *env;
	repmgr_netaddr_t *addr;
	u_int8_t *buf;
	DBT *dbt;
	__repmgr_member_args *logrec;
{
	__repmgr_membership_key_args key;
	size_t len;
	int ret;

	DB_INIT_DBT(key.host, addr->host, strlen(addr->host) + 1);
	logrec->host = key.host;
	key.port = addr->port;
	logrec->port = key.port;
	ret = __repmgr_membership_key_marshal(env,
	    &key, buf, MAX_MSG_BUF, &len);
	DB_ASSERT(env, ret == 0);
	DB_INIT_DBT(*dbt, buf, len);
}

static void
marshal_site_data(env, status, buf, dbt)
	ENV *env;
	u_int32_t status;
	u_int8_t *buf;
	DBT *dbt;
{
	__repmgr_membership_data_args member_status;

	member_status.flags = status;
	__repmgr_membership_data_marshal(env, &member_status, buf);
	DB_INIT_DBT(*dbt, buf, __REPMGR_MEMBERSHIP_DATA_SIZE);
}

/*
 * PUBLIC: void __repmgr_set_sites __P((ENV *));
 *
 * Caller must hold mutex.
 */
void
__repmgr_set_sites(env)
	ENV *env;
{
	DB_REP *db_rep;
	int ret;
	u_int32_t n;
	u_int i;

	db_rep = env->rep_handle;

	for (i = 0, n = 0; i < db_rep->site_cnt; i++) {
		if (db_rep->sites[i].membership > 0)
			n++;
	}
	ret = __rep_set_nsites_int(env, n);
	DB_ASSERT(env, ret == 0);
}
