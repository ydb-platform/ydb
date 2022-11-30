#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "jsonrpc_internal.h"
#include "spdk/string.h"
#include "spdk/util.h"

struct spdk_jsonrpc_server *
spdk_jsonrpc_server_listen(int domain, int protocol,
			   struct sockaddr *listen_addr, socklen_t addrlen,
			   spdk_jsonrpc_handle_request_fn handle_request)
{
	struct spdk_jsonrpc_server *server;
	int rc, val, flag, i;

	server = calloc(1, sizeof(struct spdk_jsonrpc_server));
	if (server == NULL) {
		return NULL;
	}

	TAILQ_INIT(&server->free_conns);
	TAILQ_INIT(&server->conns);

	for (i = 0; i < SPDK_JSONRPC_MAX_CONNS; i++) {
		TAILQ_INSERT_TAIL(&server->free_conns, &server->conns_array[i], link);
	}

	server->handle_request = handle_request;

	server->sockfd = socket(domain, SOCK_STREAM, protocol);
	if (server->sockfd < 0) {
		SPDK_ERRLOG("socket() failed\n");
		free(server);
		return NULL;
	}

	val = 1;
	setsockopt(server->sockfd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

	flag = fcntl(server->sockfd, F_GETFL);
	if (fcntl(server->sockfd, F_SETFL, flag | O_NONBLOCK) < 0) {
		SPDK_ERRLOG("fcntl can't set nonblocking mode for socket, fd: %d (%s)\n",
			    server->sockfd, spdk_strerror(errno));
		close(server->sockfd);
		free(server);
		return NULL;
	}

	rc = bind(server->sockfd, listen_addr, addrlen);
	if (rc != 0) {
		SPDK_ERRLOG("could not bind JSON-RPC server: %s\n", spdk_strerror(errno));
		close(server->sockfd);
		free(server);
		return NULL;
	}

	rc = listen(server->sockfd, 512);
	if (rc != 0) {
		SPDK_ERRLOG("listen() failed, errno = %d\n", errno);
		close(server->sockfd);
		free(server);
		return NULL;
	}

	return server;
}

static struct spdk_jsonrpc_request *
jsonrpc_server_dequeue_request(struct spdk_jsonrpc_server_conn *conn)
{
	struct spdk_jsonrpc_request *request = NULL;

	pthread_spin_lock(&conn->queue_lock);
	request = STAILQ_FIRST(&conn->send_queue);
	if (request) {
		STAILQ_REMOVE_HEAD(&conn->send_queue, link);
	}
	pthread_spin_unlock(&conn->queue_lock);
	return request;
}

static void
jsonrpc_server_free_conn_request(struct spdk_jsonrpc_server_conn *conn)
{
	struct spdk_jsonrpc_request *request;

	jsonrpc_free_request(conn->send_request);
	conn->send_request = NULL ;
	while ((request = jsonrpc_server_dequeue_request(conn)) != NULL) {
		jsonrpc_free_request(request);
	}
}

static void
jsonrpc_server_conn_close(struct spdk_jsonrpc_server_conn *conn)
{
	conn->closed = true;

	if (conn->sockfd >= 0) {
		jsonrpc_server_free_conn_request(conn);
		close(conn->sockfd);
		conn->sockfd = -1;

		if (conn->close_cb) {
			conn->close_cb(conn, conn->close_cb_ctx);
		}
	}
}

void
spdk_jsonrpc_server_shutdown(struct spdk_jsonrpc_server *server)
{
	struct spdk_jsonrpc_server_conn *conn;

	close(server->sockfd);

	TAILQ_FOREACH(conn, &server->conns, link) {
		jsonrpc_server_conn_close(conn);
	}

	free(server);
}

static void
jsonrpc_server_conn_remove(struct spdk_jsonrpc_server_conn *conn)
{
	struct spdk_jsonrpc_server *server = conn->server;

	jsonrpc_server_conn_close(conn);

	pthread_spin_destroy(&conn->queue_lock);
	assert(STAILQ_EMPTY(&conn->send_queue));

	TAILQ_REMOVE(&server->conns, conn, link);
	TAILQ_INSERT_HEAD(&server->free_conns, conn, link);
}

int
spdk_jsonrpc_conn_add_close_cb(struct spdk_jsonrpc_server_conn *conn,
			       spdk_jsonrpc_conn_closed_fn cb, void *ctx)
{
	int rc = 0;

	pthread_spin_lock(&conn->queue_lock);
	if (conn->close_cb == NULL) {
		conn->close_cb = cb;
		conn->close_cb_ctx = ctx;
	} else {
		rc = conn->close_cb == cb && conn->close_cb_ctx == ctx ? -EEXIST : -ENOSPC;
	}
	pthread_spin_unlock(&conn->queue_lock);

	return rc;
}

int
spdk_jsonrpc_conn_del_close_cb(struct spdk_jsonrpc_server_conn *conn,
			       spdk_jsonrpc_conn_closed_fn cb, void *ctx)
{
	int rc = 0;

	pthread_spin_lock(&conn->queue_lock);
	if (conn->close_cb == NULL || conn->close_cb != cb || conn->close_cb_ctx != ctx) {
		rc = -ENOENT;
	} else {
		conn->close_cb = NULL;
	}
	pthread_spin_unlock(&conn->queue_lock);

	return rc;
}

static int
jsonrpc_server_accept(struct spdk_jsonrpc_server *server)
{
	struct spdk_jsonrpc_server_conn *conn;
	int rc, flag;

	rc = accept(server->sockfd, NULL, NULL);
	if (rc >= 0) {
		conn = TAILQ_FIRST(&server->free_conns);
		assert(conn != NULL);

		conn->server = server;
		conn->sockfd = rc;
		conn->closed = false;
		conn->recv_len = 0;
		conn->outstanding_requests = 0;
		STAILQ_INIT(&conn->send_queue);
		conn->send_request = NULL;

		if (pthread_spin_init(&conn->queue_lock, PTHREAD_PROCESS_PRIVATE)) {
			SPDK_ERRLOG("Unable to create queue lock for socket: %d", conn->sockfd);
			close(conn->sockfd);
			return -1;
		}

		flag = fcntl(conn->sockfd, F_GETFL);
		if (fcntl(conn->sockfd, F_SETFL, flag | O_NONBLOCK) < 0) {
			SPDK_ERRLOG("fcntl can't set nonblocking mode for socket, fd: %d (%s)\n",
				    conn->sockfd, spdk_strerror(errno));
			close(conn->sockfd);
			pthread_spin_destroy(&conn->queue_lock);
			return -1;
		}

		TAILQ_REMOVE(&server->free_conns, conn, link);
		TAILQ_INSERT_TAIL(&server->conns, conn, link);
		return 0;
	}

	if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
		return 0;
	}

	return -1;
}

void
jsonrpc_server_handle_request(struct spdk_jsonrpc_request *request,
			      const struct spdk_json_val *method, const struct spdk_json_val *params)
{
	request->conn->server->handle_request(request, method, params);
}

void
jsonrpc_server_handle_error(struct spdk_jsonrpc_request *request, int error)
{
	const char *msg;

	switch (error) {
	case SPDK_JSONRPC_ERROR_PARSE_ERROR:
		msg = "Parse error";
		break;

	case SPDK_JSONRPC_ERROR_INVALID_REQUEST:
		msg = "Invalid request";
		break;

	case SPDK_JSONRPC_ERROR_METHOD_NOT_FOUND:
		msg = "Method not found";
		break;

	case SPDK_JSONRPC_ERROR_INVALID_PARAMS:
		msg = "Invalid parameters";
		break;

	case SPDK_JSONRPC_ERROR_INTERNAL_ERROR:
		msg = "Internal error";
		break;

	default:
		msg = "Error";
		break;
	}

	spdk_jsonrpc_send_error_response(request, error, msg);
}

static int
jsonrpc_server_conn_recv(struct spdk_jsonrpc_server_conn *conn)
{
	ssize_t rc, offset;
	size_t recv_avail = SPDK_JSONRPC_RECV_BUF_SIZE - conn->recv_len;

	rc = recv(conn->sockfd, conn->recv_buf + conn->recv_len, recv_avail, 0);
	if (rc == -1) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
			return 0;
		}
		SPDK_DEBUGLOG(rpc, "recv() failed: %s\n", spdk_strerror(errno));
		return -1;
	}

	if (rc == 0) {
		SPDK_DEBUGLOG(rpc, "remote closed connection\n");
		conn->closed = true;
		return 0;
	}

	conn->recv_len += rc;

	offset = 0;
	do {
		rc = jsonrpc_parse_request(conn, conn->recv_buf + offset, conn->recv_len - offset);
		if (rc < 0) {
			SPDK_ERRLOG("jsonrpc parse request failed\n");
			return -1;
		}

		offset += rc;
	} while (rc > 0);

	if (offset > 0) {
		/*
		 * Successfully parsed a requests - move any data past the end of the
		 * parsed requests down to the beginning.
		 */
		assert((size_t)offset <= conn->recv_len);
		memmove(conn->recv_buf, conn->recv_buf + offset, conn->recv_len - offset);
		conn->recv_len -= offset;
	}

	return 0;
}

void
jsonrpc_server_send_response(struct spdk_jsonrpc_request *request)
{
	struct spdk_jsonrpc_server_conn *conn = request->conn;

	/* Queue the response to be sent */
	pthread_spin_lock(&conn->queue_lock);
	STAILQ_INSERT_TAIL(&conn->send_queue, request, link);
	pthread_spin_unlock(&conn->queue_lock);
}


static int
jsonrpc_server_conn_send(struct spdk_jsonrpc_server_conn *conn)
{
	struct spdk_jsonrpc_request *request;
	ssize_t rc;

more:
	if (conn->outstanding_requests == 0) {
		return 0;
	}

	if (conn->send_request == NULL) {
		conn->send_request = jsonrpc_server_dequeue_request(conn);
	}

	request = conn->send_request;
	if (request == NULL) {
		/* Nothing to send right now */
		return 0;
	}

	if (request->send_len > 0) {
		rc = send(conn->sockfd, request->send_buf + request->send_offset,
			  request->send_len, 0);
		if (rc < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
				return 0;
			}

			SPDK_DEBUGLOG(rpc, "send() failed: %s\n", spdk_strerror(errno));
			return -1;
		}

		request->send_offset += rc;
		request->send_len -= rc;
	}

	if (request->send_len == 0) {
		/*
		 * Full response has been sent.
		 * Free it and set send_request to NULL to move on to the next queued response.
		 */
		conn->send_request = NULL;
		jsonrpc_free_request(request);
		goto more;
	}

	return 0;
}

int
spdk_jsonrpc_server_poll(struct spdk_jsonrpc_server *server)
{
	int rc;
	struct spdk_jsonrpc_server_conn *conn, *conn_tmp;

	TAILQ_FOREACH_SAFE(conn, &server->conns, link, conn_tmp) {
		/* If we can't receive and there are no outstanding requests close the connection. */
		if (conn->closed == true && conn->outstanding_requests == 0) {
			jsonrpc_server_conn_close(conn);
		}

		if (conn->sockfd == -1 && conn->outstanding_requests == 0) {
			jsonrpc_server_conn_remove(conn);
		}
	}

	/* Check listen socket */
	if (!TAILQ_EMPTY(&server->free_conns)) {
		jsonrpc_server_accept(server);
	}

	TAILQ_FOREACH(conn, &server->conns, link) {
		if (conn->sockfd == -1) {
			continue;
		}

		rc = jsonrpc_server_conn_send(conn);
		if (rc != 0) {
			jsonrpc_server_conn_close(conn);
			continue;
		}

		if (!conn->closed) {
			rc = jsonrpc_server_conn_recv(conn);
			if (rc != 0) {
				jsonrpc_server_conn_close(conn);
			}
		}
	}

	return 0;
}
