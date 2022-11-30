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

#include "spdk/stdinc.h"
#include "spdk/env.h"
#include "spdk/log.h"

struct sigbus_handler {
	spdk_pci_error_handler func;
	void *ctx;

	TAILQ_ENTRY(sigbus_handler) tailq;
};

static pthread_mutex_t g_sighandler_mutex = PTHREAD_MUTEX_INITIALIZER;
static TAILQ_HEAD(, sigbus_handler) g_sigbus_handler =
	TAILQ_HEAD_INITIALIZER(g_sigbus_handler);

static void
sigbus_fault_sighandler(int signum, siginfo_t *info, void *ctx)
{
	struct sigbus_handler *sigbus_handler;

	pthread_mutex_lock(&g_sighandler_mutex);
	TAILQ_FOREACH(sigbus_handler, &g_sigbus_handler, tailq) {
		sigbus_handler->func(info, sigbus_handler->ctx);
	}
	pthread_mutex_unlock(&g_sighandler_mutex);
}

__attribute__((constructor)) static void
device_set_signal(void)
{
	struct sigaction sa;

	sa.sa_sigaction = sigbus_fault_sighandler;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = SA_SIGINFO;
	sigaction(SIGBUS, &sa, NULL);
}

__attribute__((destructor)) static void
device_destroy_signal(void)
{
	struct sigbus_handler *sigbus_handler, *tmp;

	TAILQ_FOREACH_SAFE(sigbus_handler, &g_sigbus_handler, tailq, tmp) {
		free(sigbus_handler);
	}
}

int
spdk_pci_register_error_handler(spdk_pci_error_handler sighandler, void *ctx)
{
	struct sigbus_handler *sigbus_handler;

	if (!sighandler) {
		SPDK_ERRLOG("Error handler is NULL\n");
		return -EINVAL;
	}

	pthread_mutex_lock(&g_sighandler_mutex);
	TAILQ_FOREACH(sigbus_handler, &g_sigbus_handler, tailq) {
		if (sigbus_handler->func == sighandler) {
			pthread_mutex_unlock(&g_sighandler_mutex);
			SPDK_ERRLOG("Error handler has been registered\n");
			return -EINVAL;
		}
	}
	pthread_mutex_unlock(&g_sighandler_mutex);

	sigbus_handler = calloc(1, sizeof(*sigbus_handler));
	if (!sigbus_handler) {
		SPDK_ERRLOG("Failed to allocate sigbus handler\n");
		return -ENOMEM;
	}

	sigbus_handler->func = sighandler;
	sigbus_handler->ctx = ctx;

	pthread_mutex_lock(&g_sighandler_mutex);
	TAILQ_INSERT_TAIL(&g_sigbus_handler, sigbus_handler, tailq);
	pthread_mutex_unlock(&g_sighandler_mutex);

	return 0;
}

void
spdk_pci_unregister_error_handler(spdk_pci_error_handler sighandler)
{
	struct sigbus_handler *sigbus_handler;

	if (!sighandler) {
		return;
	}

	pthread_mutex_lock(&g_sighandler_mutex);
	TAILQ_FOREACH(sigbus_handler, &g_sigbus_handler, tailq) {
		if (sigbus_handler->func == sighandler) {
			TAILQ_REMOVE(&g_sigbus_handler, sigbus_handler, tailq);
			free(sigbus_handler);
			pthread_mutex_unlock(&g_sighandler_mutex);
			return;
		}
	}
	pthread_mutex_unlock(&g_sighandler_mutex);
}
