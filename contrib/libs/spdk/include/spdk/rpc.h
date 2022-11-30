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

#ifndef SPDK_RPC_CONFIG_H_
#define SPDK_RPC_CONFIG_H_

#include "spdk/stdinc.h"

#include "spdk/jsonrpc.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Verify correctness of registered RPC methods and aliases.
 *
 * Incorrect registrations include:
 * - multiple RPC methods registered with the same name
 * - RPC alias registered with a method that does not exist
 * - RPC alias registered that points to another alias
 *
 * \return true if registrations are all correct, false otherwise
 */
bool spdk_rpc_verify_methods(void);

/**
 * Start listening for RPC connections.
 *
 * \param listen_addr Listening address.
 *
 * \return 0 on success, -1 on failure.
 */
int spdk_rpc_listen(const char *listen_addr);

/**
 * Poll the RPC server.
 */
void spdk_rpc_accept(void);

/**
 * Stop listening for RPC connections.
 */
void spdk_rpc_close(void);

/**
 * Function signature for RPC request handlers.
 *
 * \param request RPC request to handle.
 * \param params Parameters associated with the RPC request.
 */
typedef void (*spdk_rpc_method_handler)(struct spdk_jsonrpc_request *request,
					const struct spdk_json_val *params);

/**
 * Register an RPC method.
 *
 * \param method Name for the registered method.
 * \param func Function registered for this method to handle the RPC request.
 * \param state_mask State mask of the registered method. If the bit of the state of
 * the RPC server is set in the state_mask, the method is allowed. Otherwise, it is rejected.
 */
void spdk_rpc_register_method(const char *method, spdk_rpc_method_handler func,
			      uint32_t state_mask);

/**
 * Register a deprecated alias for an RPC method.
 *
 * \param method Name for the registered method.
 * \param alias Alias for the registered method.
 */
void spdk_rpc_register_alias_deprecated(const char *method, const char *alias);

/**
 * Check if \c method is allowed for \c state_mask
 *
 * \param method Method name
 * \param state_mask state mask to check against
 * \return 0 if method is allowed or negative error code:
 * -EPERM method is not allowed
 * -ENOENT method not found
 */
int spdk_rpc_is_method_allowed(const char *method, uint32_t state_mask);

#define SPDK_RPC_STARTUP	0x1
#define SPDK_RPC_RUNTIME	0x2

/* Give SPDK_RPC_REGISTER a higher execution priority than
 * SPDK_RPC_REGISTER_ALIAS_DEPRECATED to ensure all of the RPCs are registered
 * before we try registering any aliases.  Some older versions of clang may
 * otherwise execute the constructors in a different order than
 * defined in the source file (see issue #892).
 */
#define SPDK_RPC_REGISTER(method, func, state_mask) \
static void __attribute__((constructor(1000))) rpc_register_##func(void) \
{ \
	spdk_rpc_register_method(method, func, state_mask); \
}

#define SPDK_RPC_REGISTER_ALIAS_DEPRECATED(method, alias) \
static void __attribute__((constructor(1001))) rpc_register_##alias(void) \
{ \
	spdk_rpc_register_alias_deprecated(#method, #alias); \
}

/**
 * Set the state mask of the RPC server. Any RPC method whose state mask is
 * equal to the state of the RPC server is allowed.
 *
 * \param state_mask New state mask of the RPC server.
 */
void spdk_rpc_set_state(uint32_t state_mask);

/**
 * Get the current state of the RPC server.
 *
 * \return The current state of the RPC server.
 */
uint32_t spdk_rpc_get_state(void);

#ifdef __cplusplus
}
#endif

#endif
