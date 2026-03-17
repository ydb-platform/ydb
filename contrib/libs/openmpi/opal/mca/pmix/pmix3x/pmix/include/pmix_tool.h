/*
 * Copyright (c) 2013-2018 Intel, Inc.  All rights reserved.
 * Copyright (c) 2015      Artem Y. Polyakov <artpol84@gmail.com>.
 *                         All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * - Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer listed
 *   in this license in the documentation and/or other materials
 *   provided with the distribution.
 *
 * - Neither the name of the copyright holders nor the names of its
 *   contributors may be used to endorse or promote products derived from
 *   this software without specific prior written permission.
 *
 * The copyright holders provide no reassurances that the source code
 * provided does not infringe any patent, copyright, or any other
 * intellectual property rights of third parties.  The copyright holders
 * disclaim any liability to any recipient for claims brought against
 * recipient by any third party for infringement of that parties
 * intellectual property rights.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * $HEADER$
 *
 * PMIx provides a "function-shipping" approach to support for
 * implementing the server-side of the protocol. This method allows
 * resource managers to implement the server without being burdened
 * with PMIx internal details. Accordingly, each PMIx API is mirrored
 * here in a function call to be provided by the server. When a
 * request is received from the client, the corresponding server function
 * will be called with the information.
 *
 * Any functions not supported by the RM can be indicated by a NULL for
 * the function pointer. Client calls to such functions will have a
 * "not supported" error returned.
 */

#ifndef PMIx_TOOL_API_H
#define PMIx_TOOL_API_H

/* provide access to the rest of the client functions */
#include <pmix.h>

#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

/****    TOOL INIT/FINALIZE FUNCTIONS    ****/

/* Initialize the PMIx tool, returning the process identifier assigned
 * to this tool in the provided pmix_proc_t struct.
 *
 * When called the PMIx tool library will check for the required connection
 * information of the local PMIx server and will establish the connection.
 * If the information is not found, or the server connection fails, then
 * an appropriate error constant will be returned.
 *
 * If successful, the function will return PMIX_SUCCESS and will fill the
 * provided structure with the server-assigned namespace and rank of the tool.
 *
 * Note that the PMIx tool library is referenced counted, and so multiple
 * calls to PMIx_tool_init are allowed. Thus, one way to obtain the namespace and
 * rank of the process is to simply call PMIx_tool_init with a non-NULL parameter.
 *
 * The info array is used to pass user requests pertaining to the init
 * and subsequent operations. Passing a _NULL_ value for the array pointer
 * is supported if no directives are desired.
 */
PMIX_EXPORT pmix_status_t PMIx_tool_init(pmix_proc_t *proc,
                                         pmix_info_t info[], size_t ninfo);

/* Finalize the PMIx tool library, closing the connection to the local server.
 * An error code will be returned if, for some reason, the connection
 * cannot be closed.
 *
 * The info array is used to pass user requests regarding the finalize
 * operation. */
PMIX_EXPORT pmix_status_t PMIx_tool_finalize(void);

/* Switch server connection. Closes the connection, if existing, to a server
 * and establishes a connection to the specified server. The target server can
 * be given as:
 *
 * - PMIX_CONNECT_TO_SYSTEM: connect solely to the system server
 *
 * - PMIX_CONNECT_SYSTEM_FIRST: a request to use the system server first,
 *   if existing, and then look for the server specified in a different
 *   attribute
 *
 * - PMIX_SERVER_URI: connect to the server at the given URI
 *
 * - PMIX_SERVER_NSPACE: connect to the server of a given nspace
 *
 * - PMIX_SERVER_PIDINFO: connect to a server embedded in the process with
 *   the given pid
 *
 * Passing a _NULL_ value for the info array pointer is not allowed and will
 * result in return of an error.
 *
 * NOTE: PMIx does not currently support on-the-fly changes to the tool's
 * identifier. Thus, the new server must be under the same nspace manager
 * (e.g., host RM) as the prior server so that the original nspace remains
 * a unique assignment. The proc parameter is included here for obsolence
 * protection in case this constraint is someday removed. Meantime, the
 * proc parameter will be filled with the tool's existing nspace/rank, and
 * the caller is welcome to pass _NULL_ in that location
 */
PMIX_EXPORT pmix_status_t PMIx_tool_connect_to_server(pmix_proc_t *proc,
                                                      pmix_info_t info[], size_t ninfo);

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif

#endif
