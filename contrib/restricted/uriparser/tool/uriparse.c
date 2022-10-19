/*
 * uriparser - RFC 3986 URI parsing library
 *
 * Copyright (C) 2013, Radu Hociung <radu.uriparser@ohmi.org>
 * Copyright (C) 2013, Sebastian Pipping <sebastian@pipping.org>
 * All rights reserved.
 *
 * Redistribution and use in source  and binary forms, with or without
 * modification, are permitted provided  that the following conditions
 * are met:
 *
 *     1. Redistributions  of  source  code   must  retain  the  above
 *        copyright notice, this list  of conditions and the following
 *        disclaimer.
 *
 *     2. Redistributions  in binary  form  must  reproduce the  above
 *        copyright notice, this list  of conditions and the following
 *        disclaimer  in  the  documentation  and/or  other  materials
 *        provided with the distribution.
 *
 *     3. Neither the  name of the  copyright holder nor the  names of
 *        its contributors may be used  to endorse or promote products
 *        derived from  this software  without specific  prior written
 *        permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND  ANY EXPRESS OR IMPLIED WARRANTIES,  INCLUDING, BUT NOT
 * LIMITED TO,  THE IMPLIED WARRANTIES OF  MERCHANTABILITY AND FITNESS
 * FOR  A  PARTICULAR  PURPOSE  ARE  DISCLAIMED.  IN  NO  EVENT  SHALL
 * THE  COPYRIGHT HOLDER  OR CONTRIBUTORS  BE LIABLE  FOR ANY  DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL,  EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA,  OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT  LIABILITY,  OR  TORT (INCLUDING  NEGLIGENCE  OR  OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include <stdio.h>
#include <stdlib.h>
#include <uriparser/Uri.h>

#ifdef _WIN32
# include <winsock2.h>
# include <ws2tcpip.h>
# if defined(__MINGW32__) &&                                              \
     (!defined(__MINGW64_VERSION_MAJOR) || __MINGW64_VERSION_MAJOR < 3 || \
      _WIN32_WINNT < _WIN32_WINNT_VISTA)
WINSOCK_API_LINKAGE const char * WSAAPI inet_ntop(
		int af, const void *src, char *dst, size_t size);
# endif
#else
# include <sys/socket.h>
# include <arpa/inet.h>
# include <netinet/in.h>
#endif


#define RANGE(x)  (int)((x).afterLast-(x).first), ((x).first)


void usage(void) {
	printf("Usage: uriparse URI [..]\n");
}


int main(int argc, char *argv[]) {
	int retval = EXIT_SUCCESS;
	int i = 1;

	if (argc < 2) {
		usage();
		exit(1);
	}

	for (; i < argc; i++) {
		UriParserStateA state;
		UriUriA uri;
		char ipstr[INET6_ADDRSTRLEN];

		state.uri = &uri;
		printf("uri:          %s\n", argv[i]);
		if (uriParseUriA(&state, argv[i]) != URI_SUCCESS) {
			/* Failure */
			printf("Failure:      %s @ '%.18s' (#%lu)\n",
					(state.errorCode == URI_ERROR_SYNTAX)
						? "syntax"
						: (state.errorCode == URI_ERROR_MALLOC)
							? "not enough memory"
							: "liburiparser bug (please report)",
					state.errorPos,
					(long unsigned int)(state.errorPos - argv[i]));
			retval = EXIT_FAILURE;
		} else {
			if (uri.scheme.first) {
				printf("scheme:       %.*s\n", RANGE(uri.scheme));
			}
			if (uri.userInfo.first) {
				printf("userInfo:     %.*s\n", RANGE(uri.userInfo));
			}
			if (uri.hostText.first) {
				printf("hostText:     %.*s\n", RANGE(uri.hostText));
			}
			if (uri.hostData.ip4) {
				inet_ntop(AF_INET, uri.hostData.ip4->data, ipstr, sizeof ipstr);
				printf("hostData.ip4: %s\n", ipstr);
			}
			if (uri.hostData.ip6) {
				inet_ntop(AF_INET6, uri.hostData.ip6->data, ipstr, sizeof ipstr);
				printf("hostData.ip6: %s\n", ipstr);
			}
			if (uri.portText.first) {
				printf("portText:     %.*s\n", RANGE(uri.portText));
			}
			if (uri.pathHead) {
				const UriPathSegmentA * p = uri.pathHead;
				for (; p; p = p->next) {
					printf(" .. pathSeg:  %.*s\n", RANGE(p->text));
				}
			}
			if (uri.query.first) {
				printf("query:        %.*s\n", RANGE(uri.query));
			}
			if (uri.fragment.first) {
				printf("fragment:     %.*s\n", RANGE(uri.fragment));
			}
			{
				const char * const absolutePathLabel = "absolutePath: ";
				printf("%s%s\n", absolutePathLabel,
						(uri.absolutePath == URI_TRUE) ? "true" : "false");
				if (uri.hostText.first != NULL) {
					printf("%*s%s\n", (int)strlen(absolutePathLabel), "",
							"(always false for URIs with host)");
				}
			}
		}
		printf("\n");

		uriFreeUriMembersA(&uri);
	}
	return retval;
}
