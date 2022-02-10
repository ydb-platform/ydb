/*
 * uriparser - RFC 3986 URI parsing library
 *
 * Copyright (C) 2007, Weijia Song <songweijia@gmail.com>
 * Copyright (C) 2007, Sebastian Pipping <sebastian@pipping.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

#undef NDEBUG  // because we rely on assert(3) further down

#include <cassert>
#include <cerrno>
#include <cstring>  // memcpy
#include <gtest/gtest.h>

#include <uriparser/Uri.h>

// For defaultMemoryManager
extern "C" {
#include "../src/UriMemory.h"
}


namespace {



static void * failingMalloc(UriMemoryManager * memory, size_t size);
static void * failingCalloc(UriMemoryManager * memory, size_t nmemb, size_t size);
static void * failingRealloc(UriMemoryManager * memory, void * ptr, size_t size);
static void * failingReallocarray(UriMemoryManager * memory, void * ptr, size_t nmemb, size_t size);
static void countingFree(UriMemoryManager * memory, void * ptr);



class FailingMemoryManager {
private:
	UriMemoryManager memoryManager;
	unsigned int callCountAlloc;
	unsigned int callCountFree;
	unsigned int failAllocAfterTimes;

	friend void * failingMalloc(UriMemoryManager * memory, size_t size);
	friend void * failingCalloc(UriMemoryManager * memory, size_t nmemb, size_t size);
	friend void * failingRealloc(UriMemoryManager * memory, void * ptr, size_t size);
	friend void * failingReallocarray(UriMemoryManager * memory, void * ptr, size_t nmemb, size_t size);
	friend void countingFree(UriMemoryManager * memory, void * ptr);

public:
	FailingMemoryManager(unsigned int failAllocAfterTimes = 0)
			: callCountAlloc(0), callCountFree(0),
			failAllocAfterTimes(failAllocAfterTimes) {
		this->memoryManager.malloc = failingMalloc;
		this->memoryManager.calloc = failingCalloc;
		this->memoryManager.realloc = failingRealloc;
		this->memoryManager.reallocarray = failingReallocarray;
		this->memoryManager.free = countingFree;
		this->memoryManager.userData = this;
	}

	UriMemoryManager * operator&() {
		return &(this->memoryManager);
	}

	unsigned int getCallCountFree() const {
		return this->callCountFree;
	}
};



static void * failingMalloc(UriMemoryManager * memory, size_t size) {
	FailingMemoryManager * const fmm = static_cast<FailingMemoryManager *>(memory->userData);
	fmm->callCountAlloc++;
	if (fmm->callCountAlloc > fmm->failAllocAfterTimes) {
		errno = ENOMEM;
		return NULL;
	}
	return malloc(size);
}



static void * failingCalloc(UriMemoryManager * memory, size_t nmemb, size_t size) {
	FailingMemoryManager * const fmm = static_cast<FailingMemoryManager *>(memory->userData);
	fmm->callCountAlloc++;
	if (fmm->callCountAlloc > fmm->failAllocAfterTimes) {
		errno = ENOMEM;
		return NULL;
	}
	return calloc(nmemb, size);
}



static void * failingRealloc(UriMemoryManager * memory, void * ptr, size_t size) {
	FailingMemoryManager * const fmm = static_cast<FailingMemoryManager *>(memory->userData);
	fmm->callCountAlloc++;
	if (fmm->callCountAlloc > fmm->failAllocAfterTimes) {
		errno = ENOMEM;
		return NULL;
	}
	return realloc(ptr, size);
}



static void * failingReallocarray(UriMemoryManager * memory, void * ptr, size_t nmemb, size_t size) {
	return uriEmulateReallocarray(memory, ptr, nmemb, size);
}



static void countingFree(UriMemoryManager * memory, void * ptr) {
	FailingMemoryManager * const fmm = static_cast<FailingMemoryManager *>(memory->userData);
	fmm->callCountFree++;
	return free(ptr);
}



static UriUriA parse(const char * sourceUriString) {
	UriParserStateA state;
	UriUriA uri;
	state.uri = &uri;
	assert(uriParseUriA(&state, sourceUriString) == URI_SUCCESS);
	return uri;
}



static UriQueryListA * parseQueryList(const char * queryString) {
	UriQueryListA * queryList;
	const char * const first = queryString;
	const char * const afterLast = first + strlen(first);
	assert(uriDissectQueryMallocA(&queryList, NULL, first, afterLast)
			== URI_SUCCESS);
	return queryList;
}

}  // namespace



TEST(MemoryManagerCompletenessSuite, AllFunctionMembersRequired) {
	UriUriA uri = parse("whatever");
	UriMemoryManager memory;

	memcpy(&memory, &defaultMemoryManager, sizeof(UriMemoryManager));
	memory.malloc = NULL;
	ASSERT_EQ(uriFreeUriMembersMmA(&uri, &memory),
			  URI_ERROR_MEMORY_MANAGER_INCOMPLETE);

	memcpy(&memory, &defaultMemoryManager, sizeof(UriMemoryManager));
	memory.calloc = NULL;
	ASSERT_EQ(uriFreeUriMembersMmA(&uri, &memory),
			  URI_ERROR_MEMORY_MANAGER_INCOMPLETE);

	memcpy(&memory, &defaultMemoryManager, sizeof(UriMemoryManager));
	memory.realloc = NULL;
	ASSERT_EQ(uriFreeUriMembersMmA(&uri, &memory),
			  URI_ERROR_MEMORY_MANAGER_INCOMPLETE);

	memcpy(&memory, &defaultMemoryManager, sizeof(UriMemoryManager));
	memory.reallocarray = NULL;
	ASSERT_EQ(uriFreeUriMembersMmA(&uri, &memory),
			  URI_ERROR_MEMORY_MANAGER_INCOMPLETE);

	memcpy(&memory, &defaultMemoryManager, sizeof(UriMemoryManager));
	memory.free = NULL;
	ASSERT_EQ(uriFreeUriMembersMmA(&uri, &memory),
			  URI_ERROR_MEMORY_MANAGER_INCOMPLETE);

	memcpy(&memory, &defaultMemoryManager, sizeof(UriMemoryManager));
	ASSERT_EQ(uriFreeUriMembersMmA(&uri, &memory), URI_SUCCESS);
}



TEST(MemoryManagerCompletenessSuite, MallocAndFreeRequiredOnly) {
	UriMemoryManager memory;
	UriMemoryManager backend;

	memcpy(&backend, &defaultMemoryManager, sizeof(UriMemoryManager));
	backend.malloc = NULL;
	ASSERT_EQ(uriCompleteMemoryManager(&memory, &backend),
			  URI_ERROR_MEMORY_MANAGER_INCOMPLETE);

	memcpy(&backend, &defaultMemoryManager, sizeof(UriMemoryManager));
	backend.free = NULL;
	ASSERT_EQ(uriCompleteMemoryManager(&memory, &backend),
			  URI_ERROR_MEMORY_MANAGER_INCOMPLETE);
}



TEST(MemoryManagerTestingSuite, DISABLED_DefaultMemoryManager) {
	ASSERT_EQ(uriTestMemoryManager(&defaultMemoryManager), URI_SUCCESS);
}



TEST(MemoryManagerTestingSuite, CompleteMemoryManager) {
	UriMemoryManager memory;
	UriMemoryManager backend;

	memset(&backend, 0, sizeof(UriMemoryManager));
	backend.malloc = defaultMemoryManager.malloc;
	backend.free = defaultMemoryManager.free;

	ASSERT_EQ(uriCompleteMemoryManager(&memory, &backend),
			URI_SUCCESS);

	ASSERT_EQ(uriTestMemoryManager(&memory), URI_SUCCESS);
}



TEST(MemoryManagerTestingSuite, DISABLED_EmulateCalloc) {
	UriMemoryManager partialEmulationMemoryManager;
	memcpy(&partialEmulationMemoryManager, &defaultMemoryManager,
			sizeof(UriMemoryManager));
	partialEmulationMemoryManager.calloc = uriEmulateCalloc;

	ASSERT_EQ(uriTestMemoryManager(&partialEmulationMemoryManager),
			URI_SUCCESS);
}



TEST(MemoryManagerTestingSuite, EmulateReallocarray) {
	UriMemoryManager partialEmulationMemoryManager;
	memcpy(&partialEmulationMemoryManager, &defaultMemoryManager,
			sizeof(UriMemoryManager));
	partialEmulationMemoryManager.reallocarray = uriEmulateReallocarray;

	ASSERT_EQ(uriTestMemoryManager(&partialEmulationMemoryManager),
			URI_SUCCESS);
}



TEST(MemoryManagerTestingOverflowDetectionSuite, EmulateCalloc) {
	EXPECT_GT(2 * sizeof(size_t), sizeof(void *));

	errno = 0;
	ASSERT_EQ(NULL, uriEmulateCalloc(
			&defaultMemoryManager, (size_t)-1, (size_t)-1));
	ASSERT_EQ(errno, ENOMEM);
}



TEST(MemoryManagerTestingOverflowDetectionSuite, EmulateReallocarray) {
	EXPECT_GT(2 * sizeof(size_t), sizeof(void *));

	errno = 0;
	ASSERT_EQ(NULL, uriEmulateReallocarray(
			&defaultMemoryManager, NULL, (size_t)-1, (size_t)-1));
	ASSERT_EQ(errno, ENOMEM);
}



TEST(MemoryManagerTestingSuite, EmulateCallocAndReallocarray) {
	UriMemoryManager partialEmulationMemoryManager;
	memcpy(&partialEmulationMemoryManager, &defaultMemoryManager,
			sizeof(UriMemoryManager));
	partialEmulationMemoryManager.calloc = uriEmulateCalloc;
	partialEmulationMemoryManager.reallocarray = uriEmulateReallocarray;

	ASSERT_EQ(uriTestMemoryManager(&partialEmulationMemoryManager),
			URI_SUCCESS);
}



TEST(FailingMemoryManagerSuite, AddBaseUriExMm) {
	UriUriA absoluteDest;
	UriUriA relativeSource = parse("foo");
	UriUriA absoluteBase = parse("http://example.org/bar");
	const UriResolutionOptions options = URI_RESOLVE_STRICTLY;
	FailingMemoryManager failingMemoryManager;

	ASSERT_EQ(uriAddBaseUriExMmA(&absoluteDest, &relativeSource,
			&absoluteBase, options, &failingMemoryManager),
			URI_ERROR_MALLOC);

	uriFreeUriMembersA(&relativeSource);
	uriFreeUriMembersA(&absoluteBase);
}



TEST(FailingMemoryManagerSuite, ComposeQueryMallocExMm) {
	char * dest = NULL;
	UriQueryListA * const queryList = parseQueryList("k1=v1");
	UriBool spaceToPlus = URI_TRUE;  // not of interest
	UriBool normalizeBreaks = URI_TRUE;  // not of interest
	FailingMemoryManager failingMemoryManager;

	ASSERT_EQ(uriComposeQueryMallocExMmA(&dest, queryList,
			spaceToPlus, normalizeBreaks, &failingMemoryManager),
			URI_ERROR_MALLOC);

	uriFreeQueryListA(queryList);
}



TEST(FailingMemoryManagerSuite, DissectQueryMallocExMm) {
	UriQueryListA * queryList;
	int itemCount;
	const char * const first = "k1=v1&k2=v2";
	const char * const afterLast = first + strlen(first);
	const UriBool plusToSpace = URI_TRUE;  // not of interest
	const UriBreakConversion breakConversion = URI_BR_DONT_TOUCH;  // not o. i.
	FailingMemoryManager failingMemoryManager;

	ASSERT_EQ(uriDissectQueryMallocExMmA(&queryList, &itemCount,
			first, afterLast, plusToSpace, breakConversion,
			&failingMemoryManager),
			URI_ERROR_MALLOC);
}



TEST(FailingMemoryManagerSuite, FreeQueryListMm) {
	UriQueryListA * const queryList = parseQueryList("k1=v1");
	FailingMemoryManager failingMemoryManager;
	ASSERT_EQ(failingMemoryManager.getCallCountFree(), 0U);

	uriFreeQueryListMmA(queryList, &failingMemoryManager);

	ASSERT_GE(failingMemoryManager.getCallCountFree(), 1U);
}



TEST(FailingMemoryManagerSuite, FreeUriMembersMm) {
	UriUriA uri = parse("http://example.org/");
	FailingMemoryManager failingMemoryManager;
	ASSERT_EQ(failingMemoryManager.getCallCountFree(), 0U);

	uriFreeUriMembersMmA(&uri, &failingMemoryManager);

	ASSERT_GE(failingMemoryManager.getCallCountFree(), 1U);
	uriFreeUriMembersA(&uri);
}

namespace {
	void testNormalizeSyntaxWithFailingMallocCallsFreeTimes(const char * uriString,
															unsigned int mask,
															unsigned int failAllocAfterTimes = 0,
															unsigned int expectedCallCountFree = 0) {
		UriUriA uri = parse(uriString);
		FailingMemoryManager failingMemoryManager(failAllocAfterTimes);

		ASSERT_EQ(uriNormalizeSyntaxExMmA(&uri, mask, &failingMemoryManager),
				  URI_ERROR_MALLOC);

		EXPECT_EQ(failingMemoryManager.getCallCountFree(), expectedCallCountFree);

		uriFreeUriMembersA(&uri);
	}
}  // namespace

TEST(FailingMemoryManagerSuite, NormalizeSyntaxExMmScheme) {
	testNormalizeSyntaxWithFailingMallocCallsFreeTimes("hTTp://example.org/path", URI_NORMALIZE_SCHEME);
}

TEST(FailingMemoryManagerSuite, NormalizeSyntaxExMmEmptyUserInfo) {
	testNormalizeSyntaxWithFailingMallocCallsFreeTimes("//@:123", URI_NORMALIZE_USER_INFO);
}

TEST(FailingMemoryManagerSuite, NormalizeSyntaxExMmEmptyHostRegname) {
	testNormalizeSyntaxWithFailingMallocCallsFreeTimes("//:123", URI_NORMALIZE_HOST);
}

TEST(FailingMemoryManagerSuite, NormalizeSyntaxExMmEmptyQuery) {
	testNormalizeSyntaxWithFailingMallocCallsFreeTimes("//:123?", URI_NORMALIZE_QUERY);
}

TEST(FailingMemoryManagerSuite, NormalizeSyntaxExMmEmptyFragment) {
	testNormalizeSyntaxWithFailingMallocCallsFreeTimes("//:123#", URI_NORMALIZE_FRAGMENT);
}

TEST(FailingMemoryManagerSuite, NormalizeSyntaxExMmHostTextIp4) {  // issue #121
	testNormalizeSyntaxWithFailingMallocCallsFreeTimes("//192.0.2.0:123" /* RFC 5737 */, URI_NORMALIZE_HOST, 1, 1);
}

TEST(FailingMemoryManagerSuite, NormalizeSyntaxExMmHostTextIp6) {  // issue #121
	testNormalizeSyntaxWithFailingMallocCallsFreeTimes("//[2001:db8::]:123" /* RFC 3849 */, URI_NORMALIZE_HOST, 1, 1);
}

TEST(FailingMemoryManagerSuite, NormalizeSyntaxExMmHostTextRegname) {  // issue #121
	testNormalizeSyntaxWithFailingMallocCallsFreeTimes("//host123.test:123" /* RFC 6761 */, URI_NORMALIZE_HOST, 1, 1);
}

TEST(FailingMemoryManagerSuite, NormalizeSyntaxExMmHostTextFuture) {  // issue #121
	testNormalizeSyntaxWithFailingMallocCallsFreeTimes("//[v7.X]:123" /* arbitrary IPvFuture */, URI_NORMALIZE_HOST, 1, 1);
}



TEST(FailingMemoryManagerSuite, ParseSingleUriExMm) {
	UriUriA uri;
	const char * const first = "k1=v1&k2=v2";
	const char * const afterLast = first + strlen(first);
	FailingMemoryManager failingMemoryManager;

	ASSERT_EQ(uriParseSingleUriExMmA(&uri, first, afterLast, NULL,
			&failingMemoryManager),
			URI_ERROR_MALLOC);
}



TEST(FailingMemoryManagerSuite, RemoveBaseUriMm) {
	UriUriA dest;
	UriUriA absoluteSource = parse("http://example.org/a/b/c/");
	UriUriA absoluteBase = parse("http://example.org/a/");
	const UriBool domainRootMode = URI_TRUE;  // not of interest
	FailingMemoryManager failingMemoryManager;

	ASSERT_EQ(uriRemoveBaseUriMmA(&dest, &absoluteSource, &absoluteBase,
			domainRootMode, &failingMemoryManager),
			URI_ERROR_MALLOC);

	uriFreeUriMembersA(&absoluteSource);
	uriFreeUriMembersA(&absoluteBase);
}
