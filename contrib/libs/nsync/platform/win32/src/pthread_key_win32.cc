/* Copyright 2016 Google Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. */

#include "headers.h"

// Struct representing an element of the "dest" array, whose elements
// indicate the destructor asscoaied with the corresponding key.
namespace {
struct KeyDestructor {
	bool allocated;         // Whether this key is allocated.
	pthread_key_t key;	// Last used version of key.
	void (*dest) (void *);  // Destructor for the key, or nullptr.
};
} // anonymous namespace


static SRWLOCK mu = SRWLOCK_INIT;  // protects dest.
static const int kMaxKeyShift = 7;
static const int kMaxKeys = 1 << kMaxKeyShift;
static const int kKeyMask = kMaxKeys - 1;
static KeyDestructor dest[kMaxKeys];  // Destructors for keys.

// Element per key per thread.
namespace {
struct KeyValue {
	pthread_key_t key;  // last key stored
	void *value;
};
} // anonymous namespace

// Each thread has a class instance that represents an array of pointers
// to client-specified thread-specific data.
// The class destructor calls the key destructors when the accociated
// thread exits.
namespace {

class PerThreadData {
 public:
  PerThreadData() {
  }
  ~PerThreadData();
  void *value(pthread_key_t key) {
    void *result = 0;
    int i = key & kKeyMask;
    if (this->array_[i].key == key) {
      result = this->array_[i].value;
    }
    return (result);
  }
  void set_value(pthread_key_t key, void *value) {
    int i = key & kKeyMask;
    this->array_[i].key = key;
    this->array_[i].value = value;
  }
 private:
  KeyValue array_[kMaxKeys];
};

PerThreadData::~PerThreadData() {
	bool again = true;
	// Try a few times to clear the thread's array.
	for (int attempt = 0; attempt != 4 && again; attempt++) { 
		again = false;
		// Take a snapshot of the destructor state.
		AcquireSRWLockExclusive (&mu);
		KeyDestructor dest_copy[kMaxKeys];
		memcpy (dest_copy, dest, sizeof (dest_copy));
		ReleaseSRWLockExclusive(&mu);
		// Iterate over the values, calling destructors
		// when both destructor and value are non-nil.
		for (int i = 1; i != kMaxKeys; i++) {
			if (dest_copy[i].allocated) {
				void (*destructor) (void *) = dest_copy[i].dest;
				void *value = this->array_[i].value;
				this->array_[i].value = nullptr;
				if (dest_copy[i].key == this->array_[i].key &&
                                    destructor != nullptr && value != nullptr) {
					again = true;  // in case destructors set values.
					(*destructor) (value);
				}
			}
		}
	}
}
} // anonymous namespace

static __declspec(thread) class PerThreadData per_thread_data;

NSYNC_C_START_

int nsync_pthread_key_create (pthread_key_t *pkey, void (*destructor) (void *)) {
	int result = EAGAIN;
	int i;
	AcquireSRWLockExclusive (&mu);
	// Search for an unused slot. Slot 1 is never used.
	for (i = 1; i != kMaxKeys && dest[i].allocated; i++) {
	}
	if (i != kMaxKeys) { // found one
		dest[i].allocated = true;
		dest[i].dest = destructor;
		dest[i].key += kMaxKeys;  // High bits are version number.
		dest[i].key |= i;  // Low bits are slot number.
		*pkey = dest[i].key;
		result = 0;
	}
	ReleaseSRWLockExclusive(&mu);
	return (result);
}

int nsync_pthread_key_delete (pthread_key_t key) {
	int result = EINVAL;
	int i = key & kKeyMask;
	AcquireSRWLockExclusive (&mu);
	if (key > 0 && dest[i].allocated) {
		dest[i].allocated = false;
		dest[i].dest = nullptr;
		result = 0;
	}
	ReleaseSRWLockExclusive(&mu);
	return (result);
}

void *nsync_pthread_getspecific (pthread_key_t key) {
	return (per_thread_data.value (key));
}

int nsync_pthread_setspecific (pthread_key_t key, void *value) {
	int result = EINVAL;
	if (key > 0) {
		per_thread_data.set_value (key, value);
		result = 0;
	}
	return (result);
}

NSYNC_C_END_
