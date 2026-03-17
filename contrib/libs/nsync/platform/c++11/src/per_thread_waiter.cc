/* Copyright 2017 Google Inc.

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

NSYNC_CPP_START_

namespace {
	class per_thread {
	public:
		per_thread();
		~per_thread();
		/* Return the per-thread value, and set the destructor to dest. */
		void *get(void (*dest) (void *));
		/* Set the per-thread value to p, and setting the destructor to dest. */
		void set(void *p, void (*dest) (void *));
	private:
		void *value;
		void (*dest) (void *);
	};
	per_thread::per_thread() {
		this->value = nullptr;
		this->dest = nullptr;
	}
	per_thread::~per_thread() {
		void *value = this->value;
		void (*dest) (void *) = this->dest;
		this->value = nullptr;
		this->dest = nullptr;
		if (dest != nullptr && value != nullptr) {
			(*dest) (value);
		}
	}
	void *per_thread::get(void (*dest) (void *)) {
		this->dest = dest;
		return (this->value);
	}
	void per_thread::set(void *p, void (*dest) (void *)) {
		this->value = p;
		this->dest = dest;
	}

	thread_local per_thread thread_specific;
}

void *nsync_per_thread_waiter_ (void (*dest) (void *)) {
	return (thread_specific.get (dest));
}

void nsync_set_per_thread_waiter_ (void *v, void (*dest) (void *)) {
	thread_specific.set (v, dest);
}

NSYNC_CPP_END_
