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

NSYNC_CPP_START_

int nsync_nanosleep (const struct timespec *request, struct timespec *remain) {
	time_t x = request->tv_sec;
	while (x > 1000) {
		x -= 1000;
		Sleep (1000 * 1000);
	}
	Sleep ((unsigned) (x * 1000 + (request->tv_nsec + 999999) / (1000 * 1000)));
	if (remain != NULL) {
		memset (remain, 0, sizeof (*remain));
	}
	return (0);
}

NSYNC_CPP_END_
