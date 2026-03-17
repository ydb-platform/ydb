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

#include "platform.h"

NSYNC_CPP_START_

typedef void (*init_once_fn_) (void);
BOOL CALLBACK nsync_init_callback_ (pthread_once_t *o, void *v, void **c) {
        init_once_fn_ *f = (init_once_fn_ *) v;
        (**f) ();
        return (TRUE);
}

NSYNC_CPP_END_
