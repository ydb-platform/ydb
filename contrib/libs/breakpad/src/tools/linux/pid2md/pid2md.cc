// Copyright 2020 Google LLC
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google LLC nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// pid2md.cc: An utility to generate a minidump from a running process

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "client/linux/minidump_writer/minidump_writer.h"
#include "common/path_helper.h"

int main(int argc, char* argv[]) {
  if (argc != 3) {
    fprintf(stderr, "Usage: %s <process id> <minidump file>\n\n",
            google_breakpad::BaseName(argv[0]).c_str());
    fprintf(stderr,
            "A tool to generate a minidump from a running process. The process "
            "resumes its\nactivity once the operation is completed. Permission "
            "to trace the process is\nrequired.\n");
    return EXIT_FAILURE;
  }

  pid_t process_id = atoi(argv[1]);
  const char* minidump_file = argv[2];

  if (!google_breakpad::WriteMinidump(minidump_file, process_id, process_id)) {
    fprintf(stderr, "Unable to generate minidump.\n");
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
