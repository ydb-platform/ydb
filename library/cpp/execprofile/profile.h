#pragma once

#include <stdio.h>

// Starts capturing execution samples
void BeginProfiling();

// Resets captured execution samples
void ResetProfile();

// Pauses capturing execution samples and dumps them to the file
// Samples are not cleared so that profiling can be continued by calling BeginProfiling()
// or it can be started from scratch by calling ResetProfile() and then BeginProfiling()
void EndProfiling(FILE* out);

// Dumps the profile to default file (basename.pid.N.profile)
void EndProfiling();
