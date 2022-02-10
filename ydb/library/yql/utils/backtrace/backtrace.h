#pragma once

#include <util/system/types.h>

class IOutputStream;

void KikimrBacktraceFormatImpl(IOutputStream* out);
void KikimrBacktraceFormatImpl(IOutputStream* out, void* const* stack, size_t stackSize);
void KikimrBackTrace(); 
void EnableKikimrBacktraceFormat();
void PrintBacktraceToStderr(int signum);
void SetFatalSignalHandler(void (*handler)(int));
 
