#include "should_continue.h" 
 
void TProgramShouldContinue::ShouldRestart() { 
    AtomicSet(State, Restart); 
} 
 
void TProgramShouldContinue::ShouldStop(int returnCode) { 
    AtomicSet(ReturnCode, returnCode); 
    AtomicSet(State, Stop); 
} 
 
TProgramShouldContinue::EState TProgramShouldContinue::PollState() { 
    return static_cast<EState>(AtomicGet(State)); 
} 
 
int TProgramShouldContinue::GetReturnCode() { 
    return static_cast<int>(AtomicGet(ReturnCode)); 
} 
 
void TProgramShouldContinue::Reset() { 
    AtomicSet(ReturnCode, 0); 
    AtomicSet(State, Continue); 
} 
