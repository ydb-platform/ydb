#include "serialize_dump.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TSerializationDumper::ConfigureMode(ESerializationDumpMode value)
{
    Mode_ = value;
    if (Mode_ == ESerializationDumpMode::Content) {
        ContentDumpLock_ = 0;
    }
}

void TSerializationDumper::DisableScopeFiltering()
{
    ContentDumpLock_ += ScopeFilterMatchLockDelta;
}

void TSerializationDumper::BeginScopeFilterMatchBlock(TStringBuf path)
{
    ContentDumpLock_ += ScopeFilterMatchLockDelta;
    BeginWrite();
    ScratchBuilder_.AppendFormat(">>> %v\n", path);
    EndWrite();
}

void TSerializationDumper::EndScopeFilterMatchBlock(TStringBuf path)
{
    ContentDumpLock_ -= ScopeFilterMatchLockDelta;
    BeginWrite();
    ScratchBuilder_.AppendFormat("<<< %v\n", path);
    EndWrite();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
