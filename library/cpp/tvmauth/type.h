#pragma once 
 
#include <library/cpp/containers/stack_vector/stack_vec.h>
 
namespace NTvmAuth {
    using TScopes = TSmallVec<TStringBuf>; 
    using TTvmId = ui32;
    using TUid = ui64;
    using TUids = TSmallVec<TUid>; 
    using TAlias = TString;
} 
