#pragma once 
#include <ydb/public/lib/yq/yq.h>
#include <ydb/public/lib/yq/helpers.h>
 
namespace NYq { 
 
void UpdateConnections( 
    NYdb::NYq::TClient& client,
    const TString& folderId,
    const TString& connectionsStr);
 
} // NYq 
