#pragma once

#include "fwd.h"

#include <library/cpp/yson/node/node_io.h> // backward compatibility

#include <yt/cpp/mapreduce/interface/node.h>
#include <yt/cpp/mapreduce/interface/common.h>
#include <library/cpp/yson/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString NodeListToYsonString(const TNode::TListType& nodes);

TNode PathToNode(const TRichYPath& path);
TNode PathToParamNode(const TRichYPath& path);

TString AttributesToYsonString(const TNode& attributes);

TString AttributeFilterToYsonString(const TAttributeFilter& filter);

TNode NodeFromTableSchema(const TTableSchema& schema);

void MergeNodes(TNode& dst, const TNode& src);

TYPath AddPathPrefix(const TYPath& path, const TString& pathPrefix);

TString GetWriteTableCommand(const TString& apiVersion);
TString GetReadTableCommand(const TString& apiVersion);
TString GetWriteFileCommand(const TString& apiVersion);
TString GetReadFileCommand(const TString& apiVersion);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
