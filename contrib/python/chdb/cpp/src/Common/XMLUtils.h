#pragma once

#include <CHDBPoco/DOM/DOMParser.h>
#include <CHDBPoco/DOM/Node.h>
#include <CHDBPoco/AutoPtr.h>
#include <base/types.h>

namespace DB_CHDB:: XMLUtils
{
/// Returns root element of the document.
CHDBPoco::XML::Node * getRootNode(CHDBPoco::XML::Document * document);

/// Finds the element in the node's subtree by the specified path and returns its inner text
/// trying to parse it as the requested type.
/// Throws an exception if path is not found.
std::string getString(const CHDBPoco::XML::Node * node, const std::string & path);
Int64 getInt64(const CHDBPoco::XML::Node * node, const std::string & path);
UInt64 getUInt64(const CHDBPoco::XML::Node * node, const std::string & path);
int getInt(const CHDBPoco::XML::Node * node, const std::string & path);
unsigned getUInt(const CHDBPoco::XML::Node * node, const std::string & path);
bool getBool(const CHDBPoco::XML::Node * node, const std::string & path);

/// Finds the element in the node's subtree by the specified path and returns its inner text
/// trying to parse it as the requested type.
/// Returns the specified default value if path is not found.
std::string getString(const CHDBPoco::XML::Node * node, const std::string & path, const std::string & default_value);
Int64 getInt64(const CHDBPoco::XML::Node * node, const std::string & path, Int64 default_value);
UInt64 getUInt64(const CHDBPoco::XML::Node * node, const std::string & path, UInt64 default_value);
int getInt(const CHDBPoco::XML::Node * node, const std::string & path, int default_value);
unsigned getUInt(const CHDBPoco::XML::Node * node, const std::string & path, unsigned default_value);
bool getBool(const CHDBPoco::XML::Node * node, const std::string & path, bool default_value);
}
