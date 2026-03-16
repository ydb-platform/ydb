#pragma once

#include <CHDBPoco/Util/AbstractConfiguration.h>
#include <CHDBPoco/DOM/AutoPtr.h>
#include <CHDBPoco/Util/XMLConfiguration.h>
#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>

namespace DB_CHDB
{

using DiskConfigurationPtr = CHDBPoco::AutoPtr<CHDBPoco::Util::AbstractConfiguration>;

/**
 * Transform a list of pairs ( key1=value1, key2=value2, ... ), where keys and values are ASTLiteral or ASTIdentifier
 * into
 * <disk>
 *     <key1>value1</key1>
 *     <key2>value2</key2>
 *     ...
 * </disk>
 *
 * Used in case disk configuration is passed via AST when creating
 * a disk object on-the-fly without any configuration file.
 */
DiskConfigurationPtr getDiskConfigurationFromAST(const ASTs & disk_args, ContextPtr context);

/// The same as above function, but return XML::Document for easier modification of result configuration.
[[ maybe_unused ]] CHDBPoco::AutoPtr<CHDBPoco::XML::Document> getDiskConfigurationFromASTImpl(const ASTs & disk_args, ContextPtr context);

/*
 * A reverse function.
 */
[[ maybe_unused ]] ASTs convertDiskConfigurationToAST(const CHDBPoco::Util::AbstractConfiguration & configuration, const std::string & config_path);

}
