#include <Disks/getDiskConfigurationFromAST.h>

#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Common/logger_useful.h>
#include <Disks/DiskFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <CHDBPoco/DOM/Document.h>
#include <CHDBPoco/DOM/Element.h>
#include <CHDBPoco/DOM/Text.h>


namespace DB_CHDB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

[[noreturn]] static void throwBadConfiguration(const std::string & message = "")
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Incorrect configuration{}. Example of expected configuration: `(type=s3 ...`)`",
        message.empty() ? "" : ": " + message);
}

CHDBPoco::AutoPtr<CHDBPoco::XML::Document> getDiskConfigurationFromASTImpl(const ASTs & disk_args, ContextPtr context)
{
    if (disk_args.empty())
        throwBadConfiguration("expected non-empty list of arguments");

    CHDBPoco::AutoPtr<CHDBPoco::XML::Document> xml_document(new CHDBPoco::XML::Document());
    CHDBPoco::AutoPtr<CHDBPoco::XML::Element> root(xml_document->createElement("disk"));
    xml_document->appendChild(root);

    for (const auto & arg : disk_args)
    {
        const auto * setting_function = arg->as<const ASTFunction>();
        if (!setting_function || setting_function->name != "equals")
            throwBadConfiguration("expected configuration arguments as key=value pairs");

        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(setting_function->arguments.get());
        if (!function_args_expr)
            throwBadConfiguration("expected a list of key=value arguments");

        auto function_args = function_args_expr->children;
        if (function_args.empty())
            throwBadConfiguration("expected a non-empty list of key=value arguments");

        auto * key_identifier = function_args[0]->as<ASTIdentifier>();
        if (!key_identifier)
            throwBadConfiguration("expected the key (key=value) to be identifier");

        const std::string & key = key_identifier->name();
        CHDBPoco::AutoPtr<CHDBPoco::XML::Element> key_element(xml_document->createElement(key));
        root->appendChild(key_element);

        if (!function_args[1]->as<ASTLiteral>() && !function_args[1]->as<ASTIdentifier>())
            throwBadConfiguration("expected values to be literals or identifiers");

        auto value = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
        CHDBPoco::AutoPtr<CHDBPoco::XML::Text> value_element(xml_document->createTextNode(convertFieldToString(value->as<ASTLiteral>()->value)));
        key_element->appendChild(value_element);
    }

    return xml_document;
}

DiskConfigurationPtr getDiskConfigurationFromAST(const ASTs & disk_args, ContextPtr context)
{
    auto xml_document = getDiskConfigurationFromASTImpl(disk_args, context);
    CHDBPoco::AutoPtr<CHDBPoco::Util::XMLConfiguration> conf(new CHDBPoco::Util::XMLConfiguration());
    conf->load(xml_document);
    return conf;
}


ASTs convertDiskConfigurationToAST(const CHDBPoco::Util::AbstractConfiguration & configuration, const std::string & config_path)
{
    ASTs result;

    CHDBPoco::Util::AbstractConfiguration::Keys keys;
    configuration.keys(config_path, keys);

    for (const auto & key : keys)
    {
        result.push_back(
            makeASTFunction(
                "equals",
                std::make_shared<ASTIdentifier>(key),
                std::make_shared<ASTLiteral>(configuration.getString(config_path + "." + key))));
    }

    return result;
}

}
