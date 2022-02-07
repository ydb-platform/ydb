#ifndef AWS_COMMON_XML_PARSER_H
#define AWS_COMMON_XML_PARSER_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/array_list.h>
#include <aws/common/byte_buf.h>

#include <aws/common/exports.h>

struct aws_xml_parser;
struct aws_xml_node;

struct aws_xml_attribute {
    struct aws_byte_cursor name;
    struct aws_byte_cursor value;
};

/**
 * Callback for when an xml node is encountered in the document. As a user you have a few options:
 *
 * 1. reject the document parsing at this point by returning false. This will immediately stop doc parsing.
 * 2. call aws_xml_node_traverse() on the node to descend into the node with a new callback and user_data.
 * 3. call aws_xml_node_as_body() to retrieve the contents of the node as text.
 *
 * return true to continue the parsing operation.
 */
typedef bool(
    aws_xml_parser_on_node_encountered_fn)(struct aws_xml_parser *parser, struct aws_xml_node *node, void *user_data);

struct aws_xml_parser_options {
    /* xml document to parse. */
    struct aws_byte_cursor doc;

    /* Max node depth used for parsing document. */
    size_t max_depth;
};

AWS_EXTERN_C_BEGIN

/**
 * Allocates an xml parser.
 */
AWS_COMMON_API
struct aws_xml_parser *aws_xml_parser_new(
    struct aws_allocator *allocator,
    const struct aws_xml_parser_options *options);

/*
 * De-allocates an xml parser.
 */
AWS_COMMON_API
void aws_xml_parser_destroy(struct aws_xml_parser *parser);

/**
 * Parse the doc until the end or until a callback rejects the document.
 * on_node_encountered will be invoked when the root node is encountered.
 */
AWS_COMMON_API
int aws_xml_parser_parse(
    struct aws_xml_parser *parser,
    aws_xml_parser_on_node_encountered_fn *on_node_encountered,
    void *user_data);

/**
 * Writes the contents of the body of node into out_body. out_body is an output parameter in this case. Upon success,
 * out_body will contain the body of the node.
 */
AWS_COMMON_API
int aws_xml_node_as_body(struct aws_xml_parser *parser, struct aws_xml_node *node, struct aws_byte_cursor *out_body);

/**
 * Traverse node and invoke on_node_encountered when a nested node is encountered.
 */
AWS_COMMON_API
int aws_xml_node_traverse(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    aws_xml_parser_on_node_encountered_fn *on_node_encountered,
    void *user_data);

/*
 * Get the name of an xml node.
 */
AWS_COMMON_API
int aws_xml_node_get_name(const struct aws_xml_node *node, struct aws_byte_cursor *out_name);

/*
 * Get the number of attributes for an xml node.
 */
AWS_COMMON_API
size_t aws_xml_node_get_num_attributes(const struct aws_xml_node *node);

/*
 * Get an attribute for an xml node by its index.
 */
AWS_COMMON_API
int aws_xml_node_get_attribute(
    const struct aws_xml_node *node,
    size_t attribute_index,
    struct aws_xml_attribute *out_attribute);

AWS_EXTERN_C_END

#endif /* AWS_COMMON_XML_PARSER_H */
