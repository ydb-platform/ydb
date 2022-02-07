/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/common/linked_hash_table.h>

static void s_element_destroy(void *value) {
    struct aws_linked_hash_table_node *node = value;

    if (node->table->user_on_value_destroy) {
        node->table->user_on_value_destroy(node->value);
    }

    aws_linked_list_remove(&node->node);
    aws_mem_release(node->table->allocator, node);
}

int aws_linked_hash_table_init(
    struct aws_linked_hash_table *table,
    struct aws_allocator *allocator,
    aws_hash_fn *hash_fn,
    aws_hash_callback_eq_fn *equals_fn,
    aws_hash_callback_destroy_fn *destroy_key_fn,
    aws_hash_callback_destroy_fn *destroy_value_fn,
    size_t initial_item_count) {
    AWS_ASSERT(table);
    AWS_ASSERT(allocator);
    AWS_ASSERT(hash_fn);
    AWS_ASSERT(equals_fn);

    table->allocator = allocator;
    table->user_on_value_destroy = destroy_value_fn;

    aws_linked_list_init(&table->list);
    return aws_hash_table_init(
        &table->table, allocator, initial_item_count, hash_fn, equals_fn, destroy_key_fn, s_element_destroy);
}

void aws_linked_hash_table_clean_up(struct aws_linked_hash_table *table) {
    /* clearing the table will remove all elements. That will also deallocate
     * any table entries we currently have. */
    aws_hash_table_clean_up(&table->table);
    AWS_ZERO_STRUCT(*table);
}

int aws_linked_hash_table_find(struct aws_linked_hash_table *table, const void *key, void **p_value) {

    struct aws_hash_element *element = NULL;
    int err_val = aws_hash_table_find(&table->table, key, &element);

    if (err_val || !element) {
        *p_value = NULL;
        return err_val;
    }

    struct aws_linked_hash_table_node *linked_node = element->value;
    *p_value = linked_node->value;

    return AWS_OP_SUCCESS;
}

int aws_linked_hash_table_find_and_move_to_back(struct aws_linked_hash_table *table, const void *key, void **p_value) {

    struct aws_hash_element *element = NULL;
    int err_val = aws_hash_table_find(&table->table, key, &element);

    if (err_val || !element) {
        *p_value = NULL;
        return err_val;
    }

    struct aws_linked_hash_table_node *linked_node = element->value;
    *p_value = linked_node->value;
    /* on access, remove from current place in list and move it to the back. */
    aws_linked_hash_table_move_node_to_end_of_list(table, linked_node);
    return AWS_OP_SUCCESS;
}

int aws_linked_hash_table_put(struct aws_linked_hash_table *table, const void *key, void *p_value) {

    struct aws_linked_hash_table_node *node =
        aws_mem_calloc(table->allocator, 1, sizeof(struct aws_linked_hash_table_node));

    if (!node) {
        return AWS_OP_ERR;
    }

    struct aws_hash_element *element = NULL;
    int was_added = 0;
    int err_val = aws_hash_table_create(&table->table, key, &element, &was_added);

    if (err_val) {
        aws_mem_release(table->allocator, node);
        return err_val;
    }

    if (element->value) {
        s_element_destroy(element->value);
    }

    node->value = p_value;
    node->key = key;
    node->table = table;
    element->value = node;

    aws_linked_list_push_back(&table->list, &node->node);

    return AWS_OP_SUCCESS;
}

int aws_linked_hash_table_remove(struct aws_linked_hash_table *table, const void *key) {
    /* allocated table memory and the linked list entry will be removed in the
     * callback. */
    return aws_hash_table_remove(&table->table, key, NULL, NULL);
}

void aws_linked_hash_table_clear(struct aws_linked_hash_table *table) {
    /* clearing the table will remove all elements. That will also deallocate
     * any entries we currently have. */
    aws_hash_table_clear(&table->table);
}

size_t aws_linked_hash_table_get_element_count(const struct aws_linked_hash_table *table) {
    return aws_hash_table_get_entry_count(&table->table);
}

void aws_linked_hash_table_move_node_to_end_of_list(
    struct aws_linked_hash_table *table,
    struct aws_linked_hash_table_node *node) {

    aws_linked_list_remove(&node->node);
    aws_linked_list_push_back(&table->list, &node->node);
}

const struct aws_linked_list *aws_linked_hash_table_get_iteration_list(const struct aws_linked_hash_table *table) {
    return &table->list;
}
