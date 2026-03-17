/*
    This is part of pyahocorasick Python module.

    Linked list implementation.

    Const time of:
    * append
    * prepend
    * pop first
    * get first/last

    Author    : Wojciech Muła, wojciech_mula@poczta.onet.pl
    WWW       : http://0x80.pl
    License   : public domain
*/
#include "slist.h"

ListItem*
list_item_new(const size_t size) {
    ListItem* item = (ListItem*)memory_alloc(size);
    if (item) {
        item->__next = 0;
    }

    return item;
}


void
list_item_delete(ListItem* item) {
    memory_free(item);
}


void
list_init(List* list) {
    if (list) {
        list->head = 0;
        list->last = 0;
    }
}


int
list_delete(List* list) {

    ListItem* item;
    ListItem* tmp;

    ASSERT(list);

    item = list->head;
    while (item) {
        tmp = item;
        item = item->__next;
        memory_free(tmp);
    }

    list->head = list->last = NULL;
    return 0;
}


ListItem*
list_append(List* list, ListItem* item) {
    ASSERT(list);

    if (item) {
        if (list->last) {
            list->last->__next = item;  // append
            list->last = item;          // set as last node
        }
        else
            list->head = list->last = item;
    }

    return item;
}


ListItem*
list_push_front(List* list, ListItem* item) {
    ASSERT(list);

    if (list->head) {
        item->__next = list->head;
        list->head = item;
    }
    else
        list->head = list->last = item;

    return item;
}


ListItem*
list_pop_first(List* list) {
    ListItem* item;

    ASSERT(list);

    if (list->head) {
        item = list->head;
        list->head = item->__next;

        if (!list->head)
            list->last = 0;

        return item;
    }
    else
        return NULL;
}

