//-----------------------------------------------------------------------------
// Copyright (c) 2023, Oracle and/or its affiliates.
//
// This software is dual-licensed to you under the Universal Permissive License
// (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
// 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
// either license.
//
// If you elect to accept the software under the Apache License, Version 2.0,
// the following applies:
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// dpiStringList.c
//   Implementation of a list of strings.
//-----------------------------------------------------------------------------

#include "dpiImpl.h"

//-----------------------------------------------------------------------------
// dpiStringList__free() [INTERNAL]
//   Frees the memory associated with the string list. Note that the strings
// themselves are stored in one contiguous block pointed to by the first
// string.
//-----------------------------------------------------------------------------
void dpiStringList__free(dpiStringList *list)
{
    uint32_t i;

    if (list->strings) {
        for (i = 0; i < list->numStrings; i++)
            dpiUtils__freeMemory((void*) list->strings[i]);
        dpiUtils__freeMemory((void*) list->strings);
        list->strings = NULL;
    }
    if (list->stringLengths) {
        dpiUtils__freeMemory(list->stringLengths);
        list->stringLengths = NULL;
    }
    list->numStrings = 0;
}


//-----------------------------------------------------------------------------
// dpiStringList__addElement() [INTERNAL]
//   Adds an element to the list, allocating additional space if needed. The
// memory accounting is done independently so that it does not need to be
// present in the public structure.
//-----------------------------------------------------------------------------
int dpiStringList__addElement(dpiStringList *list, const char *value,
        uint32_t valueLength, uint32_t *numStringsAllocated, dpiError *error)
{
    uint32_t *tempStringLengths;
    char **tempStrings;
    char *ptr;

    // allocate more space in the array, if needed
    if (*numStringsAllocated <= list->numStrings) {
        *numStringsAllocated += 64;
        if (dpiUtils__allocateMemory(*numStringsAllocated, sizeof(uint32_t), 0,
                "allocate lengths array", (void**) &tempStringLengths,
                error) < 0)
            return DPI_FAILURE;
        if (list->stringLengths) {
            memcpy(tempStringLengths, list->stringLengths,
                    list->numStrings * sizeof(uint32_t));
            dpiUtils__freeMemory(list->stringLengths);
        }
        list->stringLengths = tempStringLengths;
        if (dpiUtils__allocateMemory(*numStringsAllocated, sizeof(char*), 0,
                "allocate strings array", (void**) &tempStrings, error) < 0)
            return DPI_FAILURE;
        if (list->strings) {
            memcpy(tempStrings, list->strings,
                    list->numStrings * sizeof(char*));
            dpiUtils__freeMemory((void*) list->strings);
        }
        list->strings = (const char**) tempStrings;
    }

    // add a copy of the string to the list
    if (dpiUtils__allocateMemory(valueLength, 1, 0, "allocate string",
            (void**) &ptr, error) < 0)
        return DPI_FAILURE;
    memcpy(ptr, value, valueLength);
    list->strings[list->numStrings] = ptr;
    list->stringLengths[list->numStrings] = valueLength;
    list->numStrings++;

    return DPI_SUCCESS;
}
