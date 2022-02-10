/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/core/Globals.h> 
#include <aws/core/utils/EnumParseOverflowContainer.h> 
#include <aws/core/utils/memory/AWSMemory.h> 
 
namespace Aws 
{ 
    static const char TAG[] = "GlobalEnumOverflowContainer"; 
    static Utils::EnumParseOverflowContainer* g_enumOverflow; 
 
    Utils::EnumParseOverflowContainer* GetEnumOverflowContainer() 
    { 
        return g_enumOverflow; 
    } 
 
    void InitializeEnumOverflowContainer() 
    { 
        g_enumOverflow = Aws::New<Aws::Utils::EnumParseOverflowContainer>(TAG); 
    } 
 
    void CleanupEnumOverflowContainer() 
    { 
        Aws::Delete(g_enumOverflow); 
    } 
} 
