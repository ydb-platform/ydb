#!/bin/bash

# Test script to verify _LIBCPP_HARDENING_MODE_FAST is working
# This script demonstrates how to test the hardening feature

echo "=== YDB libc++ Hardening Verification Test ==="
echo ""
echo "This test verifies that the _LIBCPP_HARDENING_MODE_FAST flag is:"
echo "1. Properly defined during compilation (compile-time check)"
echo "2. Actually working to catch runtime violations (runtime check)"
echo ""

echo "Building the hardening test program..."
echo "Command: ya make"
echo ""
echo "Expected behavior:"
echo "- If hardening is DISABLED: Compilation will fail with error about missing _LIBCPP_HARDENING_MODE_FAST"
echo "- If hardening is ENABLED: Compilation succeeds, but runtime will abort on bounds violation"
echo ""

echo "To run this test:"
echo "1. cd /path/to/ydb/build"
echo "2. ya make"
echo "3. ./libcpp_hardening_test"
echo ""
echo "Expected runtime result with hardening ENABLED:"
echo "- Program prints initial messages"
echo "- Program aborts/terminates when accessing vec[10] (out of bounds)"
echo "- The error message 'This line should not be reached...' should NOT appear"
echo ""
echo "Expected runtime result with hardening DISABLED:"
echo "- Program runs to completion"
echo "- All messages are printed including the error message"
echo "- May show garbage value for vec[10] or crash unpredictably"