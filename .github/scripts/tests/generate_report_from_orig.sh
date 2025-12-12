#!/bin/bash
# Script to generate test report from orig_bug.json

# Configuration
ORIG_JSON="${1:-.github/config/orig_bug.json}"
OUTPUT_DIR="${2:-/tmp/test_report_output}"
OUTPUT_HTML="${OUTPUT_DIR}/summary_v2.html"

# Create output directory if it doesn't exist
mkdir -p "${OUTPUT_DIR}"

# Create summary_links file
SUMMARY_LINKS_FILE="${OUTPUT_DIR}/summary_links.txt"
echo "Report: ${OUTPUT_HTML}" > "${SUMMARY_LINKS_FILE}"

# Run generate-summary.py
python3 .github/scripts/tests/generate-summary.py \
    --public_dir "${OUTPUT_DIR}" \
    --public_dir_url "file://${OUTPUT_DIR}" \
    --summary_links "${SUMMARY_LINKS_FILE}" \
    --build_preset "relwithdebinfo" \
    --branch "main" \
    --is_retry 0 \
    --is_last_retry 0 \
    --is_test_result_ignored 0 \
    --comment_color_file "${OUTPUT_DIR}/comment_color.txt" \
    --comment_text_file "${OUTPUT_DIR}/comment_text.txt" \
    --status_report_file "${OUTPUT_DIR}/status_report.txt" \
    "Test Report" "${OUTPUT_HTML}" "${ORIG_JSON}"

echo ""
echo "Report generated: ${OUTPUT_HTML}"
echo "Open it in your browser to view the results."
