# -*- coding: utf-8 -*-
# Refer: https://source.chromium.org/chromium/chromium/src/+/main:components/crx_file/crx_file.h

# The magic string embedded in the header.
CRX_FILE_HEADER_MAGIC = b'Cr24'
CRX_FILE_HEADER_MAGIC_SIZE = 4
SIGNATURE_CONTEXT = b'CRX3 SignedData\x00'
VERSION = 3
FILE_BUFFER_SIZE = 1 << 12  # 4096
