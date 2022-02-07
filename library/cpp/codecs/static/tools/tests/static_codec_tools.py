#!/usr/bin/env python

import yatest.common as tt
import os.path as op

def test_static_codec_tools():
    tt.execute([tt.binary_path("library/cpp/codecs/static/tools/static_codec_generator/static_codec_generator")]
        + ["-m", "test codec", "-r", "sbr://143310406", "-f", "plain", "-c", "solar-8k-a:huffman", "-s", "1",
            "--fake-revision", "r2385905", "--fake-timestamp", "1467494385", "sample.txt"],
        timeout=60)
    assert(op.exists("solar-8k-a.huffman.1467494385.codec_info"))
    tt.canonical_execute(tt.binary_path("library/cpp/codecs/static/tools/static_codec_checker/static_codec_checker"),
        args=["-c", "solar-8k-a.huffman.1467494385.codec_info"],
        timeout=60)
    tt.execute([tt.binary_path("library/cpp/codecs/static/tools/static_codec_checker/static_codec_checker")]
        + ["-c", "solar-8k-a.huffman.1467494385.codec_info", "-f", "plain", "-t", "sample.txt"],
        timeout=60)
    return tt.canonical_file("solar-8k-a.huffman.1467494385.codec_info")
