#!python3

import os
import re
import pprint
import chdb
from utils import current_dir, data_file, reset_elapsed

# some formats are not supported on chdb, so we need to skip them
# TODO: add support for these formats
#   ["Template", "PrettyJSONEachRow", "Prometheus", "Protobuf", "ProtobufSingle", "Avro", "CapnProto", "MsgPack"]
formats = ["TabSeparated",  "TabSeparatedRaw", "TabSeparatedWithNames", "TabSeparatedWithNamesAndTypes", "TabSeparatedRawWithNames",
           "TabSeparatedRawWithNamesAndTypes", "CSV", "CSVWithNames", "CSVWithNamesAndTypes", "CustomSeparated",
           "CustomSeparatedWithNames", "CustomSeparatedWithNamesAndTypes", "SQLInsert", "Values", "Vertical", "JSON", "JSONStrings",
           "JSONColumns", "JSONColumnsWithMetadata", "JSONCompact", "JSONCompactStrings", "JSONCompactColumns", "JSONEachRow",
           "JSONEachRowWithProgress", "JSONStringsEachRow", "JSONStringsEachRowWithProgress", "JSONCompactEachRow",
           "JSONCompactEachRowWithNames", "JSONCompactEachRowWithNamesAndTypes", "JSONCompactStringsEachRow",
           "JSONCompactStringsEachRowWithNames", "JSONCompactStringsEachRowWithNamesAndTypes", "JSONObjectEachRow", "BSONEachRow",
           "TSKV", "Pretty", "PrettyNoEscapes", "PrettyMonoBlock", "PrettyNoEscapesMonoBlock", "PrettyCompact", "PrettyCompactNoEscapes",
           "PrettyCompactMonoBlock", "PrettyCompactNoEscapesMonoBlock", "PrettySpace", "PrettySpaceNoEscapes", "PrettySpaceMonoBlock",
           "PrettySpaceNoEscapesMonoBlock", "Parquet", "ArrowTable", "Arrow", "ArrowStream",
           "ORC", "RowBinary", "RowBinaryWithNames", "RowBinaryWithNamesAndTypes", "Native", "Null", "XML", "LineAsString",
           "RawBLOB", "Markdown"]

# generate test cases for each format and output

format_output = {}


for fmt in formats:
    res = chdb.query("SELECT * FROM file('" + data_file + "', Parquet) limit 10", fmt)
    if fmt == "ArrowTable":
        data = reset_elapsed(f"{res}")
    else:
        data = reset_elapsed(res.bytes())
    print("format: " + fmt + " size: " + str(len(data)))
    format_output[fmt] = {"len": len(data), "data": data}

# dump to py dict for import later
with open(os.path.join(current_dir, "format_output.py"), "w") as f:
    f.write("format_output = ")
    pprint.pprint(format_output, stream=f)
