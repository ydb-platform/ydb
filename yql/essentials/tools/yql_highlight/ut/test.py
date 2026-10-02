#!/usr/bin/env python3


def read(path):
    with open(path, "r") as f:
        content = f.read()
        if path.startswith("query"):
            content = content.replace("\\", "\\\\")
            content = content.replace("`", "\\`")
        return content


def patch(input_file, output_file, substitutions):
    content = read(input_file)
    for file in substitutions:
        content = content.replace("/* {{%s}} */" % file, read(file))

    with open(output_file, "w") as f:
        f.write(content)


patch(
    "monarch.template.ts",
    "monarch.patched.ts",
    (
        "YQL.monarch.json",
        "YQLs.monarch.json",
        "query.yql",
        "query.yqls",
    ),
)

patch(
    "test.template.html",
    "test.patched.html",
    (
        "monarch.patched.ts",
        "YQL.tmLanguage.json",
        "YQLs.tmLanguage.json",
        "YQL.highlightjs.json",
        "YQLs.highlightjs.json",
    ),
)

content = read("monarch.patched.ts")
with open("monarch.patched.ts", "w") as f:
    content = "const syntax = 'yql'\n\n" + content
    content = "const theme = 'light'\n\n" + content
    f.write(content)
