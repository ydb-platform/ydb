## Local build with OpenSource tools

The documentation is built using the [YFM-Docs](https://github.com/yandex-cloud/yfm-docs) utility.

The procedure for installing YFM-Docs is described on [introductory documentation page for this utility]{% if lang == "en" %}(https://ydocs.tech/en/tools/docs/){% endif %}{% if lang == "ru" %}(https://ydocs.tech/ru/tools/docs/){% endif %}.

To build the {{ ydb-short-name }} OpenSource documentation, run the command:

```bash
yfm -i <source_dir> -o <output_dir> --allowHTML
```

Where:

- `source_dir` is the directory where the contents of [{{ ydb-doc-repo }}]({{ ydb-doc-repo }}) is cloned.
- `output_dir` is the output directory for HTML files.

Building the documentation takes a few seconds and there should be no errors logged to stdout.

You can specify `.` (a dot) as `source_dir` if the yfm command is called directly from `source_dir`. For example:

```bash
yfm -i . -o ~/docs/ydboss --allowHTML
```

To view the documentation built locally, you can open the directory from your browser or use a simple web server built into Python:

```bash
python3 -m http.server 8888 -d ~/docs/ydboss
```

With the server run in this way, the locally built documentation is available at the links:

- [http://localhost:8888/ru](http://localhost:8888/ru) (in Russian)
- [http://localhost:8888/en](http://localhost:8888/en) (in English)

