"""Check local code inclusion in both output formats without publishing SDK files."""

import html
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

from cli import STAGING_ROOT, read_yaml, regions
from clean_output import clean


def main():
    cli = sys.argv[1]
    docs = Path(__file__).resolve().parents[2]
    lock = read_yaml(docs / "sdk-snippets.lock.yaml")
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory).resolve()
        source = root / "input"
        source.mkdir()
        (source / ".yfm").write_text("allowCustomResources: true\nstrict: true\nignore:\n  - .generated/sdk-snippets/**\n")
        (source / "toc.yaml").write_text(yaml.safe_dump({
            "title": "SDK snippets", "items": [
                {"name": "English", "href": "en/index.md"},
                {"name": "Русский", "href": "ru/index.md"},
            ],
        }, allow_unicode=True))
        shutil.copytree(docs / STAGING_ROOT, source / STAGING_ROOT)
        blocks = []
        for sdk, entry in lock["sources"].items():
            for path in entry["files"]:
                text = (docs / STAGING_ROOT / sdk / path).read_text()
                name = next(iter(regions(text, path)))
                blocks.append('{% code "/' + STAGING_ROOT + '/' + sdk + '/' + path
                              + '" lang="text" lines="[BEGIN ' + name + ']-[END ' + name + ']" %}')
        for locale in ("ru", "en"):
            page = source / locale / "index.md"
            page.parent.mkdir()
            page.write_text("# SDK snippets\n\n" + "\n\n".join(blocks) + "\n")
        for format in ("html", "md"):
            output = root / format
            subprocess.run([cli, "build", "-i", str(source), "-o", str(output),
                            "--output-format", format, "--allow-custom-resources", "--strict"], check=True)
            clean(output)
            page = output / "en" / ("index.html" if format == "html" else "index.md")
            rendered = html.unescape(page.read_text())
            if "{% code" in rendered or "[BEGIN " in rendered or "[END " in rendered:
                raise RuntimeError("code directives or region markers remain in " + format)
            if "CreateTopic" not in rendered and "createTopic" not in rendered and "TopicClient" not in rendered:
                raise RuntimeError("SDK source content was not rendered in " + format)
            published = [file.relative_to(output).as_posix() for file in (output / STAGING_ROOT).rglob("*") if file.is_file()]
            if published:
                raise RuntimeError("SDK staging files were published in " + format + ": " + ", ".join(published))
            other = output / "ru" / page.name
            if page.read_bytes() != other.read_bytes() and format == "md":
                # Build metadata differs by page path; source fences must still match.
                import re
                fences = lambda value: re.findall(r"```[^\n]*\n(.*?)```", value, re.S)
                if fences(page.read_text()) != fences(other.read_text()):
                    raise RuntimeError("locales render different source snippets")
    print("HTML and Markdown code inclusion passed for all locked SDK files")


if __name__ == "__main__":
    main()
