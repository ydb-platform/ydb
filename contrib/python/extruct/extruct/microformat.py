# mypy: disallow_untyped_defs=False
import mf2py


class MicroformatExtractor:
    def extract(self, htmlstring, base_url=None, encoding="UTF-8"):
        return list(self.extract_items(htmlstring, base_url=base_url))

    def extract_items(self, html, base_url=None):
        yield from mf2py.parse(html, html_parser="lxml", url=base_url)["items"]
