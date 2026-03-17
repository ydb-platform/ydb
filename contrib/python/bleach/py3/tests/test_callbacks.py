from bleach.callbacks import nofollow, target_blank


class TestNofollowCallback:
    def test_blank(self):
        attrs = {}
        assert nofollow(attrs) == attrs

    def test_no_href(self):
        attrs = {"_text": "something something"}
        assert nofollow(attrs) == attrs

    def test_basic(self):
        attrs = {(None, "href"): "http://example.com"}
        assert nofollow(attrs) == {
            (None, "href"): "http://example.com",
            (None, "rel"): "nofollow",
        }

    def test_mailto(self):
        attrs = {(None, "href"): "mailto:joe@example.com"}
        assert nofollow(attrs) == attrs

    def test_has_nofollow_already(self):
        attrs = {
            (None, "href"): "http://example.com",
            (None, "rel"): "nofollow",
        }
        assert nofollow(attrs) == attrs

    def test_other_rel(self):
        attrs = {
            (None, "href"): "http://example.com",
            (None, "rel"): "next",
        }
        assert nofollow(attrs) == {
            (None, "href"): "http://example.com",
            (None, "rel"): "next nofollow",
        }


class TestTargetBlankCallback:
    def test_empty(self):
        attrs = {}
        assert target_blank(attrs) == attrs

    def test_mailto(self):
        attrs = {(None, "href"): "mailto:joe@example.com"}
        assert target_blank(attrs) == attrs

    def test_add_target(self):
        attrs = {(None, "href"): "http://example.com"}
        assert target_blank(attrs) == {
            (None, "href"): "http://example.com",
            (None, "target"): "_blank",
        }

    def test_stomp_target(self):
        attrs = {(None, "href"): "http://example.com", (None, "target"): "foo"}
        assert target_blank(attrs) == {
            (None, "href"): "http://example.com",
            (None, "target"): "_blank",
        }
