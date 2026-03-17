"""Test quirky behaviors."""
from . import util
from bs4 import BeautifulSoup


class TestQuirks(util.TestCase):
    """Test quirky behaviors."""

    def test_quirky_user_attrs(self):
        """Test cases where a user creates weird attributes: nested sequences."""

        html = """
        <div id="test">test</div>
        """

        soup = BeautifulSoup(html, 'html.parser')
        soup.div.attrs['user'] = [['a']]
        print(soup.div.attrs)
        self.assertTrue(soup.select_one('div[user="[\'a\']"]') is not None)
