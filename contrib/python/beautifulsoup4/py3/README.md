Beautiful Soup is a library that makes it easy to scrape information
from web pages. It sits atop an HTML or XML parser, providing Pythonic
idioms for iterating, searching, and modifying the parse tree.

# Quick start

```
>>> from bs4 import BeautifulSoup
>>> soup = BeautifulSoup("<p>Some<b>bad<i>HTML")
>>> print(soup.prettify())
<html>
 <body>
  <p>
   Some
   <b>
    bad
    <i>
     HTML
    </i>
   </b>
  </p>
 </body>
</html>
>>> soup.find(string="bad")
'bad'
>>> soup.i
<i>HTML</i>
#
>>> soup = BeautifulSoup("<tag1>Some<tag2/>bad<tag3>XML", "xml")
#
>>> print(soup.prettify())
<?xml version="1.0" encoding="utf-8"?>
<tag1>
 Some
 <tag2/>
 bad
 <tag3>
  XML
 </tag3>
</tag1>
```

To go beyond the basics, [comprehensive documentation is available](https://www.crummy.com/software/BeautifulSoup/bs4/doc/).

# Links

* [Homepage](https://www.crummy.com/software/BeautifulSoup/bs4/)
* [Documentation](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)
* [Discussion group](https://groups.google.com/group/beautifulsoup/)
* [Development](https://code.launchpad.net/beautifulsoup/)
* [Bug tracker](https://bugs.launchpad.net/beautifulsoup/)
* [Complete changelog](https://git.launchpad.net/beautifulsoup/tree/CHANGELOG)

# Note on Python 2 sunsetting

Beautiful Soup's support for Python 2 was discontinued on December 31,
2020: one year after the sunset date for Python 2 itself. From this
point onward, new Beautiful Soup development will exclusively target
Python 3. The final release of Beautiful Soup 4 to support Python 2
was 4.9.3.

# Supporting the project

If you use Beautiful Soup as part of your professional work, please consider a
[Tidelift subscription](https://tidelift.com/subscription/pkg/pypi-beautifulsoup4?utm_source=pypi-beautifulsoup4&utm_medium=referral&utm_campaign=readme).
This will support many of the free software projects your organization
depends on, not just Beautiful Soup.

If you use Beautiful Soup for personal projects, the best way to say
thank you is to read
[Tool Safety](https://www.crummy.com/software/BeautifulSoup/zine/), a zine I
wrote about what Beautiful Soup has taught me about software
development.

# Building the documentation

The bs4/doc/ directory contains full documentation in Sphinx
format. Run `make html` in that directory to create HTML
documentation.

# Running the unit tests

Beautiful Soup supports unit test discovery using Pytest:

```
$ pytest
```

