# -*- coding: utf-8 -*-
import re
import weakref
import six
import unittest
import pickle

from parsel import Selector
from parsel.selector import (
    CannotRemoveElementWithoutRoot,
    CannotRemoveElementWithoutParent,
)


class SelectorTestCase(unittest.TestCase):

    sscls = Selector

    def test_pickle_selector(self):
        sel = self.sscls(text=u'<html><body><p>some text</p></body></html>')
        self.assertRaises(TypeError, lambda s: pickle.dumps(s, protocol=2), sel)

    def test_pickle_selector_list(self):
        sel = self.sscls(text=u'<html><body><ul><li>1</li><li>2</li><li>3</li></ul></body></html>')
        sel_list = sel.css('li')
        empty_sel_list = sel.css('p')
        self.assertIsInstance(sel_list, self.sscls.selectorlist_cls)
        self.assertIsInstance(empty_sel_list, self.sscls.selectorlist_cls)
        self.assertRaises(TypeError, lambda s: pickle.dumps(s, protocol=2), sel_list)
        self.assertRaises(TypeError, lambda s: pickle.dumps(s, protocol=2), empty_sel_list)

    def test_simple_selection(self):
        """Simple selector tests"""
        body = u"<p><input name='a'value='1'/><input name='b'value='2'/></p>"
        sel = self.sscls(text=body)

        xl = sel.xpath('//input')
        self.assertEqual(2, len(xl))
        for x in xl:
            assert isinstance(x, self.sscls)

        self.assertEqual(sel.xpath('//input').extract(),
                         [x.extract() for x in sel.xpath('//input')])

        self.assertEqual([x.extract() for x in sel.xpath("//input[@name='a']/@name")],
                         [u'a'])
        self.assertEqual([x.extract() for x in sel.xpath("number(concat(//input[@name='a']/@value, //input[@name='b']/@value))")],
                         [u'12.0'])

        self.assertEqual(sel.xpath("concat('xpath', 'rules')").extract(),
                         [u'xpathrules'])
        self.assertEqual([x.extract() for x in sel.xpath("concat(//input[@name='a']/@value, //input[@name='b']/@value)")],
                         [u'12'])

    def test_simple_selection_with_variables(self):
        """Using XPath variables"""
        body = u"<p><input name='a' value='1'/><input name='b' value='2'/></p>"
        sel = self.sscls(text=body)

        self.assertEqual([x.extract() for x in sel.xpath("//input[@value=$number]/@name", number=1)],
                         [u'a'])
        self.assertEqual([x.extract() for x in sel.xpath("//input[@name=$letter]/@value", letter='b')],
                         [u'2'])

        self.assertEqual(sel.xpath("count(//input[@value=$number or @name=$letter])",
                                   number=2, letter='a').extract(),
                         [u'2.0'])

        # you can also pass booleans
        self.assertEqual(sel.xpath("boolean(count(//input)=$cnt)=$test",
                                   cnt=2, test=True).extract(),
                         [u'1'])
        self.assertEqual(sel.xpath("boolean(count(//input)=$cnt)=$test",
                                   cnt=4, test=True).extract(),
                         [u'0'])
        self.assertEqual(sel.xpath("boolean(count(//input)=$cnt)=$test",
                                   cnt=4, test=False).extract(),
                         [u'1'])

        # for named nodes, you need to use "name()=node_name"
        self.assertEqual(sel.xpath("boolean(count(//*[name()=$tag])=$cnt)=$test",
                                   tag="input", cnt=2, test=True).extract(),
                         [u'1'])

    def test_simple_selection_with_variables_escape_friendly(self):
        """Using XPath variables with quotes that would need escaping with string formatting"""
        body = u"""<p>I'm mixing single and <input name='a' value='I say "Yeah!"'/>
        "double quotes" and I don't care :)</p>"""
        sel = self.sscls(text=body)

        t = 'I say "Yeah!"'
        # naive string formatting with give something like:
        # ValueError: XPath error: Invalid predicate in //input[@value="I say "Yeah!""]/@name
        self.assertRaises(ValueError, sel.xpath, '//input[@value="{}"]/@name'.format(t))

        # with XPath variables, escaping is done for you
        self.assertEqual([x.extract() for x in sel.xpath("//input[@value=$text]/@name", text=t)],
                         [u'a'])
        lt = """I'm mixing single and "double quotes" and I don't care :)"""
        # the following gives you something like
        # ValueError: XPath error: Invalid predicate in //p[normalize-space()='I'm mixing single and "double quotes" and I don't care :)']//@name
        self.assertRaises(ValueError, sel.xpath, "//p[normalize-space()='{}']//@name".format(lt))

        self.assertEqual([x.extract() for x in sel.xpath("//p[normalize-space()=$lng]//@name",
                                                         lng=lt)],
                         [u'a'])

    def test_accessing_attributes(self):
        body = u"""
<html lang="en" version="1.0">
    <body>
        <ul id="some-list" class="list-cls" class="list-cls">
            <li class="item-cls" id="list-item-1">
            <li class="item-cls active" id="list-item-2">
            <li class="item-cls" id="list-item-3">
        </ul>
    </body>
</html>
        """
        sel = self.sscls(text=body)
        self.assertEqual({'lang': 'en', 'version': '1.0'}, sel.attrib)
        self.assertEqual({'id': 'some-list', 'class': 'list-cls'}, sel.css('ul')[0].attrib)

        # for a SelectorList, bring the attributes of first-element only
        self.assertEqual({'id': 'some-list', 'class': 'list-cls'}, sel.css('ul').attrib)
        self.assertEqual({'class': 'item-cls', 'id': 'list-item-1'}, sel.css('li').attrib)
        self.assertEqual({}, sel.css('body').attrib)
        self.assertEqual({}, sel.css('non-existing-element').attrib)

        self.assertEqual(
            [{'class': 'item-cls', 'id': 'list-item-1'},
             {'class': 'item-cls active', 'id': 'list-item-2'},
             {'class': 'item-cls', 'id': 'list-item-3'}],
            [e.attrib for e in sel.css('li')])

    def test_representation_slice(self):
        body = u"<p><input name='{}' value='\xa9'/></p>".format(50 * 'b')
        sel = self.sscls(text=body)

        representation = "<Selector xpath='//input/@name' data='{}...'>".format(37 * 'b')
        if six.PY2:
            representation = "<Selector xpath='//input/@name' data=u'{}...'>".format(37 * 'b')

        self.assertEqual(
            [repr(it) for it in sel.xpath('//input/@name')],
            [representation]
        )

    def test_representation_unicode_query(self):
        body = u"<p><input name='{}' value='\xa9'/></p>".format(50 * 'b')

        representation = '<Selector xpath=\'//input[@value="©"]/@value\' data=\'©\'>'
        if six.PY2:
            representation = "<Selector xpath=u'//input[@value=\"\\xa9\"]/@value' data=u'\\xa9'>"

        sel = self.sscls(text=body)
        self.assertEqual(
            [repr(it) for it in sel.xpath(u'//input[@value="\xa9"]/@value')],
            [representation]
        )

    def test_check_text_argument_type(self):
        self.assertRaisesRegexp(TypeError, 'text argument should be of type',
                                self.sscls, b'<html/>')

    def test_extract_first(self):
        """Test if extract_first() returns first element"""
        body = u'<ul><li id="1">1</li><li id="2">2</li></ul>'
        sel = self.sscls(text=body)

        self.assertEqual(sel.xpath('//ul/li/text()').extract_first(),
                         sel.xpath('//ul/li/text()').extract()[0])

        self.assertEqual(sel.xpath('//ul/li[@id="1"]/text()').extract_first(),
                         sel.xpath('//ul/li[@id="1"]/text()').extract()[0])

        self.assertEqual(sel.xpath('//ul/li[2]/text()').extract_first(),
                         sel.xpath('//ul/li/text()').extract()[1])

        self.assertEqual(sel.xpath('/ul/li[@id="doesnt-exist"]/text()').extract_first(), None)

    def test_extract_first_default(self):
        """Test if extract_first() returns default value when no results found"""
        body = u'<ul><li id="1">1</li><li id="2">2</li></ul>'
        sel = self.sscls(text=body)

        self.assertEqual(sel.xpath('//div/text()').extract_first(default='missing'), 'missing')

    def test_selector_get_alias(self):
        """Test if get() returns extracted value on a Selector"""
        body = u'<ul><li id="1">1</li><li id="2">2</li><li id="3">3</li></ul>'
        sel = self.sscls(text=body)

        self.assertEqual(sel.xpath('//ul/li[position()>1]')[0].get(), u'<li id="2">2</li>')
        self.assertEqual(sel.xpath('//ul/li[position()>1]/text()')[0].get(), u'2')

    def test_selector_getall_alias(self):
        """Test if get() returns extracted value on a Selector"""
        body = u'<ul><li id="1">1</li><li id="2">2</li><li id="3">3</li></ul>'
        sel = self.sscls(text=body)

        self.assertListEqual(sel.xpath('//ul/li[position()>1]')[0].getall(), [u'<li id="2">2</li>'])
        self.assertListEqual(sel.xpath('//ul/li[position()>1]/text()')[0].getall(), [u'2'])

    def test_selectorlist_get_alias(self):
        """Test if get() returns first element for a selection call"""
        body = u'<ul><li id="1">1</li><li id="2">2</li><li id="3">3</li></ul>'
        sel = self.sscls(text=body)

        self.assertEqual(sel.xpath('//ul/li').get(), u'<li id="1">1</li>')
        self.assertEqual(sel.xpath('//ul/li/text()').get(), u'1')

    def test_re_first(self):
        """Test if re_first() returns first matched element"""
        body = u'<ul><li id="1">1</li><li id="2">2</li></ul>'
        sel = self.sscls(text=body)

        self.assertEqual(sel.xpath('//ul/li/text()').re_first(r'\d'),
                         sel.xpath('//ul/li/text()').re(r'\d')[0])

        self.assertEqual(sel.xpath('//ul/li[@id="1"]/text()').re_first(r'\d'),
                         sel.xpath('//ul/li[@id="1"]/text()').re(r'\d')[0])

        self.assertEqual(sel.xpath('//ul/li[2]/text()').re_first(r'\d'),
                         sel.xpath('//ul/li/text()').re(r'\d')[1])

        self.assertEqual(sel.xpath('/ul/li/text()').re_first(r'\w+'), None)
        self.assertEqual(sel.xpath('/ul/li[@id="doesnt-exist"]/text()').re_first(r'\d'), None)

        self.assertEqual(sel.re_first(r'id="(\d+)'), '1')
        self.assertEqual(sel.re_first(r'foo'), None)
        self.assertEqual(sel.re_first(r'foo', default='bar'), 'bar')

    def test_extract_first_re_default(self):
        """Test if re_first() returns default value when no results found"""
        body = u'<ul><li id="1">1</li><li id="2">2</li></ul>'
        sel = self.sscls(text=body)

        self.assertEqual(sel.xpath('//div/text()').re_first(r'\w+', default='missing'), 'missing')
        self.assertEqual(sel.xpath('/ul/li/text()').re_first(r'\w+', default='missing'), 'missing')

    def test_select_unicode_query(self):
        body = u"<p><input name='\xa9' value='1'/></p>"
        sel = self.sscls(text=body)
        self.assertEqual(sel.xpath(u'//input[@name="\xa9"]/@value').extract(), [u'1'])

    def test_list_elements_type(self):
        """Test Selector returning the same type in selection methods"""
        text = u'<p>test<p>'
        assert isinstance(self.sscls(text=text).xpath("//p")[0], self.sscls)
        assert isinstance(self.sscls(text=text).css("p")[0], self.sscls)

    def test_boolean_result(self):
        body = u"<p><input name='a'value='1'/><input name='b'value='2'/></p>"
        xs = self.sscls(text=body)
        self.assertEqual(xs.xpath("//input[@name='a']/@name='a'").extract(), [u'1'])
        self.assertEqual(xs.xpath("//input[@name='a']/@name='n'").extract(), [u'0'])

    def test_differences_parsing_xml_vs_html(self):
        """Test that XML and HTML Selector's behave differently"""
        # some text which is parsed differently by XML and HTML flavors
        text = u'<div><img src="a.jpg"><p>Hello</div>'
        hs = self.sscls(text=text, type='html')
        self.assertEqual(hs.xpath("//div").extract(),
                         [u'<div><img src="a.jpg"><p>Hello</p></div>'])

        xs = self.sscls(text=text, type='xml')
        self.assertEqual(xs.xpath("//div").extract(),
                         [u'<div><img src="a.jpg"><p>Hello</p></img></div>'])

    def test_error_for_unknown_selector_type(self):
        self.assertRaises(ValueError, self.sscls, text=u'', type='_na_')

    def test_text_or_root_is_required(self):
        self.assertRaisesRegexp(ValueError,
                                'Selector needs either text or root argument',
                                self.sscls)

    def test_bool(self):
        text = u'<a href="" >false</a><a href="nonempty">true</a>'
        hs = self.sscls(text=text, type='html')
        falsish = hs.xpath('//a/@href')[0]
        self.assertEqual(falsish.extract(), u'')
        self.assertFalse(falsish)
        trueish = hs.xpath('//a/@href')[1]
        self.assertEqual(trueish.extract(), u'nonempty')
        self.assertTrue(trueish)

    def test_slicing(self):
        text = u'<div><p>1</p><p>2</p><p>3</p></div>'
        hs = self.sscls(text=text, type='html')
        self.assertIsInstance(hs.css('p')[2], self.sscls)
        self.assertIsInstance(hs.css('p')[2:3], self.sscls.selectorlist_cls)
        self.assertIsInstance(hs.css('p')[:2], self.sscls.selectorlist_cls)
        self.assertEqual(hs.css('p')[2:3].extract(), [u'<p>3</p>'])
        self.assertEqual(hs.css('p')[1:3].extract(), [u'<p>2</p>', u'<p>3</p>'])

    def test_nested_selectors(self):
        """Nested selector tests"""
        body = u"""<body>
                    <div class='one'>
                      <ul>
                        <li>one</li><li>two</li>
                      </ul>
                    </div>
                    <div class='two'>
                      <ul>
                        <li>four</li><li>five</li><li>six</li>
                      </ul>
                    </div>
                  </body>"""

        x = self.sscls(text=body)
        divtwo = x.xpath('//div[@class="two"]')
        self.assertEqual(divtwo.xpath("//li").extract(),
                         ["<li>one</li>", "<li>two</li>", "<li>four</li>", "<li>five</li>", "<li>six</li>"])
        self.assertEqual(divtwo.xpath("./ul/li").extract(),
                         ["<li>four</li>", "<li>five</li>", "<li>six</li>"])
        self.assertEqual(divtwo.xpath(".//li").extract(),
                         ["<li>four</li>", "<li>five</li>", "<li>six</li>"])
        self.assertEqual(divtwo.xpath("./li").extract(), [])

    def test_selectorlist_getall_alias(self):
        """Nested selector tests using getall()"""
        body = u"""<body>
                    <div class='one'>
                      <ul>
                        <li>one</li><li>two</li>
                      </ul>
                    </div>
                    <div class='two'>
                      <ul>
                        <li>four</li><li>five</li><li>six</li>
                      </ul>
                    </div>
                  </body>"""

        x = self.sscls(text=body)
        divtwo = x.xpath('//div[@class="two"]')
        self.assertEqual(divtwo.xpath("//li").getall(),
                         ["<li>one</li>", "<li>two</li>", "<li>four</li>", "<li>five</li>", "<li>six</li>"])
        self.assertEqual(divtwo.xpath("./ul/li").getall(),
                         ["<li>four</li>", "<li>five</li>", "<li>six</li>"])
        self.assertEqual(divtwo.xpath(".//li").getall(),
                         ["<li>four</li>", "<li>five</li>", "<li>six</li>"])
        self.assertEqual(divtwo.xpath("./li").getall(), [])

    def test_mixed_nested_selectors(self):
        body = u'''<body>
                    <div id=1>not<span>me</span></div>
                    <div class="dos"><p>text</p><a href='#'>foo</a></div>
               </body>'''
        sel = self.sscls(text=body)
        self.assertEqual(sel.xpath('//div[@id="1"]').css('span::text').extract(), [u'me'])
        self.assertEqual(sel.css('#1').xpath('./span/text()').extract(), [u'me'])

    def test_dont_strip(self):
        sel = self.sscls(text=u'<div>fff: <a href="#">zzz</a></div>')
        self.assertEqual(sel.xpath("//text()").extract(), [u'fff: ', u'zzz'])

    def test_namespaces_simple(self):
        body = u"""
        <test xmlns:somens="http://scrapy.org">
           <somens:a id="foo">take this</a>
           <a id="bar">found</a>
        </test>
        """

        x = self.sscls(text=body, type='xml')

        x.register_namespace("somens", "http://scrapy.org")
        self.assertEqual(x.xpath("//somens:a/text()").extract(),
                         [u'take this'])

    def test_namespaces_adhoc(self):
        body = u"""
        <test xmlns:somens="http://scrapy.org">
           <somens:a id="foo">take this</a>
           <a id="bar">found</a>
        </test>
        """

        x = self.sscls(text=body, type='xml')

        self.assertEqual(x.xpath("//somens:a/text()",
                                 namespaces={"somens": "http://scrapy.org"}).extract(),
                         [u'take this'])

    def test_namespaces_adhoc_variables(self):
        body = u"""
        <test xmlns:somens="http://scrapy.org">
           <somens:a id="foo">take this</a>
           <a id="bar">found</a>
        </test>
        """

        x = self.sscls(text=body, type='xml')

        self.assertEqual(x.xpath("//somens:a/following-sibling::a[@id=$identifier]/text()",
                                 namespaces={"somens": "http://scrapy.org"},
                                 identifier="bar").extract(),
                         [u'found'])

    def test_namespaces_multiple(self):
        body = u"""<?xml version="1.0" encoding="UTF-8"?>
<BrowseNode xmlns="http://webservices.amazon.com/AWSECommerceService/2005-10-05"
            xmlns:b="http://somens.com"
            xmlns:p="http://www.scrapy.org/product" >
    <b:Operation>hello</b:Operation>
    <TestTag b:att="value"><Other>value</Other></TestTag>
    <p:SecondTestTag><material>iron</material><price>90</price><p:name>Dried Rose</p:name></p:SecondTestTag>
</BrowseNode>
        """
        x = self.sscls(text=body, type='xml')
        x.register_namespace("xmlns", "http://webservices.amazon.com/AWSECommerceService/2005-10-05")
        x.register_namespace("p", "http://www.scrapy.org/product")
        x.register_namespace("b", "http://somens.com")
        self.assertEqual(len(x.xpath("//xmlns:TestTag")), 1)
        self.assertEqual(x.xpath("//b:Operation/text()").extract()[0], 'hello')
        self.assertEqual(x.xpath("//xmlns:TestTag/@b:att").extract()[0], 'value')
        self.assertEqual(x.xpath("//p:SecondTestTag/xmlns:price/text()").extract()[0], '90')
        self.assertEqual(x.xpath("//p:SecondTestTag").xpath("./xmlns:price/text()")[0].extract(), '90')
        self.assertEqual(x.xpath("//p:SecondTestTag/xmlns:material/text()").extract()[0], 'iron')

    def test_namespaces_multiple_adhoc(self):
        body = u"""<?xml version="1.0" encoding="UTF-8"?>
<BrowseNode xmlns="http://webservices.amazon.com/AWSECommerceService/2005-10-05"
            xmlns:b="http://somens.com"
            xmlns:p="http://www.scrapy.org/product" >
    <b:Operation>hello</b:Operation>
    <TestTag b:att="value"><Other>value</Other></TestTag>
    <p:SecondTestTag><material>iron</material><price>90</price><p:name>Dried Rose</p:name></p:SecondTestTag>
</BrowseNode>
        """
        x = self.sscls(text=body, type='xml')
        x.register_namespace("xmlns", "http://webservices.amazon.com/AWSECommerceService/2005-10-05")
        self.assertEqual(len(x.xpath("//xmlns:TestTag")), 1)

        # "b" namespace is not declared yet
        self.assertRaises(ValueError, x.xpath, "//xmlns:TestTag/@b:att")

        # "b" namespace being passed ad-hoc
        self.assertEqual(
            x.xpath("//b:Operation/text()",
                    namespaces={"b": "http://somens.com"}).extract()[0],
            'hello')

        # "b" namespace declaration is not cached
        self.assertRaises(ValueError, x.xpath, "//xmlns:TestTag/@b:att")

        # "xmlns" is still defined
        self.assertEqual(
            x.xpath("//xmlns:TestTag/@b:att",
                    namespaces={"b": "http://somens.com"}).extract()[0],
            'value')

        # chained selectors still have knowledge of register_namespace() operations
        self.assertEqual(
            x.xpath("//p:SecondTestTag",
                    namespaces={"p": "http://www.scrapy.org/product"})
             .xpath("./xmlns:price/text()")[0].extract(),
            '90')

        # but chained selector don't know about parent ad-hoc declarations
        self.assertRaises(
            ValueError,
            x.xpath("//p:SecondTestTag",
                    namespaces={"p": "http://www.scrapy.org/product"})
             .xpath,
            "p:name/text()")

        # ad-hoc declarations need repeats when chaining
        self.assertEqual(
            x.xpath("//p:SecondTestTag",
                    namespaces={"p": "http://www.scrapy.org/product"})
             .xpath("p:name/text()",
                    namespaces={"p": "http://www.scrapy.org/product"})
             .extract_first(),
            'Dried Rose')

        # declaring several ad-hoc namespaces
        self.assertEqual(
            x.xpath(
                "string(//b:Operation/following-sibling::xmlns:TestTag"
                "/following-sibling::*//p:name)",
                namespaces={"b": "http://somens.com",
                            "p": "http://www.scrapy.org/product"})
             .extract_first(),
            'Dried Rose')

        # "p" prefix is not cached from previous calls
        self.assertRaises(ValueError, x.xpath, "//p:SecondTestTag/xmlns:price/text()")

        x.register_namespace("p", "http://www.scrapy.org/product")
        self.assertEqual(x.xpath("//p:SecondTestTag/xmlns:material/text()").extract()[0], 'iron')

    def test_make_links_absolute(self):
        text = u'<a href="file.html">link to file</a>'
        sel = Selector(text=text, base_url='http://example.com')
        sel.root.make_links_absolute()
        self.assertEqual(u'http://example.com/file.html', sel.xpath('//a/@href').extract_first())

    def test_re(self):
        body = u"""<div>Name: Mary
                    <ul>
                      <li>Name: John</li>
                      <li>Age: 10</li>
                      <li>Name: Paul</li>
                      <li>Age: 20</li>
                    </ul>
                    Age: 20
                  </div>"""
        x = self.sscls(text=body)

        name_re = re.compile(r"Name: (\w+)")
        self.assertEqual(x.xpath("//ul/li").re(name_re),
                         ["John", "Paul"])
        self.assertEqual(x.xpath("//ul/li").re(r"Age: (\d+)"),
                         ["10", "20"])

        # Test named group, hit and miss
        x = self.sscls(text=u'foobar')
        self.assertEqual(x.re('(?P<extract>foo)'), ['foo'])
        self.assertEqual(x.re('(?P<extract>baz)'), [])

        # A purposely constructed test for an edge case
        x = self.sscls(text=u'baz')
        self.assertEqual(x.re('(?P<extract>foo)|(?P<bar>baz)'), [])

    def test_re_replace_entities(self):
        body = u"""<script>{"foo":"bar &amp; &quot;baz&quot;"}</script>"""
        x = self.sscls(text=body)

        name_re = re.compile('{"foo":(.*)}')

        # by default, only &amp; and &lt; are preserved ;
        # other entities are converted
        expected = u'"bar &amp; "baz""'
        self.assertEqual(x.xpath("//script/text()").re(name_re), [expected])
        self.assertEqual(x.xpath("//script").re(name_re), [expected])
        self.assertEqual(x.xpath("//script/text()")[0].re(name_re), [expected])
        self.assertEqual(x.xpath("//script")[0].re(name_re), [expected])

        # check that re_first() works the same way for single value output
        self.assertEqual(x.xpath("//script").re_first(name_re), expected)
        self.assertEqual(x.xpath("//script")[0].re_first(name_re), expected)

        # switching off replace_entities will preserve &quot; also
        expected = u'"bar &amp; &quot;baz&quot;"'
        self.assertEqual(x.xpath("//script/text()").re(name_re, replace_entities=False), [expected])
        self.assertEqual(x.xpath("//script")[0].re(name_re, replace_entities=False), [expected])

        self.assertEqual(x.xpath("//script/text()").re_first(name_re, replace_entities=False), expected)
        self.assertEqual(x.xpath("//script")[0].re_first(name_re, replace_entities=False), expected)

    def test_re_intl(self):
        body = u'<div>Evento: cumplea\xf1os</div>'
        x = self.sscls(text=body)
        self.assertEqual(x.xpath("//div").re(r"Evento: (\w+)"), [u'cumplea\xf1os'])

    def test_selector_over_text(self):
        hs = self.sscls(text=u'<root>lala</root>')
        self.assertEqual(hs.extract(), u'<html><body><root>lala</root></body></html>')
        xs = self.sscls(text=u'<root>lala</root>', type='xml')
        self.assertEqual(xs.extract(), u'<root>lala</root>')
        self.assertEqual(xs.xpath('.').extract(), [u'<root>lala</root>'])

    def test_invalid_xpath(self):
        "Test invalid xpath raises ValueError with the invalid xpath"
        x = self.sscls(text=u"<html></html>")
        xpath = "//test[@foo='bar]"
        self.assertRaisesRegexp(ValueError, re.escape(xpath), x.xpath, xpath)

    def test_invalid_xpath_unicode(self):
        "Test *Unicode* invalid xpath raises ValueError with the invalid xpath"
        x = self.sscls(text=u"<html></html>")
        xpath = u"//test[@foo='\u0431ar]"
        encoded = xpath if six.PY3 else xpath.encode('unicode_escape')
        self.assertRaisesRegexp(ValueError, re.escape(encoded), x.xpath, xpath)

    def test_http_header_encoding_precedence(self):
        # u'\xa3'     = pound symbol in unicode
        # u'\xc2\xa3' = pound symbol in utf-8
        # u'\xa3'     = pound symbol in latin-1 (iso-8859-1)

        text = u'''<html>
        <head><meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1"></head>
        <body><span id="blank">\xa3</span></body></html>'''
        x = self.sscls(text=text)
        self.assertEqual(x.xpath("//span[@id='blank']/text()").extract(),
                         [u'\xa3'])

    def test_empty_bodies_shouldnt_raise_errors(self):
        self.sscls(text=u'').xpath('//text()').extract()

    def test_bodies_with_comments_only(self):
        sel = self.sscls(text=u'<!-- hello world -->', base_url='http://example.com')
        self.assertEqual(u'http://example.com', sel.root.base)

    def test_null_bytes_shouldnt_raise_errors(self):
        text = u'<root>pre\x00post</root>'
        self.sscls(text).xpath('//text()').extract()

    def test_replacement_char_from_badly_encoded_body(self):
        # \xe9 alone isn't valid utf8 sequence
        text = u'<html><p>an Jos\ufffd de</p><html>'
        self.assertEqual([u'an Jos\ufffd de'],
                         self.sscls(text).xpath('//text()').extract())

    def test_select_on_unevaluable_nodes(self):
        r = self.sscls(text=u'<span class="big">some text</span>')
        # Text node
        x1 = r.xpath('//text()')
        self.assertEqual(x1.extract(), [u'some text'])
        self.assertEqual(x1.xpath('.//b').extract(), [])
        # Tag attribute
        x1 = r.xpath('//span/@class')
        self.assertEqual(x1.extract(), [u'big'])
        self.assertEqual(x1.xpath('.//text()').extract(), [])

    def test_select_on_text_nodes(self):
        r = self.sscls(text=u'<div><b>Options:</b>opt1</div><div><b>Other</b>opt2</div>')
        x1 = r.xpath("//div/descendant::text()[preceding-sibling::b[contains(text(), 'Options')]]")
        self.assertEqual(x1.extract(), [u'opt1'])

        x1 = r.xpath("//div/descendant::text()/preceding-sibling::b[contains(text(), 'Options')]")
        self.assertEqual(x1.extract(), [u'<b>Options:</b>'])

    @unittest.skip("Text nodes lost parent node reference in lxml")
    def test_nested_select_on_text_nodes(self):
        # FIXME: does not work with lxml backend [upstream]
        r = self.sscls(text=u'<div><b>Options:</b>opt1</div><div><b>Other</b>opt2</div>')
        x1 = r.xpath("//div/descendant::text()")
        x2 = x1.xpath("./preceding-sibling::b[contains(text(), 'Options')]")
        self.assertEqual(x2.extract(), [u'<b>Options:</b>'])

    def test_weakref_slots(self):
        """Check that classes are using slots and are weak-referenceable"""
        x = self.sscls(text=u'')
        weakref.ref(x)
        assert not hasattr(x, '__dict__'), "%s does not use __slots__" % \
            x.__class__.__name__

    def test_remove_namespaces(self):
        xml = u"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xml:lang="en-US" xmlns:media="http://search.yahoo.com/mrss/">
  <link type="text/html"/>
  <entry>
    <link type="text/html"/>
  </entry>
  <link type="application/atom+xml"/>
</feed>
"""
        sel = self.sscls(text=xml, type='xml')
        self.assertEqual(len(sel.xpath("//link")), 0)
        self.assertEqual(len(sel.xpath("./namespace::*")), 3)
        sel.remove_namespaces()
        self.assertEqual(len(sel.xpath("//link")), 3)
        self.assertEqual(len(sel.xpath("./namespace::*")), 1)

    def test_remove_namespaces_embedded(self):
        xml = u"""
        <feed xmlns="http://www.w3.org/2005/Atom">
          <link type="text/html"/>
          <entry>
            <link type="text/html"/>
          </entry>
          <svg xmlns="http://www.w3.org/2000/svg" version="1.1" viewBox="0 0 100 100">
            <linearGradient id="gradient">
              <stop class="begin" offset="0%" style="stop-color:yellow;"/>
              <stop class="end" offset="80%" style="stop-color:green;"/>
            </linearGradient>
            <circle cx="50" cy="50" r="30" style="fill:url(#gradient)" />
          </svg>
        </feed>
        """
        sel = self.sscls(text=xml, type='xml')
        self.assertEqual(len(sel.xpath("//link")), 0)
        self.assertEqual(len(sel.xpath("//stop")), 0)
        self.assertEqual(len(sel.xpath("./namespace::*")), 2)
        self.assertEqual(len(sel.xpath("//f:link", namespaces={'f': 'http://www.w3.org/2005/Atom'})), 2)
        self.assertEqual(len(sel.xpath("//s:stop", namespaces={'s': 'http://www.w3.org/2000/svg'})), 2)
        sel.remove_namespaces()
        self.assertEqual(len(sel.xpath("//link")), 2)
        self.assertEqual(len(sel.xpath("//stop")), 2)
        self.assertEqual(len(sel.xpath("./namespace::*")), 1)

    def test_remove_attributes_namespaces(self):
        xml = u"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:atom="http://www.w3.org/2005/Atom" xml:lang="en-US" xmlns:media="http://search.yahoo.com/mrss/">
  <link atom:type="text/html"/>
  <entry>
    <link atom:type="text/html"/>
  </entry>
  <link atom:type="application/atom+xml"/>
</feed>
"""
        sel = self.sscls(text=xml, type='xml')
        self.assertEqual(len(sel.xpath("//link/@type")), 0)
        sel.remove_namespaces()
        self.assertEqual(len(sel.xpath("//link/@type")), 3)

    def test_smart_strings(self):
        """Lxml smart strings return values"""

        class SmartStringsSelector(Selector):
            _lxml_smart_strings = True

        body = u"""<body>
                    <div class='one'>
                      <ul>
                        <li>one</li><li>two</li>
                      </ul>
                    </div>
                    <div class='two'>
                      <ul>
                        <li>four</li><li>five</li><li>six</li>
                      </ul>
                    </div>
                  </body>"""

        # .getparent() is available for text nodes and attributes
        # only when smart_strings are on
        x = self.sscls(text=body)
        li_text = x.xpath('//li/text()')
        self.assertFalse(any(map(lambda e: hasattr(e.root, 'getparent'), li_text)))
        div_class = x.xpath('//div/@class')
        self.assertFalse(any(map(lambda e: hasattr(e.root, 'getparent'), div_class)))

        x = SmartStringsSelector(text=body)
        li_text = x.xpath('//li/text()')
        self.assertTrue(all(map(lambda e: hasattr(e.root, 'getparent'), li_text)))
        div_class = x.xpath('//div/@class')
        self.assertTrue(all(map(lambda e: hasattr(e.root, 'getparent'), div_class)))

    def test_xml_entity_expansion(self):
        malicious_xml = u'<?xml version="1.0" encoding="ISO-8859-1"?>'\
            '<!DOCTYPE foo [ <!ELEMENT foo ANY > <!ENTITY xxe SYSTEM '\
            '"file:///etc/passwd" >]><foo>&xxe;</foo>'

        sel = self.sscls(text=malicious_xml, type='xml')

        self.assertEqual(sel.extract(), '<foo>&xxe;</foo>')

    def test_configure_base_url(self):
        sel = self.sscls(text=u'nothing', base_url='http://example.com')
        self.assertEqual(u'http://example.com', sel.root.base)

    def test_extending_selector(self):
        class MySelectorList(Selector.selectorlist_cls):
            pass

        class MySelector(Selector):
            selectorlist_cls = MySelectorList

        sel = MySelector(text=u'<html><div>foo</div></html>')
        self.assertIsInstance(sel.xpath('//div'), MySelectorList)
        self.assertIsInstance(sel.xpath('//div')[0], MySelector)
        self.assertIsInstance(sel.css('div'), MySelectorList)
        self.assertIsInstance(sel.css('div')[0], MySelector)

    def test_replacement_null_char_from_body(self):
        text = u'<html>\x00<body><p>Grainy</p></body></html>'
        self.assertEqual(u'<html><body><p>Grainy</p></body></html>',
                         self.sscls(text).extract())

    def test_remove_selector_list(self):
        sel = self.sscls(text=u'<html><body><ul><li>1</li><li>2</li><li>3</li></ul></body></html>')
        sel_list = sel.css('li')
        sel_list.remove()
        self.assertIsInstance(sel.css('li'), self.sscls.selectorlist_cls)
        self.assertEqual(sel.css('li'), [])

    def test_remove_selector(self):
        sel = self.sscls(text=u'<html><body><ul><li>1</li><li>2</li><li>3</li></ul></body></html>')
        sel_list = sel.css('li')
        sel_list[0].remove()
        self.assertIsInstance(sel.css('li'), self.sscls.selectorlist_cls)
        self.assertEqual(sel.css('li::text').getall(), ['2', '3'])

    def test_remove_pseudo_element_selector_list(self):
        sel = self.sscls(text=u'<html><body><ul><li>1</li><li>2</li><li>3</li></ul></body></html>')
        sel_list = sel.css('li::text')
        self.assertEqual(sel_list.getall(), ['1', '2', '3'])
        with self.assertRaises(CannotRemoveElementWithoutRoot):
            sel_list.remove()

        self.assertIsInstance(sel.css('li'), self.sscls.selectorlist_cls)
        self.assertEqual(sel.css('li::text').getall(), ['1', '2', '3'])

    def test_remove_pseudo_element_selector(self):
        sel = self.sscls(text=u'<html><body><ul><li>1</li><li>2</li><li>3</li></ul></body></html>')
        sel_list = sel.css('li::text')
        self.assertEqual(sel_list.getall(), ['1', '2', '3'])
        with self.assertRaises(CannotRemoveElementWithoutRoot):
            sel_list[0].remove()

        self.assertIsInstance(sel.css('li'), self.sscls.selectorlist_cls)
        self.assertEqual(sel.css('li::text').getall(), ['1', '2', '3'])

    def test_remove_root_element_selector(self):
        sel = self.sscls(text=u'<html><body><ul><li>1</li><li>2</li><li>3</li></ul></body></html>')
        sel_list = sel.css('li::text')
        self.assertEqual(sel_list.getall(), ['1', '2', '3'])
        with self.assertRaises(CannotRemoveElementWithoutParent):
            sel.remove()

        with self.assertRaises(CannotRemoveElementWithoutParent):
            sel.css('html').remove()

        self.assertIsInstance(sel.css('li'), self.sscls.selectorlist_cls)
        self.assertEqual(sel.css('li::text').getall(), ['1', '2', '3'])

        sel.css('body').remove()
        self.assertEqual(sel.get(), '<html></html>')


class ExsltTestCase(unittest.TestCase):

    sscls = Selector

    def test_regexp(self):
        """EXSLT regular expression tests"""
        body = u"""
        <p><input name='a' value='1'/><input name='b' value='2'/></p>
        <div class="links">
        <a href="/first.html">first link</a>
        <a href="/second.html">second link</a>
        <a href="http://www.bayes.co.uk/xml/index.xml?/xml/utils/rechecker.xml">EXSLT match example</a>
        </div>
        """
        sel = self.sscls(text=body)

        # re:test()
        self.assertEqual(
            sel.xpath(
                '//input[re:test(@name, "[A-Z]+", "i")]').extract(),
            [x.extract() for x in sel.xpath('//input[re:test(@name, "[A-Z]+", "i")]')])
        self.assertEqual(
            [x.extract()
             for x in sel.xpath(
                 r'//a[re:test(@href, "\.html$")]/text()')],
            [u'first link', u'second link'])
        self.assertEqual(
            [x.extract()
             for x in sel.xpath(
                 '//a[re:test(@href, "first")]/text()')],
            [u'first link'])
        self.assertEqual(
            [x.extract()
             for x in sel.xpath(
                 '//a[re:test(@href, "second")]/text()')],
            [u'second link'])

        # re:match() is rather special: it returns a node-set of <match> nodes
        # [u'<match>http://www.bayes.co.uk/xml/index.xml?/xml/utils/rechecker.xml</match>',
        # u'<match>http</match>',
        # u'<match>www.bayes.co.uk</match>',
        # u'<match></match>',
        # u'<match>/xml/index.xml?/xml/utils/rechecker.xml</match>']
        self.assertEqual(
            sel.xpath(r're:match(//a[re:test(@href, "\.xml$")]/@href,'
                      r'"(\w+):\/\/([^/:]+)(:\d*)?([^# ]*)")/text()').extract(),
            [u'http://www.bayes.co.uk/xml/index.xml?/xml/utils/rechecker.xml',
             u'http',
             u'www.bayes.co.uk',
             u'',
             u'/xml/index.xml?/xml/utils/rechecker.xml'])

        # re:replace()
        self.assertEqual(
            sel.xpath(r're:replace(//a[re:test(@href, "\.xml$")]/@href,'
                      r'"(\w+)://(.+)(\.xml)", "","https://\2.html")').extract(),
            [u'https://www.bayes.co.uk/xml/index.xml?/xml/utils/rechecker.html'])

    def test_set(self):
        """EXSLT set manipulation tests"""
        # microdata example from http://schema.org/Event
        body = u"""
        <div itemscope itemtype="http://schema.org/Event">
          <a itemprop="url" href="nba-miami-philidelphia-game3.html">
          NBA Eastern Conference First Round Playoff Tickets:
          <span itemprop="name"> Miami Heat at Philadelphia 76ers - Game 3 (Home Game 1) </span>
          </a>

          <meta itemprop="startDate" content="2016-04-21T20:00">
            Thu, 04/21/16
            8:00 p.m.

          <div itemprop="location" itemscope itemtype="http://schema.org/Place">
            <a itemprop="url" href="wells-fargo-center.html">
            Wells Fargo Center
            </a>
            <div itemprop="address" itemscope itemtype="http://schema.org/PostalAddress">
              <span itemprop="addressLocality">Philadelphia</span>,
              <span itemprop="addressRegion">PA</span>
            </div>
          </div>

          <div itemprop="offers" itemscope itemtype="http://schema.org/AggregateOffer">
            Priced from: <span itemprop="lowPrice">$35</span>
            <span itemprop="offerCount">1938</span> tickets left
          </div>
        </div>
        """
        sel = self.sscls(text=body)

        self.assertEqual(
            sel.xpath('''//div[@itemtype="http://schema.org/Event"]
                            //@itemprop''').extract(),
            [u'url',
             u'name',
             u'startDate',
             u'location',
             u'url',
             u'address',
             u'addressLocality',
             u'addressRegion',
             u'offers',
             u'lowPrice',
             u'offerCount']
        )

        self.assertEqual(sel.xpath('''
                set:difference(//div[@itemtype="http://schema.org/Event"]
                                    //@itemprop,
                               //div[@itemtype="http://schema.org/Event"]
                                    //*[@itemscope]/*/@itemprop)''').extract(),
                         [u'url', u'name', u'startDate', u'location', u'offers'])
