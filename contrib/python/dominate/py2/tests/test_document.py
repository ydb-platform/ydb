from dominate import document
from dominate.tags import *

def test_doc():
  d = document()
  assert d.render() == \
'''<!DOCTYPE html>
<html>
  <head>
    <title>Dominate</title>
  </head>
  <body></body>
</html>'''


def test_decorator():
  @document()
  def foo():
    p('Hello World')

  f = foo()
  assert f.render() == \
'''<!DOCTYPE html>
<html>
  <head>
    <title>Dominate</title>
  </head>
  <body>
    <p>Hello World</p>
  </body>
</html>'''


def test_bare_decorator():
  @document
  def foo():
    p('Hello World')

  assert foo().render() == \
'''<!DOCTYPE html>
<html>
  <head>
    <title>Dominate</title>
  </head>
  <body>
    <p>Hello World</p>
  </body>
</html>'''


def test_title():
  d = document()
  assert d.title == 'Dominate'

  d = document(title='foobar')
  assert d.title == 'foobar'

  d.title = 'baz'
  assert d.title == 'baz'

  d.title = title('bar')
  assert d.title == 'bar'

  assert d.render() == \
'''<!DOCTYPE html>
<html>
  <head>
    <title>bar</title>
  </head>
  <body></body>
</html>'''

def test_containers():
  d = document()
  with d.footer:
    div('footer')
  with d:
    div('main1')
  with d.main:
    div('main2')
  print(d.header)
  print(d)
  print(d.body.children)
  with d.header:
    div('header1')
    div('header2')
  assert d.render() == \
'''<!DOCTYPE html>
<html>
  <head>
    <title>Dominate</title>
  </head>
  <body>
    <div>header1</div>
    <div>header2</div>
  ''''''
    <div>main1</div>
    <div>main2</div>
  ''''''
    <div>footer</div>
  </body>
</html>'''


def test_attributes():
  d = document(title=None, doctype=None, lang='en')
  assert d.render() == \
'''<html lang="en">
  <head></head>
  <body></body>
</html>'''

def test_repr():
  d = document(title='foo')
  assert d.__repr__() == '<dominate.document "foo">'

if __name__ == '__main__':
  # test_doc()
  test_decorator()
