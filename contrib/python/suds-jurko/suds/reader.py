# This program is free software; you can redistribute it and/or modify
# it under the terms of the (LGPL) GNU Lesser General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Lesser General Public License for more details at
# ( http://www.gnu.org/licenses/lgpl.html ).
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# written by: Jeff Ortel ( jortel@redhat.com )

"""
  XML document reader classes providing integration with the suds library's
caching system.
"""


from suds.cache import Cache, NoCache
from suds.plugin import PluginContainer
from suds.sax.parser import Parser
from suds.store import DocumentStore
from suds.transport import Request


class Reader:
    """
    Provides integration with the cache.
    @ivar options: An options object.
    @type options: I{Options}
    """

    def __init__(self, options):
        """
        @param options: An options object.
        @type options: I{Options}
        """
        self.options = options
        self.plugins = PluginContainer(options.plugins)

    def mangle(self, name, x):
        """
        Mangle the name by hashing the I{name} and appending I{x}.
        @return: the mangled name.
        """
        h = abs(hash(name))
        return '%s-%s' % (h, x)


class DocumentReader(Reader):
    """
    Provides integration between the SAX L{Parser} and the document cache.
    """

    def open(self, url):
        """
        Open an XML document at the specified I{URL}.
        First, the document attempted to be retrieved from the I{object cache}.
        If not found, it is downloaded and parsed using the SAX parser. The
        result is added to the cache for the next open().
        @param url: A document URL.
        @type url: str.
        @return: The specified XML document.
        @rtype: I{Document}
        """
        cache = self.cache()
        id = self.mangle(url, 'document')
        d = cache.get(id)
        if d is None:
            d = self.download(url)
            cache.put(id, d)
        self.plugins.document.parsed(url=url, document=d.root())
        return d

    def download(self, url):
        """
        Download the document.
        @param url: A document URL.
        @type url: str.
        @return: A file pointer to the document.
        @rtype: file-like
        """
        content = None
        store = self.options.documentStore
        if store is not None:
            content = store.open(url)
        if content is None:
            fp = self.options.transport.open(Request(url))
            try:
                content = fp.read()
            finally:
                fp.close()
        ctx = self.plugins.document.loaded(url=url, document=content)
        content = ctx.document
        sax = Parser()
        return sax.parse(string=content)

    def cache(self):
        """
        Get the cache.
        @return: The I{cache} when I{cachingpolicy} = B{0}.
        @rtype: L{Cache}
        """
        if self.options.cachingpolicy == 0:
            return self.options.cache
        return NoCache()


class DefinitionsReader(Reader):
    """
    Provides integration between the WSDL Definitions object and the object
    cache.
    @ivar fn: A factory function (constructor) used to
        create the object not found in the cache.
    @type fn: I{Constructor}
    """

    def __init__(self, options, fn):
        """
        @param options: An options object.
        @type options: I{Options}
        @param fn: A factory function (constructor) used to create the object
            not found in the cache.
        @type fn: I{Constructor}
        """
        Reader.__init__(self, options)
        self.fn = fn

    def open(self, url):
        """
        Open a WSDL at the specified I{URL}.
        First, the WSDL attempted to be retrieved from
        the I{object cache}.  After unpickled from the cache, the
        I{options} attribute is restored.
        If not found, it is downloaded and instantiated using the
        I{fn} constructor and added to the cache for the next open().
        @param url: A WSDL URL.
        @type url: str.
        @return: The WSDL object.
        @rtype: I{Definitions}
        """
        cache = self.cache()
        id = self.mangle(url, 'wsdl')
        d = cache.get(id)
        if d is None:
            d = self.fn(url, self.options)
            cache.put(id, d)
        else:
            d.options = self.options
            for imp in d.imports:
                imp.imported.options = self.options
        return d

    def cache(self):
        """
        Get the cache.
        @return: The I{cache} when I{cachingpolicy} = B{1}.
        @rtype: L{Cache}
        """
        if self.options.cachingpolicy == 1:
            return self.options.cache
        return NoCache()
