AIOHTTP XMLRPC
==============

.. image:: https://coveralls.io/repos/github/mosquito/aiohttp-xmlrpc/badge.svg?branch=master
   :target: https://coveralls.io/github/mosquito/aiohttp-xmlrpc?branch=master

.. image:: https://github.com/mosquito/aiohttp-xmlrpc/workflows/tox/badge.svg
    :target: https://github.com/mosquito/aiohttp-xmlrpc/actions

.. image:: https://img.shields.io/pypi/v/aiohttp-xmlrpc.svg
    :target: https://pypi.python.org/pypi/aiohttp-xmlrpc/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/wheel/aiohttp-xmlrpc.svg
    :target: https://pypi.python.org/pypi/aiohttp-xmlrpc/

.. image:: https://img.shields.io/pypi/pyversions/aiohttp-xmlrpc.svg
    :target: https://pypi.python.org/pypi/aiohttp-xmlrpc/

.. image:: https://img.shields.io/pypi/l/aiohttp-xmlrpc.svg
    :target: https://pypi.python.org/pypi/aiohttp-xmlrpc/


XML-RPC server and client implementation based on aiohttp. Using lxml and aiohttp.Client.


Server example
---------------

.. code-block:: python

    from aiohttp import web
    from aiohttp_xmlrpc import handler
    from aiohttp_xmlrpc.handler import rename


    class XMLRPCExample(handler.XMLRPCView):

        @rename("nested.test")
        def rpc_test(self):
            return None

        def rpc_args(self, *args):
            return len(args)

        def rpc_kwargs(self, **kwargs):
            return len(kwargs)

        def rpc_args_kwargs(self, *args, **kwargs):
            return len(args) + len(kwargs)

        @rename("nested.exception")
        def rpc_exception(self):
            raise Exception("YEEEEEE!!!")


    app = web.Application()
    app.router.add_route('*', '/', XMLRPCExample)

    if __name__ == "__main__":
        web.run_app(app)




Client example
--------------

.. code-block:: python

    import asyncio
    from aiohttp_xmlrpc.client import ServerProxy


    loop = asyncio.get_event_loop()
    client = ServerProxy("http://127.0.0.1:8080/", loop=loop)

    async def main():
        # 'nested.test' method call
        print(await client.nested.test())

        # 'args' method call
        print(await client.args(1, 2, 3))

        client.close()

    if __name__ == "__main__":
        loop.run_until_complete(main())
