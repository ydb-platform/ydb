#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Frame Manager module."""

import asyncio
from collections import OrderedDict
import logging
from types import SimpleNamespace
from typing import Any, Awaitable, Dict, Generator, List, Optional, Set, Union

from pyee import EventEmitter

from pyppeteer import helper
from pyppeteer.connection import CDPSession
from pyppeteer.element_handle import ElementHandle
from pyppeteer.errors import NetworkError
from pyppeteer.execution_context import ExecutionContext, JSHandle
from pyppeteer.errors import ElementHandleError, PageError, TimeoutError
from pyppeteer.util import merge_dict

logger = logging.getLogger(__name__)


class FrameManager(EventEmitter):
    """FrameManager class."""

    Events = SimpleNamespace(
        FrameAttached='frameattached',
        FrameNavigated='framenavigated',
        FrameDetached='framedetached',
        LifecycleEvent='lifecycleevent',
        FrameNavigatedWithinDocument='framenavigatedwithindocument',
    )

    def __init__(self, client: CDPSession, frameTree: Dict, page: Any) -> None:
        """Make new frame manager."""
        super().__init__()
        self._client = client
        self._page = page
        self._frames: OrderedDict[str, Frame] = OrderedDict()
        self._mainFrame: Optional[Frame] = None
        self._contextIdToContext: Dict[str, ExecutionContext] = dict()

        client.on('Page.frameAttached',
                  lambda event: self._onFrameAttached(
                      event.get('frameId', ''), event.get('parentFrameId', ''))
                  )
        client.on('Page.frameNavigated',
                  lambda event: self._onFrameNavigated(event.get('frame')))
        client.on('Page.navigatedWithinDocument',
                  lambda event: self._onFrameNavigatedWithinDocument(
                      event.get('frameId'), event.get('url')
                  ))
        client.on('Page.frameDetached',
                  lambda event: self._onFrameDetached(event.get('frameId')))
        client.on('Page.frameStoppedLoading',
                  lambda event: self._onFrameStoppedLoading(
                      event.get('frameId')
                  ))
        client.on('Runtime.executionContextCreated',
                  lambda event: self._onExecutionContextCreated(
                      event.get('context')))
        client.on('Runtime.executionContextDestroyed',
                  lambda event: self._onExecutionContextDestroyed(
                      event.get('executionContextId')))
        client.on('Runtime.executionContextsCleared',
                  lambda event: self._onExecutionContextsCleared())
        client.on('Page.lifecycleEvent',
                  lambda event: self._onLifecycleEvent(event))

        self._handleFrameTree(frameTree)

    def _onLifecycleEvent(self, event: Dict) -> None:
        frame = self._frames.get(event['frameId'])
        if not frame:
            return
        frame._onLifecycleEvent(event['loaderId'], event['name'])
        self.emit(FrameManager.Events.LifecycleEvent, frame)

    def _onFrameStoppedLoading(self, frameId: str) -> None:
        frame = self._frames.get(frameId)
        if not frame:
            return
        frame._onLoadingStopped()
        self.emit(FrameManager.Events.LifecycleEvent, frame)

    def _handleFrameTree(self, frameTree: Dict) -> None:
        frame = frameTree['frame']
        if 'parentId' in frame:
            self._onFrameAttached(
                frame['id'],
                frame['parentId'],
            )
        self._onFrameNavigated(frame)
        if 'childFrames' not in frameTree:
            return
        for child in frameTree['childFrames']:
            self._handleFrameTree(child)

    @property
    def mainFrame(self) -> Optional['Frame']:
        """Return main frame."""
        return self._mainFrame

    def frames(self) -> List['Frame']:
        """Return all frames."""
        return list(self._frames.values())

    def frame(self, frameId: str) -> Optional['Frame']:
        """Return :class:`Frame` of ``frameId``."""
        return self._frames.get(frameId)

    def _onFrameAttached(self, frameId: str, parentFrameId: str) -> None:
        if frameId in self._frames:
            return
        parentFrame = self._frames.get(parentFrameId)
        frame = Frame(self._client, parentFrame, frameId)
        self._frames[frameId] = frame
        self.emit(FrameManager.Events.FrameAttached, frame)

    def _onFrameNavigated(self, framePayload: dict) -> None:
        isMainFrame = not framePayload.get('parentId')
        if isMainFrame:
            frame = self._mainFrame
        else:
            frame = self._frames.get(framePayload.get('id', ''))
        if not (isMainFrame or frame):
            raise PageError('We either navigate top level or have old version '
                            'of the navigated frame')

        # Detach all child frames first.
        if frame:
            for child in frame.childFrames:
                self._removeFramesRecursively(child)

        # Update or create main frame.
        _id = framePayload.get('id', '')
        if isMainFrame:
            if frame:
                # Update frame id to retain frame identity on cross-process navigation.  # noqa: E501
                self._frames.pop(frame._id, None)
                frame._id = _id
            else:
                # Initial main frame navigation.
                frame = Frame(self._client, None, _id)
            self._frames[_id] = frame
            self._mainFrame = frame

        # Update frame payload.
        frame._navigated(framePayload)  # type: ignore
        self.emit(FrameManager.Events.FrameNavigated, frame)

    def _onFrameNavigatedWithinDocument(self, frameId: str, url: str) -> None:
        frame = self._frames.get(frameId)
        if not frame:
            return
        frame._navigatedWithinDocument(url)
        self.emit(FrameManager.Events.FrameNavigatedWithinDocument, frame)
        self.emit(FrameManager.Events.FrameNavigated, frame)

    def _onFrameDetached(self, frameId: str) -> None:
        frame = self._frames.get(frameId)
        if frame:
            self._removeFramesRecursively(frame)

    def _onExecutionContextCreated(self, contextPayload: Dict) -> None:
        if (contextPayload.get('auxData') and
                contextPayload['auxData'].get('frameId')):
            frameId = contextPayload['auxData']['frameId']
        else:
            frameId = None

        frame = self._frames.get(frameId)

        def _createJSHandle(obj: Dict) -> JSHandle:
            context = self.executionContextById(contextPayload['id'])
            return self.createJSHandle(context, obj)

        context = ExecutionContext(
            self._client,
            contextPayload,
            _createJSHandle,
            frame,
        )
        self._contextIdToContext[contextPayload['id']] = context

        if frame:
            frame._addExecutionContext(context)

    def _onExecutionContextDestroyed(self, executionContextId: str) -> None:
        context = self._contextIdToContext.get(executionContextId)
        if not context:
            return
        del self._contextIdToContext[executionContextId]

        frame = context.frame
        if frame:
            frame._removeExecutionContext(context)

    def _onExecutionContextsCleared(self) -> None:
        for context in self._contextIdToContext.values():
            frame = context.frame
            if frame:
                frame._removeExecutionContext(context)
        self._contextIdToContext.clear()

    def executionContextById(self, contextId: str) -> ExecutionContext:
        """Get stored ``ExecutionContext`` by ``id``."""
        context = self._contextIdToContext.get(contextId)
        if not context:
            raise ElementHandleError(
                f'INTERNAL ERROR: missing context with id = {contextId}'
            )
        return context

    def createJSHandle(self, context: ExecutionContext,
                       remoteObject: Dict = None) -> JSHandle:
        """Create JS handle associated to the context id and remote object."""
        if remoteObject is None:
            remoteObject = dict()
        if remoteObject.get('subtype') == 'node':
            return ElementHandle(context, self._client, remoteObject,
                                 self._page, self)
        return JSHandle(context, self._client, remoteObject)

    def _removeFramesRecursively(self, frame: 'Frame') -> None:
        for child in frame.childFrames:
            self._removeFramesRecursively(child)
        frame._detach()
        self._frames.pop(frame._id, None)
        self.emit(FrameManager.Events.FrameDetached, frame)


class Frame(object):
    """Frame class.

    Frame objects can be obtained via :attr:`pyppeteer.page.Page.mainFrame`.
    """

    def __init__(self, client: CDPSession, parentFrame: Optional['Frame'],
                 frameId: str) -> None:
        self._client = client
        self._parentFrame = parentFrame
        self._url = ''
        self._detached = False
        self._id = frameId

        self._documentPromise: Optional[ElementHandle] = None
        self._contextResolveCallback = lambda _: None
        self._setDefaultContext(None)

        self._waitTasks: Set[WaitTask] = set()  # maybe list
        self._loaderId = ''
        self._lifecycleEvents: Set[str] = set()
        self._childFrames: Set[Frame] = set()  # maybe list
        if self._parentFrame:
            self._parentFrame._childFrames.add(self)

    def _addExecutionContext(self, context: ExecutionContext) -> None:
        if context._isDefault:
            self._setDefaultContext(context)

    def _removeExecutionContext(self, context: ExecutionContext) -> None:
        if context._isDefault:
            self._setDefaultContext(None)

    def _setDefaultContext(self, context: Optional[ExecutionContext]) -> None:
        if context is not None:
            self._contextResolveCallback(context)  # type: ignore
            self._contextResolveCallback = lambda _: None
            for waitTask in self._waitTasks:
                self._client._loop.create_task(waitTask.rerun())
        else:
            self._documentPromise = None
            self._contextPromise = self._client._loop.create_future()
            self._contextResolveCallback = (
                lambda _context: self._contextPromise.set_result(_context)
            )

    async def executionContext(self) -> Optional[ExecutionContext]:
        """Return execution context of this frame.

        Return :class:`~pyppeteer.execution_context.ExecutionContext`
        associated to this frame.
        """
        return await self._contextPromise

    async def evaluateHandle(self, pageFunction: str, *args: Any) -> JSHandle:
        """Execute function on this frame.

        Details see :meth:`pyppeteer.page.Page.evaluateHandle`.
        """
        context = await self.executionContext()
        if context is None:
            raise PageError('this frame has no context.')
        return await context.evaluateHandle(pageFunction, *args)

    async def evaluate(self, pageFunction: str, *args: Any,
                       force_expr: bool = False) -> Any:
        """Evaluate pageFunction on this frame.

        Details see :meth:`pyppeteer.page.Page.evaluate`.
        """
        context = await self.executionContext()
        if context is None:
            raise ElementHandleError('ExecutionContext is None.')
        return await context.evaluate(
            pageFunction, *args, force_expr=force_expr)

    async def querySelector(self, selector: str) -> Optional[ElementHandle]:
        """Get element which matches `selector` string.

        Details see :meth:`pyppeteer.page.Page.querySelector`.
        """
        document = await self._document()
        value = await document.querySelector(selector)
        return value

    async def _document(self) -> ElementHandle:
        if self._documentPromise:
            return self._documentPromise
        context = await self.executionContext()
        if context is None:
            raise PageError('No context exists.')
        document = (await context.evaluateHandle('document')).asElement()
        self._documentPromise = document
        if document is None:
            raise PageError('Could not find `document`.')
        return document

    async def xpath(self, expression: str) -> List[ElementHandle]:
        """Evaluate the XPath expression.

        If there are no such elements in this frame, return an empty list.

        :arg str expression: XPath string to be evaluated.
        """
        document = await self._document()
        value = await document.xpath(expression)
        return value

    async def querySelectorEval(self, selector: str, pageFunction: str,
                                *args: Any) -> Any:
        """Execute function on element which matches selector.

        Details see :meth:`pyppeteer.page.Page.querySelectorEval`.
        """
        document = await self._document()
        return await document.querySelectorEval(selector, pageFunction, *args)

    async def querySelectorAllEval(self, selector: str, pageFunction: str,
                                   *args: Any) -> Optional[Dict]:
        """Execute function on all elements which matches selector.

        Details see :meth:`pyppeteer.page.Page.querySelectorAllEval`.
        """
        document = await self._document()
        value = await document.JJeval(selector, pageFunction, *args)
        return value

    async def querySelectorAll(self, selector: str) -> List[ElementHandle]:
        """Get all elements which matches `selector`.

        Details see :meth:`pyppeteer.page.Page.querySelectorAll`.
        """
        document = await self._document()
        value = await document.querySelectorAll(selector)
        return value

    #: Alias to :meth:`querySelector`
    J = querySelector
    #: Alias to :meth:`xpath`
    Jx = xpath
    #: Alias to :meth:`querySelectorEval`
    Jeval = querySelectorEval
    #: Alias to :meth:`querySelectorAll`
    JJ = querySelectorAll
    #: Alias to :meth:`querySelectorAllEval`
    JJeval = querySelectorAllEval

    async def content(self) -> str:
        """Get the whole HTML contents of the page."""
        return await self.evaluate('''
() => {
  let retVal = '';
  if (document.doctype)
    retVal = new XMLSerializer().serializeToString(document.doctype);
  if (document.documentElement)
    retVal += document.documentElement.outerHTML;
  return retVal;
}
        '''.strip())

    async def setContent(self, html: str) -> None:
        """Set content to this page."""
        func = '''
function(html) {
  document.open();
  document.write(html);
  document.close();
}
'''
        await self.evaluate(func, html)

    @property
    def name(self) -> str:
        """Get frame name."""
        return self.__dict__.get('_name', '')

    @property
    def url(self) -> str:
        """Get url of the frame."""
        return self._url

    @property
    def parentFrame(self) -> Optional['Frame']:
        """Get parent frame.

        If this frame is main frame or detached frame, return ``None``.
        """
        return self._parentFrame

    @property
    def childFrames(self) -> List['Frame']:
        """Get child frames."""
        return list(self._childFrames)

    def isDetached(self) -> bool:
        """Return ``True`` if this frame is detached.

        Otherwise return ``False``.
        """
        return self._detached

    async def injectFile(self, filePath: str) -> str:
        """[Deprecated] Inject file to the frame."""
        logger.warning('`injectFile` method is deprecated.'
                       ' Use `addScriptTag` method instead.')
        with open(filePath) as f:
            contents = f.read()
        contents += '/* # sourceURL= {} */'.format(filePath.replace('\n', ''))
        return await self.evaluate(contents)

    async def addScriptTag(self, options: Dict) -> ElementHandle:  # noqa: C901
        """Add script tag to this frame.

        Details see :meth:`pyppeteer.page.Page.addScriptTag`.
        """
        context = await self.executionContext()
        if context is None:
            raise ElementHandleError('ExecutionContext is None.')

        addScriptUrl = '''
        async function addScriptUrl(url, type) {
            const script = document.createElement('script');
            script.src = url;
            if (type)
                script.type = type;
            const promise = new Promise((res, rej) => {
                script.onload = res;
                script.onerror = rej;
            });
            document.head.appendChild(script);
            await promise;
            return script;
        }'''

        addScriptContent = '''
        function addScriptContent(content, type = 'text/javascript') {
            const script = document.createElement('script');
            script.type = type;
            script.text = content;
            let error = null;
            script.onerror = e => error = e;
            document.head.appendChild(script);
            if (error)
                throw error;
            return script;
        }'''

        if isinstance(options.get('url'), str):
            url = options['url']
            args = [addScriptUrl, url]
            if 'type' in options:
                args.append(options['type'])
            try:
                return (await context.evaluateHandle(*args)  # type: ignore
                        ).asElement()
            except ElementHandleError as e:
                raise PageError(f'Loading script from {url} failed') from e

        if isinstance(options.get('path'), str):
            with open(options['path']) as f:
                contents = f.read()
            contents = contents + '//# sourceURL={}'.format(
                options['path'].replace('\n', ''))
            args = [addScriptContent, contents]
            if 'type' in options:
                args.append(options['type'])
            return (await context.evaluateHandle(*args)  # type: ignore
                    ).asElement()

        if isinstance(options.get('content'), str):
            args = [addScriptContent, options['content']]
            if 'type' in options:
                args.append(options['type'])
            return (await context.evaluateHandle(*args)  # type: ignore
                    ).asElement()

        raise ValueError(
            'Provide an object with a `url`, `path` or `content` property')

    async def addStyleTag(self, options: Dict) -> ElementHandle:
        """Add style tag to this frame.

        Details see :meth:`pyppeteer.page.Page.addStyleTag`.
        """
        context = await self.executionContext()
        if context is None:
            raise ElementHandleError('ExecutionContext is None.')

        addStyleUrl = '''
        async function (url) {
            const link = document.createElement('link');
            link.rel = 'stylesheet';
            link.href = url;
            const promise = new Promise((res, rej) => {
                link.onload = res;
                link.onerror = rej;
            });
            document.head.appendChild(link);
            await promise;
            return link;
        }'''

        addStyleContent = '''
        async function (content) {
            const style = document.createElement('style');
            style.type = 'text/css';
            style.appendChild(document.createTextNode(content));
            const promise = new Promise((res, rej) => {
                style.onload = res;
                style.onerror = rej;
            });
            document.head.appendChild(style);
            await promise;
            return style;
        }'''

        if isinstance(options.get('url'), str):
            url = options['url']
            try:
                return (await context.evaluateHandle(  # type: ignore
                    addStyleUrl, url)).asElement()
            except ElementHandleError as e:
                raise PageError(f'Loading style from {url} failed') from e

        if isinstance(options.get('path'), str):
            with open(options['path']) as f:
                contents = f.read()
            contents = contents + '/*# sourceURL={}*/'.format(
                options['path'].replace('\n', ''))
            return (await context.evaluateHandle(  # type: ignore
                addStyleContent, contents)).asElement()

        if isinstance(options.get('content'), str):
            return (await context.evaluateHandle(  # type: ignore
                addStyleContent, options['content'])).asElement()

        raise ValueError(
            'Provide an object with a `url`, `path` or `content` property')

    async def click(self, selector: str, options: dict = None, **kwargs: Any
                    ) -> None:
        """Click element which matches ``selector``.

        Details see :meth:`pyppeteer.page.Page.click`.
        """
        options = merge_dict(options, kwargs)
        handle = await self.J(selector)
        if not handle:
            raise PageError('No node found for selector: ' + selector)
        await handle.click(options)
        await handle.dispose()

    async def focus(self, selector: str) -> None:
        """Focus element which matches ``selector``.

        Details see :meth:`pyppeteer.page.Page.focus`.
        """
        handle = await self.J(selector)
        if not handle:
            raise PageError('No node found for selector: ' + selector)
        await self.evaluate('element => element.focus()', handle)
        await handle.dispose()

    async def hover(self, selector: str) -> None:
        """Mouse hover the element which matches ``selector``.

        Details see :meth:`pyppeteer.page.Page.hover`.
        """
        handle = await self.J(selector)
        if not handle:
            raise PageError('No node found for selector: ' + selector)
        await handle.hover()
        await handle.dispose()

    async def select(self, selector: str, *values: str) -> List[str]:
        """Select options and return selected values.

        Details see :meth:`pyppeteer.page.Page.select`.
        """
        for value in values:
            if not isinstance(value, str):
                raise TypeError(
                    'Values must be string. '
                    f'Found {value} of type {type(value)}'
                )
        return await self.querySelectorEval(  # type: ignore
            selector, '''
(element, values) => {
    if (element.nodeName.toLowerCase() !== 'select')
        throw new Error('Element is not a <select> element.');

    const options = Array.from(element.options);
    element.value = undefined;
    for (const option of options) {
        option.selected = values.includes(option.value);
        if (option.selected && !element.multiple)
            break;
    }

    element.dispatchEvent(new Event('input', { 'bubbles': true }));
    element.dispatchEvent(new Event('change', { 'bubbles': true }));
    return options.filter(option => option.selected).map(options => options.value)
}
        ''', values)  # noqa: E501

    async def tap(self, selector: str) -> None:
        """Tap the element which matches the ``selector``.

        Details see :meth:`pyppeteer.page.Page.tap`.
        """
        handle = await self.J(selector)
        if not handle:
            raise PageError('No node found for selector: ' + selector)
        await handle.tap()
        await handle.dispose()

    async def type(self, selector: str, text: str, options: dict = None,
                   **kwargs: Any) -> None:
        """Type ``text`` on the element which matches ``selector``.

        Details see :meth:`pyppeteer.page.Page.type`.
        """
        options = merge_dict(options, kwargs)
        handle = await self.querySelector(selector)
        if handle is None:
            raise PageError('Cannot find {} on this page'.format(selector))
        await handle.type(text, options)
        await handle.dispose()

    def waitFor(self, selectorOrFunctionOrTimeout: Union[str, int, float],
                options: dict = None, *args: Any, **kwargs: Any
                ) -> Union[Awaitable, 'WaitTask']:
        """Wait until `selectorOrFunctionOrTimeout`.

        Details see :meth:`pyppeteer.page.Page.waitFor`.
        """
        options = merge_dict(options, kwargs)
        if isinstance(selectorOrFunctionOrTimeout, (int, float)):
            fut: Awaitable[None] = self._client._loop.create_task(
                asyncio.sleep(selectorOrFunctionOrTimeout / 1000))
            return fut
        if not isinstance(selectorOrFunctionOrTimeout, str):
            fut = self._client._loop.create_future()
            fut.set_exception(TypeError(
                'Unsupported target type: ' +
                str(type(selectorOrFunctionOrTimeout))
            ))
            return fut

        if args or helper.is_jsfunc(selectorOrFunctionOrTimeout):
            return self.waitForFunction(
                selectorOrFunctionOrTimeout, options, *args)
        if selectorOrFunctionOrTimeout.startswith('//'):
            return self.waitForXPath(selectorOrFunctionOrTimeout, options)
        return self.waitForSelector(selectorOrFunctionOrTimeout, options)

    def waitForSelector(self, selector: str, options: dict = None,
                        **kwargs: Any) -> 'WaitTask':
        """Wait until element which matches ``selector`` appears on page.

        Details see :meth:`pyppeteer.page.Page.waitForSelector`.
        """
        options = merge_dict(options, kwargs)
        return self._waitForSelectorOrXPath(selector, False, options)

    def waitForXPath(self, xpath: str, options: dict = None,
                     **kwargs: Any) -> 'WaitTask':
        """Wait until element which matches ``xpath`` appears on page.

        Details see :meth:`pyppeteer.page.Page.waitForXPath`.
        """
        options = merge_dict(options, kwargs)
        return self._waitForSelectorOrXPath(xpath, True, options)

    def waitForFunction(self, pageFunction: str, options: dict = None,
                        *args: Any, **kwargs: Any) -> 'WaitTask':
        """Wait until the function completes.

        Details see :meth:`pyppeteer.page.Page.waitForFunction`.
        """
        options = merge_dict(options, kwargs)
        timeout = options.get('timeout',  30000)  # msec
        polling = options.get('polling', 'raf')
        return WaitTask(self, pageFunction, 'function', polling, timeout,
                        self._client._loop, *args)

    def _waitForSelectorOrXPath(self, selectorOrXPath: str, isXPath: bool,
                                options: dict = None, **kwargs: Any
                                ) -> 'WaitTask':
        options = merge_dict(options, kwargs)
        timeout = options.get('timeout', 30000)
        waitForVisible = bool(options.get('visible'))
        waitForHidden = bool(options.get('hidden'))
        polling = 'raf' if waitForHidden or waitForVisible else 'mutation'
        title = '{} "{}"{}'.format(
            'XPath' if isXPath else 'selector',
            selectorOrXPath,
            ' to be hidden' if waitForHidden else '',
        )

        predicate = '''
(selectorOrXPath, isXPath, waitForVisible, waitForHidden) => {
    const node = isXPath
        ? document.evaluate(selectorOrXPath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
        : document.querySelector(selectorOrXPath);
    if (!node)
        return waitForHidden;
    if (!waitForVisible && !waitForHidden)
        return node;
    const element = /** @type {Element} */ (node.nodeType === Node.TEXT_NODE ? node.parentElement : node);

    const style = window.getComputedStyle(element);
    const isVisible = style && style.visibility !== 'hidden' && hasVisibleBoundingBox();
    const success = (waitForVisible === isVisible || waitForHidden === !isVisible)
    return success ? node : null

    function hasVisibleBoundingBox() {
        const rect = element.getBoundingClientRect();
        return !!(rect.top || rect.bottom || rect.width || rect.height);
    }
}
        '''  # noqa: E501

        return WaitTask(
            self,
            predicate,
            title,
            polling,
            timeout,
            self._client._loop,
            selectorOrXPath,
            isXPath,
            waitForVisible,
            waitForHidden,
        )

    async def title(self) -> str:
        """Get title of the frame."""
        return await self.evaluate('() => document.title')

    def _navigated(self, framePayload: dict) -> None:
        self._name = framePayload.get('name', '')
        self._navigationURL = framePayload.get('url', '')
        self._url = framePayload.get('url', '')

    def _navigatedWithinDocument(self, url: str) -> None:
        self._url = url

    def _onLifecycleEvent(self, loaderId: str, name: str) -> None:
        if name == 'init':
            self._loaderId = loaderId
            self._lifecycleEvents.clear()
        else:
            self._lifecycleEvents.add(name)

    def _onLoadingStopped(self) -> None:
        self._lifecycleEvents.add('DOMContentLoaded')
        self._lifecycleEvents.add('load')

    def _detach(self) -> None:
        for waitTask in self._waitTasks:
            waitTask.terminate(
                PageError('waitForFunction failed: frame got detached.'))
        self._detached = True
        if self._parentFrame:
            self._parentFrame._childFrames.remove(self)
        self._parentFrame = None


class WaitTask(object):
    """WaitTask class.

    Instance of this class is awaitable.
    """

    def __init__(self, frame: Frame, predicateBody: str,  # noqa: C901
                 title: str, polling: Union[str, int], timeout: float,
                 loop: asyncio.AbstractEventLoop, *args: Any) -> None:
        if isinstance(polling, str):
            if polling not in ['raf', 'mutation']:
                raise ValueError(f'Unknown polling: {polling}')
        elif isinstance(polling, (int, float)):
            if polling <= 0:
                raise ValueError(
                    f'Cannot poll with non-positive interval: {polling}'
                )
        else:
            raise ValueError(f'Unknown polling option: {polling}')

        self._frame = frame
        self._polling = polling
        self._timeout = timeout
        self._loop = loop
        if args or helper.is_jsfunc(predicateBody):
            self._predicateBody = f'return ({predicateBody})(...args)'
        else:
            self._predicateBody = f'return {predicateBody}'
        self._args = args
        self._runCount = 0
        self._terminated = False
        self._timeoutError = False
        frame._waitTasks.add(self)

        self.promise = self._loop.create_future()

        async def timer(timeout: Union[int, float]) -> None:
            await asyncio.sleep(timeout / 1000)
            self._timeoutError = True
            self.terminate(TimeoutError(
                f'Waiting for {title} failed: timeout {timeout}ms exceeds.'
            ))

        if timeout:
            self._timeoutTimer = self._loop.create_task(timer(self._timeout))
        self._runningTask = self._loop.create_task(self.rerun())

    def __await__(self) -> Generator:
        """Make this class **awaitable**."""
        result = yield from self.promise
        if isinstance(result, Exception):
            raise result
        return result

    def terminate(self, error: Exception) -> None:
        """Terminate this task."""
        self._terminated = True
        if not self.promise.done():
            self.promise.set_result(error)
        self._cleanup()

    async def rerun(self) -> None:  # noqa: C901
        """Start polling."""
        runCount = self._runCount = self._runCount + 1
        success: Optional[JSHandle] = None
        error = None

        try:
            context = await self._frame.executionContext()
            if context is None:
                raise PageError('No execution context.')
            success = await context.evaluateHandle(
                waitForPredicatePageFunction,
                self._predicateBody,
                self._polling,
                self._timeout,
                *self._args,
            )
        except Exception as e:
            error = e

        if self.promise.done():
            return

        if self._terminated or runCount != self._runCount:
            if success:
                await success.dispose()
            return

        # Add try/except referring to puppeteer.
        try:
            if not error and success and (
                    await self._frame.evaluate('s => !s', success)):
                await success.dispose()
                return
        except NetworkError:
            if success is not None:
                await success.dispose()
            return

        # page is navigated and context is destroyed.
        # Try again in the new execution context.
        if (isinstance(error, NetworkError) and
                'Execution context was destroyed' in error.args[0]):
            return

        # Try again in the new execution context.
        if (isinstance(error, NetworkError) and
                'Cannot find context with specified id' in error.args[0]):
            return

        if error:
            self.promise.set_exception(error)
        else:
            self.promise.set_result(success)

        self._cleanup()

    def _cleanup(self) -> None:
        if self._timeout and not self._timeoutError:
            self._timeoutTimer.cancel()
        self._frame._waitTasks.remove(self)


waitForPredicatePageFunction = """
async function waitForPredicatePageFunction(predicateBody, polling, timeout, ...args) {
  const predicate = new Function('...args', predicateBody);
  let timedOut = false;
  if (timeout)
    setTimeout(() => timedOut = true, timeout);
  if (polling === 'raf')
    return await pollRaf();
  if (polling === 'mutation')
    return await pollMutation();
  if (typeof polling === 'number')
    return await pollInterval(polling);

  /**
   * @return {!Promise<*>}
   */
  function pollMutation() {
    const success = predicate.apply(null, args);
    if (success)
      return Promise.resolve(success);

    let fulfill;
    const result = new Promise(x => fulfill = x);
    const observer = new MutationObserver(mutations => {
      if (timedOut) {
        observer.disconnect();
        fulfill();
      }
      const success = predicate.apply(null, args);
      if (success) {
        observer.disconnect();
        fulfill(success);
      }
    });
    observer.observe(document, {
      childList: true,
      subtree: true,
      attributes: true
    });
    return result;
  }

  /**
   * @return {!Promise<*>}
   */
  function pollRaf() {
    let fulfill;
    const result = new Promise(x => fulfill = x);
    onRaf();
    return result;

    function onRaf() {
      if (timedOut) {
        fulfill();
        return;
      }
      const success = predicate.apply(null, args);
      if (success)
        fulfill(success);
      else
        requestAnimationFrame(onRaf);
    }
  }

  /**
   * @param {number} pollInterval
   * @return {!Promise<*>}
   */
  function pollInterval(pollInterval) {
    let fulfill;
    const result = new Promise(x => fulfill = x);
    onTimeout();
    return result;

    function onTimeout() {
      if (timedOut) {
        fulfill();
        return;
      }
      const success = predicate.apply(null, args);
      if (success)
        fulfill(success);
      else
        setTimeout(onTimeout, pollInterval);
    }
  }
}
"""  # noqa: E501
