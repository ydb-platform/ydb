class ExecutedOutsideContext(Exception):
    """
    Exception to be raised if a fetcher was called outside its context
    """
    pass


class MultiContextRequestIdFetcher(object):
    """
    A callable that can fetch request id from different context as Flask, Celery etc.
    """

    def __init__(self):
        """
        Initialize
        """
        self.ctx_fetchers = []

    def __call__(self):

        for ctx_fetcher in self.ctx_fetchers:
            try:
                return ctx_fetcher()
            except ExecutedOutsideContext:
                continue
        return None

    def register_fetcher(self, ctx_fetcher):
        """
        Register another context-specialized fetcher
        :param Callable ctx_fetcher: A callable that will return the id or raise ExecutedOutsideContext if it was
         executed outside its context
        """
        if ctx_fetcher not in self.ctx_fetchers:
            self.ctx_fetchers.append(ctx_fetcher)
