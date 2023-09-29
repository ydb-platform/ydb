class FirstMatchRegistry(object):
    def __init__(self):
        self._responses = []

    @property
    def registered(self):
        return self._responses

    def reset(self):
        self._responses = []

    def find(self, request):
        found = None
        found_match = None
        match_failed_reasons = []
        for i, response in enumerate(self.registered):
            match_result, reason = response.matches(request)
            if match_result:
                if found is None:
                    found = i
                    found_match = response
                else:
                    if self.registered[found].call_count > 0:
                        # that assumes that some responses were added between calls
                        self.registered.pop(found)
                        found_match = response
                        break
                    # Multiple matches found.  Remove & return the first response.
                    return self.registered.pop(found), match_failed_reasons
            else:
                match_failed_reasons.append(reason)
        return found_match, match_failed_reasons

    def add(self, response):
        self.registered.append(response)

    def remove(self, response):
        while response in self.registered:
            self.registered.remove(response)

    def replace(self, response):
        try:
            index = self.registered.index(response)
        except ValueError:
            raise ValueError(
                "Response is not registered for URL {}".format(response.url)
            )
        self.registered[index] = response
