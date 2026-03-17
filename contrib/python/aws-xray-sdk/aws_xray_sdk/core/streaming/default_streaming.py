import threading


class DefaultStreaming:
    """
    The default streaming strategy. It uses the total count of a
    segment's children subsegments as a threshold. If the threshold is
    breached, it uses subtree streaming to stream out.
    """
    def __init__(self, streaming_threshold=30):
        self._threshold = streaming_threshold
        self._lock = threading.Lock()

    def is_eligible(self, segment):
        """
        A segment is eligible to have its children subsegments streamed
        if it is sampled and it breaches streaming threshold.
        """
        if not segment or not segment.sampled:
            return False

        return segment.get_total_subsegments_size() > self.streaming_threshold

    def stream(self, entity, callback):
        """
        Stream out all eligible children of the input entity.

        :param entity: The target entity to be streamed.
        :param callback: The function that takes the node and
            actually send it out.
        """
        with self._lock:
            self._stream(entity, callback)

    def _stream(self, entity, callback):
        children = entity.subsegments

        children_ready = []
        if len(children) > 0:
            for child in children:
                if self._stream(child, callback):
                    children_ready.append(child)

        # If all children subtrees and this root are ready, don't stream yet.
        # Mark this root ready and return to parent.
        if len(children_ready) == len(children) and not entity.in_progress:
            return True

        # Otherwise stream all ready children subtrees and return False
        for child in children_ready:
            callback(child)
            entity.remove_subsegment(child)

        return False

    @property
    def streaming_threshold(self):
        return self._threshold

    @streaming_threshold.setter
    def streaming_threshold(self, value):
        self._threshold = value
