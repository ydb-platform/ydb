
class OpenTelemetryTraceContext(object):
    traceparent_tpl = 'xx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-xxxxxxxxxxxxxxxx-xx'
    translation = str.maketrans('1234567890abcdef', 'xxxxxxxxxxxxxxxx')

    def __init__(self, traceparent, tracestate):
        # xx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-xxxxxxxxxxxxxxxx-xx
        # ^              ^                           ^         ^
        # version     trace_id                    span_id      flags

        self.trace_id = None  # UUID
        self.span_id = None  # UInt64
        self.tracestate = tracestate  # String
        self.trace_flags = None  # UInt8

        if traceparent is not None:
            self.parse_traceparent(traceparent)

        super(OpenTelemetryTraceContext, self).__init__()

    def parse_traceparent(self, traceparent):
        traceparent = traceparent.lower()

        if len(traceparent) != len(self.traceparent_tpl):
            raise ValueError('unexpected length {}, expected {}'.format(
                len(traceparent), len(self.traceparent_tpl)
            ))

        if traceparent.translate(self.translation) != self.traceparent_tpl:
            raise ValueError(
                'Malformed traceparant header: {}'.format(traceparent)
            )

        parts = traceparent.split('-')
        version = int(parts[0], 16)
        if version != 0:
            raise ValueError(
                'unexpected version {}, expected 00'.format(parts[0])
            )

        self.trace_id = (int(parts[1][16:], 16) << 64) + int(parts[1][:16], 16)
        self.span_id = int(parts[2], 16)
        self.trace_flags = int(parts[3], 16)
