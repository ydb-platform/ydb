from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    m = self.yamakes["."]

    m.after(
        "CFLAGS",
        Switch(
            OS_WINDOWS=Linkable(
                CFLAGS=["-DAWS_EVENT_STREAM_EXPORTS"],
            ),
        ),
    )


aws_c_event_stream = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-event-stream",
    nixattr="aws-c-event-stream",
    owners=["g:cpp-contrib"],
    ignore_targets=[
        "aws-c-event-stream-tests",
        "aws-c-event-stream-pipe",
        "aws-c-event-stream-write-test-case",
    ],
    post_install=post_install,
)
