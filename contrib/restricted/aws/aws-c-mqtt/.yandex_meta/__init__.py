from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.after(
            "CFLAGS",
            Switch(
                OS_WINDOWS=Linkable(
                    CFLAGS=["-DAWS_MQTT_EXPORTS"],
                ),
            ),
        )


aws_c_mqtt = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-mqtt",
    nixattr="aws-c-mqtt",
    post_install=post_install,
)
