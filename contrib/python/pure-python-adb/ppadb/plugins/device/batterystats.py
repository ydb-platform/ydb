from ppadb.plugins.device import batterystats_section as section_module
from ppadb.plugins import Plugin
from ppadb.utils.logger import AdbLogging

logger = AdbLogging.get_logger(__name__)


class BatteryStats(Plugin):
    def get_battery_level(self):
        battery = self.shell("dumpsys battery")

        for line in battery.split('\n'):
            tokens = line.split(":")
            if tokens[0].strip() == "level" and len(tokens) == 2:
                return int(tokens[1])

        return None


    def get_batterystats(self):
        result = self.shell("dumpsys batterystats -c")
        sections = {}
        for line in result.split("\n"):
            if not line.strip():
                continue

            tokens = line.split(",", 4)
            if len(tokens) < 5:
                continue

            dummy, uid, mode, id, remaining_fields = tokens
            print(dummy, uid, mode, id, remaining_fields)
            SectionClass = section_module.get_section(id)
            if not SectionClass:
                logger.error("Unknown section {} in batterystats".format(id))
                continue

            if id not in sections:
                sections[id] = []

            sections[id].append(SectionClass(*remaining_fields.split(",")))

        return sections
