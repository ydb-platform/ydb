import matplotlib.pyplot as plt
import enum
import sys
import argparse
from datetime import datetime, timezone
from matplotlib import dates, patches, collections
from importlib import metadata
from packaging import version

import json


# TODO read from a table scheme
class FirstPkColumnType(enum.Enum):
    Integer = 0
    Timestamp = 1


class Portion:
    def __init__(self, pk0type: FirstPkColumnType):
        self.Pk0Type = pk0type
        self.PlanStepMin = None
        self.PlanStepMax = None
        self.PkMin = None
        self.PkMax = None
        self.CompactionLevel = None

    @staticmethod
    def GetPlanStep(detailsSnapshot) -> datetime:
        planStepMilliSeconds = detailsSnapshot["plan_step"]
        return datetime.fromtimestamp(planStepMilliSeconds / 1000.0, tz=timezone.utc)

    @staticmethod
    def GetPk(detailsPk: str, pk0Type: FirstPkColumnType):
        # Use only the 1st PK column currently
        pk0 = detailsPk.split(',')[0]
        if (pk0Type == FirstPkColumnType.Integer):
            return int(pk0)
        if (pk0Type == FirstPkColumnType.Timestamp):
            return datetime.fromtimestamp(int(pk0) / 1000000.0, tz=timezone.utc)
        raise Exception(f"Unsupported PK column type: {str(pk0Type)}")

    @staticmethod
    def FromJson(portionDoc, pk0type: FirstPkColumnType):
        parsedDetails = json.loads(portionDoc["Details"])
        portion = Portion(pk0type)
        portion.PlanStepMin = Portion.GetPlanStep(parsedDetails["snapshot_min"])
        portion.PlanStepMax = Portion.GetPlanStep(parsedDetails["snapshot_max"])
        portion.PkMin = Portion.GetPk(parsedDetails["primary_key_min"], pk0type)
        portion.PkMax = Portion.GetPk(parsedDetails["primary_key_max"], pk0type)
        portion.CompactionLevel = portionDoc["CompactionLevel"]
        return portion

    def __repr__(self):
        return f"pk:[{self.PkMin}..{self.PkMax}], plan_step:[{self.PlanStepMin}..{self.PlanStepMax}]"

    def ToRectangle(self, levelColors) -> patches.Rectangle:
        if self.Pk0Type == FirstPkColumnType.Integer:
            x0 = self.PkMin
            dx = self.PkMax - self.PkMin
        elif self.Pk0Type == FirstPkColumnType.Timestamp:
            x0 = dates.date2num(self.PkMin)
            dx = dates.date2num(self.PkMax) - dates.date2num(self.PkMin)
        else:
            raise Exception("Unsupported PK column type")
        y0 = dates.date2num(self.PlanStepMin)
        dy = dates.date2num(self.PlanStepMax) - dates.date2num(self.PlanStepMin)
        return patches.Rectangle(
            (x0, y0),
            dx,
            dy,
            linestyle="dashed",
            linewidth=1,
            edgecolor=levelColors[self.CompactionLevel] if self.CompactionLevel == 0 else "black",
            fc=levelColors[self.CompactionLevel]
        )


class Portions:
    def __init__(self, pk0Type: FirstPkColumnType):
        self.Portions = []
        if pk0Type == FirstPkColumnType.Timestamp:
            self.PkMin = datetime.fromtimestamp(253402300799, tz=timezone.utc)
            self.PkMax = datetime.fromtimestamp(0, tz=timezone.utc)
        else:
            self.PkMin = sys.maxsize
            self.PkMax = -sys.maxsize
        self.PlanStepMin = datetime.fromtimestamp(253402300799, tz=timezone.utc)
        self.PlanStepMax = datetime.fromtimestamp(0, tz=timezone.utc)
        self.MaxCompactionLevel = 0

    def Add(self, portion: Portion):
        self.Portions.append(portion)
        self.PkMin = min(self.PkMin, portion.PkMin)
        self.PkMax = max(self.PkMax, portion.PkMax)
        self.PlanStepMin = min(self.PlanStepMin, portion.PlanStepMin)
        self.PlanStepMax = max(self.PlanStepMax, portion.PlanStepMax)
        self.MaxCompactionLevel = max(self.MaxCompactionLevel, portion.CompactionLevel)


def ParsePortionStatFile(path: str, pk0type: FirstPkColumnType) -> Portions:
    portions = Portions(pk0type)
    with open(path, 'r') as file:
        for line in file:
            doc = json.loads(line)
            portions.Add(Portion.FromJson(doc, pk0type))
    return portions


def GetLevelColours(maxLevel):
    levelColors = {level: 'blue' for level in range(portions.MaxCompactionLevel + 1)}
    levelColors[maxLevel] = 'green'
    levelColors[0] = 'red'
    return levelColors


def GenerateIntersectionData(ranges):
    points = []
    for minPk, maxPk in ranges:
        points.append((minPk, 1))
        points.append((maxPk, -1))

    points.sort(key=lambda p: (p[0], -p[1]))

    cur = points[0][1]
    prevPk = points[0][0]
    prevClosed = False
    lastUsedPk = prevPk
    for (pk, delta) in points[1:]:
        if pk != prevPk:
            if cur > 1:
                yield (lastUsedPk, pk - lastUsedPk, cur - 1)
            lastUsedPk = pk
        elif delta < 0 and not prevClosed:
            lastUsedPk = pk
            if isinstance(lastUsedPk, int):
                lastUsedPk += 1
            if cur > 1:
                yield (prevPk, lastUsedPk - prevPk, cur - 1)
        cur += delta
        prevPk = pk
        prevClosed = delta < 0


assert list(GenerateIntersectionData([(1, 10), (3, 3), (3, 8), (9, 15), (20, 25), (23, 30)])) == [(3, 1, 2), (4, 4, 1), (9, 1, 1), (23, 2, 1)]


def GetIntersections(portions):
    intersections = []
    maxIntersections = 0
    for pk, width, n in GenerateIntersectionData((p.PkMin, p.PkMax) for p in portions):
        r = patches.Rectangle(
            (pk, 0),
            width, n,
            linestyle="dashed",
            linewidth=1,
        )
        intersections.append(r)
        maxIntersections = max(maxIntersections, n)
    return intersections, maxIntersections


def get_interactive_backends():
    print(f"matplotlib version: {metadata.version("matplotlib")}")
    if version.Version(metadata.version("matplotlib")) < version.Version("3.9"):
        from matplotlib import rcsetup
        return rcsetup.interactive_bk
    else:
        from matplotlib import backends
        return backends.backend_registry.list_builtin(backends.BackendFilter.INTERACTIVE)


if __name__ == '__main__':
    parser = argparse.ArgumentParser("""Visualize portions from YDB Column table.
To get portion info for a table, use ydb cli:
    ydb -e grpc://<node>:2135 -d <path_to_db> sql -s 'select * from `<table>/.sys/primary_index_portion_stats` where TabletId==<id>' --format json-unicode
                                     """)
    parser.add_argument("-i", "--input-file", help="File with primary_index_portion_stats data in json per row format", required=True)
    parser.add_argument("-t", "--type", help="First PK column type: Integer or Timestamp", required=True)
    parser.add_argument("-o", "--output-file", help="Output file", required=False)
    args = parser.parse_args()
    inputFile = args.input_file
    pk0Type = FirstPkColumnType[args.type]

    if args.output_file is None:
        if plt.get_backend() not in get_interactive_backends():
            print("""No interactive rendering backend is available. Only output to file mode can be used(--output-file).
Or you can run this script in some environment with installed interactive backend, i.e venv
""")
            sys.exit(1)

    print(f"Loading file: {inputFile}...")
    portions = ParsePortionStatFile(inputFile, pk0Type)
    print(f"Loading file: {inputFile}... completed, {len(portions.Portions)} portions")

    levelColors = GetLevelColours(portions.MaxCompactionLevel)
    rectangles = [p.ToRectangle(levelColors) for p in portions.Portions]

    fig, ax = plt.subplots(nrows=2, ncols=1)
    for i in [0, 1]:
        ax[i].set_xlabel("pk")
        if pk0Type == FirstPkColumnType.Timestamp:
            ax[i].xaxis.set_major_locator(dates.AutoDateLocator())
            ax[i].xaxis.set_major_formatter(dates.DateFormatter('%Y-%m-%d'))
            xMin = dates.date2num(portions.PkMin)
            xMax = dates.date2num(portions.PkMax)
        elif pk0Type == FirstPkColumnType.Integer:
            xMin = portions.PkMin
            xMax = portions.PkMax
        else:
            raise Exception(f"Unsupported PK column type: {str(pk0Type)}")
        dx = xMax - xMin
        ax[i].set_xlim(xMin - 0.05 * dx, xMax + 0.05 * dx)

    ax[0].set_title("Column table portions")
    ax[0].add_collection(collections.PatchCollection(rectangles, match_original=True))

    ax[0].set_ylabel("plan_step")
    ax[0].yaxis.set_major_locator(dates.AutoDateLocator())
    ax[0].yaxis.set_major_formatter(dates.DateFormatter('%Y-%m-%d %H:%M:%S\n'))
    yMin = dates.date2num(portions.PlanStepMin)
    yMax = dates.date2num(portions.PlanStepMax)
    dy = yMax - yMin
    ax[0].set_ylim(yMin - 0.05 * dy, yMax + 0.05 * dy)

    intersections, maxIntersections = GetIntersections(portions.Portions)
    ax[1].set_title("Portion intersections")
    ax[1].add_collection(collections.PatchCollection(intersections, match_original=True))

    ax[1].set_ylabel("intersection")
    ax[1].set_ylim(0, maxIntersections * 1.1 + 1)
    plt.tight_layout()

    if args.output_file is None:
        plt.show()
    else:
        dpi = 300
        plt.savefig(args.output_file, dpi=dpi)
