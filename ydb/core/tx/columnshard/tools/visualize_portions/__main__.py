import matplotlib.pyplot as plt
import enum
import sys
import matplotlib.dates as mdates
import argparse
from datetime import datetime, timezone
from matplotlib.patches import Rectangle
from matplotlib.collections import PatchCollection
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

    def ToRectangle(self, levelColors) -> Rectangle:
        if self.Pk0Type == FirstPkColumnType.Integer:
            x0 = self.PkMin
            dx = self.PkMax - self.PkMin
        elif self.Pk0Type == FirstPkColumnType.Timestamp:
            x0 = mdates.date2num(self.PkMin)
            dx = mdates.date2num(self.PkMax) - mdates.date2num(self.PkMin)
        else:
            raise Exception("Unsupported PK column type")
        y0 = mdates.date2num(self.PlanStepMin)
        dy = mdates.date2num(self.PlanStepMax) - mdates.date2num(self.PlanStepMin)
        return Rectangle(
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
    levelColors = {l: 'blue' for l in range(portions.MaxCompactionLevel + 1)}
    levelColors[maxLevel] = 'green'
    levelColors[0] = 'red'
    return levelColors


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
    print(f"Loading file: {inputFile}...")
    portions = ParsePortionStatFile(inputFile, pk0Type)
    print(f"Loading file: {inputFile}... completed, {len(portions.Portions)} portions")

    levelColors = GetLevelColours(portions.MaxCompactionLevel)
    rectangles = [p.ToRectangle(levelColors) for p in portions.Portions]

    dpi = 300
    figsize = (12000 / dpi, 9000 / dpi)
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    ax.set_title("Column table portions")
    ax.add_collection(PatchCollection(rectangles, match_original=True))

    ax.set_xlabel("pk")
    if pk0Type == FirstPkColumnType.Timestamp:
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        xMin = mdates.date2num(portions.PkMin)
        xMax = mdates.date2num(portions.PkMax)
    elif pk0Type == FirstPkColumnType.Integer:
        xMin = portions.PkMin
        xMax = portions.PkMax
    else:
        raise Exception(f"Unsupported PK column type: {str(pk0Type)}")
    dx = xMax - xMin
    ax.set_xlim(xMin - 0.05 * dx, xMax + 0.05 * dx)

    ax.set_ylabel("plan_step")
    ax.yaxis.set_major_locator(mdates.AutoDateLocator())
    ax.yaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S\n'))
    yMin = mdates.date2num(portions.PlanStepMin)
    yMax = mdates.date2num(portions.PlanStepMax)
    dy = yMax - yMin
    ax.set_ylim(yMin - 0.05 * dy, yMax + 0.05 * dy)

    if args.output_file is None:
        plt.show()
    else:
        plt.savefig(args.output_file, dpi=dpi)
