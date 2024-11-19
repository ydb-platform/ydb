import matplotlib.pyplot as plt
import enum
import sys
import re
import time
import matplotlib.dates as mdates
import getopt, sys
from datetime import datetime
from matplotlib.patches import Rectangle
from matplotlib.collections import PatchCollection
from operator import attrgetter

LIMIT_ROWS = -1


# Return number of days since the epoch "1970-01-01T00:00:00" (default)
def data_to_num(val):
    try:
        return mdates.date2num(datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f"))
    except ValueError:
        return mdates.date2num(datetime.strptime(val, "%Y-%m-%d %H:%M:%S"))


def num_to_date(val) -> datetime:
    return datetime.fromtimestamp(mdates.num2date(val).timestamp())


def parse_primary_key(string: str):
    pos_first_digit = re.search(r"\d", string)
    if pos_first_digit == None:
        return None

    pos_last_digit = string.find(";")
    if pos_last_digit == -1:
        return None
    ts = string[pos_first_digit.start() : pos_last_digit]
    return data_to_num(ts)


def parse_primary_key_by_index(string: str, start_idx: int, end_idx: int):
    ts = string[start_idx:end_idx]
    return data_to_num(ts)


def parse_snapsot_plan_step(string: str) -> float:
    pos_first_digit = re.search(r"\d", string).start()
    if pos_first_digit == None:
        return None
    res = int(string[pos_first_digit:])
    return res


def init_limit():
    limit_x = [sys.maxsize, -sys.maxsize]
    limit_y = [sys.maxsize, -sys.maxsize]
    return limit_x, limit_y


def change_limit(limit_x, limit_y, primary_key_min, primary_key_max, snapshot_plan_step_min, snapshot_plan_step_max):
    limit_x[0] = min(limit_x[0], primary_key_min)
    limit_x[1] = max(limit_x[1], primary_key_max)
    limit_y[0] = min(limit_y[0], snapshot_plan_step_min)
    limit_y[1] = max(limit_y[1], snapshot_plan_step_max)
    return limit_x, limit_y


def create_rectangle(primary_key_min, primary_key_max, snapshot_plan_step_min, snapshot_plan_step_max) -> Rectangle:
    color = "black"
    line_width = 1
    linestyle = "dashed"
    if primary_key_max - primary_key_min == 0 or snapshot_plan_step_max - snapshot_plan_step_min == 0:
        color = "red"
        line_width = 2
        linestyle = "solid"
    return Rectangle(
        (primary_key_min, snapshot_plan_step_min),
        primary_key_max - primary_key_min,
        snapshot_plan_step_max - snapshot_plan_step_min,
        linestyle=linestyle,
        linewidth=line_width,
        edgecolor=color,
        fc='none',
    )


def parse_txt(
    path: str,
    save_result: bool,
    border_pk_min: float,
    border_snapshot_plan_step_min: int,
    border_pk_max: float,
    border_snapshot_plan_step_max: int,
) -> list[Rectangle]:
    result = []
    limit_x, limit_y = init_limit()

    count_lines = 0
    with open(path) as f:
        count_lines = sum(1 for _ in f)
    print(f"Parsing rectangles: {count_lines}")
    with open(path, 'r') as file:
        number_line = 0
        for line in file:
            if LIMIT_ROWS != -1 and number_line > LIMIT_ROWS:
                break
            primary_key_min = None
            primary_key_max = None
            snapshot_plan_step_min = None
            snapshot_plan_step_max = None

            parts = line.split(',')
            primary_key_min = parse_primary_key_by_index(parts[5], 22, 48)
            primary_key_max = parse_primary_key_by_index(parts[4], 34, 60)
            snapshot_plan_step_min = int(parts[6][32:])
            snapshot_plan_step_max = int(parts[8][32:])
            if primary_key_min == None:
                raise Exception("Can't read primary_key_min")
            if primary_key_max == None:
                raise Exception("Can't read primary_key_max")
            if snapshot_plan_step_min == None:
                raise Exception("Can't read snapshot_plan_step_min")
            if snapshot_plan_step_max == None:
                raise Exception("Can't read snapshot_plan_step_max")

            rect = create_rectangle(primary_key_min, primary_key_max, snapshot_plan_step_min, snapshot_plan_step_max)
            if check_border(
                rect, border_pk_min, border_snapshot_plan_step_min, border_pk_max, border_snapshot_plan_step_max
            ):
                limit_x, limit_y = change_limit(
                    limit_x, limit_y, primary_key_min, primary_key_max, snapshot_plan_step_min, snapshot_plan_step_max
                )
                result.append(rect)

            number_line += 1
            if number_line % (int(count_lines * 0.1)) == 0:
                print(f"remaining rectangles: {count_lines - number_line}")

    result = sorted(result, key=attrgetter("xy"))

    if save_result:
        with open(f"{path.split('.')[0]}_save_parsing.txt", 'w') as file:
            for rect in result:
                file.write(
                    f"{num_to_date(rect.get_x())};{rect.get_y()};{num_to_date(rect.get_x() + rect.get_width())};{rect.get_y() + rect.get_height()}\n"
                )
    return result, limit_x, limit_y


def read_result(
    path: str,
    border_pk_min: float,
    border_snapshot_plan_step_min: int,
    border_pk_max: float,
    border_snapshot_plan_step_max: int,
):
    result: list[Rectangle] = []
    limit_x, limit_y = init_limit()
    count_lines = 0

    with open(path) as f:
        count_lines = sum(1 for _ in f)
    print(f"Read rectangles: {count_lines}")

    with open(path, 'r') as file:
        number_line = 0
        for line in file:
            if LIMIT_ROWS != -1 and number_line > LIMIT_ROWS:
                break
            parts = line.split(';')
            primary_key_min = data_to_num(parts[0])
            snapshot_plan_step_min = int(parts[1])
            primary_key_max = data_to_num(parts[2])
            snapshot_plan_step_max = int(parts[3])
            rect = create_rectangle(primary_key_min, primary_key_max, snapshot_plan_step_min, snapshot_plan_step_max)
            if check_border(
                rect, border_pk_min, border_snapshot_plan_step_min, border_pk_max, border_snapshot_plan_step_max
            ):
                limit_x, limit_y = change_limit(
                    limit_x, limit_y, primary_key_min, primary_key_max, snapshot_plan_step_min, snapshot_plan_step_max
                )
                result.append(rect)
            number_line += 1
            if number_line % int(count_lines * 0.1) == 0:
                print(f"remaining rectangles: {count_lines - number_line}")

    return result, limit_x, limit_y


def check_border(
    rectangle: Rectangle, pk_min: float, snapshot_plan_step_min: int, pk_max: float, snapshot_plan_step_max: int
) -> bool:
    if pk_min == None and pk_max == None and snapshot_plan_step_min == None and snapshot_plan_step_max == None:
        return True
    if pk_min != None and rectangle.get_x() < pk_min:
        return False
    if snapshot_plan_step_min != None and rectangle.get_y() < snapshot_plan_step_min:
        return False
    if pk_max != None and rectangle.get_x() + rectangle.get_width() > pk_max:
        return False
    if snapshot_plan_step_max != None and rectangle.get_y() + rectangle.get_height() > pk_max:
        return False
    return True


def process_border(
    rectangles: list[Rectangle], pk_min: float, snapshot_plan_step_min: int, pk_max: float, snapshot_plan_step_max: int
):
    if pk_min == None and pk_max == None and snapshot_plan_step_min == None and snapshot_plan_step_max == None:
        return rectangles
    result = []
    limit_x, limit_y = init_limit()

    for rect in rectangles:
        if pk_min != None and rect.get_x() < pk_min:
            continue
        if pk_max != None and rect.get_x() + rect.get_width() > pk_max:
            continue
        if snapshot_plan_step_min != None and rect.get_y() < snapshot_plan_step_min:
            continue
        if snapshot_plan_step_max != None and rect.get_y() + rect.get_height() > snapshot_plan_step_max:
            continue
        limit_x, limit_y = change_limit(
            limit_x,
            limit_y,
            rect.get_x(),
            rect.get_x() + rect.get_width(),
            rect.get_y(),
            rect.get_y() + rect.get_height(),
        )
        result.append(rect)

    return result, limit_x, limit_y


class Interval:
    def __init__(self, start: float, end: float, count_intersect: int):
        self.start = start
        self.end = end
        self.count_intersect = count_intersect

    def get_length(self) -> float:
        return self.end - self.start


class Border(enum.Enum):
    START = 0
    END = 1


class PointInterval:
    def __init__(self, coord: float, border: Border, num_rectangle: int):
        self.coord = coord
        self.border = border
        self.num_rectangle = num_rectangle

    def __eq__(self, other):
        return self.coord == other.coord and self.border == other.border

    def __lt__(self, other):
        if self.coord == other.coord:
            return self.border.value < other.border.value
        return self.coord < other.coord


def binary_search(rectangles: list[Rectangle], x, getter_func) -> int:
    low = 0
    high = len(rectangles) - 1
    mid = 0
    while low <= high:
        mid = low + (high - low) // 2
        if getter_func(rectangles[mid]) < x:
            low = mid + 1
        elif getter_func(rectangles[mid]) > x:
            high = mid - 1
        else:
            return mid
    return -1


def max_intersect_interval_by_x(rectangles: list[Rectangle]) -> Interval:
    x_coord: list[PointInterval] = []
    num_rectangle = 0
    for rect in rectangles.copy():
        x_coord.append(PointInterval(rect.get_x(), Border.START, num_rectangle))
        x_coord.append(PointInterval(rect.get_x() + rect.get_width(), Border.END, num_rectangle))
        num_rectangle += 1
    x_coord = sorted(x_coord)
    max_intersect_interval = Interval(-1, -1, -1)
    current_intersect = 0
    for point in x_coord:
        if point.border == Border.START:
            current_intersect += 1
        elif point.border == Border.END:
            if max_intersect_interval.count_intersect < current_intersect:
                max_intersect_interval.count_intersect = current_intersect
                max_intersect_interval.start = rectangles[point.num_rectangle].get_x()
                max_intersect_interval.end = max_intersect_interval.start + rectangles[point.num_rectangle].get_width()
            current_intersect -= 1
    return max_intersect_interval


argumentList = sys.argv[1:]
options = "hmo:"
long_options = [
    "parsing_file=",
    "read_from_save=",
    "pk_min=",
    "pk_max=",
    "snapshot_plan_step_min=",
    "snapshot_plan_step_max=",
    "max_intersect",
    "drawing_step=",
    "limit_rows=",
]

# example run: python3 intersect_portions.py --parsing_file=example.txt

# args:
# parsing_file - path to file with portion's logs (after run, file is saved with name *_save_parsing.txt for next read with --read_from_save)
# read_from_save - path to file for read portion's logs after first run with --parsing_file
# pk_min - left border PK for drawing
# pk_max - right border PK for drawing
# snapshot_plan_step_min - down border PK for drawing
# snapshot_plan_step_max - up border PK for drawing
# max_intersect - finds area with maximum intersection and moves to it.
# drawing_step - step to select rectangles
# limit_rows - read first limit_rows rows from file


if __name__ == '__main__':
    arguments, values = getopt.getopt(argumentList, options, long_options)
    parsing_file = str()
    read_from_save = str()
    border_pk_min = None
    border_pk_max = None
    border_snapshot_plan_step_min = None
    border_snapshot_plan_step_max = None
    max_intersect = False
    drawing_step = 1
    for current_argument, current_value in arguments:
        if current_argument in ("--parsing_file"):
            parsing_file = current_value
        elif current_argument in ("--read_from_save"):
            read_from_save = current_value
        elif current_argument in ("--pk_min"):
            border_pk_min = datetime.datetime.strptime(current_value, "%Y-%m-%d %H:%M:%S.%f")
            print(f"pk_min: {border_pk_min}")
        elif current_argument in ("--pk_max"):
            border_pk_max = datetime.datetime.strptime(current_value, "%Y-%m-%d %H:%M:%S.%f")
        elif current_argument in ("--snapshot_plan_step_min"):
            border_snapshot_plan_step_min = int(current_value)
        elif current_argument in ("--snapshot_plan_step_max"):
            border_snapshot_plan_step_max = int(current_value)
        elif current_argument in ("--max_intersect"):
            max_intersect = True
        elif current_argument in ("--drawing_step"):
            drawing_step = int(current_value)
        elif current_argument in ("--limit_rows"):
            LIMIT_ROWS = int(current_value)

    rectangles: list[Rectangle] = []
    limit_x, limit_y = init_limit()
    if len(read_from_save) == 0:
        start = time.time()
        rectangles, limit_x, limit_y = parse_txt(
            parsing_file,
            True,
            border_pk_min,
            border_snapshot_plan_step_min,
            border_pk_max,
            border_snapshot_plan_step_max,
        )
        print(f"parsing time: {(time.time() - start):.2f} seconds")
    else:
        start = time.time()
        rectangles, limit_x, limit_y = read_result(
            read_from_save, border_pk_min, border_snapshot_plan_step_min, border_pk_max, border_snapshot_plan_step_max
        )
        print(f"read time: {(time.time() - start):.2f} seconds")

    if drawing_step != 1:
        rectangles = rectangles[:-1:drawing_step]

    if max_intersect:
        max_intersect_interval: Interval = max_intersect_interval_by_x(rectangles)
        left_rect_id = binary_search(rectangles, max_intersect_interval.start, lambda rect: rect.get_x())
        limit_x, limit_y = init_limit()
        limit_x, limit_y = change_limit(
            limit_x,
            limit_y,
            max_intersect_interval.start,
            max_intersect_interval.end,
            rectangles[left_rect_id].get_y(),
            rectangles[left_rect_id].get_y() + rectangles[left_rect_id].get_height(),
        )
        intersect_rectangles: list[Rectangle] = []
        for i in range(left_rect_id, len(rectangles)):
            if (
                rectangles[i].get_x() >= max_intersect_interval.start
                and rectangles[i].get_x() <= max_intersect_interval.end
            ):
                intersect_rectangles.append(rectangles[i])
                limit_x, limit_y = change_limit(
                    limit_x,
                    limit_y,
                    intersect_rectangles[-1].get_x(),
                    intersect_rectangles[-1].get_x() + intersect_rectangles[-1].get_width(),
                    intersect_rectangles[-1].get_y(),
                    intersect_rectangles[-1].get_y() + intersect_rectangles[-1].get_height(),
                )
        print(
            f"max_intersect_interval: ({mdates.num2date(max_intersect_interval.start)}, {mdates.num2date(max_intersect_interval.end)}), max count intersect: {max_intersect_interval.count_intersect}"
        )

    print(f"count drawn rectangles: {len(rectangles)}")

    fig, ax = plt.subplots()
    ax.set_title("intersect portions")
    ax.add_collection(PatchCollection(rectangles, match_original=True))

    plt.xlabel("primary key (DateTime)")
    plt.ylabel("snapshot plan step")
    plt.title(f"intersect portions")

    locator = mdates.AutoDateLocator()
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S.%f\n'))
    ax.yaxis.get_major_formatter().set_useOffset(False)
    ax.yaxis.get_major_formatter().set_scientific(False)

    plt.xlim(limit_x[0], limit_x[1])
    plt.ylim(limit_y[0], limit_y[1])
    fig.autofmt_xdate()
    plt.show()
