import matplotlib.pyplot as plt
import enum
import sys
import re
import time
import datetime
import getopt, sys
from matplotlib.patches import Rectangle
from matplotlib.collections import PatchCollection
from operator import attrgetter

# LIMIT_ROWS = 5000


def parse_primary_key(string: str) -> float:
    pos_first_digit = re.search(r"\d", string)
    if pos_first_digit == None:
        return None

    pos_last_digit = string.find(";")
    if pos_last_digit == -1:
        return None
    ts = string[pos_first_digit.start() : pos_last_digit]
    res = time.mktime(datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f").timetuple())
    return res


def parse_primary_key_by_index(string: str, start_idx: int, end_idx: int) -> float:
    ts = string[start_idx:end_idx]
    res = time.mktime(datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f").timetuple())
    return res


def parse_snapsot_plan_step(string: str) -> float:
    pos_first_digit = re.search(r"\d", string).start()
    if pos_first_digit == None:
        return None
    res = int(string[pos_first_digit:])
    return res


def change_limit(limit_x, limit_y, primary_key_min, primary_key_max, snapshot_plan_step_min, snapshot_plan_step_max):
    limit_x[0] = min(limit_x[0], primary_key_min)
    limit_x[1] = max(limit_x[1], primary_key_max)
    limit_y[0] = min(limit_y[0], snapshot_plan_step_min)
    limit_y[1] = max(limit_y[1], snapshot_plan_step_max)
    return limit_x, limit_y


def parse_txt(
    path: str,
    save_result: bool,
    border_pk_min: float,
    border_snapshot_plan_step_min: int,
    border_pk_max: float,
    border_snapshot_plan_step_max: int,
) -> list[Rectangle]:
    result = []
    limit_x = [sys.maxsize, -sys.maxsize]
    limit_y = [sys.maxsize, -sys.maxsize]

    count_lines = 0
    with open(path) as f:
        count_lines = sum(1 for _ in f)
    print(f"total rectangles: {count_lines}")
    with open(path, 'r') as file:
        number_line = 0
        for line in file:
            # if (number_line > LIMIT_ROWS):
            #     break
            primary_key_min = None
            primary_key_max = None
            snapshot_plan_step_min = None
            snapshot_plan_step_max = None

            parts = line.split(',')
            # primary_key_min = parse_primary_key(parts[5])
            # primary_key_max = parse_primary_key(parts[4])
            # snapshot_plan_step_min = parse_snapsot(parts[6])
            # snapshot_plan_step_max = parse_snapsot(parts[8])
            primary_key_min = parse_primary_key_by_index(parts[5], 22, 48)
            primary_key_max = parse_primary_key_by_index(parts[4], 34, 60)
            snapshot_plan_step_min = int(parts[6][32:])
            snapshot_plan_step_max = int(parts[8][32:])
            if primary_key_min == None:
                raise Exception("Can't parse primary_key_min")
            if primary_key_max == None:
                raise Exception("Can't parse primary_key_max")
            if snapshot_plan_step_min == None:
                raise Exception("Can't parse snapshot_plan_step_min")
            if snapshot_plan_step_max == None:
                raise Exception("Can't parse snapshot_plan_step_max")

            # print(f'({primary_key_min},{snapshot_plan_step_min}) w = {primary_key_max - primary_key_min}, h = {snapshot_plan_step_max - snapshot_plan_step_min}')
            rect = Rectangle(
                (primary_key_min, snapshot_plan_step_min),
                primary_key_max - primary_key_min,
                snapshot_plan_step_max - snapshot_plan_step_min,
                color="black",
                fc='none',
            )
            if check_border(
                rect, border_pk_min, border_snapshot_plan_step_min, border_pk_max, border_snapshot_plan_step_max
            ):
                limit_x, limit_y = change_limit(
                    limit_x, limit_y, primary_key_min, primary_key_max, snapshot_plan_step_min, snapshot_plan_step_max
                )
                result.append(rect)

            number_line += 1
            if number_line % 1000000 == 0:
                print(f"remaining lines: {count_lines - number_line}")

    result = sorted(result, key=attrgetter("xy"))

    if save_result:
        with open(f"{path.split('.')[0]}_save_parsing.txt", 'w') as file:
            for rect in result:
                file.write(
                    f"{rect.get_x()};{rect.get_y()};{rect.get_x() + rect.get_width()};{rect.get_y() + rect.get_height()}\n"
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
    limit_x = [sys.maxsize, -sys.maxsize]
    limit_y = [sys.maxsize, -sys.maxsize]
    with open(path, 'r') as file:
        number_line = 0
        for line in file:
            # if (number_line > LIMIT_ROWS):
            #     break
            parts = line.split(';')
            primary_key_min = float(parts[0])
            snapshot_plan_step_min = int(parts[1])
            primary_key_max = float(parts[2])
            snapshot_plan_step_max = int(parts[3])

            # print(f'({primary_key_min},{snapshot_plan_step_min}) w = {primary_key_max - primary_key_min}, h = {snapshot_plan_step_max - snapshot_plan_step_min}')
            rect = Rectangle(
                (primary_key_min, snapshot_plan_step_min),
                primary_key_max - primary_key_min,
                snapshot_plan_step_max - snapshot_plan_step_min,
                color="black",
                fc='none',
            )
            if check_border(
                rect, border_pk_min, border_snapshot_plan_step_min, border_pk_max, border_snapshot_plan_step_max
            ):
                limit_x, limit_y = change_limit(
                    limit_x, limit_y, primary_key_min, primary_key_max, snapshot_plan_step_min, snapshot_plan_step_max
                )
                result.append(rect)
            number_line += 1

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
    limit_x = [sys.maxsize, -sys.maxsize]
    limit_y = [sys.maxsize, -sys.maxsize]

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


class Border(enum.Enum):
    START = 0
    END = 1


class PointInterval:
    def __init__(self, coord: float, border: Border, num_rectangle: int):
        self.coord = coord
        self.border = border
        self.num_rectangle = num_rectangle


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
    for rect in rectangles:
        x_coord.append(PointInterval(rect.get_x(), Border.START, num_rectangle))
        x_coord.append(PointInterval(rect.get_x() + rect.get_width(), Border.END, num_rectangle))
        num_rectangle += 1
    x_coord = sorted(x_coord, key=attrgetter('coord'))
    max_interval = Interval(0, 0, 0)
    current_intersect = 0
    for point in x_coord:
        if point.border == Border.START:
            current_intersect += 1
        elif point.border == Border.END:
            current_intersect -= 1
            if max_interval.count_intersect < current_intersect:
                print(point.num_rectangle)
                max_interval.count_intersect = current_intersect
                max_interval.start = rectangles[point.num_rectangle].get_x()
                max_interval.end = max_interval.start + rectangles[point.num_rectangle].get_width()
    return max_interval


argumentList = sys.argv[1:]
options = "hmo:"
long_options = [
    "parsing_file=",
    "result_parsing=",
    "pk_min=",
    "pk_max=",
    "snapshot_plan_step_min=",
    "snapshot_plan_step_max=",
    "max_intersect",
]

if __name__ == '__main__':
    arguments, values = getopt.getopt(argumentList, options, long_options)
    parsing_file = str()
    result_parsing = str()
    border_pk_min = None
    border_pk_max = None
    border_snapshot_plan_step_min = None
    border_snapshot_plan_step_max = None
    max_intersect = False
    for currentArgument, currentValue in arguments:
        if currentArgument in ("--parsing_file"):
            parsing_file = currentValue
        elif currentArgument in ("--result_parsing"):
            result_parsing = currentValue
        elif currentArgument in ("--pk_min"):
            border_pk_min = time.mktime(datetime.datetime.strptime(currentValue, "%Y-%m-%d %H:%M:%S.%f").timetuple())
            print(f"pk_min: {border_pk_min}")
        elif currentArgument in ("--pk_max"):
            border_pk_max = time.mktime(datetime.datetime.strptime(currentValue, "%Y-%m-%d %H:%M:%S.%f").timetuple())
        elif currentArgument in ("--snapshot_plan_step_min"):
            border_snapshot_plan_step_min = int(currentValue)
        elif currentArgument in ("--snapshot_plan_step_max"):
            border_snapshot_plan_step_max = int(currentValue)
        elif currentArgument in ("--max_intersect"):
            max_intersect = True

    # rectangles, limit_x, limit_y = parse_json('example.json')
    rectangles = None
    limit_x = None
    limit_y = None
    if len(result_parsing) == 0:
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
            result_parsing, border_pk_min, border_snapshot_plan_step_min, border_pk_max, border_snapshot_plan_step_max
        )
        print(f"read time: {(time.time() - start):.2f} seconds")

    if max_intersect:
        max_interval: Interval = max_intersect_interval_by_x(rectangles)
        limit_x[0] = max_interval.start
        limit_x[1] = max_interval.end
        left_rect_id = binary_search(rectangles, max_interval.start, lambda rect: rect.get_x())
        while rectangles[left_rect_id].get_x() + rectangles[left_rect_id].get_width() < limit_x[1]:
            left_rect_id += 1

        intersect_rectangles, _, _ = process_border(rectangles, limit_x[0], None, limit_x[1], None)
        print(
            f"left_rect_id {left_rect_id}, {rectangles[left_rect_id].get_x()}, {rectangles[left_rect_id].get_x() + rectangles[left_rect_id].get_width()}"
        )
        print(f"intersect_rectangles: {len(intersect_rectangles)}")
        # right_rect_id = binary_search(rectangles, max_interval.start, lambda rect: rect.get_x() + rect.get_width())
        print(
            f"max_interval: ({max_interval.start}, {max_interval.end}), count intersect: {max_interval.count_intersect}"
        )

    print(f"count drawn rectangles: {len(rectangles)}")

    rectangles = []
    for i in range(0, 5):
        rectangles.append(Rectangle((i, i), 1, 1, color="black", fc='none'))
    for i in range(5, 0, -1):
        rectangles.append(Rectangle((i + 5, i - 1), 1, 1, color="black", fc='none'))
    max_interval: Interval = max_intersect_interval_by_x(rectangles)
    print(f"max_interval: ({max_interval.start}, {max_interval.end}), count intersect: {max_interval.count_intersect}")

    fig, ax = plt.subplots()
    ax.set_title("intersect portions")
    ax.add_collection(PatchCollection(rectangles, match_original=True))

    plt.xlabel("primary key (TimeStamp)")
    plt.ylabel("snapshot plan step")
    plt.title("intersect portions")
    # plt.xlim(limit_x[0] , limit_x[1])
    # plt.ylim(limit_y[0] , limit_y[1])
    plt.show()
