import collections
import psutil
from functools import wraps


def safe(name):
    def decorator_safe(func):
        """
        Decorator for try-catch on string assembly
        """

        @wraps(func)
        def wrap_safe(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                return "Failed to get {}: {}".format(name, e)

        return wrap_safe

    return decorator_safe


def get_proc_attrib(attr):
    if callable(attr):
        try:
            return attr()
        except psutil.Error:
            return None
    else:
        return attr


@safe("cpu/mem info")
def _cpu_mem_str():
    vm = psutil.virtual_memory()
    cpu_tp = psutil.cpu_times_percent(0.1)

    str_items = []
    str_items.append(
        "CPU:  Idle: {}%  User: {}%  System: {}%  IOwait: {}%\n".format(
            cpu_tp.idle, cpu_tp.user, cpu_tp.system, cpu_tp.iowait
        )
    )

    str_items.append(
        "MEM:  total {} Gb  available: {} Gb  used: {} Gb  free: {} Gb  active: {} Gb  inactive: {} Gb  shared: {} Gb\n".format(
            round(vm.total / 1e9, 2),
            round(vm.available / 1e9, 2),
            round(vm.used / 1e9, 2),
            round(vm.free / 1e9, 2),
            round(vm.active / 1e9, 2),
            round(vm.inactive / 1e9, 2),
            round(vm.shared / 1e9, 2),
        )
    )

    str_items.append("Used swap: {}%\n".format(psutil.swap_memory().percent))

    return "".join(str_items)


@safe("processes tree")
def _proc_tree_str():
    tree = collections.defaultdict(list)
    for p in psutil.process_iter():
        try:
            tree[p.ppid()].append(p.pid)
        except (psutil.NoSuchProcess, psutil.ZombieProcess):
            pass
    # on systems supporting PID 0, PID 0's parent is usually 0
    if 0 in tree and 0 in tree[0]:
        tree[0].remove(0)

    return _print_proc_tree(min(tree), tree)


def _print_proc_tree(parent_root, tree, indent_root=''):
    stack = [(parent_root, indent_root, "")]
    str_items = list()

    while len(stack) > 0:
        try:
            parent, indent, prefix = stack.pop()
            p = psutil.Process(parent)
            name = get_proc_attrib(p.name)
            str_items.append("{}({}, '{}'".format(prefix, parent, name if name else '?'))

            exe = get_proc_attrib(p.exe)
            if exe:
                str_items.append(" [{}]".format(exe))

            str_items.append(") ")
            str_items.append("  st: {}".format(p.status()))
            str_items.append("  mem: {}%".format(round(p.memory_percent(), 2)))

            ndfs = get_proc_attrib(p.num_fds)
            if ndfs and ndfs > 0:
                str_items.append("  fds: {}".format(ndfs))

            conns = get_proc_attrib(p.connections)
            if conns and len(conns) > 1:
                str_items.append("  num con: {}".format(len(conns)))

            ths = get_proc_attrib(p.num_threads)
            if ths and ths > 1:
                str_items.append("  threads: {}".format(ths))

            str_items.append("\n")
        except psutil.Error:
            name = "?"
            str_items.append("({}, '{}')\n".format(parent, name))

        if parent not in tree:
            continue

        child = tree[parent][-1]
        stack.append((child, indent + "  ", indent + "`_ "))

        children = tree[parent][:-1]
        children.reverse()
        for child in children:
            stack.append((child, indent + "| ", indent + "|- "))

    return "".join(str_items)


@safe("network info")
def _network_conn_str():
    str_items = list()

    counters = psutil.net_io_counters()
    str_items.append(
        "\nPackSent: {}  PackRecv: {}  ErrIn: {}  ErrOut: {}  DropIn: {}  DropOut: {}\n\n".format(
            counters.packets_sent,
            counters.packets_recv,
            counters.errin,
            counters.errout,
            counters.dropin,
            counters.dropout,
        )
    )

    ifaces = psutil.net_if_addrs()
    conns = psutil.net_connections()
    list_ip = collections.defaultdict(list)
    for con in conns:
        list_ip[con.laddr.ip].append(con)

    for name, addrs in ifaces.iteritems():
        str_items.append("{}:\n".format(name))

        for ip in addrs:
            str_items.append("   {}".format(ip.address))
            if ip.netmask:
                str_items.append("   mask={}".format(ip.netmask))
            if ip.broadcast:
                str_items.append("   bc={}".format(ip.broadcast))
            str_items.append("\n")

            for con in list_ip[ip.address]:
                str_items.append("      {}".format(con.laddr.port))
                if con.raddr:
                    str_items.append(" <--> {} : {}".format(con.raddr.ip, con.raddr.port))
                str_items.append("   (stat: {}".format(con.status))
                if con.pid:
                    str_items.append("  proc: {} (pid={})".format(psutil.Process(con.pid).exe(), con.pid))
                str_items.append(")\n")

            del list_ip[ip.address]

    str_items.append("***\n")
    for ip, conns in list_ip.iteritems():
        str_items.append("   {}\n".format(ip))

        for con in conns:
            str_items.append("      {}".format(con.laddr.port))
            if con.raddr:
                str_items.append(" <--> {} : {}".format(con.raddr.ip, con.raddr.port))
            str_items.append("   (stat: {}".format(con.status))
            if con.pid:
                str_items.append("  proc: {} (pid={})".format(psutil.Process(con.pid).exe(), con.pid))
            str_items.append(")\n")

    return "".join(str_items)


@safe("info")
def get_system_info():
    str_items = list()

    str_items.append("\n --- CPU MEM --- \n")
    str_items.append(_cpu_mem_str())
    str_items.append("\n")

    str_items.append("\n --- PROCESSES TREE --- \n")
    str_items.append(_proc_tree_str())
    str_items.append("\n")

    str_items.append("\n --- NETWORK INFO --- \n")
    str_items.append(_network_conn_str())
    str_items.append("\n")

    return "".join(str_items)
