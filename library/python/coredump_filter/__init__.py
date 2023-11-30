#!/usr/bin/env python
# coding: utf-8

from __future__ import print_function

import six
import enum
import datetime
import os
import re
import pkgutil
import sys
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


class CoredumpMode(enum.Enum):
    GDB = "gdb"
    LLDB = "lldb"
    SDC_ASSERT = "sdc_assert"


ARCADIA_ROOT_LINK = "https://a.yandex-team.ru/arc/trunk/arcadia/"

ARCADIA_ROOT_DIRS = [
    # hottest paths
    "/util/",
    "/contrib/",
    "/library/",
    "/kernel/",
    "/build/",
    "/search/",

    # "/gcc-4.8.2/",

    # system paths
    # "/lib/x86_64-linux-gnu/",

    # all other stuff
    "/aapi/",
    "/addappter/",
    "/adfox/",
    "/admins/",
    "/ads/",
    "/adv/",
    "/advq/",
    "/afisha/",
    "/afro/",
    "/alet/",
    "/alice/",
    "/analytics/",
    "/antiadblock/",
    "/antirobot/",
    "/apphost/",
    "/april/",
    "/arc/",
    "/arcanum/",
    "/augur/",
    "/aurora/",
    "/autocheck/",
    "/balancer/",
    "/bass/",
    "/billing/",
    "/bindings/",
    "/browser/",
    "/build/",
    "/bunker/",
    "/caas/",
    "/canvas/",
    "/captcha/",
    "/catboost/",
    "/certs/",
    "/ci/",
    "/clickhouse/",
    "/client_analytics/",
    "/cloud/",
    "/cmicot/",
    "/cmnt/",
    "/comdep_analytics/",
    "/commerce/",
    "/contrib/",
    "/crm/",
    "/crowdsourcing/",
    "/crypta/",
    "/cv/",
    "/datacloud/",
    "/datalens/",
    "/data-ui/",
    "/devtools/",
    "/dict/",
    "/direct/",
    "/disk/",
    "/distribution/",
    "/distribution_interface/",
    "/district/",
    "/dj/",
    "/docs/",
    "/douber/",
    "/drive/",
    "/edadeal/",
    "/education/",
    "/entity/",
    "/ether/",
    "/extdata/",
    "/extsearch/",
    "/FactExtract/",
    "/fintech/",
    "/frontend/",
    "/fuzzing/",
    "/games/",
    "/gencfg/",
    "/geobase/",
    "/geoproduct/",
    "/geosuggest/",
    "/geotargeting/",
    "/glycine/",
    "/groups/",
    "/haas/",
    "/health/",
    "/helpdesk/",
    "/hitman/",
    "/home/",
    "/htf/",
    "/hw_watcher/",
    "/hypercube/",
    "/iaas/",
    "/iceberg/",
    "/infra/",
    "/intranet/",
    "/inventori/",
    "/ipreg/",
    "/irt/",
    "/it-office/",
    "/jdk/",
    "/juggler/",
    "/junk/",
    "/jupytercloud/",
    "/kernel/",
    "/keyboard/",
    "/kikimr/",
    "/kinopoisk/",
    "/kinopoisk-ott/",
    "/laas/",
    "/lbs/",
    "/library/",
    "/load/",
    "/locdoc/",
    "/logbroker/",
    "/logfeller/",
    "/mail/",
    "/mapreduce/",
    "/maps/",
    "/maps_adv/",
    "/market/",
    "/mb/",
    "/mds/",
    "/media/",
    "/media-billing/",
    "/media-crm/",
    "/mediapers/",
    "/mediaplanner/",
    "/mediastat/",
    "/media-stories/",
    "/metrika/",
    "/milab/",
    "/ml/",
    "/mlp/",
    "/mlportal/",
    "/mobile/",
    "/modadvert/",
    "/ms/",
    "/mssngr/",
    "/music/",
    "/musickit/",
    "/netsys/",
    "/nginx/",
    "/nirvana/",
    "/noc/",
    "/ofd/",
    "/offline_data/",
    "/opensource/",
    "/orgvisits/",
    "/ott/",
    "/packages/",
    "/partner/",
    "/passport/",
    "/payplatform/",
    "/paysys/",
    "/plus/",
    "/portal/",
    "/portalytics/",
    "/pythia/",
    "/quality/",
    "/quasar/",
    "/razladki/",
    "/regulargeo/",
    "/release_machine/",
    "/rem/",
    "/repo/",
    "/rnd_toolbox/",
    "/robot/",
    "/rtc/",
    "/rtline/",
    "/rtmapreduce/",
    "/rt-research/",
    "/saas/",
    "/samogon/",
    "/samsara/",
    "/sandbox/",
    "/scarab/",
    "/sdc/",
    "/search/",
    "/security/",
    "/semantic-web/",
    "/serp/",
    "/sitesearch/",
    "/skynet/",
    "/smart_devices/",
    "/smarttv/",
    "/smm/",
    "/solomon/",
    "/specsearches/",
    "/speechkit/",
    "/sport/",
    "/sprav/",
    "/statbox/",
    "/strm/",
    "/suburban-trains/",
    "/sup/",
    "/switch/",
    "/talents/",
    "/tasklet/",
    "/taxi/",
    "/taxi_efficiency/",
    "/testenv/",
    "/testpalm/",
    "/testpers/",
    "/toloka/",
    "/toolbox/",
    "/tools/",
    "/tracker/",
    "/traffic/",
    "/transfer_manager/",
    "/travel/",
    "/trust/",
    "/urfu/",
    "/vcs/",
    "/velocity/",
    "/vendor/",
    "/vh/",
    "/voicetech/",
    "/weather/",
    "/web/",
    "/wmconsole/",
    "/xmlsearch/",
    "/yabs/",
    "/yadoc/",
    "/yandex_io/",
    "/yaphone/",
    "/ydf/",
    "/ydo/",
    "/yp/",
    "/yql/",
    "/ysite/",
    "/yt/",
    "/yweb/",
    "/zen/",
    "/zapravki/",
    "/zen/",
    "/zootopia/",
    "/zora/",
]

MY_PATH = os.path.dirname(os.path.abspath(__file__))

# 0.2.x uses stable hashing
CORE_PROC_VERSION = "0.2.1"

ARCADIA_ROOT_SIGN = "$S/"
SIGNAL_NOT_FOUND = "signal not found"


class SourceRoot(object):
    def __init__(self):
        self.root = None

    def detect(self, source):
        if not source:
            # For example, regexp_4
            return

        if source.startswith("/-S/"):
            return source[4:]

        if source.startswith("../"):
            return source

        """
        if self.root is not None:
            return self.root
        """

        min_pos = 100000
        for root_dir in ARCADIA_ROOT_DIRS:
            pos = source.find(root_dir)
            if pos < 0:
                continue

            if pos < min_pos:
                min_pos = pos

        if min_pos < len(source):
            self.root = source[:min_pos + 1]

    def crop(self, source):
        if not source:
            return ""

        # detection attempt
        self.detect(source)

        if self.root is not None:
            return source.replace(self.root, ARCADIA_ROOT_SIGN, 1)

        # when traceback contains only ??, source root cannot be detected
        return source


def highlight_func(s):
    return (
        s
        .replace("=", '<span class="symbol">=</span>')
        .replace("(", '<span class="symbol">(</span>')
        .replace(")", '<span class="symbol">)</span>')
    )


class FrameBase(object):
    def __init__(
        self,
        frame_no=None,
        addr="",
        func="",
        source="",
        source_no="",
        func_name="",
    ):
        self.frame_no = frame_no
        self.addr = addr
        self.func = func
        self.source = source
        self.source_no = source_no
        self.func_name = func_name

    def __str__(self):
        return "{}\t{}\t{}".format(
            self.frame_no,
            self.func,
            self.source,
        )

    def to_json(self):
        return {
            "frame_no": self.frame_no,
            "addr": self.addr,
            "func": self.func,
            "func_name": self.func_name,
            "source": self.source,
            "source_no": self.source_no,
        }

    def fingerprint(self):
        return self.func_name

    def cropped_source(self):
        return self.source

    def raw(self):
        return "{frame} {func} {source}".format(
            frame=self.frame_no,
            func=self.func,
            source=self.source,
        )

    def html(self):
        source, source_fmt = self.find_source()
        return (
            '<span class="frame">{frame}</span>'
            '<span class="func">{func}</span> '
            '<span class="source">{source}</span>{source_fmt}\n'.format(
                frame=self.frame_no,
                func=highlight_func(self.func.replace("&", "&amp;").replace("<", "&lt;")),
                source=source,
                source_fmt=source_fmt,
            )
        )


class LLDBFrame(FrameBase):
    SOURCE_NO_RE = re.compile(r"(.*?[^\d]):(\d+)")
    FUNC_RE = re.compile(r"(\w+\s)?(\w+[\w,:,_,<,>,\s,*]+).*$")

    def __init__(
        self,
        frame_no=None,
        addr="",
        func="",
        source="",
        source_no="",
        func_name="",
    ):
        super(LLDBFrame, self).__init__(
            frame_no=frame_no,
            addr=addr,
            func=func,
            source=source,
            source_no=source_no,
            func_name=func_name,
        )
        # .source calculation

        func = func.replace("(anonymous namespace)::", "")
        m = self.FUNC_RE.match(func)
        if m:
            self.func_name = m.group(2)  # overwrite func_name if name is in func

        if source_no:
            self.source_no = source_no
            self.source = source
        else:
            m = self.SOURCE_NO_RE.match(source)
            if m:
                self.source = m.group(1)
                self.source_no = m.group(2)

    def find_source(self):
        """
        :return: pair (source, source_fmt)
        """
        source_fmt = ""

        if self.source_no:
            source_fmt = ' +<span class="source-no">{}</span>'.format(self.source_no)

        return self.source, source_fmt


class GDBFrame(FrameBase):
    SOURCE_NO_RE = re.compile(r"(.*):(\d+)")
    # #7  0x00007f105f3a221d in NAppHost::NTransport::TCoroutineExecutor::Poll (this=0x7f08416a5d00,
    #     tasks=empty TVector (capacity=32)) at /-S/apphost/lib/executors/executors.cpp:373
    # We match with non-greedy regex a function name that cannot contain equal sign
    FUNC_RE = re.compile(r"(.*?) \(([a-zA-Z0-9_]+=.*|)\)$")   # function with kwarg-params or zero params

    def __init__(
        self,
        frame_no=None,
        addr="",
        func="",
        source="",
        source_no="",
        func_name="",
    ):
        super(GDBFrame, self).__init__(
            frame_no=frame_no,
            addr=addr,
            func=func,
            source=source,
            source_no=source_no,
            func_name=func_name,
        )
        if not source_no:
            m = self.SOURCE_NO_RE.match(source)
            if m:
                self.source = m.group(1)
                self.source_no = m.group(2)
        if not func_name:
            m = self.FUNC_RE.match(self.func)
            if m:
                self.func_name = m.group(1)

    def find_source(self):
        """
        Returns link to arcadia if source is path in arcadia, else just string with path
        :return: pair (source, source_fmt)
        """
        source_fmt = ""
        source = ""
        link = ""
        dirs = self.source.split("/")
        if len(dirs) > 1 and "/{dir}/".format(dir=dirs[1]) in ARCADIA_ROOT_DIRS:
            link = self.source.replace(ARCADIA_ROOT_SIGN, ARCADIA_ROOT_LINK)
        else:
            source = self.source
        if self.source_no:
            source_fmt = ' +<span class="source-no">{}</span>'.format(self.source_no)
            if link:
                link += "?#L{line}".format(line=self.source_no)

        if link:
            source = '<a href="{link}">{source}</a>'.format(
                link=link,
                source=self.source,
            )
        return source, source_fmt


class SDCAssertFrame(LLDBFrame):

    def __init__(
        self,
        frame_no=None,
        addr="",
        func="",
        source="",
        source_no="",
        func_name="",
    ):
        super(SDCAssertFrame, self).__init__(
            frame_no=frame_no,
            addr=addr,
            func=func,
            source=source,
            source_no=source_no,
            func_name=func_name,
        )
        # .source calculation

        self.source = source or ""
        if isinstance(source_no, str) and len(source_no) > 0:
            source_no = int(source_no, 16)
        self.source_no = source_no or ""

        m = self.FUNC_RE.match(func)
        if m:
            self.func_name = m.group(2)


class Stack(object):
    # priority classes
    LOW_IMPORTANT = 25
    DEFAULT_IMPORTANT = 50
    SUSPICIOUS_IMPORTANT = 75
    MAX_IMPORTANT = 100

    # default coredump's type
    mode = CoredumpMode.GDB

    max_depth = None

    fingerprint_blacklist = [
        # bottom frames
        "raise",
        "abort",
        "__gnu_cxx::__verbose_terminate_handler",
        "_cxxabiv1::__terminate",
        "std::terminate",
        "__cxxabiv1::__cxa_throw",
        # top frames
        "start_thread",
        "clone",
        "??",
        "__clone",
        "__libc_start_main",
        "_start",
        "__nanosleep",
    ]

    fingerprint_blacklist_prefix = ()

    suspicious_functions = [
        "CheckedDelete",
        "NPrivate::Panic",
        "abort",
        "close_all_fds",
        "__cxa_throw",
    ]

    low_important_functions_eq = [
        "poll ()",
        "recvfrom ()",
        "pthread_join ()",
    ]

    low_important_functions_match = [
        "TCommonSockOps::SendV",
        "WaitD (",
        "SleepT (",
        "Join (",
        "epoll_wait",
        "nanosleep",
        "pthread_cond_wait",
        "pthread_cond_timedwait",
        "gsignal",
        "std::detail::_",
        "std::type_info",
        "ros::NodeHandle",
    ]

    def __init__(
        self,
        lines=None,
        source_root=None,
        thread_ptr=0,
        thread_id=None,
        frames=None,
        important=None,
        stack_fp=None,
        fingerprint_hash=None,
        stream=None,
        mode=None,  # type: CoredumpMode
        ignore_bad_frames=True,
    ):
        self.lines = lines
        self.source_root = source_root
        self.thread_ptr = thread_ptr
        self.thread_id = thread_id
        if mode is not None:
            self.mode = mode

        self.frames = frames or []
        if self.frames and isinstance(frames[0], dict):
            self.frames = [self.frame_factory(f) for f in self.frames]
        self.important = important or self.DEFAULT_IMPORTANT
        if thread_id == 1:
            self.important = self.MAX_IMPORTANT
        self.fingerprint_hash = fingerprint_hash
        self.stack_fp = stack_fp
        self.stream = stream
        self.ignore_bad_frames = ignore_bad_frames

    def to_json(self):
        """Should be symmetric with `from_json`."""
        return {
            "mode": self.mode.value,
            "frames": [frame.to_json() for frame in self.frames],
            "important": self.important,
        }

    @staticmethod
    def from_json(stack):
        """Should be symmetric with `to_json`."""
        mode = CoredumpMode(stack.get("mode", CoredumpMode.GDB.value))
        # old serialization format support, should be dropped
        lldb_mode = stack.get("lldb_mode", False)
        if lldb_mode:
            mode = CoredumpMode.LLDB

        unpacked_stack = {
            "mode": mode,
            "frames": stack["frames"],
            "important": stack.get("important", Stack.DEFAULT_IMPORTANT),
        }
        return mode, unpacked_stack

    def frame_factory(self, args):
        frames = {
            CoredumpMode.GDB: GDBFrame,
            CoredumpMode.LLDB: LLDBFrame,
            CoredumpMode.SDC_ASSERT: SDCAssertFrame,
        }

        class_object = frames.get(self.mode)
        if not class_object:
            raise Exception("Invalid mode: {}".format(self.mode.value))

        return class_object(**args)

    def low_important(self):
        return self.important <= self.LOW_IMPORTANT

    def check_importance(self, frame):
        # raised priority cannot be lowered
        if self.important > self.DEFAULT_IMPORTANT:
            return

        # detect suspicious stacks
        for name in self.suspicious_functions:
            if name in frame.func:
                self.important = self.SUSPICIOUS_IMPORTANT
                return

        for name in self.low_important_functions_eq:
            if name == frame.func:
                self.important = self.LOW_IMPORTANT

        for name in self.low_important_functions_match:
            if name in frame.func:
                self.important = self.LOW_IMPORTANT

    def push_frame(self, frame):
        self.check_importance(frame)
        # ignore duplicated frames
        if len(self.frames) and self.frames[-1].frame_no == frame.frame_no:
            return
        self.frames.append(frame)

    def parse(self):
        """
        Parse one stack
        """
        assert self.lines is not None
        assert self.source_root is not None

        for line in self.lines:
            match_found = False
            for regexp in self.REGEXPS:
                m = regexp.match(line)
                if m:
                    frame_args = m.groupdict()
                    if "source" in frame_args:
                        frame_args["source"] = self.source_root.crop(frame_args["source"])

                    self.push_frame(self.frame_factory(frame_args))
                    match_found = True
                    break

            if not match_found:
                self.bad_frame(line)

    def bad_frame(self, line):
        if self.ignore_bad_frames:
            logger.warning("Bad frame: %s", line)
            return

        raise Exception("Bad frame: `{}`, frame `{}`".format(
            line,
            self.debug(return_result=True),
        ))

    def debug(self, return_result=False):
        if self.low_important():
            return ""

        res = "\n".join([str(f) for f in self.frames])
        res += "----------------------------- DEBUG END\n"
        if return_result:
            return res

        self.stream.write(res)

    def raw(self):
        return "\n".join([frame.raw() for frame in self.frames])

    def html(self, same_hash=False, same_count=1, return_result=False):
        ans = ""
        pre_class = "important-" + str(self.important)
        if same_hash:
            pre_class += " same-hash"

        ans += '<pre class="{0}">'.format(pre_class)
        if not same_hash:
            ans += '<a name="stack{0}"></a>'.format(self.hash())

        ans += '<span class="hash"><a href="#stack{0}">#{0}</a>, {1} stack(s) with same hash</span>\n'.format(
            self.hash(), same_count,
        )

        for f in self.frames:
            ans += f.html()
        ans += "</pre>\n"

        if return_result:
            return ans

        self.stream.write(ans)

    def fingerprint(self, max_num=None):
        """
        Stack fingerprint: concatenation of non-common stack frames
        FIXME: wipe away `max_num`
        """
        stack_fp = list()
        len_frames = min((max_num or len(self.frames)), len(self.frames))

        for f in self.frames[:len_frames]:
            fp = f.fingerprint()
            if not fp:
                continue

            if fp in self.fingerprint_blacklist:
                continue

            if fp.startswith(self.fingerprint_blacklist_prefix):
                continue

            if fp in stack_fp:
                # FIXME: optimize duplicate remover: check only previous frame
                # see also `push_frame`
                continue

            stack_fp.append(fp.strip())

            if self.max_depth is not None and len(stack_fp) >= self.max_depth:
                break

        return "\n".join(stack_fp)

    def simple_html(self, num_frames=None):
        if not num_frames:
            num_frames = len(self.frames)
        pre_class = "important-0"
        ans = '<pre class="{0}">'.format(pre_class)
        for i in range(min(len(self.frames), num_frames)):
            ans += self.frames[i].html()
        ans += "</pre>\n"
        return ans

    def __str__(self):
        return "\n".join(map(str, self.frames))

    def hash(self, max_num=None):
        """
        Entire stack hash for merging same stacks
        """
        if self.fingerprint_hash is None:
            self.fingerprint_hash = int(hashlib.md5(self.fingerprint(max_num).encode("utf-8")).hexdigest()[0:15], 16)

        return self.fingerprint_hash


class GDBStack(Stack):

    mode = CoredumpMode.GDB

    REGEXPS = [
        # #6  0x0000000001d9203e in NAsio::TIOService::TImpl::Run (this=0x137b1ec00) at /place/
        # sandbox-data/srcdir/arcadia_cache/library/neh/asio/io_service_impl.cpp:77

        re.compile(
            r"#(?P<frame_no>\d+)[ \t]+(?P<addr>0x[0-9a-f]+) in (?P<func>.*) at (?P<source>.*)"
        ),

        # #5 TCondVar::WaitD (this=this@entry=0x10196b2b8, mutex=..., deadLine=..., deadLine@entry=...)
        # at /place/sandbox-data/srcdir/arcadia_cache/util/system/condvar.cpp:150
        re.compile(
            r"#(?P<frame_no>\d+)[ \t]+(?P<func>.*) at (?P<source>/.*)"
        ),

        # #0  0x00007faf8eb31d84 in pthread_cond_wait@@GLIBC_2.3.2 ()
        # from /lib/x86_64-linux-gnu/libpthread.so.0
        re.compile(
            r"#(?P<frame_no>\d+)[ \t]+(?P<addr>0x[0-9a-f]+) in (?P<func>.*) from (?P<source>.*)"
        ),

        # #0  pthread_cond_wait@@GLIBC_2.3.2 () at ../sysdeps/unix/sysv/linux/x86_64/pthread_cond_wait.S:185
        re.compile(
            r"#(?P<frame_no>\d+)[ \t]+ (?P<func>.*) at (?P<source>.*)"
        ),

        # #10 0x0000000000000000 in ?? ()
        re.compile(
            r"#(?P<frame_no>\d+)[ \t]+(?P<addr>0x[0-9a-f]+) in (?P<func>.*)"
        ),
    ]


class LLDBStack(Stack):

    mode = CoredumpMode.LLDB

    REGEXPS = [
        # 0x00007fd7b300a886 libthird_Uparty_Sros_Sros_Ucomm_Sclients_Sroscpp_Sliblibroscpp.so`
        # std::thread::_State_impl<std::thread::_Invoker<std::tuple<ros::PollManager::PollManager()::$_1> > >::_M_run()
        # [inlined] ros::PollManager::threadFunc(this=0x00007fd7b30dab20) at poll_manager.cpp:75:16  # noqa
        re.compile(
            r"[ *]*frame #(?P<frame_no>\d+): (?P<addr>0x[0-9a-f]+).+inlined]\s(?P<func>.+)\sat\s(?P<source>.+)"
        ),

        re.compile(
            r"[ *]*frame #(?P<frame_no>\d+): (?P<addr>0x[0-9a-f]+).+?`(?P<func>.+)\sat\s(?P<source>.+)"
        ),

        # * frame #0: 0x00007fd7aee51f47 libc.so.6`gsignal + 199
        re.compile(
            r"[ *]*frame #(?P<frame_no>\d+): (?P<addr>0x[0-9a-f]+)\s(?P<source>.+)`(?P<func>.+)\s\+\s(?P<source_no>\d+)"
        ),
    ]

    # Take not more than `max_depth` non-filtered frames into fingerprint
    # See CORES-180
    max_depth = 10

    fingerprint_blacklist = Stack.fingerprint_blacklist + [
        "ros::ros_wallsleep",
    ]

    fingerprint_blacklist_prefix = Stack.fingerprint_blacklist_prefix + (
        "___lldb_unnamed_symbol",
        "__gnu_cxx",
        "__gthread",
        "__pthread",
        "decltype",
        "myriapoda::BuildersRunner",
        "non",
        "std::_Function_handler",
        "std::_Sp_counted_ptr_inplace",
        "std::__invoke_impl",
        "std::__invoke_result",
        "std::__shared_ptr",
        "std::conditional",
        "std::shared_ptr",
        "std::thread::_Invoker",
        "std::thread::_State_impl",
        "yandex::sdc::assert_details_",
    )

    suspicious_functions = Stack.suspicious_functions + [
        "Xml",
        "boost",
        "ros",
        "supernode",
        "tensorflow",
        "yandex::sdc",
    ]


class PythonStack(Stack):

    REGEXPS = [
        re.compile(
            r'File "(?P<source>.*)", line (?P<source_no>\d+), in (?P<func_name>.*)'
        ),
    ]


class SDCAssertStack(LLDBStack):

    mode = CoredumpMode.SDC_ASSERT

    REGEXPS = [
        # 0: ./modules/_shared/libcore_Stools_Slibassert.so(yandex::sdc::assert_details_::PanicV(char const*,
        # long, char const*, char const*, bool, char const*, __va_list_tag*)
        # +0x2aa)[0x7fb83268feaa]
        re.compile(
            r"(?P<frame_no>\d+):\s(?P<source>.+.so)\((?P<func>.+)\+(?P<source_no>.+).+\[(?P<addr>0x[0-9a-f]+)"
        ),

        re.compile(
            r"(?P<frame_no>\d+):\s(?P<source>\w+)\((?P<func>.+)\+(?P<source_no>.+).+\[(?P<addr>0x[0-9a-f]+)"
        )
    ]


def parse_python_traceback(trace):
    trace = trace.replace("/home/zomb-sandbox/client/", "/")
    trace = trace.replace("/home/zomb-sandbox/tasks/", "/sandbox/")
    trace = trace.split("\n")
    exception = trace[-1]  # noqa: F841
    trace = trace[1: -1]
    pairs = zip(trace[::2], trace[1::2])
    stack = Stack(lines=[])
    for frame_no, (path, row) in enumerate(pairs):
        # FIXME: wrap into generic tracer
        m = PythonStack.REGEXPS[0].match(path.strip())
        if m:
            frame_args = m.groupdict()
            if not frame_args["source"].startswith("/"):
                frame_args["source"] = "/" + frame_args["source"]
            frame_args["frame_no"] = str(frame_no)
            frame_args["func"] = row.strip()
            stack.push_frame(GDBFrame(**frame_args))
    return [[stack]], [[stack.raw()]], 6


def stack_factory(stack):
    mode, unpacked_stack = Stack.from_json(stack)

    if mode == CoredumpMode.GDB:
        return GDBStack(**unpacked_stack)
    elif mode == CoredumpMode.LLDB:
        return LLDBStack(**unpacked_stack)
    elif mode == CoredumpMode.SDC_ASSERT:
        return SDCAssertStack(**unpacked_stack)

    raise Exception("Invalid stack mode: {}. ".format(mode))


def _read_file(file_name):
    with open(file_name) as f:
        return f.read()


def _file_contents(file_name):
    """Return file (or resource) contents as unicode string."""
    if getattr(sys, "is_standalone_binary", False):
        try:
            contents = pkgutil.get_data(__package__, file_name)
        except Exception:
            raise IOError("Failed to find resource: " + file_name)
    else:
        if not os.path.exists(file_name):
            file_name = os.path.join(MY_PATH, file_name)
        contents = _read_file(file_name)
    # py23 compatibility
    if not isinstance(contents, six.text_type):
        contents = contents.decode("utf-8")
    return contents


def html_prolog(stream, timestamp):
    prolog = _file_contents("prolog.html")
    assert isinstance(prolog, six.string_types)
    stream.write(prolog.format(
        style=_file_contents("styles.css"),
        coredump_js=_file_contents("core_proc.js"),
        version=CORE_PROC_VERSION,
        timestamp=timestamp,
    ))


def html_epilog(stream):
    stream.write(_file_contents("epilog.html"))


def detect_coredump_mode(core_text):
    if len(core_text) == 0:
        raise Exception("Text stacktrace is blank")

    if "Panic at unixtime" in core_text:
        return CoredumpMode.SDC_ASSERT

    if "(lldb)" in core_text:
        return CoredumpMode.LLDB

    return CoredumpMode.GDB


def filter_stack_dump(
    core_text=None,
    stack_file_name=None,
    use_fingerprint=False,
    sandbox_failed_task_id=None,
    output_stream=None,
    timestamp=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    ignore_bad_frames=True,
):
    """New interface for stacktrace filtering. Preferred to use."""
    if not core_text and not stack_file_name:
        raise ValueError("Either `core_text` or `stack_file_name` should be passed to `filter_stack_dump`. ")

    if core_text is not None and stack_file_name:
        raise ValueError("Only one of `core_text` and `stack_file_name` cannot be specified for `filter_stack_dump`. ")

    if stack_file_name:
        core_text = _read_file(stack_file_name)
    # further processing uses `core_text` only

    mode = detect_coredump_mode(core_text)
    core_lines = core_text.split("\n")

    return filter_stackdump(
        file_lines=core_lines,
        ignore_bad_frames=ignore_bad_frames,
        mode=mode,
        sandbox_failed_task_id=sandbox_failed_task_id,
        stream=output_stream,
        timestamp=timestamp,
        use_fingerprint=use_fingerprint,
        use_stream=output_stream is not None,
    )


class StackDumperBase(object):

    SANDBOX_TASK_RE = re.compile(r".*/[0-9a-f]/[0-9a-f]/([0-9]+)/.*")
    MAX_SAME_STACKS = 30

    def __init__(
        self,
        use_fingerprint,
        sandbox_failed_task_id,
        stream,
        use_stream,
        file_lines,
        timestamp,
        mode,
        file_name=None,
        ignore_bad_frames=True,
    ):
        self.source_root = SourceRoot()
        self.use_fingerprint = use_fingerprint
        self.sandbox_task_id = None
        self.sandbox_failed_task_id = sandbox_failed_task_id
        self.stream = stream or sys.stdout
        self.use_stream = use_stream
        self.file_name = file_name
        self.file_lines = file_lines
        self.timestamp = timestamp
        self.ignore_bad_frames = ignore_bad_frames
        self.stack_class = self.get_stack_class(mode)

        self.signal = SIGNAL_NOT_FOUND
        self.stacks = []
        self._main_info = []

    @staticmethod
    def is_ignored_line(line):
        raise NotImplementedError("Not implemented static method `is_ignored_line`. ")

    @staticmethod
    def get_stack_class(mode):
        exist_modes = {}
        for cls in [GDBStack, LLDBStack, SDCAssertStack]:
            current_mode = cls.mode
            if current_mode in exist_modes:
                raise Exception("Duplicate modes are disallowed. Repeated mode: `{}`".format(current_mode.value))
            exist_modes[current_mode] = cls

        if mode not in exist_modes:
            raise Exception("Unexpected coredump processing mode: `{}`".format(mode.value))

        return exist_modes[mode]

    def check_signal(self, line):
        raise NotImplementedError("Not implemented `check_signal`.")

    def set_sandbox_task_id(self, task_id):
        self.sandbox_task_id = task_id

    def add_main_line(self, line):
        self._main_info.append(line)

    def add_stack(self, stack_lines, thread_id):
        if not stack_lines:
            return

        stack = self.stack_class(
            lines=stack_lines,
            source_root=self.source_root,
            thread_id=thread_id,
            stream=self.stream,
            ignore_bad_frames=self.ignore_bad_frames,
        )
        self.stacks.append(stack)

    def dump(self):
        if self.file_lines is None:
            # FIXME(mvel): LLDB is not handled here
            self.file_lines = get_parsable_gdb_text(_read_file(self.file_name))

        self._collect_stacks()

        for stack in self.stacks:
            stack.parse()
            # stack.debug()

        if self.use_stream:
            if self.use_fingerprint:
                for stack in self.stacks:
                    self.stream.write(stack.fingerprint() + "\n")
                    self.stream.write("--------------------------------------\n")
                return
            else:
                html_prolog(self.stream, self.timestamp)

                if self.sandbox_task_id is not None:
                    self.stream.write(
                        '<div style="padding-top: 6px; font-size: 18px; font-weight: bold;">'
                        'Coredumped binary build task: '
                        '<a href="https://sandbox.yandex-team.ru/task/{0}">{0}</a></div>\n'.format(
                            self.sandbox_task_id
                        )
                    )

                if self.sandbox_failed_task_id is not None:
                    self.stream.write(
                        '<div style="padding-top: 6px; font-size: 18px; font-weight: bold;">'
                        'Sandbox failed task: '
                        '<a href="https://sandbox.yandex-team.ru/task/{0}">{0}</a></div>\n'.format(
                            self.sandbox_failed_task_id
                        )
                    )

                pre_class = ""
                self.stream.write('<pre class="{0}">\n'.format(pre_class))
                for line in self._main_info:
                    self.stream.write(line.replace("&", "&amp;").replace("<", "&lt;") + "\n")
                self.stream.write("</pre>\n")

        sorted_stacks = sorted(self.stacks, key=lambda x: (x.important, x.fingerprint()), reverse=True)

        prev_hash = None
        all_hash_stacks = []
        cur_hash_stacks = []
        for stack in sorted_stacks:
            if stack.hash() == 0:
                continue

            if stack.hash() == prev_hash:
                if len(cur_hash_stacks) < self.MAX_SAME_STACKS:
                    # do not collect too much
                    cur_hash_stacks.append(stack)
                continue

            # hash changed
            if cur_hash_stacks:
                all_hash_stacks.append(cur_hash_stacks)

            prev_hash = stack.hash()
            cur_hash_stacks = [stack, ]

        # push last
        if cur_hash_stacks:
            all_hash_stacks.append(cur_hash_stacks)

        if self.use_stream:
            for cur_hash_stacks in all_hash_stacks:
                same_hash = False
                for stack in cur_hash_stacks:
                    stack.html(same_hash=same_hash, same_count=len(cur_hash_stacks))
                    same_hash = True

            html_epilog(self.stream)
        else:
            raw_hash_stacks = [
                [stack.raw() for stack in common_hash_stacks]
                for common_hash_stacks in all_hash_stacks
            ]
            return all_hash_stacks, raw_hash_stacks, self.signal

    def _collect_stacks(self):
        stack_lines = []
        stack_detected = False
        thread_id = None

        for line in self.file_lines:
            line = line.strip()
            if self.is_ignored_line(line):
                continue

            if "Core was generated" in line:
                m = self.SANDBOX_TASK_RE.match(line)
                if m:
                    self.set_sandbox_task_id(int(m.group(1)))

            self.check_signal(line)

            # [Switching to thread 55 (Thread 0x7f100a94c700 (LWP 21034))]
            # Thread 584 (Thread 0x7ff363c03700 (LWP 2124)):

            # see test2 and test3
            tm = self.THREAD_RE.match(line)
            if tm:
                stack_detected = True
                self.add_stack(
                    stack_lines=stack_lines,
                    thread_id=thread_id,
                )
                stack_lines = []
                thread_id = int(tm.group(1))
                continue

            if stack_detected:
                stack_lines.append(line)
            else:
                self.add_main_line(line)

        # parse last stack
        self.add_stack(
            stack_lines=stack_lines,
            thread_id=thread_id,
        )


class StackDumperGDB(StackDumperBase):

    SIGNAL_FLAG = "Program terminated with signal"
    THREAD_RE = re.compile(r".*[Tt]hread (\d+) .*")
    LINE_IN = re.compile(r"\d+\tin ")

    def is_ignored_line(self, line):
        if not line:
            return True

        if line.startswith("[New "):
            # LWP, Thread, process
            return True

        if line.startswith("[Thread "):
            return True

        if line.startswith("Using "):
            return True

        if line.startswith("warning:"):
            return True

        if line.startswith("Python Exception"):
            # TODO: handle this more carefully
            return True

        if line[0] != "#" and "No such file or directory" in line:
            return True

        if self.LINE_IN.match(line):
            # see test1.txt for example
            # 641 in /place/sandbox-data/srcdir/arcadia/library/coroutine/engine/impl.h
            return True

        return False

    def check_signal(self, line):
        if self.SIGNAL_FLAG in line:
            self.signal = line[line.find(self.SIGNAL_FLAG) + len(self.SIGNAL_FLAG):].split(",")[0]


class StackDumperLLDB(StackDumperBase):

    SIGNAL_FLAG = "stop reason = signal"

    THREAD_RE = re.compile(r".*thread #(\d+), .*")

    SKIP_LINES = {
        "(lldb) bt all",
        "(lldb) script import sys",
        "(lldb) target create",
        "Core file",

        # Drop signal interceptor call
        # * frame #0: 0x00007efd49042fb7 libc.so.6`__GI___libc_sigaction at sigaction.c:54
        # TODO(epsilond1): Set MAX_IMPORTANT for some thread
        "__GI___libc_sigaction",

        # Drop unnamed symbols at lines like
        # frame #4: 0x00007fd8054156df libstdc++.so.6`___lldb_unnamed_symbol440$$libstdc++.so.6 + 15
        "$$",
    }

    @staticmethod
    def is_ignored_line(line):
        if not line:
            return True

        for skip_line in StackDumperLLDB.SKIP_LINES:
            if skip_line in line:
                return True
        return False

    def check_signal(self, line):
        if self.SIGNAL_FLAG in line and self.signal == SIGNAL_NOT_FOUND:
            self.signal = line.split()[-1]


class StackDumperSDCAssert(StackDumperBase):

    THREAD_RE = re.compile(
        r"(\d+)(:\s)"
    )

    def is_ignored_line(self, line):
        if not line:
            return True

        return not re.match(self.THREAD_RE, line)

    def check_signal(self, line):
        self.signal = SIGNAL_NOT_FOUND

    def _collect_stacks(self):
        stack_lines = []
        for line in self.file_lines:
            line = line.strip()
            if self.is_ignored_line(line):
                continue
            stack_lines.append(line)
            self.check_signal(line)

        self.add_stack(
            stack_lines=stack_lines,
            thread_id=0,
        )


def filter_stackdump(
    file_name=None,
    use_fingerprint=False,
    sandbox_failed_task_id=None,
    stream=None,
    file_lines=None,
    use_stream=True,
    timestamp=None,
    ignore_bad_frames=True,
    mode=None,
):
    if mode is None and file_name is not None:
        mode = detect_coredump_mode(_read_file(file_name))
    if mode == CoredumpMode.GDB:
        stack_dumper_cls = StackDumperGDB
    elif mode == CoredumpMode.LLDB:
        stack_dumper_cls = StackDumperLLDB
    elif mode == CoredumpMode.SDC_ASSERT:
        stack_dumper_cls = StackDumperSDCAssert
    else:
        raise Exception("Invalid mode: {}".format(mode.value))

    dumper = stack_dumper_cls(
        file_name=file_name,
        use_fingerprint=use_fingerprint,
        sandbox_failed_task_id=sandbox_failed_task_id,
        stream=stream,
        use_stream=use_stream,
        file_lines=file_lines,
        timestamp=timestamp,
        ignore_bad_frames=ignore_bad_frames,
        mode=mode,
    )

    return dumper.dump()


def get_parsable_gdb_text(core_text):
    # FIXME(mvel): Check encoding?
    # core_text = core_text.encode("ascii", "ignore").decode("ascii")
    core_text = (
        core_text
        # .replace("#", "\n#")  # bug here
        .replace("No core", "\nNo core")
        .replace("[New", "\n[New")
        .replace("\n\n", "\n")
    )

    return core_text.split("\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write(
            """Traceback filter "Tri Korochki"
https://wiki.yandex-team.ru/cores-aggregation/
Usage:
    core_proc.py <traceback.txt> [-f|--fingerprint]
    core_proc.py -v|--version
"""
        )
        sys.exit(1)

    if sys.argv[1] == "--version" or sys.argv[1] == "-v":
        if os.system("svn info 2>/dev/null | grep '^Revision'") != 0:
            print(CORE_PROC_VERSION)
        sys.exit(0)

    sandbox_failed_task_id = None

    use_fingerprint = False
    if len(sys.argv) >= 3:
        if sys.argv[2] == "-f" or sys.argv[2] == "--fingerprint":
            use_fingerprint = True
        sandbox_failed_task_id = sys.argv[2]

    filter_stack_dump(
        core_text=_read_file(sys.argv[1]),
        use_fingerprint=use_fingerprint,
        sandbox_failed_task_id=sandbox_failed_task_id,
        output_stream=sys.stdout,
    )


"""
Stack group is a `Stack` objects list with the same hash (fingerprint).
"""


class StackEncoder(json.JSONEncoder):
    """Stack JSON serializer."""

    def default(self, obj):
        if isinstance(obj, Stack):
            return obj.to_json()

        return json.JSONEncoder.default(obj)


def serialize_stacks(stack_groups):
    """
    Serialize list of stack groups to string (using JSON format).

    :param stack_groups: list of stack groups.
    :return: JSON serialized to string
    """
    return json.dumps(stack_groups, cls=StackEncoder)


def deserialize_stacks(stack_groups_str):
    """
    Restore JSON-serialized stack data into stack groups.

    :param stack_groups_str: JSON-serialized data.
    :return: list of stack groups
    """
    stack_groups_json = json.loads(stack_groups_str)
    # please do not use `map` hell here, it's impossible to debug
    all_stacks = [
        [stack_factory(stack) for stack in stacks]
        for stacks in stack_groups_json
    ]
    return all_stacks
