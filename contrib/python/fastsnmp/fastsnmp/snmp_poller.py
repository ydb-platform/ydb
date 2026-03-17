#!/usr/bin/python3
# -*- coding: utf-8 -*-
import select
import sys

if hasattr(select, 'epoll'):
    from select import epoll as poll
    from select import EPOLLIN as POLLIN
    from select import EPOLLERR as POLLERR
elif hasattr(select, 'poll'):
    from select import poll
    from select import POLLIN
    from select import POLLERR
else:
    print("The current platform does not support epoll", file=sys.stderr)
    sys.exit(1)

import logging
import socket
import queue
import collections
from fastsnmp import snmp_parser
from time import time, monotonic
from typing import List, Optional, Tuple, Union
import random
from itertools import cycle

from dataclasses import dataclass

try:
    import mass_resolver
except ImportError:
    mass_resolver = None

DEBUG = False
logger = logging.getLogger(__name__)
MAX_SOCKETS_COUNT = 100
SNMP_PORT = 161


class Timeout(Exception):
    pass


@dataclass
class Job:
    name: str
    ip: str
    oids_to_poll: Tuple[str, ...]
    main_oids: Tuple[str, ...]
    sent: int = 0

    def new(self, oids_to_poll, ) -> 'Job':
        return Job(name=self.name, ip=self.ip, main_oids=self.main_oids, oids_to_poll=oids_to_poll)


@dataclass
class Result:
    name: str
    main_oid: Tuple[str, ...]
    index_part: str
    value: Union[bytes, Exception]
    ts: float
    duration: float


def resolve(hosts, to_v6=True):
    if mass_resolver:
        res = mass_resolver.resolve(hosts)
    else:
        # slow way
        res = dict()
        for host in hosts:
            host_ips = res.setdefault(host, list())
            try:
                ips = [x[4][0] for x in socket.getaddrinfo(host, 0, proto=socket.IPPROTO_TCP)]
            except socket.gaierror:
                logger.error("unable to resolve %s. skipping this host" % host)
                continue
            if to_v6:
                new_ips = []
                for ip in ips:
                    if ":" not in ip:
                        ip = "::ffff:" + ip
                    new_ips.append(ip)
                ips = new_ips
            host_ips.extend(ips)
    return res


def poller(hosts: List[str], oids_groups: List[List[str]], community: str, timeout: int = 3, backoff: int = 2, retry: int = 2,
           msg_type="GetBulk", start_reqid: Optional[int] = None, reqid_step: int = 1, max_repetitions: int = 60):
    """
    A generator that yields SNMP data

    :param hosts: hosts
    :param oids_groups: oids_groups
    :param community: community
    :type hosts: list | tuple
    :type oids_groups: list | tuple
    :type community: str
    :return: host, main_oid, index_part, value
    :rtype: tuple
    """
    job_queue = queue.Queue()
    socksize = 0x2000000
    retried_req = collections.defaultdict(int)

    # message cache
    pending_query = {}
    # ip => fqdn
    target_info = {}

    # fqdn => ips
    target_info_r = resolve(hosts)
    reqid_to_target = {}

    for fqdn, ips in list(target_info_r.items()):
        if ips:
            ip = ips[0]
            target_info[ip] = fqdn
        else:
            logger.error("unable to resolve %s. skipping this host", fqdn)
            del target_info_r[fqdn]

    # preparation of targets
    if start_reqid is None:
        start_reqid = random.randint(1, 30000)

    for oids_group in oids_groups:
        if not isinstance(oids_group, (tuple, list)):
            raise Exception("unexpected type of %s. expected list or tuple" % oids_group)
        if isinstance(oids_group, list):
            oids_group = tuple(oids_group)
        for fqdn, ips in target_info_r.items():
            oids_group = [x.strip(".") for x in oids_group]
            reqid_to_target[start_reqid] = Job(name=fqdn, ip=ips[0], oids_to_poll=oids_group, main_oids=oids_group)
            job_queue.put(start_reqid)
            start_reqid += reqid_step

    # preparation of sockets
    epoll = poll()
    new_sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    new_sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, False)
    new_sock.bind(("::", 0))
    new_sock.setblocking(False)
    new_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
    epoll.register(new_sock, POLLIN)

    # main loop
    while True:
        qsize = job_queue.qsize()
        for _ in range(min(qsize, 1000)):
            pdudata_reqid = job_queue.get()
            try:
                job = reqid_to_target[pdudata_reqid]
            except KeyError:
                logger.debug("%s is not found", pdudata_reqid)
                continue
            message = snmp_parser.msg_encode(pdudata_reqid, community, job.oids_to_poll, max_repetitions=max_repetitions, msg_type=msg_type)
            new_sock.sendto(message, (job.ip, SNMP_PORT))
            job.sent = monotonic()

            pending_query[pdudata_reqid] = int(time())

            if DEBUG:
                logger.debug("sendto %s reqid=%s", job, pdudata_reqid)
            job_queue.task_done()

        events = epoll.poll(0.01)
        for fileno, event in events:
            if event & POLLERR:
                raise Exception("epoll error")
            while True:
                try:
                    data, remotehost = new_sock.recvfrom(socksize)
                except BlockingIOError:
                    break
                ts = time()
                try:
                    pdudata_reqid, error_status, error_index, var_bind_list = snmp_parser.msg_decode(data)
                except Exception as e:
                    logger.critical("%r. unable to decode PDU from %s. data=%r", e, remotehost, data)
                    continue
                recv_time = monotonic()
                if pdudata_reqid not in reqid_to_target:  # received after timeout?
                    continue
                recv_job = reqid_to_target[pdudata_reqid]
                duration = recv_time - recv_job.sent
                if pending_query.pop(pdudata_reqid, None) is None:
                    if DEBUG:
                        logger.debug("received answer after timeout from %s reqid=%s", recv_job, pdudata_reqid)
                    continue

                if error_status:
                    logger.error("%s get error_status %s at %s", recv_job, error_status, error_index)
                    continue
                if DEBUG:
                    logger.debug('%s recv reqid=%s' % (recv_job, pdudata_reqid))

                reqid_to_target.pop(pdudata_reqid, None)

                main_oids_len = len(recv_job.main_oids)
                main_oids_positions = cycle(range(main_oids_len))
                var_bind_list_len = len(var_bind_list)

                skip_column = {}
                # if some oid in requested oids is not supported, column with it is index will
                # be filled with another oid. need to skip
                last_seen_index = {}

                for var_bind_pos in range(var_bind_list_len):
                    oid, value = var_bind_list[var_bind_pos]
                    # oids in received var_bind_list in round-robin order respectively query
                    main_oids_pos = next(main_oids_positions)
                    if value is None or value is snmp_parser.end_of_mib_view:
                        if DEBUG:
                            logger.debug('found none value %s %s %s' % (recv_job, oid, value))
                        skip_column[main_oids_pos] = True
                    if main_oids_pos in skip_column:
                        continue
                    main_oid = recv_job.main_oids[main_oids_pos]
                    if msg_type == "GetBulk":
                        if oid.startswith(main_oid + "."):
                            index_part = oid[len(main_oid) + 1:]
                            last_seen_index[main_oids_pos] = index_part
                            res = Result(name=recv_job.name, main_oid=main_oid, index_part=index_part, value=value, ts=ts,
                                         duration=duration)

                            yield res
                        else:
                            if DEBUG:
                                logger.debug(
                                    "host_ip=%s column_pos=%s skip oid %s=%s, reqid=%s. Not found in %s" % (recv_job,
                                                                                                            main_oids_pos,
                                                                                                            oid,
                                                                                                            value,
                                                                                                            pdudata_reqid,
                                                                                                            recv_job.main_oids))
                                logger.debug("vp=%s oid=%s main_oid=%s main_oids_pos=%s main_oids=%s", var_bind_pos,
                                             oid, main_oid, main_oids_pos, recv_job.main_oids)
                            skip_column[main_oids_pos] = True
                            if len(skip_column) == var_bind_list_len:
                                break
                    else:
                        res = Result(name=recv_job.name, main_oid=main_oid, index_part="", value=value, ts=ts, duration=duration)
                        yield res
                        skip_column[main_oids_pos] = True
                if len(skip_column) < main_oids_len:
                    if len(skip_column):
                        oids_to_poll = list()
                        new_main_oids = list()
                        for pos in range(main_oids_len):
                            if pos in skip_column:
                                continue
                            oids_to_poll.append("%s.%s" % (recv_job.main_oids[pos], last_seen_index[pos]))
                            new_main_oids.append(recv_job.main_oids[pos])
                        oids_to_poll = tuple(oids_to_poll)
                    else:
                        oids_to_poll = tuple(
                            "%s.%s" % (recv_job.main_oids[p], last_seen_index[p]) for p in range(main_oids_len))

                    start_reqid += reqid_step

                    reqid_to_target[start_reqid] = recv_job.new(oids_to_poll)
                    job_queue.put(start_reqid)
                else:
                    if DEBUG:
                        logger.debug('found not interesting oid=%s value=%s job=%s reqid=%s',
                                     oid, value, recv_job, pdudata_reqid)

        if pending_query:  # check timeouts
            cur_time = int(time())
            timeouted_querys = []
            for query, query_time in pending_query.items():
                attempt = retried_req.get(query, 1)
                if attempt == 1:
                    query_timeout = attempt * timeout
                else:
                    query_timeout = attempt * backoff * timeout
                if cur_time - query_time > query_timeout:
                    timeouted_querys.append(query)
                    if DEBUG:
                        logger.debug("timeout %s > %s. attempt=%s, %s", cur_time - query_time, query_timeout, attempt, query)
            cmt = monotonic()
            for timeouted_query in timeouted_querys:
                if retried_req[timeouted_query] < retry:
                    if DEBUG:
                        logger.debug("resend %s", timeouted_query)
                    job_queue.put(timeouted_query)
                    retried_req[timeouted_query] += 1
                else:
                    timeouted_job = reqid_to_target.pop(timeouted_query)
                    logger.debug("%s query timeout", timeouted_job)
                    duration = cmt - timeouted_job.sent
                    res = Result(name=timeouted_job.name, main_oid=timeouted_job.main_oids, index_part="", value=Timeout(),
                                 ts=time(), duration=duration)
                    yield res
                del pending_query[timeouted_query]
        elif job_queue.empty():
            break
