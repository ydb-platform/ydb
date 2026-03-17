'''
This tool is intended to decode existing data dumps produced with
other tools like tcpdump or strace, and print the data out in
JSON format.

The strace tool is not as convenient since version 4.13, as it
started to parse some of netlink messages at least partly,
rendering them useless for third party decoders. So if you plan
to use strace to obtain messages, be sure it is older than 4.13.
The strace related manual can be found in archive documentation
for older pyroute2 versions.

This manual is focused on pcap dumps.

An example session:

.. code-block:: console

    # set up netlink monitoring interface
    sudo ip link add dev nlmon0 type nlmon
    sudo ip link set dev nlmon0 up

    # dump the traffic into a pcap file
    # run netlink communication to be captured at the same time
    sudo tcpdump -i nlmon0 -w nl.pcap
    ^C

    # decode RTNL messages from the dump
    pyroute2-decoder \\
            -c pyroute2.netlink.rtnl.marshal.MarshalRtnl \\
            -d nl.pcap \\
            -m "ll_header{family=0}"

The result will be printed out in JSON format, so you can load
it directly from stdout, or use jq tool to navigate:

.. code-block:: console

    # print only pcap headers information
    pyroute2-decoder ... | jq '.[]."pcap header"'

pcap data dumps
~~~~~~~~~~~~~~~

This format is the default for `pyroute2-decoder`. To explicitly instruct
the decoder to use the pcap format, use `-f pcap` or `--format pcap`.

An ordinary everyday normal pcap dumps produced by tcpdump. The format
is described here shortly and only to the extent that is important for
the decoder. Please see other resources for detailed pcap format
descriptions. Pyroute2 decoder expect these headers in the pcap dump:

* Pcap file header. This header is being decoded, but not used by the
  tools as for now.
* Packet header. From this header the decoder uses only `incl_len` to
  properly read the stored data.
* Link layer header. From this header only the family field is used as
  for now, it can be matched with `ll_header{family=...}` expression.

hex data dumps
~~~~~~~~~~~~~~

Use `-f hex` or `--format hex`.

Just a raw data flow with no service headers added. The decoder uses
message headers to calculate the buffer lengths to read. This dump
can be obtained using strace or the IPBatch compiler.

Data should use hex bytes representation either in escaped or in
colon separated format. Equivalent variants:

* `\\\\x49\\\\x61\\\\x03\\\\x55`
* `49:61:03:55`

Comment strings start with `#`, comments and whitespaces are ignored.
A message example:

.. code-block::

    # ifinfmsg headers
    #
    # nlmsg header
    \\x84\\x00\\x00\\x00  # length
    \\x10\\x00          # type
    \\x05\\x06          # flags
    \\x49\\x61\\x03\\x55  # sequence number
    \\x00\\x00\\x00\\x00  # pid
    # RTNL header
    \\x00\\x00          # ifi_family
    \\x00\\x00          # ifi_type
    \\x00\\x00\\x00\\x00  # ifi_index
    \\x00\\x00\\x00\\x00  # ifi_flags
    \\x00\\x00\\x00\\x00  # ifi_change
    # ...


message classes
~~~~~~~~~~~~~~~

In order to properly debug the stream, one should specify either
a message class, or a marshal class:

.. code-block:: console

    # use a message class
    pyroute2-decoder \\
            -c pyroute2.netlink.generic.ipvs.ipvsmsg \\
            ...

    # use a marshal class
    pyroute2-decoder \\
            -c pyroute2.netlink.rtnl.marshal.MarshalRtnl \\
            ...

The decoder will try to use the specified class to decode every
matching message. That work well for generic protocols, but for other
protocols like RTNL it's more convenient to use marshal classes
that return corresponding message classes for different message
types.

generic protocols ids
~~~~~~~~~~~~~~~~~~~~~

Generic netlink protocols have dynamic IDs, so the first operation is to
get the ID. The message class used for that is `pyroute2.netlink.ctrlmsg`,
the request is `CTRL_CMD_GETFAMILY == 3`, and the response is
`CTRL_CMD_NEWFAMILY == 1`. The command is one byte right after the netlink
header, so the filters are:

* `ll_header{family=16}` match family 16, NETLINK_GENERIC
* `data{fmt='B', offset=16, value=1}` match one byte with
  value 1 by offset 16

Here is the code to get the family ID:

.. code-block:: console

    pyroute2-decoder \\
        -c pyroute2.netlink.ctrlmsg \\
        -d nl.pcap \\
        -m "ll_header{family=16} AND data{fmt='B', offset=16, value=1}" | \\
    jq \\
        '.[0].data.attrs[] | select(.[0] | contains("FAMILY"))'

    [
          "CTRL_ATTR_FAMILY_NAME",
          "IPVS"
    ]
    [
          "CTRL_ATTR_FAMILY_ID",
          37
    ]

Having the family ID you can filter out relevant messages. The filters:

* `ll_header{family=16}` match family 16, NETLINK_GENERIC
* `data{fmt='H', offset=4, value=37}` match IPVS family ID in
  the message header
* `data{fmt='B', offset=16, value=1}` match IPVS_CMD_NEW_SERVICE

.. code-block:: console

    pyroute2-decoder \\
        -c pyroute2.netlink.generic.ipvs.ipvsmsg \\
        -d nl0.pcap \\
        -m "ll_header{family=16} \\
            AND data{fmt='H', offset=4, value=37} \\
            AND data{fmt='B', offset=16, value=1}"


'''

import json

from pyroute2.common import hexdump
from pyroute2.decoder.args import parse_args
from pyroute2.decoder.loader import get_loader


def run():
    loader = get_loader(parse_args())
    ret = []
    for message in loader.data:
        ret.append(message.dump())
    print(json.dumps(ret, indent=4, default=lambda x: hexdump(x)))


if __name__ == "__main__":
    run()
