# coding=utf-8
#
# Copyright © 2011-2015 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""

.. topic:: Design Notes

  1. Commands are constrained to this ABNF grammar::

        command       = command-name *[wsp option] *[wsp [dquote] field-name [dquote]]
        command-name  = alpha *( alpha / digit )
        option        = option-name [wsp] "=" [wsp] option-value
        option-name   = alpha *( alpha / digit / "_" )
        option-value  = word / quoted-string
        word          = 1*( %01-%08 / %0B / %0C / %0E-1F / %21 / %23-%FF ) ; Any character but DQUOTE and WSP
        quoted-string = dquote *( word / wsp / "\" dquote / dquote dquote ) dquote
        field-name    = ( "_" / alpha ) *( alpha / digit / "_" / "." / "-" )

     It does not show that :code:`field-name` values may be comma-separated. This is because Splunk strips commas from
     the command line. A search command will never see them.

  2. Search commands targeting versions of Splunk prior to 6.3 must be statically configured as follows:

     .. code-block:: text
        :linenos:

        [command_name]
        filename = command_name.py
        supports_getinfo = true
        supports_rawargs = true

     No other static configuration is required or expected and may interfere with command execution.

  3. Commands support dynamic probing for settings.

     Splunk probes for settings dynamically when :code:`supports_getinfo=true`.
     You must add this line to the commands.conf stanza for each of your search
     commands.

  4. Commands do not support parsed arguments on the command line.

     Splunk parses arguments when :code:`supports_rawargs=false`. The
     :code:`SearchCommand` class sets this value unconditionally. You cannot
     override it.

     **Rationale**

     Splunk parses arguments by stripping quotes, nothing more. This may be useful
     in some cases, but doesn't work well with our chosen grammar.

  5. Commands consume input headers.

     An input header is provided by Splunk when :code:`enableheader=true`. The
     :class:`SearchCommand` class sets this value unconditionally. You cannot
     override it.

  6. Commands produce an output messages header.

     Splunk expects a command to produce an output messages header when
     :code:`outputheader=true`. The :class:`SearchCommand` class sets this value
     unconditionally. You cannot override it.

  7. Commands support multi-value fields.

     Multi-value fields are provided and consumed by Splunk when
     :code:`supports_multivalue=true`. This value is fixed. You cannot override
     it.

  8. This module represents all fields on the output stream in multi-value
     format.

     Splunk recognizes two kinds of data: :code:`value` and :code:`list(value)`.
     The multi-value format represents these data in field pairs. Given field
     :code:`name` the multi-value format calls for the creation of this pair of
     fields.

     ================= =========================================================
     Field name         Field data
     ================= =========================================================
     :code:`name`      Value or text from which a list of values was derived.

     :code:`__mv_name` Empty, if :code:`field` represents a :code:`value`;
                       otherwise, an encoded :code:`list(value)`. Values in the
                       list are wrapped in dollar signs ($) and separated by
                       semi-colons (;). Dollar signs ($) within a value are
                       represented by a pair of dollar signs ($$).
     ================= =========================================================

     Serializing data in this format enables streaming and reduces a command's
     memory footprint at the cost of one extra byte of data per field per record
     and a small amount of extra processing time by the next command in the
     pipeline.

  9. A :class:`ReportingCommand` must override :meth:`~ReportingCommand.reduce`
     and may override :meth:`~ReportingCommand.map`. Map/reduce commands on the
     Splunk processing pipeline are distinguished as this example illustrates.

     **Splunk command**

     .. code-block:: text

         sum total=total_date_hour date_hour

     **Map command line**

     .. code-block:: text

        sum __GETINFO__ __map__ total=total_date_hour date_hour
        sum __EXECUTE__ __map__ total=total_date_hour date_hour

     **Reduce command line**

     .. code-block:: text

        sum __GETINFO__ total=total_date_hour date_hour
        sum __EXECUTE__ total=total_date_hour date_hour

     The :code:`__map__` argument is introduced by
     :meth:`ReportingCommand._execute`. Search command authors cannot influence
     the contents of the command line in this release.

.. topic:: References

  1. `Custom Search Command manual: <https://dev.splunk.com/enterprise/docs/devtools/customsearchcommands>`__

  2. `Create Custom Search Commands with commands.conf.spec <http://docs.splunk.com/Documentation/Splunk/latest/Admin/Commandsconf>`_

  3. `Configure seach assistant with searchbnf.conf <https://docs.splunk.com/Documentation/Splunk/latest/Admin/Searchbnfconf>`_
  
  4. `Control search distribution with distsearch.conf <https://docs.splunk.com/Documentation/Splunk/latest/Admin/Distsearchconf>`_

"""

from __future__ import absolute_import, division, print_function, unicode_literals

from .environment import *
from .decorators import *
from .validators import *

from .generating_command import GeneratingCommand
from .streaming_command import StreamingCommand
from .eventing_command import EventingCommand
from .reporting_command import ReportingCommand

from .external_search_command import execute, ExternalSearchCommand
from .search_command import dispatch, SearchMetric
