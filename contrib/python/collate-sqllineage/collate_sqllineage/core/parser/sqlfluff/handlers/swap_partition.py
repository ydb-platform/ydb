from sqlfluff.core.parser import BaseSegment

from collate_sqllineage.core.holders import SubQueryLineageHolder
from collate_sqllineage.core.parser.sqlfluff.handlers.base import SegmentBaseHandler
from collate_sqllineage.core.parser.sqlfluff.models import SqlFluffTable
from collate_sqllineage.core.parser.sqlfluff.utils import (
    get_grandchild,
    get_grandchildren,
)
from collate_sqllineage.utils.helpers import escape_identifier_name


class SwapPartitionHandler(SegmentBaseHandler):
    """
    A handler for swap_partitions_between_tables function
    """

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        """
        Handle the segment, and update the lineage result accordingly in the holder
        :param segment: segment to be handled
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        """
        function_from_select = get_grandchild(
            segment, "select_clause_element", "function"
        )
        if (
            function_from_select
            and function_from_select.first_non_whitespace_segment_raw_upper
            == "SWAP_PARTITIONS_BETWEEN_TABLES"
        ):
            function_parameters = get_grandchildren(
                function_from_select, "bracketed", "expression"
            )
            if function_parameters:
                holder.add_read(
                    SqlFluffTable(escape_identifier_name(function_parameters[0].raw))
                )
                holder.add_write(
                    SqlFluffTable(escape_identifier_name(function_parameters[3].raw))
                )
            else:
                function_parameters = get_grandchild(
                    function_from_select, "function_contents", "bracketed"
                )
                if function_parameters:
                    function_parameters = function_parameters.get_children("expression")
                    if function_parameters:
                        holder.add_read(
                            SqlFluffTable(
                                escape_identifier_name(function_parameters[0].raw)
                            )
                        )
                        holder.add_write(
                            SqlFluffTable(
                                escape_identifier_name(function_parameters[3].raw)
                            )
                        )
