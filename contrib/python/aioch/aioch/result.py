from clickhouse_driver.progress import Progress


class QueryResult(object):
    """
    Stores query result from multiple blocks.
    """

    def __init__(
            self, packet_generator,
            with_column_types=False, columnar=False):
        self.packet_generator = packet_generator
        self.with_column_types = with_column_types

        self.data = []
        self.columns_with_types = []
        self.columnar = columnar

        super(QueryResult, self).__init__()

    def store(self, packet):
        block = getattr(packet, 'block', None)
        if block is None:
            return

        # Header block contains no rows. Pick columns from it.
        if block.rows:
            if self.columnar:
                columns = block.get_columns()
                if self.data:
                    # Extend corresponding column.
                    for i, column in enumerate(columns):
                        self.data[i] += column
                else:
                    self.data.extend(columns)
            else:
                self.data.extend(block.get_rows())

        elif not self.columns_with_types:
            self.columns_with_types = block.columns_with_types

    async def get_result(self):
        """
        :return: Stored query result.
        """

        async for packet in self.packet_generator:
            self.store(packet)

        if self.with_column_types:
            return self.data, self.columns_with_types
        else:
            return self.data


class ProgressQueryResult(QueryResult):
    """
    Stores query result and progress information from multiple blocks.
    Provides iteration over query progress.
    """

    def __init__(
            self, packet_generator,
            with_column_types=False, columnar=False):
        self.progress_totals = Progress()

        super(ProgressQueryResult, self).__init__(
            packet_generator, with_column_types, columnar
        )

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            packet = await self.packet_generator.__anext__()
            progress_packet = getattr(packet, 'progress', None)
            if progress_packet:
                self.progress_totals.increment(progress_packet)
                return (
                    self.progress_totals.rows, self.progress_totals.total_rows
                )
            else:
                self.store(packet)

    async def get_result(self):
        # Read all progress packets.
        async for _ in self:
            pass

        return await super(ProgressQueryResult, self).get_result()


class IterQueryResult(object):
    """
    Provides iteration over returned data by chunks (streaming by chunks).
    """

    def __init__(
            self, packet_generator,
            with_column_types=False):
        self.packet_generator = packet_generator
        self.with_column_types = with_column_types

        self.first_block = True
        super(IterQueryResult, self).__init__()

    def __aiter__(self):
        return self

    async def __anext__(self):
        packet = await self.packet_generator.__anext__()
        block = getattr(packet, 'block', None)
        if block is None:
            return []

        if self.first_block and self.with_column_types:
            self.first_block = False
            rv = [block.columns_with_types]
            rv.extend(block.get_rows())
            return rv
        else:
            return block.get_rows()
