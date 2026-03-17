"""StorageDescriptorBuilder."""
from typing import List, Dict

from hive_metastore_client.builders.abstract_builder import AbstractBuilder
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import (  # type: ignore # noqa: E501
    StorageDescriptor,
    FieldSchema,
    SerDeInfo,
    Order,
    SkewedInfo,
)


class StorageDescriptorBuilder(AbstractBuilder):
    """Builds thrift StorageDescriptor object."""

    def __init__(
        self,
        columns: List[FieldSchema],
        location: str,
        input_format: str,
        output_format: str,
        serde_info: SerDeInfo,
        compressed: bool = None,
        num_buckets: int = None,
        bucket_cols: List[str] = None,
        sort_cols: List[Order] = None,
        parameters: Dict[str, str] = None,
        skewed_info: SkewedInfo = None,
        stored_as_sub_directories: bool = None,
    ) -> None:
        """
        Constructor.

        :param columns: list<FieldSchema>
        :param location: the table file location path
        :param input_format: SequenceFileInputFormat (binary) or
        TextInputFormat or custom format
        :param output_format: SequenceFileOutputFormat (binary) or
        IgnoreKeyTextOutputFormat or custom format
        :param serde_info: ser. and des. information (SerDeInfo object)
        :param compressed: whether it is compressed or not
        :param num_buckets: must be specified if there are dimension columns
        :param bucket_cols: reducer grouping columns and clustering columns
        and bucketing columns
        :param sort_cols: sort order of the data in each bucket
        :param parameters: any user supplied key value hash
        :param skewed_info: skewed information
        :param stored_as_sub_directories: stored as subdirectories or not
        """
        self.columns = columns
        self.location = location
        self.input_format = input_format
        self.output_format = output_format
        self.compressed = compressed
        self.num_buckets = num_buckets
        self.serde_info = serde_info
        self.bucket_cols = bucket_cols
        self.sort_cols = sort_cols
        self.parameters = parameters
        self.skewed_info = skewed_info
        self.stored_as_sub_directories = stored_as_sub_directories

    def build(self) -> StorageDescriptor:
        """Returns the thrift StorageDescriptor object."""
        return StorageDescriptor(
            cols=self.columns,
            location=self.location,
            inputFormat=self.input_format,
            outputFormat=self.output_format,
            compressed=self.compressed,
            numBuckets=self.num_buckets,
            serdeInfo=self.serde_info,
            bucketCols=self.bucket_cols,
            sortCols=self.sort_cols,
            parameters=self.parameters,
            skewedInfo=self.skewed_info,
            storedAsSubDirectories=self.stored_as_sub_directories,
        )
