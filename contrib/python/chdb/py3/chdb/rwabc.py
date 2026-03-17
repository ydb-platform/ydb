from abc import ABC, abstractmethod
from typing import List, Any


class PyReader(ABC):
    def __init__(self, data: Any):
        """
        Initialize the reader with data. The exact type and structure of `data` can vary.

        Args:
            data (Any): The data with which to initialize the reader, format and type are not strictly defined.
        """
        self.data = data

    @abstractmethod
    def read(self, col_names: List[str], count: int) -> List[Any]:
        """
        Read a specified number of rows from the given columns and return a list of objects,
        where each object is a sequence of values for a column.

        Args:
            col_names (List[str]): List of column names to read.
            count (int): Maximum number of rows to read.

        Returns:
            List[Any]: List of sequences, one for each column.
        """
        pass


class PyWriter(ABC):
    def __init__(self, col_names: List[str], types: List[type], data: Any):
        """
        Initialize the writer with column names, their types, and initial data.

        Args:
            col_names (List[str]): List of column names.
            types (List[type]): List of types corresponding to each column.
            data (Any): Initial data to setup the writer, format and type are not strictly defined.
        """
        self.col_names = col_names
        self.types = types
        self.data = data
        self.blocks = []

    @abstractmethod
    def write(self, col_names: List[str], columns: List[List[Any]]) -> None:
        """
        Save columns of data to blocks. Must be implemented by subclasses.

        Args:
            col_names (List[str]): List of column names that are being written.
            columns (List[List[Any]]): List of columns data, each column is represented by a list.
        """
        pass

    @abstractmethod
    def finalize(self) -> bytes:
        """
        Assemble and return the final data from blocks. Must be implemented by subclasses.

        Returns:
            bytes: The final serialized data.
        """
        pass
