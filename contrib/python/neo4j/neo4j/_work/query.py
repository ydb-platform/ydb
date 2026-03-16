# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from __future__ import annotations

from .. import _typing as t


if t.TYPE_CHECKING:
    _T = t.TypeVar("_T")


class Query:
    """
    A query with attached extra data.

    This wrapper class for queries is used to attach extra data to queries
    passed to :meth:`.Session.run`/:meth:`.AsyncSession.run` and
    :meth:`.Driver.execute_query`/:meth:`.AsyncDriver.execute_query`,
    fulfilling a similar role as :func:`.unit_of_work` for transactions
    functions.

    :param text: The query text.
    :type text: typing.LiteralString
    :param metadata:
        a dictionary with metadata.
        Specified metadata will be attached to the executing transaction
        and visible in the output of ``SHOW TRANSACTIONS YIELD *``
        It will also get logged to the ``query.log``.
        This functionality makes it easier to tag transactions and is
        equivalent to the ``dbms.setTXMetaData`` procedure, see
        https://neo4j.com/docs/cypher-manual/current/clauses/transaction-clauses/#query-listing-transactions
        and https://neo4j.com/docs/operations-manual/current/reference/procedures/
        for reference.
    :type metadata: typing.Dict[str, typing.Any] | None
    :param timeout:
        the transaction timeout in seconds.
        Transactions that execute longer than the configured timeout will
        be terminated by the database.
        This functionality allows user code to limit query/transaction
        execution time.
        The specified timeout overrides the default timeout configured in
        the database using the ``db.transaction.timeout`` setting
        (``dbms.transaction.timeout`` before Neo4j 5.0).
        Values higher than ``db.transaction.timeout`` will be ignored and
        will fall back to the default for server versions between 4.2 and
        5.2 (inclusive).
        The value should not represent a negative duration.
        A ``0`` duration will make the transaction execute indefinitely.
        :data:`None` will use the default timeout configured on the server.
    :type timeout: float | None
    """

    def __init__(
        self,
        text: t.LiteralString,
        metadata: dict[str, t.Any] | None = None,
        timeout: float | None = None,
    ) -> None:
        self.text = text

        self.metadata = metadata
        self.timeout = timeout

    def __str__(self) -> t.LiteralString:
        # we know that if Query is constructed with a LiteralString,
        # str(self.text) will be a LiteralString as well. The conversion isn't
        # necessary if the user adheres to the type hints. However, it was
        # here before, and we don't want to break backwards compatibility.
        text: t.LiteralString = str(self.text)  # type: ignore[assignment]
        return text


def unit_of_work(
    metadata: dict[str, t.Any] | None = None,
    timeout: float | None = None,
) -> t.Callable[[_T], _T]:
    """
    Configure a transaction function.

    This function is a decorator for transaction functions that allows extra
    control over how the transaction is carried out.

    For example, a timeout may be applied::

        from neo4j import unit_of_work


        @unit_of_work(timeout=100)
        def count_people_tx(tx):
            result = tx.run("MATCH (a:Person) RETURN count(a) AS persons")
            record = result.single()
            return record["persons"]

    :param metadata:
        a dictionary with metadata.
        Specified metadata will be attached to the executing transaction
        and visible in the output of ``SHOW TRANSACTIONS YIELD *``
        It will also get logged to the ``query.log``.
        This functionality makes it easier to tag transactions and is
        equivalent to the ``dbms.setTXMetaData`` procedure, see
        https://neo4j.com/docs/cypher-manual/current/clauses/transaction-clauses/#query-listing-transactions
        and https://neo4j.com/docs/operations-manual/current/reference/procedures/
        for reference.
    :type metadata: typing.Dict[str, typing.Any] | None

    :param timeout:
        the transaction timeout in seconds.
        Transactions that execute longer than the configured timeout will
        be terminated by the database.
        This functionality allows user code to limit query/transaction
        execution time.
        The specified timeout overrides the default timeout configured in
        the database using the ``db.transaction.timeout`` setting
        (``dbms.transaction.timeout`` before Neo4j 5.0).
        Values higher than ``db.transaction.timeout`` will be ignored and
        will fall back to the default for server versions between 4.2 and
        5.2 (inclusive).
        The value should not represent a negative duration.
        A ``0`` duration will make the transaction execute indefinitely.
        :data:`None` will use the default timeout configured on the server.
    :type timeout: float | None

    :rtype: typing.Callable[[T], T]
    """

    def wrapper(f):
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        wrapped.metadata = metadata
        wrapped.timeout = timeout
        return wrapped

    return wrapper
