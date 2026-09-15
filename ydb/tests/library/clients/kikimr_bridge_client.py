#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import logging

import grpc

from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc as grpc_server
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge_api
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

logger = logging.getLogger()


def bridge_client_factory(server, port, cluster=None, retry_count=1, timeout=None):
    return BridgeClient(
        server, port, cluster=cluster,
        retry_count=retry_count, timeout=timeout
    )


class BridgeClient(object):
    def __init__(self, server, port, cluster=None, retry_count=1, timeout=None):
        self.server = server
        self.port = port
        self._cluster = cluster
        self.__retry_count = retry_count
        self.__retry_sleep_seconds = 10
        self._timeout = timeout
        self._options = [
            ('grpc.max_receive_message_length', 64 * 10 ** 6),
            ('grpc.max_send_message_length', 64 * 10 ** 6)
        ]
        self._channel = grpc.insecure_channel("%s:%s" % (self.server, self.port), options=self._options)
        self._stub = grpc_server.BridgeServiceStub(self._channel)
        self._auth_token = None

    def set_auth_token(self, token):
        self._auth_token = token

    def _get_invoke_callee(self, method):
        return getattr(self._stub, method)

    def invoke(self, request, method, retry_status_codes=None):
        """
        Invoke a method on the bridge service with retry logic.

        Args:
            request: The request object
            method: The method name to call
            retry_status_codes: List of status codes that should trigger a retry
                               (e.g., [StatusIds.INTERNAL_ERROR, StatusIds.UNAVAILABLE])
        """
        if retry_status_codes is None:
            retry_status_codes = [StatusIds.INTERNAL_ERROR, StatusIds.UNAVAILABLE, StatusIds.TIMEOUT]

        retry = self.__retry_count
        while True:
            try:
                callee = self._get_invoke_callee(method)
                metadata = []
                if self._auth_token:
                    metadata.append(('x-ydb-auth-ticket', self._auth_token))

                response = callee(request, metadata=metadata, timeout=self._timeout)

                # Check if response status should trigger a retry
                if (hasattr(response, 'operation') and
                        hasattr(response.operation, 'status') and
                        response.operation.status in retry_status_codes):

                    logger.debug("Response status %s triggers retry, attempts left: %d",
                                 response.operation.status, retry - 1)

                    retry -= 1
                    if not retry:
                        return response  # Return the failed response instead of raising

                    time.sleep(self.__retry_sleep_seconds)
                    continue

                return response

            except (RuntimeError, grpc.RpcError):
                retry -= 1

                if not retry:
                    raise

                time.sleep(self.__retry_sleep_seconds)

    def get_cluster_state(self):
        request = bridge_api.GetClusterStateRequest()
        return self.invoke(request, 'GetClusterState')

    def get_cluster_state_result(self):
        response = self.get_cluster_state()
        if response.operation.status != StatusIds.SUCCESS:
            logger.error("Failed to get cluster state: %s", response.operation.status)
            return None

        result = bridge_api.GetClusterStateResult()
        response.operation.result.Unpack(result)
        return result

    def update_cluster_state(self, updates, quorum_piles=None):
        request = bridge_api.UpdateClusterStateRequest()
        request.updates.extend(updates)
        if quorum_piles is not None:
            request.quorum_piles.extend(quorum_piles)
        return self.invoke(request, 'UpdateClusterState')

    def update_cluster_state_result(self, updates, quorum_piles=None, expected_status=StatusIds.SUCCESS):
        response = self.update_cluster_state(updates, quorum_piles)
        logger.debug("Update cluster state response: %s", response)
        if response.operation.status != expected_status:
            logger.error("Failed to update cluster state: %s", response.operation.status)
            return None

        result = bridge_api.UpdateClusterStateResult()
        response.operation.result.Unpack(result)
        return result

    @property
    def per_pile_state(self):
        """
        Find all piles from cluster state.

        Returns:
            List of PileState objects
        """
        cluster_state = self.get_cluster_state_result()
        if cluster_state is None:
            logger.error("Failed to get cluster state")
            return None

        return cluster_state.pile_states

    @property
    def primary_pile(self):
        """
        Check if pile is primary.
        """
        for pile in self.per_pile_state:
            if pile.state == PileState.PRIMARY:
                return pile.pile_name
        return None

    def switchover(self, primary_pile_id):
        """
        Switch pile to primary.

        Args:
            pile_id: Pile ID to switch to primary

        Returns:
            True if successful, False otherwise
        """
        updates = [
            PileState(pile_name=primary_pile_id, state=PileState.PRIMARY),
        ]
        result = self.update_cluster_state_result(updates)
        if result is None:
            logger.error("Failed to update pile %d to PRIMARY", primary_pile_id)
            return False

        logger.info("Switched: pile %d to PRIMARY", primary_pile_id)
        return True

    def failover(self, pile_name, primary_pile_name=None):
        """
        Switch pile to disconnected and another pile to primary.

        Args:
            pile_name: Pile name to switch to disconnected
            primary_pile_name: Pile name with primary state

        Returns:
            True if successful, False otherwise
        """

        updates = []
        current_primary_pile_name = self.primary_pile
        if pile_name == current_primary_pile_name:
            synchronized_pile_name = None
            if primary_pile_name is not None:
                synchronized_pile_name = primary_pile_name
            else:
                for pile in self.per_pile_state:
                    if pile.state == PileState.SYNCHRONIZED:
                        synchronized_pile_name = pile.pile_name
                        break

            if synchronized_pile_name is None:
                logger.error("No synchronized pile found")
                return False

            updates.append(PileState(pile_name=synchronized_pile_name, state=PileState.PRIMARY))

        updates.append(PileState(pile_name=pile_name, state=PileState.DISCONNECTED))
        result = self.update_cluster_state_result(updates)
        if result is None:
            if pile_name == current_primary_pile_name:
                logger.error("Failed to update pile %s to PRIMARY", synchronized_pile_name)
            logger.error("Failed to update pile %s to DISCONNECTED", pile_name)
            return False

        if pile_name == current_primary_pile_name:
            logger.info("Switched: pile %s to PRIMARY", synchronized_pile_name)
        logger.info("Switched: pile %s to DISCONNECTED", pile_name)
        return True

    def rejoin(self, pile_name, primary_pile_name=None):
        """
        Rejoin pile to unsynchronized using specific pile ids.

        Args:
            pile_name: Pile name to restore
            primary_pile_name: Pile name to use for restore

        Returns:
            True if successful, False otherwise
        """

        # TODO (@apkobzev): Implement split brain recovery

        current_primary_pile_name = self.primary_pile
        updates = [
            PileState(pile_name=pile_name, state=PileState.NOT_SYNCHRONIZED),
        ]
        result = self.update_cluster_state_result(updates, quorum_piles=[current_primary_pile_name])
        if result is None:
            logger.error("Failed to update pile %s to NOT_SYNCHRONIZED using specific pile ids [%s]", pile_name, current_primary_pile_name)
            return False

        logger.info("Switched: pile %s from DISCONNECTED to NOT_SYNCHRONIZED using specific pile ids [%s]", pile_name, current_primary_pile_name)

        result = self.update_cluster_state_result(updates, quorum_piles=[pile_name])
        if result is None:
            logger.error("Failed to update pile %s to NOT_SYNCHRONIZED using specific pile ids [%s]", pile_name, pile_name)
            return False

        logger.info("Switched: pile %s from DISCONNECTED to NOT_SYNCHRONIZED using specific pile ids [%s]", pile_name, pile_name)
        return True

    def takedown(self, pile_name, primary_pile_name=None):
        """
        Takedown pile.

        Args:
            pile_name: Pile name to takedown

        Returns:
            True if successful, False otherwise
        """
        updates = []
        current_primary_pile_name = self.primary_pile
        if pile_name == current_primary_pile_name:
            synchronized_pile_name = None
            if primary_pile_name is not None:
                synchronized_pile_name = primary_pile_name
            else:
                for pile in self.per_pile_state:
                    if pile.state == PileState.SYNCHRONIZED:
                        synchronized_pile_name = pile.pile_name
                        break

            if synchronized_pile_name is None:
                logger.error("No synchronized pile found")
                return False

            updates.append(PileState(pile_name=synchronized_pile_name, state=PileState.PRIMARY))

        updates.append(PileState(pile_name=pile_name, state=PileState.DISCONNECTED))
        result = self.update_cluster_state_result(updates)
        if result is not None:
            if current_primary_pile_name == pile_name:
                logger.info("Switched: pile %s to PRIMARY", synchronized_pile_name)
            logger.info("Switched: pile %s to DISCONNECTED", pile_name)
        else:
            if current_primary_pile_name == pile_name:
                logger.error("Failed to update pile %s to PRIMARY", synchronized_pile_name)
            logger.error("Failed to update pile %s to DISCONNECTED", pile_name)
            return False

        return True

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()
