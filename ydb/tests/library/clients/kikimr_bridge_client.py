#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import logging

import grpc

from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc as grpc_server
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge_api
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

    def update_cluster_state(self, updates, specific_pile_ids=None):
        request = bridge_api.UpdateClusterStateRequest()
        request.updates.extend(updates)
        if specific_pile_ids is not None:
            request.specific_pile_ids.extend(specific_pile_ids)
        return self.invoke(request, 'UpdateClusterState')

    def update_cluster_state_result(self, updates, specific_pile_ids=None, expected_status=StatusIds.SUCCESS):
        response = self.update_cluster_state(updates, specific_pile_ids)
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

        return cluster_state.per_pile_state

    def is_primary_pile(self, pile_id):
        """
        Check if pile is primary.
        """
        return self.per_pile_state[pile_id].state == bridge_api.PileState.PRIMARY

    def switchover_scenario(self, pile_id):
        """
        Switch pile to disconnected.

        Args:
            pile_id: Pile ID to switch to disconnected

        Returns:
            True if successful, False otherwise
        """
        updates = [
            bridge_api.PileStateUpdate(pile_id=pile_id, state=bridge_api.PileState.DISCONNECTED),
        ]
        result = self.update_cluster_state_result(updates)
        if result is None:
            logger.error("Failed to update pile %d to DISCONNECTED", pile_id)
            return False

        logger.info("Switched: pile %d to DISCONNECTED", pile_id)
        return True

    def failover_scenario(self, primary_pile_id, another_pile_id):
        """
        Switch primary pile to disconnected and another pile to primary.

        Args:
            primary_pile_id: Pile ID to switch to disconnected
            another_pile_id: Pile ID to switch to primary

        Returns:
            True if successful, False otherwise
        """
        updates = [
            bridge_api.PileStateUpdate(pile_id=another_pile_id, state=bridge_api.PileState.PRIMARY),
            bridge_api.PileStateUpdate(pile_id=primary_pile_id, state=bridge_api.PileState.DISCONNECTED),
        ]
        result = self.update_cluster_state_result(updates)
        if result is None:
            logger.error("Failed to update pile %d to PRIMARY and pile %d to DISCONNECTED", another_pile_id, primary_pile_id)
            return False

        logger.info("Switched: pile %d to PRIMARY", another_pile_id)
        logger.info("Switched: pile %d from PRIMARY to DISCONNECTED", primary_pile_id)
        return True

    def restore_scenario(self, disconnected_pile_id, another_pile_id):
        """
        Restore disconnected pile to unsynchronized using specific pile ids.

        Args:
            disconnected_pile_id: Pile ID to restore
            another_pile_id: Pile ID to use for restore

        Returns:
            True if successful, False otherwise
        """
        updates = [
            bridge_api.PileStateUpdate(pile_id=disconnected_pile_id, state=bridge_api.PileState.NOT_SYNCHRONIZED),
        ]
        result = self.update_cluster_state_result(updates, specific_pile_ids=[another_pile_id])
        if result is not None:
            logger.info("Switched: pile %d from DISCONNECTED to NOT_SYNCHRONIZED using specific pile ids [%d]", disconnected_pile_id, another_pile_id)
        else:
            logger.error("Failed to update pile %d to NOT_SYNCHRONIZED using specific pile ids [%d]", disconnected_pile_id, another_pile_id)
            return False

        result = self.update_cluster_state_result(updates, specific_pile_ids=[disconnected_pile_id])
        if result is not None:
            logger.info("Switched: pile %d from DISCONNECTED to NOT_SYNCHRONIZED using specific pile ids [%d]", disconnected_pile_id, disconnected_pile_id)
        else:
            logger.error("Failed to update pile %d to NOT_SYNCHRONIZED using specific pile ids [%d]", disconnected_pile_id, disconnected_pile_id)
            return False

        return True

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()
