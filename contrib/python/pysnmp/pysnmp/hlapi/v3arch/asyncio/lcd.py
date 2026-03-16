#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pysnmp import error, nextid
from pysnmp.entity import config
from pysnmp.entity.engine import SnmpEngine
from pysnmp.hlapi.v3arch.asyncio.auth import CommunityData, UsmUserData
from pysnmp.hlapi.v3arch.asyncio.transport import AbstractTransportTarget

__all__ = ["CommandGeneratorLcdConfigurator", "NotificationOriginatorLcdConfigurator"]


class AbstractLcdConfigurator:
    next_id = nextid.Integer(0xFFFFFFFF)
    cache_keys = []

    def _get_cache(self, snmpEngine: SnmpEngine):
        cacheId = self.__class__.__name__
        cache = snmpEngine.get_user_context(cacheId)
        if cache is None:
            cache = {x: {} for x in self.cache_keys}
            snmpEngine.set_user_context(**{cacheId: cache})
        return cache

    def configure(self, snmpEngine, *args, **kwargs):
        pass

    def unconfigure(self, snmpEngine, *args, **kwargs):
        pass


class CommandGeneratorLcdConfigurator(AbstractLcdConfigurator):
    """Local configuration data (LCD) for Command Generator."""

    cache_keys = ["auth", "parm", "tran", "addr"]

    def configure(
        self,
        snmpEngine: SnmpEngine,
        authData,
        transportTarget: AbstractTransportTarget,
        contextName=b"",
        **options,
    ):
        """Configure command generator targets on the SNMP engine."""
        cache = self._get_cache(snmpEngine)
        if isinstance(authData, CommunityData):
            if authData.communityIndex not in cache["auth"]:
                config.add_v1_system(
                    snmpEngine,
                    authData.communityIndex,
                    authData.communityName,
                    authData.contextEngineId,
                    authData.context_name,
                    authData.tag,
                    authData.securityName,
                )
                cache["auth"][authData.communityIndex] = authData
        elif isinstance(authData, UsmUserData):
            authDataKey = authData.userName, authData.securityEngineId
            if authDataKey not in cache["auth"]:
                add_user = True

            elif self._usm_auth_changed(cache["auth"][authDataKey], authData):
                config.delete_v3_user(
                    snmpEngine, authData.userName, authData.securityEngineId
                )
                add_user = True

            else:
                add_user = False

            if add_user:
                config.add_v3_user(
                    snmpEngine,
                    authData.userName,
                    authData.authentication_protocol,
                    authData.authentication_key,
                    authData.privacy_protocol,
                    authData.privacy_key,
                    securityEngineId=authData.securityEngineId,
                    securityName=authData.securityName,
                    authKeyType=authData.authKeyType,
                    privKeyType=authData.privKeyType,
                )
                cache["auth"][authDataKey] = authData
        else:
            raise error.PySnmpError("Unsupported authentication object")

        paramsKey = (
            authData.securityName,
            authData.security_level,
            authData.message_processing_model,
        )
        if paramsKey in cache["parm"]:
            paramsName, useCount = cache["parm"][paramsKey]
            cache["parm"][paramsKey] = paramsName, useCount + 1
        else:
            paramsName = "p%s" % self.next_id()
            config.add_target_parameters(
                snmpEngine,
                paramsName,
                authData.securityName,
                authData.security_level,
                authData.message_processing_model,
            )
            cache["parm"][paramsKey] = paramsName, 1

        if transportTarget.TRANSPORT_DOMAIN in cache["tran"]:
            transport, useCount = cache["tran"][transportTarget.TRANSPORT_DOMAIN]
            transportTarget.verify_dispatcher_compatibility(snmpEngine)
            cache["tran"][transportTarget.TRANSPORT_DOMAIN] = transport, useCount + 1
        elif config.get_transport(snmpEngine, transportTarget.TRANSPORT_DOMAIN):
            transportTarget.verify_dispatcher_compatibility(snmpEngine)
        else:
            transport = transportTarget.open_client_mode()
            config.add_transport(
                snmpEngine, transportTarget.TRANSPORT_DOMAIN, transport
            )
            cache["tran"][transportTarget.TRANSPORT_DOMAIN] = transport, 1

        transportKey = (
            paramsName,
            transportTarget.TRANSPORT_DOMAIN,
            transportTarget.transport_address,
            transportTarget.timeout,
            transportTarget.retries,
            transportTarget.tagList,
            transportTarget.iface,
        )

        if transportKey in cache["addr"]:
            addrName, useCount = cache["addr"][transportKey]
            cache["addr"][transportKey] = addrName, useCount + 1
        else:
            addrName = "a%s" % self.next_id()
            config.add_target_address(
                snmpEngine,
                addrName,
                transportTarget.TRANSPORT_DOMAIN,
                transportTarget.transport_address,
                paramsName,
                transportTarget.timeout * 100,
                transportTarget.retries,
                transportTarget.tagList,
            )
            cache["addr"][transportKey] = addrName, 1

        return addrName, paramsName

    def unconfigure(
        self, snmpEngine: SnmpEngine, authData=None, contextName=b"", **options
    ):
        """Remove command generator targets from the SNMP engine."""
        cache = self._get_cache(snmpEngine)
        if authData:
            if isinstance(authData, CommunityData):
                authDataKey = authData.communityIndex
            elif isinstance(authData, UsmUserData):
                authDataKey = authData.userName, authData.securityEngineId
            else:
                raise error.PySnmpError("Unsupported authentication object")
            if authDataKey in cache["auth"]:
                authDataKeys = (authDataKey,)
            else:
                raise error.PySnmpError(f"Unknown authData {authData}")
        else:
            authDataKeys = list(cache["auth"].keys())

        addrNames, paramsNames = set(), set()

        for authDataKey in authDataKeys:
            authDataX = cache["auth"][authDataKey]
            del cache["auth"][authDataKey]
            if isinstance(authDataX, CommunityData):
                config.delete_v1_system(snmpEngine, authDataX.communityIndex)
            elif isinstance(authDataX, UsmUserData):
                config.delete_v3_user(
                    snmpEngine, authDataX.userName, authDataX.securityEngineId
                )
            else:
                raise error.PySnmpError("Unsupported authentication object")

            paramsKey = (
                authDataX.securityName,
                authDataX.security_level,
                authDataX.message_processing_model,
            )
            if paramsKey in cache["parm"]:
                paramsName, useCount = cache["parm"][paramsKey]
                useCount -= 1
                if useCount:
                    cache["parm"][paramsKey] = paramsName, useCount
                else:
                    del cache["parm"][paramsKey]
                    config.delete_target_parameters(snmpEngine, paramsName)
                    paramsNames.add(paramsName)
            else:
                raise error.PySnmpError(f"Unknown target {paramsKey}")

            addrKeys = [x for x in cache["addr"] if x[0] == paramsName]

            for addrKey in addrKeys:
                addrName, useCount = cache["addr"][addrKey]
                useCount -= 1
                if useCount:
                    cache["addr"][addrKey] = addrName, useCount
                else:
                    config.delete_target_address(snmpEngine, addrName)
                    del cache["addr"][addrKey]
                    addrNames.add(addrKey)

                    if addrKey[1] in cache["tran"]:
                        transport, useCount = cache["tran"][addrKey[1]]
                        if useCount > 1:
                            useCount -= 1
                            cache["tran"][addrKey[1]] = transport, useCount
                        else:
                            config.delete_transport(snmpEngine, addrKey[1])
                            transport.closeTransport()
                            del cache["tran"][addrKey[1]]

        return addrNames, paramsNames

    @staticmethod
    def _usm_auth_changed(cachedAuthData: UsmUserData, newAuthData: UsmUserData):
        changed = False

        changed |= cachedAuthData.authentication_key != newAuthData.authentication_key
        changed |= (
            cachedAuthData.authentication_protocol
            != newAuthData.authentication_protocol
        )
        changed |= cachedAuthData.privacy_key != newAuthData.privacy_key
        changed |= cachedAuthData.privacy_protocol != newAuthData.privacy_protocol

        return changed


class NotificationOriginatorLcdConfigurator(AbstractLcdConfigurator):
    """Local configuration data (LCD) for Notification Originator."""

    cache_keys = ["auth", "name"]
    _lcd_configurator = CommandGeneratorLcdConfigurator()

    def configure(
        self,
        snmpEngine: SnmpEngine,
        authData: "CommunityData | UsmUserData",
        transportTarget: AbstractTransportTarget,
        notifyType: str,
        contextName=None,
        **options,
    ):
        """Configure notification targets on the SNMP engine."""
        cache = self._get_cache(snmpEngine)
        notifyName = None

        # Create matching transport tags if not given by user. Not good!
        if not transportTarget.tagList:
            transportTarget.tagList = str(
                hash((authData.securityName, transportTarget.transport_address))
            )
        if isinstance(authData, CommunityData) and not authData.tag:
            authData.tag = transportTarget.tagList.split()[0]

        addrName, paramsName = self._lcd_configurator.configure(
            snmpEngine, authData, transportTarget, contextName, **options
        )
        tagList = transportTarget.tagList.split()
        if not tagList:
            tagList = [""]
        for tag in tagList:
            notifyNameKey = paramsName, tag, notifyType
            if notifyNameKey in cache["name"]:
                notifyName, paramsName, useCount = cache["name"][notifyNameKey]
                cache["name"][notifyNameKey] = notifyName, paramsName, useCount + 1
            else:
                notifyName = "n%s" % self.next_id()
                config.add_notification_target(
                    snmpEngine, notifyName, paramsName, tag, notifyType
                )
                cache["name"][notifyNameKey] = notifyName, paramsName, 1
        authDataKey = (
            authData.securityName,
            authData.security_model,
            authData.security_level,
            contextName,
        )
        if authDataKey in cache["auth"]:
            authDataX, subTree, useCount = cache["auth"][authDataKey]
            cache["auth"][authDataKey] = authDataX, subTree, useCount + 1
        else:
            subTree = (1, 3, 6)
            config.add_vacm_user(
                snmpEngine,
                authData.security_model,
                authData.securityName,
                authData.security_level,
                (),
                (),
                subTree,
                contextName=contextName,
            )
            cache["auth"][authDataKey] = authData, subTree, 1

        return notifyName

    def unconfigure(
        self, snmpEngine: SnmpEngine, authData=None, contextName=b"", **options
    ):
        """Remove notification targets from the SNMP engine."""
        cache = self._get_cache(snmpEngine)
        if authData:
            authDataKey = (
                authData.securityName,
                authData.securityModel,
                authData.securityLevel,
                contextName,
            )
            if authDataKey in cache["auth"]:
                authDataKeys = (authDataKey,)
            else:
                raise error.PySnmpError(f"Unknown authData {authData}")
        else:
            authDataKeys = tuple(cache["auth"])

        addrNames, paramsNames = self._lcd_configurator.unconfigure(
            snmpEngine, authData, contextName, **options
        )

        notifyAndParamsNames = [
            (cache["name"][x], x) for x in cache["name"].keys() if x[0] in paramsNames
        ]

        for (notifyName, paramsName, useCount), notifyNameKey in notifyAndParamsNames:
            useCount -= 1
            if useCount:
                cache["name"][notifyNameKey] = notifyName, paramsName, useCount
            else:
                config.delete_notification_target(snmpEngine, notifyName, paramsName)
                del cache["name"][notifyNameKey]

        for authDataKey in authDataKeys:
            authDataX, subTree, useCount = cache["auth"][authDataKey]
            useCount -= 1
            if useCount:
                cache["auth"][authDataKey] = authDataX, subTree, useCount
            else:
                config.delTrapUser(
                    snmpEngine,
                    authDataX.securityModel,
                    authDataX.securityName,
                    authDataX.securityLevel,
                    subTree,
                )
                del cache["auth"][authDataKey]
