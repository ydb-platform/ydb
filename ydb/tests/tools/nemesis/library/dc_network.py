# -*- coding: utf-8 -*-
"""
Инструментальная поддержка для сетевых DC nemesis.

Этот модуль содержит инфраструктуру для работы с DataCenter nemesis:
- Глобальный реестр для отслеживания активных nemesis
- Signal handlers для автоматической очистки при прерывании
- Интеграция с каталогом nemesis
"""
import signal
import sys
import logging
import atexit

from ydb.tests.library.nemesis.dc_nemesis_network import (
    DataCenterNetworkNemesis,
    SingleDataCenterFailureNemesis, 
    DataCenterRouteUnreachableNemesis,
    DataCenterIptablesBlockPortsNemesis
)


# Глобальный реестр для отслеживания активных DC nemesis
class DataCenterNemesisRegistry:
    def __init__(self):
        self._active_nemesis = {}
        self._logger = logging.getLogger("DCNemesisRegistry")
        self._signal_handlers_installed = False
    
    def register(self, nemesis):
        """Регистрирует DC nemesis в глобальном реестре"""
        nemesis_id = id(nemesis)
        self._active_nemesis[nemesis_id] = nemesis
        self._logger.info("Registered DC nemesis: %s (ID: %d)", nemesis.__class__.__name__, nemesis_id)
        
        # Устанавливаем signal handlers только один раз
        if not self._signal_handlers_installed:
            self._install_signal_handlers()
            self._signal_handlers_installed = True
    
    def unregister(self, nemesis):
        """Удаляет DC nemesis из реестра"""
        nemesis_id = id(nemesis)
        if nemesis_id in self._active_nemesis:
            del self._active_nemesis[nemesis_id]
            self._logger.info("Unregistered DC nemesis: %s (ID: %d)", nemesis.__class__.__name__, nemesis_id)
    
    def _install_signal_handlers(self):
        """Устанавливает обработчики сигналов"""
        self._logger.info("Installing signal handlers for DC nemesis cleanup")
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        atexit.register(self._cleanup_all_nemesis)
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов SIGTERM/SIGINT"""
        signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        self._logger.warning("Received %s signal, cleaning up DC nemesis...", signal_name)
        
        try:
            self._cleanup_all_nemesis()
            self._logger.info("DC nemesis cleanup completed successfully")
        except Exception as e:
            self._logger.error("Error during DC nemesis cleanup: %s", str(e))
        finally:
            sys.exit(0)
    
    def _cleanup_all_nemesis(self):
        """Очищает состояние всех зарегистрированных DC nemesis"""
        if not self._active_nemesis:
            self._logger.info("No active DC nemesis to cleanup")
            return
        
        self._logger.info("Cleaning up %d active DC nemesis", len(self._active_nemesis))
        
        for nemesis_id, nemesis in list(self._active_nemesis.items()):
            try:
                self._cleanup_single_nemesis(nemesis)
            except Exception as e:
                self._logger.error("Failed to cleanup nemesis %s (ID: %d): %s", 
                                 nemesis.__class__.__name__, nemesis_id, str(e))
        
        self._active_nemesis.clear()
    
    def _cleanup_single_nemesis(self, nemesis):
        """Очищает состояние одного nemesis с подробным логированием"""
        nemesis_name = nemesis.__class__.__name__
        self._logger.info("Cleaning up %s...", nemesis_name)
        
        # Получаем текущее состояние nemesis
        state_info = self._get_nemesis_state_info(nemesis)
        self._logger.info("%s current state: %s", nemesis_name, state_info)
        
        # Вызываем extract_fault для восстановления
        try:
            result = nemesis.extract_fault()
            if result:
                self._logger.info("%s cleanup completed successfully", nemesis_name)
            else:
                self._logger.info("%s had no active faults to cleanup", nemesis_name)
        except Exception as e:
            self._logger.error("%s cleanup failed: %s", nemesis_name, str(e))
            raise
    
    def _get_nemesis_state_info(self, nemesis):
        """Получает информацию о текущем состоянии nemesis"""
        if hasattr(nemesis, '_stopped_nodes') and nemesis._stopped_nodes:
            return f"stopped_nodes={len(nemesis._stopped_nodes)}, current_dc={getattr(nemesis, '_current_dc', 'None')}"
        elif hasattr(nemesis, '_blocked_routes') and nemesis._blocked_routes:
            return f"blocked_routes={len(nemesis._blocked_routes)}, current_dc={getattr(nemesis, '_current_dc', 'None')}"
        elif hasattr(nemesis, '_blocked_hosts') and nemesis._blocked_hosts:
            return f"blocked_hosts={len(nemesis._blocked_hosts)}, current_dc={getattr(nemesis, '_current_dc', 'None')}"
        else:
            return "no_active_faults"


# Глобальный экземпляр реестра
_dc_nemesis_registry = DataCenterNemesisRegistry()


class ManagedDataCenterNetworkNemesis(DataCenterNetworkNemesis):
    """
    DataCenterNetworkNemesis с автоматической регистрацией в signal handler.
    """
    
    def __init__(self, cluster, schedule=(300, 600), stop_duration=60):
        super(ManagedDataCenterNetworkNemesis, self).__init__(cluster, schedule, stop_duration)
        # Регистрируемся в глобальном реестре DC nemesis
        _dc_nemesis_registry.register(self)

    def extract_fault(self):
        result = super(ManagedDataCenterNetworkNemesis, self).extract_fault()
        if result:
            # Разрегистрируемся из глобального реестра при нормальном завершении
            _dc_nemesis_registry.unregister(self)
        return result


class ManagedSingleDataCenterFailureNemesis(SingleDataCenterFailureNemesis):
    """
    SingleDataCenterFailureNemesis с автоматической регистрацией в signal handler.
    """
    
    def __init__(self, cluster, schedule=(1200, 2400), stop_duration=3600):
        super(ManagedSingleDataCenterFailureNemesis, self).__init__(cluster, schedule, stop_duration)
        # Регистрируемся в глобальном реестре DC nemesis
        _dc_nemesis_registry.register(self)

    def extract_fault(self):
        result = super(ManagedSingleDataCenterFailureNemesis, self).extract_fault()
        if result:
            # Разрегистрируемся из глобального реестра при нормальном завершении
            _dc_nemesis_registry.unregister(self)
        return result


class ManagedDataCenterRouteUnreachableNemesis(DataCenterRouteUnreachableNemesis):
    """
    DataCenterRouteUnreachableNemesis с автоматической регистрацией в signal handler.
    """
    
    def __init__(self, cluster, schedule=(1800, 3600), block_duration=120):
        super(ManagedDataCenterRouteUnreachableNemesis, self).__init__(cluster, schedule, block_duration)
        # Регистрируемся в глобальном реестре DC nemesis
        _dc_nemesis_registry.register(self)

    def extract_fault(self):
        result = super(ManagedDataCenterRouteUnreachableNemesis, self).extract_fault()
        if result:
            # Разрегистрируемся из глобального реестра при нормальном завершении
            _dc_nemesis_registry.unregister(self)
        return result


class ManagedDataCenterIptablesBlockPortsNemesis(DataCenterIptablesBlockPortsNemesis):
    """
    DataCenterIptablesBlockPortsNemesis с автоматической регистрацией в signal handler.
    """
    
    def __init__(self, cluster, schedule=(1800, 3600), block_duration=120):
        super(ManagedDataCenterIptablesBlockPortsNemesis, self).__init__(cluster, schedule, block_duration)
        # Регистрируемся в глобальном реестре DC nemesis
        _dc_nemesis_registry.register(self)

    def extract_fault(self):
        result = super(ManagedDataCenterIptablesBlockPortsNemesis, self).extract_fault()
        if result:
            # Разрегистрируемся из глобального реестра при нормальном завершении
            _dc_nemesis_registry.unregister(self)
        return result


def datacenter_nemesis_list(cluster):
    """
    Создает список managed nemesis для тестирования отказов на уровне ДЦ.
    
    :param cluster: Кластер YDB
    :return: Список managed nemesis объектов с автоматическим signal handling
    """
    return [
        ManagedDataCenterNetworkNemesis(cluster),
        ManagedSingleDataCenterFailureNemesis(cluster),
        ManagedDataCenterRouteUnreachableNemesis(cluster),
        ManagedDataCenterIptablesBlockPortsNemesis(cluster)
    ] 