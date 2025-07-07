# -*- coding: utf-8 -*-
import unittest
import tempfile
from unittest.mock import Mock, patch

from ydb.tests.tools.nemesis.library.dc_network import datacenter_nemesis_list
from ydb.tests.library.nemesis.dc_nemesis_network import (
    SingleDataCenterFailureNemesis,
    DataCenterRouteUnreachableNemesis,
    DataCenterIptablesBlockPortsNemesis,
    validate_multiple_datacenters
)


class TestValidateMultipleDatacenters(unittest.TestCase):
    
    def setUp(self):
        # Создаем мок кластера
        self.cluster = Mock()
        
        test_config = {
            'hosts': [
                {'name': 'host1.zone-a.com', 'location': {'data_center': 'zone-a'}},
                {'name': 'host2.zone-a.com', 'location': {'data_center': 'zone-a'}},
                {'name': 'host3.zone-b.com', 'location': {'data_center': 'zone-b'}},
            ]
        }
        self.cluster._ExternalKiKiMRCluster__yaml_config = test_config
        
        dc_to_nodes, data_centers = validate_multiple_datacenters(self.cluster, min_datacenters=2)
        
        self.assertIsNotNone(dc_to_nodes)
        self.assertIsNotNone(data_centers)
        
        # Теперь можем безопасно использовать переменные
        if dc_to_nodes is not None and data_centers is not None:
            self.assertEqual(len(data_centers), 2)
            self.assertIn('zone-a', data_centers)
            self.assertIn('zone-b', data_centers)
            self.assertEqual(len(dc_to_nodes['zone-a']), 2)
            self.assertEqual(len(dc_to_nodes['zone-b']), 1)
        
    def test_validate_multiple_datacenters_insufficient(self):
        # Тестирует валидацию с недостаточным количеством ДЦ.
        test_config = {
            'hosts': [
                {'name': 'host1.zone-a.com', 'location': {'data_center': 'zone-a'}},
                {'name': 'host2.zone-a.com', 'location': {'data_center': 'zone-a'}},
            ]
        }
        self.cluster._ExternalKiKiMRCluster__yaml_config = test_config
        
        dc_to_nodes, data_centers = validate_multiple_datacenters(self.cluster, min_datacenters=2)
        
        self.assertIsNone(dc_to_nodes)
        self.assertIsNone(data_centers)


class TestSingleDataCenterFailureNemesis(unittest.TestCase):
    
    def setUp(self):
        # Создаем мок кластера
        self.cluster = Mock()
        
        # Создаем тестовую YAML конфигурацию
        self.test_config = {
            'hosts': [
                {
                    'name': 'host1.zone-a.example.com',
                    'location': {'data_center': 'zone-a', 'rack': '1'}
                },
                {
                    'name': 'host2.zone-a.example.com', 
                    'location': {'data_center': 'zone-a', 'rack': '2'}
                },
                {
                    'name': 'host3.zone-b.example.com',
                    'location': {'data_center': 'zone-b', 'rack': '1'}
                },
                {
                    'name': 'host4.zone-b.example.com',
                    'location': {'data_center': 'zone-b', 'rack': '2'}
                }
            ]
        }
        
        # Настраиваем мок кластера
        self.cluster._ExternalKiKiMRCluster__yaml_config = self.test_config
        
        # Создаем мок ноды
        self.mock_nodes = {}
        for i, host_config in enumerate(self.test_config['hosts'], 1):
            mock_node = Mock()
            mock_node.host = host_config['name']
            mock_node.stop = Mock()
            mock_node.start = Mock()
            mock_node.ssh_command = Mock()
            self.mock_nodes[i] = mock_node
            
        self.cluster.nodes = self.mock_nodes
        
    def test_prepare_state_with_multiple_datacenters(self):
        # Тестирует подготовку состояния с несколькими ДЦ.
        nemesis = SingleDataCenterFailureNemesis(self.cluster)
        nemesis.prepare_state()
        
        # Проверяем, что ДЦ правильно распознаны
        self.assertEqual(len(nemesis._data_centers), 2)
        self.assertIn('zone-a', nemesis._data_centers)
        self.assertIn('zone-b', nemesis._data_centers)
        
        # Проверяем, что хосты правильно сгруппированы по ДЦ
        zone_a_hosts = nemesis._dc_to_nodes['zone-a']
        zone_b_hosts = nemesis._dc_to_nodes['zone-b']
        
        self.assertEqual(len(zone_a_hosts), 2)
        self.assertEqual(len(zone_b_hosts), 2)
        
        self.assertIn('host1.zone-a.example.com', zone_a_hosts)
        self.assertIn('host2.zone-a.example.com', zone_a_hosts)
        self.assertIn('host3.zone-b.example.com', zone_b_hosts)
        self.assertIn('host4.zone-b.example.com', zone_b_hosts)
        
    def test_prepare_state_with_single_datacenter(self):
        # Модифицируем конфигурацию для одного ДЦ
        single_dc_config = {
            'hosts': [
                {'name': 'host1.zone-a.example.com', 'location': {'data_center': 'zone-a'}},
                {'name': 'host2.zone-a.example.com', 'location': {'data_center': 'zone-a'}}
            ]
        }
        self.cluster._ExternalKiKiMRCluster__yaml_config = single_dc_config
        
        nemesis = SingleDataCenterFailureNemesis(self.cluster)
        nemesis.prepare_state()
        
        # С одним ДЦ nemesis не должен инициализироваться
        self.assertEqual(len(nemesis._data_centers), 0)
        
    def test_stop_datacenter_services(self):
        # Тестирует остановку сервисов в ДЦ.
        nemesis = SingleDataCenterFailureNemesis(self.cluster)
        nemesis.prepare_state()
        
        # Останавливаем сервисы в zone-a
        nemesis._stop_datacenter_services('zone-a')
        
        # Проверяем, что правильные ноды были остановлены
        stopped_hosts = [node.host for _, node in nemesis._stopped_nodes]
        self.assertIn('host1.zone-a.example.com', stopped_hosts)
        self.assertIn('host2.zone-a.example.com', stopped_hosts)
        self.assertEqual(len(nemesis._stopped_nodes), 2)
        
        # Проверяем, что метод stop был вызван для правильных нод
        for node_id, node in nemesis._stopped_nodes:
            node.stop.assert_called_once()
            
    def test_extract_fault(self):
        # Тестирует восстановление сервисов.
        nemesis = SingleDataCenterFailureNemesis(self.cluster)
        nemesis.prepare_state()
        
        # Сначала останавливаем сервисы
        nemesis._stop_datacenter_services('zone-a')
        self.assertEqual(len(nemesis._stopped_nodes), 2)
        
        # Теперь восстанавливаем
        result = nemesis.extract_fault()
        
        # Проверяем, что сервисы восстановлены
        self.assertTrue(result)
        self.assertEqual(len(nemesis._stopped_nodes), 0)
        self.assertIsNone(nemesis._current_dc)
        self.assertIsNone(nemesis._stop_time)
        
        # Проверяем, что метод start был вызван для всех остановленных нод
        for mock_node in self.mock_nodes.values():
            if mock_node.host.startswith('host1.zone-a') or mock_node.host.startswith('host2.zone-a'):
                mock_node.start.assert_called_once()

    def test_inject_fault_with_insufficient_datacenters(self):
        # Мок с одним ДЦ
        single_dc_config = {
            'hosts': [{'name': 'host1.example.com', 'location': {'data_center': 'zone-a'}}]
        }
        self.cluster._ExternalKiKiMRCluster__yaml_config = single_dc_config
        
        nemesis = SingleDataCenterFailureNemesis(self.cluster)
        nemesis.prepare_state()
        
        # inject_fault не должен ничего делать с одним ДЦ
        with patch.object(nemesis, '_stop_datacenter_services') as mock_stop:
            nemesis.inject_fault()
            mock_stop.assert_not_called()


class TestDataCenterRouteUnreachableNemesis(unittest.TestCase):
    
    def setUp(self):
        self.cluster = Mock()
        
        # Создаем тестовую YAML конфигурацию с 3 ДЦ
        self.test_config = {
            'hosts': [
                {'name': 'host1.zone-a.com', 'location': {'data_center': 'zone-a'}},
                {'name': 'host2.zone-a.com', 'location': {'data_center': 'zone-a'}},
                {'name': 'host3.zone-b.com', 'location': {'data_center': 'zone-b'}},
                {'name': 'host4.zone-b.com', 'location': {'data_center': 'zone-b'}},
                {'name': 'host5.zone-c.com', 'location': {'data_center': 'zone-c'}},
            ]
        }
        
        self.cluster._ExternalKiKiMRCluster__yaml_config = self.test_config
        
        # Создаем мок ноды
        self.mock_nodes = {}
        for i, host_config in enumerate(self.test_config['hosts'], 1):
            mock_node = Mock()
            mock_node.host = host_config['name']
            mock_node.ssh_command = Mock()
            self.mock_nodes[i] = mock_node
            
        self.cluster.nodes = self.mock_nodes
        
    def test_prepare_state_route_nemesis(self):
        nemesis = DataCenterRouteUnreachableNemesis(self.cluster)
        nemesis.prepare_state()
        
        # Проверяем, что ДЦ правильно распознаны
        self.assertEqual(len(nemesis._data_centers), 3)
        self.assertIn('zone-a', nemesis._data_centers)
        self.assertIn('zone-b', nemesis._data_centers)
        self.assertIn('zone-c', nemesis._data_centers)
        
    def test_find_node_by_host(self):
        nemesis = DataCenterRouteUnreachableNemesis(self.cluster)
        nemesis.prepare_state()
        
        # Ищем существующий хост
        node = nemesis._find_node_by_host('host1.zone-a.com')
        self.assertIsNotNone(node)
        if node is not None:
            self.assertEqual(node.host, 'host1.zone-a.com')
        
        # Ищем несуществующий хост
        node = nemesis._find_node_by_host('nonexistent.host.com')
        self.assertIsNone(node)
        
    def test_block_routes_to_current_dc(self):
        # Тестирует блокировку маршрутов к current ДЦ.
        nemesis = DataCenterRouteUnreachableNemesis(self.cluster)
        nemesis.prepare_state()
        nemesis._current_dc = 'zone-a'
        
        # Блокируем маршруты к zone-a
        nemesis._block_routes_to_current_dc()
        
        # Проверяем, что команды SSH были вызваны для хостов other ДЦ
        other_dc_nodes = [node for node in self.mock_nodes.values() 
                         if not node.host.endswith('zone-a.com')]
        
        for node in other_dc_nodes:
            # Проверяем, что вызывались команды создания файла и блокировки маршрутов
            self.assertTrue(node.ssh_command.called)
            self.assertGreater(node.ssh_command.call_count, 0)

    def test_extract_fault_route_nemesis(self):
        # Тестирует восстановление заблокированных маршрутов.
        nemesis = DataCenterRouteUnreachableNemesis(self.cluster)
        nemesis.prepare_state()
        
        # Имитируем заблокированные маршруты
        nemesis._blocked_routes = [
            {
                'node': self.mock_nodes[3],
                'host': 'host3.zone-b.com',
                'other_dc': 'zone-b',
                'temp_file': '/tmp/nemesis_current_dc_ips.txt'
            }
        ]
        nemesis._current_dc = 'zone-a'
        
        # Восстанавливаем маршруты
        result = nemesis.extract_fault()
        
        # Проверяем, что восстановление прошло успешно
        self.assertTrue(result)
        self.assertEqual(len(nemesis._blocked_routes), 0)
        self.assertIsNone(nemesis._current_dc)
        
        # Проверяем, что команды восстановления были выполнены
        self.mock_nodes[3].ssh_command.assert_called()


class TestDataCenterIptablesBlockPortsNemesis(unittest.TestCase):
    
    def setUp(self):
        # Подготавливает тестовое окружение для тестирования блокировки портов через iptables.
        self.cluster = Mock()
        
        # Создаем тестовую YAML конфигурацию с 2 ДЦ
        self.test_config = {
            'hosts': [
                {'name': 'host1.zone-a.com', 'location': {'data_center': 'zone-a'}},
                {'name': 'host2.zone-a.com', 'location': {'data_center': 'zone-a'}},
                {'name': 'host3.zone-b.com', 'location': {'data_center': 'zone-b'}},
                {'name': 'host4.zone-b.com', 'location': {'data_center': 'zone-b'}},
            ]
        }
        
        self.cluster._ExternalKiKiMRCluster__yaml_config = self.test_config
        
        # Создаем мок ноды
        self.mock_nodes = {}
        for i, host_config in enumerate(self.test_config['hosts'], 1):
            mock_node = Mock()
            mock_node.host = host_config['name']
            mock_node.ssh_command = Mock()
            # По умолчанию команды возвращают успешный результат
            mock_result = Mock()
            mock_result.exit_code = 0
            mock_node.ssh_command.return_value = mock_result
            self.mock_nodes[i] = mock_node
            
        self.cluster.nodes = self.mock_nodes
        
    def test_prepare_state_iptables_nemesis(self):
        nemesis = DataCenterIptablesBlockPortsNemesis(self.cluster)
        nemesis.prepare_state()
        
        # Проверяем, что ДЦ правильно распознаны
        self.assertEqual(len(nemesis._data_centers), 2)
        self.assertIn('zone-a', nemesis._data_centers)
        self.assertIn('zone-b', nemesis._data_centers)
        
    def test_block_ports_on_current_dc_success(self):
        # Тестирует успешную блокировку портов в current ДЦ.
        nemesis = DataCenterIptablesBlockPortsNemesis(self.cluster)
        nemesis.prepare_state()
        nemesis._current_dc = 'zone-a'
        
        # Блокируем порты в zone-a
        nemesis._block_ports_on_current_dc()
        
        # Проверяем, что команды блокировки были вызваны для хостов zone-a
        zone_a_nodes = [node for node in self.mock_nodes.values() 
                       if node.host.endswith('zone-a.com')]
        
        for node in zone_a_nodes:
            node.ssh_command.assert_called_with(nemesis._block_ports_cmd)
            
        # Проверяем, что заблокированные хосты добавлены в список
        self.assertEqual(len(nemesis._blocked_hosts), 2)
        blocked_hostnames = [info['host'] for info in nemesis._blocked_hosts]
        self.assertIn('host1.zone-a.com', blocked_hostnames)
        self.assertIn('host2.zone-a.com', blocked_hostnames)
        
    def test_block_ports_with_missing_chain(self):
        # Тестирует поведение при отсутствии цепочки YDB_FW.
        nemesis = DataCenterIptablesBlockPortsNemesis(self.cluster)
        nemesis.prepare_state()
        nemesis._current_dc = 'zone-a'
        
        # Настраиваем один узел для возврата ошибки (нет цепочки YDB_FW)
        zone_a_node = [node for node in self.mock_nodes.values() 
                      if node.host == 'host1.zone-a.com'][0]
        error_result = Mock()
        error_result.exit_code = 1
        zone_a_node.ssh_command.return_value = error_result
        
        # Блокируем порты в zone-a
        nemesis._block_ports_on_current_dc()
        
        # Проверяем, что только один хост был заблокирован (host2), host1 пропущен
        self.assertEqual(len(nemesis._blocked_hosts), 1)
        blocked_hostnames = [info['host'] for info in nemesis._blocked_hosts]
        self.assertIn('host2.zone-a.com', blocked_hostnames)
        self.assertNotIn('host1.zone-a.com', blocked_hostnames)
        
    def test_extract_fault_iptables_nemesis(self):
        # Тестирует восстановление заблокированных портов.
        nemesis = DataCenterIptablesBlockPortsNemesis(self.cluster)
        nemesis.prepare_state()
        
        # Имитируем заблокированные порты
        nemesis._blocked_hosts = [
            {
                'node': self.mock_nodes[1],
                'host': 'host1.zone-a.com',
                'dc': 'zone-a'
            },
            {
                'node': self.mock_nodes[2],
                'host': 'host2.zone-a.com',
                'dc': 'zone-a'
            }
        ]
        nemesis._current_dc = 'zone-a'
        
        # Восстанавливаем порты
        result = nemesis.extract_fault()
        
        # Проверяем, что восстановление прошло успешно
        self.assertTrue(result)
        self.assertEqual(len(nemesis._blocked_hosts), 0)
        self.assertIsNone(nemesis._current_dc)
        
        for mock_node in [self.mock_nodes[1], self.mock_nodes[2]]:
            # Проверяем, что была вызвана команда flush
            calls = mock_node.ssh_command.call_args_list
            restore_calls = [call for call in calls if nemesis._restore_ports_cmd in str(call)]
            self.assertGreater(len(restore_calls), 0)
            
    def test_inject_fault_iptables_cycle(self):
        nemesis = DataCenterIptablesBlockPortsNemesis(self.cluster)
        nemesis.prepare_state()
        
        # Первый вызов должен выбрать первый ДЦ
        nemesis.inject_fault()
        first_dc = nemesis._current_dc
        self.assertIsNotNone(first_dc)
        
        # Очищаем состояние для следующего теста
        nemesis.extract_fault()
        
        # Второй вызов должен выбрать следующий ДЦ
        nemesis.inject_fault()
        second_dc = nemesis._current_dc
        self.assertIsNotNone(second_dc)
        self.assertNotEqual(first_dc, second_dc)


class TestDatacenterNemesisList(unittest.TestCase):
    
    def test_datacenter_nemesis_list(self):
        cluster = Mock()
        nemesis_list = datacenter_nemesis_list(cluster)
        
        self.assertEqual(len(nemesis_list), 3)
        self.assertIsInstance(nemesis_list[0], SingleDataCenterFailureNemesis)
        self.assertIsInstance(nemesis_list[1], DataCenterRouteUnreachableNemesis)
        self.assertIsInstance(nemesis_list[2], DataCenterIptablesBlockPortsNemesis)


if __name__ == '__main__':
    unittest.main() 
