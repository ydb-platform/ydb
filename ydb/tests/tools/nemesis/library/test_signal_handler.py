#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тест для проверки signal handler для DC nemesis.
Симулирует работу nemesis и прерывание сигналом.
"""

import time
import signal
import os
import sys
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Мок-классы для тестирования
class MockCluster:
    def __init__(self):
        self.nodes = {
            1: MockNode("host1", "zone-a"),
            2: MockNode("host2", "zone-b"), 
            3: MockNode("host3", "zone-c")
        }
        self._ExternalKiKiMRCluster__yaml_config = {
            'hosts': [
                {'name': 'host1', 'location': {'data_center': 'zone-a'}},
                {'name': 'host2', 'location': {'data_center': 'zone-b'}},
                {'name': 'host3', 'location': {'data_center': 'zone-c'}}
            ]
        }

class MockNode:
    def __init__(self, host, dc):
        self.host = host
        self.dc = dc
        self._stopped = False
        
    def stop(self):
        self._stopped = True
        print(f"[MOCK] Stopped node {self.host}")
        
    def start(self):
        self._stopped = False
        print(f"[MOCK] Started node {self.host}")
        
    def ssh_command(self, cmd, raise_on_error=True):
        print(f"[MOCK] SSH command on {self.host}: {cmd}")
        return 0

def test_signal_handler():
    """Тестирует signal handler для DC nemesis"""
    
    # Импортируем наши классы
    from hosts_network import DataCenterNetworkNemesis, DataCenterRouteUnreachableNemesis
    
    print("🧪 Тестирование signal handler для DC nemesis")
    
    # Создаем мок-кластер
    cluster = MockCluster()
    
    # Создаем nemesis (они автоматически регистрируются)
    print("\n📝 Создание nemesis...")
    network_nemesis = DataCenterNetworkNemesis(cluster, schedule=(5, 10))
    route_nemesis = DataCenterRouteUnreachableNemesis(cluster, schedule=(5, 10))
    
    # Подготавливаем состояние
    print("\n⚙️ Подготовка состояния...")
    network_nemesis.prepare_state()
    route_nemesis.prepare_state()
    
    # Симулируем активность nemesis
    print("\n🎯 Симуляция активных nemesis...")
    
    # Симулируем что network_nemesis остановил ноды
    network_nemesis._current_dc = "zone-a"
    network_nemesis._stopped_nodes = [(1, cluster.nodes[1])]
    network_nemesis._stop_time = time.time()
    
    # Симулируем что route_nemesis заблокировал маршруты  
    route_nemesis._current_dc = "zone-b"
    route_nemesis._blocked_routes = [
        {'node': cluster.nodes[2], 'host': 'host2', 'temp_file': '/tmp/test_routes'}
    ]
    route_nemesis._block_time = time.time()
    
    print(f"Network nemesis state: stopped_nodes={len(network_nemesis._stopped_nodes)}, current_dc={network_nemesis._current_dc}")
    print(f"Route nemesis state: blocked_routes={len(route_nemesis._blocked_routes)}, current_dc={route_nemesis._current_dc}")
    
    print(f"\n⏰ Через 3 секунды будет отправлен SIGTERM (PID: {os.getpid()})")
    print("Signal handler должен автоматически очистить состояние...")
    
    # Запускаем таймер для отправки сигнала
    def send_signal():
        time.sleep(3)
        print(f"\n🚨 Отправляю SIGTERM процессу {os.getpid()}")
        os.kill(os.getpid(), signal.SIGTERM)
    
    import threading
    timer_thread = threading.Thread(target=send_signal)
    timer_thread.daemon = True
    timer_thread.start()
    
    # Ждем сигнал
    try:
        print("⏳ Ожидание сигнала...")
        time.sleep(10)  # Этого не должно произойти, signal handler должен прервать
        print("❌ ERROR: Сигнал не был получен!")
    except KeyboardInterrupt:
        print("❌ Получен SIGINT вместо SIGTERM")

if __name__ == "__main__":
    test_signal_handler() 