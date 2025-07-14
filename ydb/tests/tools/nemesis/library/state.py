# -*- coding: utf-8 -*-
import time
import threading
import logging



class NemesisState:    
    def __init__(self):
        self._lock = threading.Lock()
        self._active_until = None
        self._active_nemesis_type = None
        self._active_nemesis_instance = None
        self.logger = logging.getLogger(__name__)
        
    def try_acquire_lock(self, nemesis_instance):
        with self._lock:
            current_time = time.time()
            
            # If another nemesis is active (either preparing or with duration set), reject
            if self._active_nemesis_instance is not None:
                self.logger.warning("BLOCKING: %s (instance %s) - active nemesis: %s (instance %s)", 
                                  nemesis_instance.__class__.__name__, id(nemesis_instance),
                                  self._active_nemesis_type, id(self._active_nemesis_instance))
                
                if self._active_until is not None and current_time < self._active_until:
                    return False, {
                        'type': self._active_nemesis_type,
                        'until': self._active_until,
                        'remaining': self._active_until - current_time
                    }
                elif self._active_until is None:
                    return False, {
                        'type': self._active_nemesis_type,
                        'until': None,
                        'remaining': 'preparing'
                    }
                else:
                    expired_type = self._active_nemesis_type
                    self._active_until = None
                    self._active_nemesis_type = None
                    self._active_nemesis_instance = None
                    return True, {'expired_cleanup': expired_type}
            
            self.logger.info("ACQUIRING LOCK: %s (instance %s)", nemesis_instance.__class__.__name__, id(nemesis_instance))
            self._active_nemesis_type = nemesis_instance.__class__.__name__
            self._active_nemesis_instance = nemesis_instance
            return True, None
    
    def start_duration(self, nemesis_instance, duration):
        with self._lock:
            if self._active_nemesis_instance is nemesis_instance:
                current_time = time.time()
                self._active_until = current_time + duration
                self.logger.info("DURATION STARTED: %s (instance %s) - duration %d seconds, expires at %s", 
                               nemesis_instance.__class__.__name__, id(nemesis_instance), duration, self._active_until)
                return True
            else:
                self.logger.error("DURATION START FAILED: %s (instance %s) not the active nemesis", 
                                nemesis_instance.__class__.__name__, id(nemesis_instance))
            return False
            
    def end_execution(self, nemesis_instance):
        with self._lock:
            if self._active_nemesis_instance is nemesis_instance:
                self.logger.info("RELEASING LOCK: %s (instance %s) completing execution", nemesis_instance.__class__.__name__, id(nemesis_instance))
                self._active_until = None
                self._active_nemesis_type = None
                self._active_nemesis_instance = None
            else:
                self.logger.warning("INVALID RELEASE: %s (instance %s) tried to release lock, but active nemesis is %s (instance %s)", 
                                  nemesis_instance.__class__.__name__, id(nemesis_instance),
                                  self._active_nemesis_type, id(self._active_nemesis_instance) if self._active_nemesis_instance else None)
                
    def get_active_info(self):
        with self._lock:
            current_time = time.time()
            if self._active_nemesis_type is not None:
                if self._active_until is not None and current_time < self._active_until:
                    return {
                        'type': self._active_nemesis_type,
                        'until': self._active_until,
                        'remaining': self._active_until - current_time
                    }
                elif self._active_until is None:
                    return {
                        'type': self._active_nemesis_type,
                        'until': None,
                        'remaining': 'preparing'
                    }
            return None
    
    def is_duration_expired(self, nemesis_instance):
        with self._lock:
            if self._active_nemesis_instance is nemesis_instance:
                if self._active_until is None:
                    return False  # Still preparing, not expired
                current_time = time.time()
                expired = current_time >= self._active_until
                return expired
            return False  # Not the active nemesis
            
    def check_and_cleanup_expired(self):
        with self._lock:
            if (self._active_nemesis_instance is not None and 
                self._active_until is not None):
                current_time = time.time()
                if current_time >= self._active_until:
                    expired_instance = self._active_nemesis_instance
                    expired_type = self._active_nemesis_type
                    # Don't clear the state yet - let the nemesis extract_fault properly
                    return expired_instance, expired_type
            return None, None
            
    def is_active(self, nemesis_instance):
        with self._lock:
            current_time = time.time()
            return (self._active_nemesis_instance is nemesis_instance and 
                    (self._active_until is None or current_time < self._active_until))
    
    def debug_state(self):
        with self._lock:
            current_time = time.time()
            return {
                'active_type': self._active_nemesis_type,
                'active_instance_id': id(self._active_nemesis_instance) if self._active_nemesis_instance else None,
                'active_until': self._active_until,
                'current_time': current_time,
                'expired': self._active_until is not None and current_time >= self._active_until
            }
