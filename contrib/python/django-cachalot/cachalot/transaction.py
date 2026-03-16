from .settings import cachalot_settings


class AtomicCache(dict):
    def __init__(self, parent_cache, db_alias):
        super().__init__()
        self.parent_cache = parent_cache
        self.db_alias = db_alias
        self.to_be_invalidated = set()

    def set(self, k, v, timeout):
        self[k] = v

    def get_many(self, keys):
        # Get values present in this cache level
        data = {k: self[k] for k in keys if k in self}
        
        # Identify keys that are missing at this level
        missing_keys = set(keys)
        missing_keys.difference_update(data)
        
        # Only proceed if there are missing keys
        if missing_keys:
            # Find the bottom-most non-AtomicCache to avoid recursion
            current_cache = self.parent_cache
            visited_caches = set([id(self)])
            
            # Continue until we hit a non-AtomicCache or visit a cache we've seen before
            while isinstance(current_cache, AtomicCache) and id(current_cache) not in visited_caches:
                # Mark this cache as visited
                visited_caches.add(id(current_cache))
                
                # Get any keys available in this cache level
                available_keys = missing_keys.intersection(current_cache.keys())
                if available_keys:
                    for k in available_keys:
                        data[k] = current_cache[k]
                    missing_keys.difference_update(available_keys)
                
                # If we found all keys, no need to go further
                if not missing_keys:
                    break
                
                # Move to the parent cache
                current_cache = current_cache.parent_cache
            
            # If we've reached a non-AtomicCache and still have missing keys, use its get_many method
            if missing_keys and not isinstance(current_cache, AtomicCache) and hasattr(current_cache, 'get_many'):
                parent_data = current_cache.get_many(missing_keys)
                data.update(parent_data)
        
        return data

    def set_many(self, data, timeout):
        self.update(data)

    def commit(self):
        # We import this here to avoid a circular import issue.
        from .utils import _invalidate_tables

        if self:
            self.parent_cache.set_many(
                self, cachalot_settings.CACHALOT_TIMEOUT)
        # The previous `set_many` is not enough.  The parent cache needs to be
        # invalidated in case another transaction occurred in the meantime.
        _invalidate_tables(self.parent_cache, self.db_alias,
                           self.to_be_invalidated)
