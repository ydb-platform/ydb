class WorkloadManager(object):

    def verify_pool(self, user, expected_pool, query):
        # It's funny, but the only way to find resource pool that was used is to use YDB CLI
        stats = self.run_cli(["--user", user, '--no-password', "sql", "-s", query, '--stats', 'full', '--format', 'json-unicode'])
        resource_pool_in_use = WorkloadManager.find_resource_pool_id(stats)
        assert expected_pool == resource_pool_in_use, f"Resource pool {expected_pool} was not used. Instead used {resource_pool_in_use}"

    @staticmethod
    def find_resource_pool_id(text):
        key = '"ResourcePoolId"'
        if key in text:
            start_idx = text.find(key) + len(key)
            # Skip spaces and colon
            while start_idx < len(text) and text[start_idx] in ' :':
                start_idx += 1
            # Skip opening double quote
            if start_idx < len(text) and text[start_idx] == '"':
                start_idx += 1
                end_idx = text.find('"', start_idx)
                if end_idx != -1:
                    return text[start_idx:end_idx]
        return None
