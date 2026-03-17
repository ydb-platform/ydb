import re


class KeyMaker:
    """
    Map the different keys amazon uses for the same property to a single one


    Examples:
        literal blocks::

            key_mapping = {
                'sku': ['seller_sku', 'sellerSku'],
                'title': ['product_name']
            }

            res = KeyMaker(key_mapping, deep=True).convert_keys({
                'seller_sku': 1,
                'product_name': {
                    'sellerSku': [
                        'seller_sku',
                        3,
                        {
                            'sellerSku': 22,
                            'product_name': {
                                'title': 'Foo',
                                'x': 'bar'
                            }
                        }
                    ]
                }
            })

            res

            {'sku': 1, 'title': {'sku': ['seller_sku', 3, {'sku': 22, 'title': {'title': 'Foo', 'x': 'bar'}}]}}
    """

    def __init__(self, key_mapping=None, *, deep=True):
        if key_mapping is None:
            key_mapping = {}
        self.key_mapping = key_mapping
        self.deep = deep

    def convert_keys(self, data: dict or list):
        """
        convert_keys(self, data: dict or list)

        Map the different keys amazon uses for the same property to a single one


        Args:
            data: [dict] or dict

        Returns:
            Transformed data
        """
        if isinstance(data, list):
            return [self.convert_keys(d) for d in data]
        if not isinstance(data, dict):
            return data
        return {
            self._map_to_key_mapping(k): self.convert_keys(v) if self.deep else v
            for k, v in data.items()
        }

    def _map_to_key_mapping(self, key):
        for k, v in self.key_mapping.items():
            if key in v or k == key:
                return k
        return self._replace_dash(key)

    @staticmethod
    def _replace_dash(key):
        return key[0].lower() + ''.join(
            word.title() if i > 0 else word for i, word in enumerate(re.sub(r'[-\s]', '_', key[1:]).split('_')))
