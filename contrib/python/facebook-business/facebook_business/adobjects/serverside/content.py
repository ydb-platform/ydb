# Copyright 2014 Facebook, Inc.

# You are hereby granted a non-exclusive, worldwide, royalty-free license to
# use, copy, modify, and distribute this software in source code or binary
# form for use in connection with the web services and APIs provided by
# Facebook.

# As with any software that integrates with the Facebook platform, your use
# of this software is subject to the Facebook Developer Principles and
# Policies [http://developers.facebook.com/policy/]. This copyright notice
# shall be included in all copies or substantial portions of the software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import pprint
import six

from facebook_business.adobjects.serverside.delivery_category import DeliveryCategory

class Content(object):
    """
    Content objects that contain the product IDs associated with the event plus information about the products.
    """
    param_types = {
        'product_id': 'str',
        'quantity': 'str',
        'item_price': 'float',
        'title': 'str',
        'description': 'str',
        'brand': 'str',
        'category': 'str',
        'delivery_category': 'str',
    }

    def __init__(
        self,
        product_id=None,
        quantity=None,
        item_price=None,
        title=None,
        description=None,
        brand=None,
        category=None,
        delivery_category=None,
    ):
        # type: (str, int, float, str, str, str, str, str) -> None

        self._product_id = None
        self._quantity = None
        self._item_price = None
        self._title = None
        self._description = None
        self._brand = None
        self._category = None
        self._delivery_category = None
        if product_id is not None:
            self.product_id = product_id
        if quantity is not None:
            self.quantity = quantity
        if item_price is not None:
            self.item_price = item_price
        if title is not None:
            self.title = title
        if description is not None:
            self.description = description
        if brand is not None:
            self.brand = brand
        if category is not None:
            self.category = category
        if delivery_category is not None:
            self.delivery_category = delivery_category

    @property
    def product_id(self):
        """
        Gets Product Id.
        :rtype: str
        """
        return self._product_id

    @product_id.setter
    def product_id(self, product_id):
        """
        Sets Product Id.
        """
        self._product_id = product_id

    @property
    def quantity(self):
        """
        Gets number of product.
        :rtype: int
        """
        return self._quantity

    @quantity.setter
    def quantity(self, quantity):
        """
        Set number of product.
        """
        self._quantity = quantity

    @property
    def item_price(self):
        """
        Gets Item Price.
        :rtype: float
        """
        return self._item_price

    @item_price.setter
    def item_price(self, item_price):
        """
        Sets Item Price.
        """
        self._item_price = item_price

    @property
    def title(self):
        """
        Gets title.
        :rtype: float
        """
        return self._title

    @title.setter
    def title(self, title):
        """
        Sets title.
        """
        self._title = title

    @property
    def description(self):
        """
        Gets description.
        :rtype: float
        """
        return self._description

    @description.setter
    def description(self, description):
        """
        Sets description.
        """
        self._description = description

    @property
    def brand(self):
        """
        Gets brand.
        :rtype: float
        """
        return self._brand

    @brand.setter
    def brand(self, brand):
        """
        Sets brand.
        """
        self._brand = brand

    @property
    def category(self):
        """
        Gets category.
        :rtype: float
        """
        return self._category

    @category.setter
    def category(self, category):
        """
        Sets category.
        """
        self._category = category

    @property
    def delivery_category(self):
        """Gets the Type of Delivery Category.

        :return: The Delivery Category type.
        :rtype: DeliveryCategory
        """
        return self._delivery_category

    @delivery_category.setter
    def delivery_category(self, delivery_category):
        """Sets the Type of Delivery Category.

        Use with Purchase events.

        :param delivery_category: The Delivery Category type.
        :type: DeliveryCategory
        """
        if not isinstance(delivery_category, DeliveryCategory):
            raise TypeError('delivery_category must be of type DeliveryCategory. Passed invalid category: ' + delivery_category)

        self._delivery_category = delivery_category

    def normalize(self):
        normalized_payload = {
            'id': self.product_id,
            'quantity': self.quantity,
            'item_price': self.item_price,
            'title': self.title,
            'description': self.description,
            'brand': self.brand,
            'category': self.category,
        }

        if self.delivery_category is not None:
            normalized_payload['delivery_category'] = self.delivery_category.value

        normalized_payload = {k: v for k, v in normalized_payload.items() if v is not None}
        return normalized_payload

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.param_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value
        if issubclass(Content, dict):
            for key, value in self.items():
                result[key] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, Content):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
