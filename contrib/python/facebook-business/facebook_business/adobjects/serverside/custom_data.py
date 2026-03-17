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

from facebook_business.adobjects.serverside.content import Content
from facebook_business.adobjects.serverside.normalize import Normalize
from facebook_business.adobjects.serverside.delivery_category import DeliveryCategory

class CustomData(object):
    """
    CustomData includes additional business data about the event.
    """
    param_types = {
        'value': 'float',
        'net_revenue': 'float',
        'currency': 'str',
        'content_name': 'str',
        'content_category': 'str',
        'content_ids': 'list[str]',
        'contents': 'list[Content]',
        'content_type': 'str',
        'order_id': 'str',
        'predicted_ltv': 'float',
        'num_items': 'int',
        'status': 'str',
        'search_string' : 'str',
        'delivery_category' : 'DeliveryCategory',
        'item_number': 'str',
        'custom_properties' : 'dict'
    }

    def __init__(
        self,
        value=None,
        net_revenue=None,
        currency=None,
        content_name=None,
        content_category=None,
        content_ids=None,
        contents=None,
        content_type=None,
        order_id=None,
        predicted_ltv=None,
        num_items=None,
        status=None,
        search_string=None,
        delivery_category=None,
        item_number=None,
        custom_properties={},
    ):
        # type: (float, float, str, str, str, list[str], list[Content], str, str, float, int, str, str, DeliveryCategory ,str, dict) -> None

        self._value = None
        self._net_revenue = None
        self._currency = None
        self._content_name = None
        self._content_category = None
        self._content_ids = None
        self._contents = None
        self._content_type = None
        self._order_id = None
        self._predicted_ltv = None
        self._num_items = None
        self._status = None
        self._search_string = None
        self._delivery_category = None
        self._item_number = None
        self._custom_properties = None
        if value is not None:
            self.value = value
        if net_revenue is not None:
            self.net_revenue = net_revenue
        if currency is not None:
            self.currency = currency
        if content_name is not None:
            self.content_name = content_name
        if content_category is not None:
            self.content_category = content_category
        if content_ids is not None:
            self.content_ids = content_ids
        if contents is not None:
            self.contents = contents
        if content_type is not None:
            self.content_type = content_type
        if order_id is not None:
            self.order_id = order_id
        if predicted_ltv is not None:
            self.predicted_ltv = predicted_ltv
        if num_items is not None:
            self.num_items = num_items
        if status is not None:
            self.status = status
        if search_string is not None:
            self.search_string = search_string
        if delivery_category is not None:
            self.delivery_category = delivery_category
        if item_number is not None:
            self.item_number = item_number
        if custom_properties:
            self.custom_properties = custom_properties

    @property
    def value(self):
        """Gets the value.

        A numeric value associated with this event. This could be a monetary value or a value in some other metric.

        :return: The value.
        :rtype: float or int
        """
        return self._value

    @value.setter
    def value(self, value):
        """Sets the value.

        A numeric value associated with this event. This could be a monetary value or a value in some other metric.

        :param value: The value.
        :type: float or int
        """
        if not isinstance(value, (float, int)):
            raise TypeError('CustomData.value must be a float or int. TypeError on value: %s' % value)
        self._value = value

    @property
    def net_revenue(self):
        """Gets the net_revenue.

        A numeric net revenue associated with this event.

        :return: The net_revenue.
        :rtype: float or int
        """
        return self._net_revenue

    @net_revenue.setter
    def net_revenue(self, net_revenue):
        """Sets the net_revenue.

        A numeric net revenue associated with this event.

        :param net_revenue: The net_revenue.
        :type: float or int
        """
        if not isinstance(net_revenue, (float, int)):
            raise TypeError('CustomData.net_revenue must be a float or int. TypeError on value: %s' % net_revenue)
        self._net_revenue = net_revenue

    @property
    def currency(self):
        """Gets the currency.

        The currency for the value specified, if applicable. Currency must be a valid ISO 4217 three digit currency code.

        :return: The currency.
        :rtype: str
        """
        return self._currency

    @currency.setter
    def currency(self, currency):
        """Sets the currency.

        The currency for the value specified, if applicable. Currency must be a valid ISO 4217 three digit currency code.

        :param currency: The currency.
        :type: str
        """

        self._currency = currency

    @property
    def content_name(self):
        """Gets the content name.

        The name of the page or product associated with the event. Example: 'lettuce'.

        :return: The content name.
        :rtype: str
        """
        return self._content_name

    @content_name.setter
    def content_name(self, content_name):
        """Sets the content name.

        The name of the page or product associated with the event. Example: 'lettuce'.

        :param content_name: The content name.
        :type: str
        """

        self._content_name = content_name

    @property
    def content_category(self):
        """Gets the content category.

        The category of the content associated with the event. Example: 'grocery'

        :return: The content category.
        :rtype: str
        """
        return self._content_category

    @content_category.setter
    def content_category(self, content_category):
        """Sets the content category.

        The category of the content associated with the event. Example: 'grocery'

        :param content_category: The content category.
        :type: str
        """

        self._content_category = content_category

    @property
    def content_ids(self):
        """Gets the content ids.

        The content IDs associated with the event, such as product SKUs for items in an
        AddToCart event: ['ABC123', 'XYZ789']. If content_type is a product, then your content IDs must
        be an array with a single string value. Otherwise, this array can contain any number of string values.

        :return: The content ids.
        :rtype: list[str]
        """
        return self._content_ids

    @content_ids.setter
    def content_ids(self, content_ids):
        """Sets the content_ids.

        The content IDs associated with the event, such as product SKUs for items in an AddToCart event:
         ['ABC123', 'XYZ789']. If content_type is a product, then your content IDs must be an array with
         a single string value. Otherwise, this array can contain any number of string values.

        :param content_ids: The content_ids.
        :type: list[str]
        """

        self._content_ids = content_ids

    @property
    def contents(self):
        """Gets the contents.

        A list of Content objects that contain the product IDs associated with the event plus information about
        the products. id, quantity, and item_price are available fields.

        :return: The contents.
        :rtype: list[Content]
        """
        return self._contents

    @contents.setter
    def contents(self, contents):
        """Sets the contents.

        A list of Content objects that contain the product IDs associated with the event plus information about
        the products. id, quantity, and item_price are available fields.

        :param contents: The contents.
        :type: list[Content]
        """

        self._contents = contents

    @property
    def content_type(self):
        """Gets the content type.

        A String equal to either 'product' or 'product_group'. Set to product if the keys you send content_ids or
        contents represent products. Set to product_group if the keys you send in content_ids represent product groups.

        :return: The content type.
        :rtype: str
        """
        return self._content_type

    @content_type.setter
    def content_type(self, content_type):
        """Sets the content type.

        A String equal to either 'product' or 'product_group'. Set to product if the keys you send content_ids or
        contents represent products. Set to product_group if the keys you send in content_ids represent product groups.

        :param content_type: The content type.
        :type: str
        """

        self._content_type = content_type

    @property
    def order_id(self):
        """Gets the order id.

        The order ID for this transaction as a String.

        :return: The order id.
        :rtype: str
        """
        return self._order_id

    @order_id.setter
    def order_id(self, order_id):
        """Sets the order id.

        The order ID for this transaction as a String.

        :param order_id: The order id.
        :type: str
        """

        self._order_id = order_id

    @property
    def predicted_ltv(self):
        """Gets the predicted ltv.

        The predicted lifetime value of a conversion event

        :return: The predicted ltv.
        :rtype: float or int
        """
        return self._predicted_ltv

    @predicted_ltv.setter
    def predicted_ltv(self, predicted_ltv):
        """Sets the predicted_ltv.

        The predicted lifetime value of a conversion event

        :param predicted_ltv: The predicted_ltv.
        :type: float or int
        """
        if not isinstance(predicted_ltv, (float, int)):
            raise TypeError('CustomData.predicted_ltv must be a float or int. TypeError on predicted_ltv: %s' % predicted_ltv)
        self._predicted_ltv = predicted_ltv

    @property
    def num_items(self):
        """Gets the num items.

        Use only with InitiateCheckout events. The number of items that a user tries to buy during checkout.

        :return: The num_items.
        :rtype: int
        """
        return self._num_items

    @num_items.setter
    def num_items(self, num_items):
        """Sets the num items.

        Use only with InitiateCheckout events. The number of items that a user tries to buy during checkout.

        :param num_items: The num_items.
        :type: int
        """

        self._num_items = num_items

    @property
    def status(self):
        """Gets the status.

        Use only with CompleteRegistration events. The status of the registration event, as a String.

        :return: The status.
        :rtype: str
        """
        return self._status

    @status.setter
    def status(self, status):
        """Sets the status.

        Use only with CompleteRegistration events. The status of the registration event, as a String.

        :param status: The status.
        :type: str
        """

        self._status = status

    @property
    def search_string(self):
        """Gets the search query made by a user.

        :return: The search query.
        :rtype: str
        """
        return self._search_string

    @search_string.setter
    def search_string(self, search_string):
        """Sets the search query made by a user.

        Use only with Search events. A search query made by a user.

        :param search_string: The search query.
        :type: str
        """

        self._search_string = search_string

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

    @property
    def item_number(self):
        """Gets the item number.

        :return: The item number.
        :rtype: str
        """
        return self._item_number

    @item_number.setter
    def item_number(self, item_number):
        """Sets the item number.

        :param item_number: The item number.
        :type: str
        """

        self._item_number = item_number

    @property
    def custom_properties(self):
        """Gets the custom properties to be included in the Custom Data.

        :return: The custom properties dictionary.
        :rtype: dict
        """
        return self._custom_properties

    @custom_properties.setter
    def custom_properties(self, custom_properties):
        """Sets the custom properties to be included in the Custom Data.

        These are properties that are not defined in the standard list of the custom data.

        :param custom_properties: The custom properties dictionary.
        :type: dict
        """

        self._custom_properties = custom_properties

    def add_custom_property(self, key, value):

        """Adds the custom property (key, value) to the custom property bag.

        :param key: The Key for the property to be added.
        :type: str
        :param value: The Value for the property to be added.
        :type: str
        """

        self.custom_properties[key] = value

    def normalize(self):
        normalized_payload = {
            'value': self.value,
            'net_revenue': self.net_revenue,
            'currency': Normalize.normalize_field_skip_hashing('currency', self.currency),
            'content_name': self.content_name,
            'content_category': self.content_category,
            'content_ids': self.content_ids,
            'content_type': self.content_type,
            'order_id': self.order_id,
            'predicted_ltv': self.predicted_ltv,
            'num_items': self.num_items,
            'status': self.status,
            'search_string': self.search_string,
            'item_number': self.item_number,
        }

        if self.delivery_category is not None:
            normalized_payload['delivery_category'] = self.delivery_category.value

        if self.contents is not None:
            contents = []
            for content in self.contents:
                if content is not None:
                    contents.append(content.normalize())

            normalized_payload['contents'] = contents

        # Append the custom_properties to the custom_data normalized payload.
        if self.custom_properties:
            for key in self.custom_properties:
                if key in normalized_payload.keys():
                    raise Exception('Duplicate key in custom_properties:"' + key + '". Please make sure the keys defined in the custom_properties are not already available in standard custom_data property list.')
                normalized_payload[key] = self.custom_properties[key]

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
        if issubclass(CustomData, dict):
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
        if not isinstance(other, CustomData):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other
