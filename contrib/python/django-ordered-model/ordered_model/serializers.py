from rest_framework import serializers, fields


class OrderedModelSerializer(serializers.ModelSerializer):
    """
    A ModelSerializer to provide a serializer that can be update and create
    objects in a specific order.

    Typically a `models.PositiveIntegerField` field called `order` is used to
    store the order of the Model objects. This field can be customized by setting
    the `order_field_name` attribute on the Model class.

    This serializer will move the object to the correct
    order if the ordering field is passed in the validated data.
    """

    def get_order_field(self):
        """
        Return the field name for the ordering field.

        If inheriting from `OrderedModelBase`, the `order_field_name` attribute
        must be set on the Model class. If inheriting from `OrderedModel`, the
        `order_field_name` attribute is not required, as the `OrderedModel`
        has the `order_field_name` attribute defaulting to 'order'.

        Returns:
            str: The field name for the ordering field.

        Raises:
            AttributeError: If the `order_field_name` attribute is not set,
                either on the Model class or on the serializer's Meta class.
        """

        ModelClass = self.Meta.model  # pylint: disable=no-member,invalid-name
        order_field_name = getattr(ModelClass, "order_field_name")

        if not order_field_name:
            raise AttributeError(
                "The `order_field_name` attribute must be set to use the "
                "OrderedModelSerializer. Either inherit from OrderedModel "
                "(to use the default `order` field) or inherit from "
                "`OrderedModelBase` and set the `order_field_name` attribute "
                "on the " + ModelClass.__name__ + " Model class."
            )

        return order_field_name

    def get_fields(self):
        # make sure that DRF considers the ordering field writable
        order_field = self.get_order_field()
        d = super().get_fields()
        for name, field in d.items():
            if name == order_field:
                if field.read_only:
                    d[name] = fields.IntegerField()
        return d

    def update(self, instance, validated_data):
        """
        Update the instance.

        If the `order_field_name` attribute is passed in the validated data,
        the instance will be moved to the specified order.

        Returns:
            Model: The updated instance.
        """

        order = None
        order_field = self.get_order_field()

        if order_field in validated_data:
            order = validated_data.pop(order_field)

        instance = super().update(instance, validated_data)

        if order is not None:
            instance.to(order)

        return instance

    def create(self, validated_data):
        """
        Create a new instance.

        If the `order_field_name` attribute is passed in the validated data,
        the instance will be created at the specified order.

        Returns:
            Model: The created instance.
        """
        order = None
        order_field = self.get_order_field()

        if order_field in validated_data:
            order = validated_data.pop(order_field)

        instance = super().create(validated_data)

        if order is not None:
            instance.to(order)

        return instance
