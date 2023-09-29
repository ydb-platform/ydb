PY2_LIBRARY()

LICENSE(Apache-2.0)

VERSION(1.3.7)

NO_LINT()

PEERDIR(
    contrib/python/botocore
    contrib/python/boto
    contrib/python/cookies
    contrib/python/requests
    contrib/python/responses
    contrib/python/Werkzeug
    contrib/python/Jinja2
    contrib/python/xmltodict
    contrib/python/pytz
    contrib/python/Flask
    library/python/resource
)

PY_SRCS(
    TOP_LEVEL
    moto/__init__.py
    moto/settings.py
    moto/compat.py
    moto/backends.py
    moto/server.py

    moto/core/__init__.py
    moto/core/exceptions.py
    moto/core/models.py
    moto/core/responses.py
    moto/core/urls.py
    moto/core/utils.py

    moto/ec2/__init__.py
    moto/ec2/exceptions.py
    moto/ec2/models.py
    moto/ec2/responses/__init__.py
    moto/ec2/responses/account_attributes.py
    moto/ec2/responses/amazon_dev_pay.py
    moto/ec2/responses/amis.py
    moto/ec2/responses/availability_zones_and_regions.py
    moto/ec2/responses/customer_gateways.py
    moto/ec2/responses/dhcp_options.py
    moto/ec2/responses/elastic_block_store.py
    moto/ec2/responses/elastic_ip_addresses.py
    moto/ec2/responses/elastic_network_interfaces.py
    moto/ec2/responses/general.py
    moto/ec2/responses/instances.py
    moto/ec2/responses/internet_gateways.py
    moto/ec2/responses/ip_addresses.py
    moto/ec2/responses/key_pairs.py
    moto/ec2/responses/monitoring.py
    moto/ec2/responses/nat_gateways.py
    moto/ec2/responses/network_acls.py
    moto/ec2/responses/placement_groups.py
    moto/ec2/responses/reserved_instances.py
    moto/ec2/responses/route_tables.py
    moto/ec2/responses/security_groups.py
    moto/ec2/responses/spot_fleets.py
    moto/ec2/responses/spot_instances.py
    moto/ec2/responses/subnets.py
    moto/ec2/responses/tags.py
    moto/ec2/responses/virtual_private_gateways.py
    moto/ec2/responses/vm_export.py
    moto/ec2/responses/vm_import.py
    moto/ec2/responses/vpc_peering_connections.py
    moto/ec2/responses/vpcs.py
    moto/ec2/responses/vpn_connections.py
    moto/ec2/responses/windows.py
    moto/ec2/urls.py
    moto/ec2/utils.py

    moto/iam/aws_managed_policies.py
    moto/iam/exceptions.py
    moto/iam/__init__.py
    moto/iam/models.py
    moto/iam/responses.py
    moto/iam/urls.py
    moto/iam/utils.py

    moto/kms/__init__.py
    moto/kms/models.py
    moto/kms/responses.py
    moto/kms/urls.py
    moto/kms/utils.py

    moto/route53/__init__.py
    moto/route53/models.py
    moto/route53/responses.py
    moto/route53/urls.py

    moto/s3/__init__.py
    moto/s3/exceptions.py
    moto/s3/models.py
    moto/s3/urls.py
    moto/s3/utils.py
    moto/s3/responses.py

    moto/s3bucket_path/__init__.py
    moto/s3bucket_path/utils.py

    moto/sts/__init__.py
    moto/sts/models.py
    moto/sts/responses.py
    moto/sts/urls.py

    moto/packages/__init__.py
    moto/packages/httpretty/__init__.py
    moto/packages/httpretty/compat.py
    moto/packages/httpretty/core.py
    moto/packages/httpretty/errors.py
    moto/packages/httpretty/http.py
    moto/packages/httpretty/utils.py

    moto/instance_metadata/__init__.py
    moto/instance_metadata/models.py
    moto/instance_metadata/responses.py
    moto/instance_metadata/urls.py
)

RESOURCE(
    moto/ec2/resources/amis.json resource/amis.json
    moto/ec2/resources/instance_types.json resource/instance_types.json
)
END()
