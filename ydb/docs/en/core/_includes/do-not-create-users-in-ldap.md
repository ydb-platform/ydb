{% note info %}

The scope of the commands `CREATE USER`, `ALTER USER`, and `DROP USER` does not extend to external user directories. Keep this in mind if users with third-party authentication (e.g., LDAP) are connecting to {{ ydb-short-name }}. For example, the `CREATE USER` command does not create a user in the LDAP directory. Learn more about [{{ ydb-short-name }}'s interaction with the LDAP directory](../security/authentication.md#ldap-auth-provider).

{% endnote %}
