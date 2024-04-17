# Short access control notation

When describing or logging the permissions granted to users (e.g., in the [audit log](./audit-log.md) records), a special short notation for access control may be used. The notation can vary slightly depending on the list of permissions and their inheritance from child objects.

## Notation format

Each entry begins with a `+` sign and consists of 2 or 3 attributes listed through the `:` symbol.
These attributes are:
- List of permissions. If there are multiple, they are wrapped in round brackets and separated by `|`. *Mandatory.*
- SID of the subject granted permissions. *Mandatory.*
- Inheritance type. *Optional.*

When the inheritance type is not specified, it means that permission wasn't inherited.

### Examples

* `+R:subject:O`
* `+W:subject`
* `+(SR|UR):subject`
* `+(SR|ConnDB):subject:OC+`

## List of permissions {#access-rights}

A short abbreviation is used to record each permission.

### Permission Groups

Permission groups are unions of several permissions. Where possible, one of the groups will be indicated in the short notation.
For example, `+R:subject` — permission to read.

| Group	| Description |
|:----:|:----|
| `L` | (list) enumeration. It consists of permissions to read ACL attributes and describe objects.|
| `R` | (read) reading. It consists of permissions to enumerate and read from a table and a topic.|
| `W` | (write) writing. It consists of permissions to update and delete table records, write ACL attributes, create subdirectories, create tables, and topics, modify and delete objects, and change user attributes.|
| `U` | (use) use. It consists of permissions for reading, writing, granting access rights, and sending requests to the database.|
| `UL` | (use legacy) obsolete version of use. It consists of permissions for reading, writing, and granting access rights.|
| `M` | (manage) management. It consists of permissions to create and delete databases.|
| `F` | (full) all rights. It consists of permissions for use and management.|
| `FL` | (full legacy) obsolete version of all rights. It consists of permissions for use (obsolete) and management.|

### Simple Permissions

If there's no matching permission group, the list of permissions will be provided in parentheses separated by the vertical bar `|` symbol.

For example, `+(SR|UR):subject` — permission for reading and updating table records.

| Permission | Description |
|:----:|:----|
| `SR` | (select row) reading from the table |
| `UR` | (update row) updating table records |
| `ER` | (erase row) deleting table records |
| `RA` | (read attributes) reading ACL attributes |
| `WA` | (write attributes) writing ACL attributes |
| `CD` | (create directory) creating subdirectory |
| `CT` | (create table) creating table |
| `CQ` | (create queue) creating queue |
| `RS` | (remove schema) deleting objects |
| `DS` | (describe schema) describing objects, listing directories content |
| `AS` | (alter schema) modifying objects |
| `CDB` | (create database) creating database |
| `DDB` | (drop database) deleting database |
| `GAR` | (grant access rights) granting access rights (not exceeding their own) |
| `WUA` | (write user attributes) changing user attributes |
| `ConnDB` | (connect database) connecting and sending requests to the database |

## Inheritance Types {#inheritance-types}

One or more inheritance flags can be used to describe the passing of permissions to child objects.

| Flag	| Description |
|:----:|:----|
| `-`	        | without inheritance |
| `O`	| this entry will be inherited by child objects |
| `C`	| this entry will be inherited by child containers |
| `+`	| this entry will be used only for inheritance and will not be used for access checking on the current object |
