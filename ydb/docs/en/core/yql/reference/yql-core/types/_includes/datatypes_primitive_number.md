| Type | Description | Notes |
| ----- | ----- | ----- |
| `Bool ` | Boolean value. |
| `Int8` | A signed integer.<br/>Acceptable values: from -2<sup>7</sup> to 2<sup>7</sup>–1. |
| `Int16` | A signed integer.<br/>Acceptable values: from –2<sup>15</sup> to 2<sup>15</sup>–1. |
| `Int32` | A signed integer.<br/>Acceptable values: from –2<sup>31</sup> to 2<sup>31</sup>–1. |
| `Int64` | A signed integer.<br/>Acceptable values: from –2<sup>63</sup> to 2<sup>63</sup>–1. |
| `Uint8` | An unsigned integer.<br/>Acceptable values: from 0 to 2<sup>8</sup>–1. |
| `Uint16` | An unsigned integer.<br/>Acceptable values: from 0 to 2<sup>16</sup>–1. |
| `Uint32` | An unsigned integer.<br/>Acceptable values: from 0 to 2<sup>32</sup>–1. |
| `Uint64` | An unsigned integer.<br/>Acceptable values: from 0 to 2<sup>64</sup>–1. |
| `Float` | A real number with variable precision, 4 bytes in size. | {% if feature_map_tables %}Can't be used in the primary key{% endif %} |
| `Double` | A real number with variable precision, 8 bytes in size. | {% if feature_map_tables %}Can't be used in the primary key{% endif %} |
| `Decimal` | A real number with the specified precision, up to 35 decimal digits | {% if feature_map_tables %}When used in table columns, the precision is fixed: Decimal(22,9){% endif %} |
{% if feature_map_tables %}
|`DyNumber` | A binary representation of a real number with an accuracy of up to 38 digits.<br/>Acceptable values: positive numbers from 1×10<sup>-130</sup> up to 1×10<sup>126</sup>–1, negative numbers from -1×10<sup>126</sup>–1 to -1×10<sup>-130</sup>, and 0.<br/>Compatible with the `Number` type in AWS DynamoDB. It's not recommended for {{ backend_name_lower }}-native applications. | |
{% endif %}
