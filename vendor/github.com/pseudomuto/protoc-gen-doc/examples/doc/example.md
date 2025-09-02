# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [Booking.proto](#Booking-proto)
    - [Booking](#com-example-Booking)
    - [BookingStatus](#com-example-BookingStatus)
    - [BookingStatusID](#com-example-BookingStatusID)
    - [EmptyBookingMessage](#com-example-EmptyBookingMessage)
  
    - [BookingService](#com-example-BookingService)
  
- [Customer.proto](#Customer-proto)
    - [Address](#com-example-Address)
    - [Customer](#com-example-Customer)
  
- [Vehicle.proto](#Vehicle-proto)
    - [Manufacturer](#com-example-Manufacturer)
    - [Model](#com-example-Model)
    - [Vehicle](#com-example-Vehicle)
    - [Vehicle.Category](#com-example-Vehicle-Category)
  
    - [Manufacturer.Category](#com-example-Manufacturer-Category)
  
    - [File-level Extensions](#Vehicle-proto-extensions)
  
- [Scalar Value Types](#scalar-value-types)



<a name="Booking-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## Booking.proto
Booking related messages.

This file is really just an example. The data model is completely
fictional.


<a name="com-example-Booking"></a>

### Booking
Represents the booking of a vehicle.

Vehicles are some cool shit. But drive carefully!


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| vehicle_id | [int32](#int32) |  | ID of booked vehicle. |
| customer_id | [int32](#int32) |  | Customer that booked the vehicle. |
| status | [BookingStatus](#com-example-BookingStatus) |  | Status of the booking. |
| confirmation_sent | [bool](#bool) |  | Has booking confirmation been sent? |
| payment_received | [bool](#bool) |  | Has payment been received? |
| color_preference | [string](#string) |  | **Deprecated.** Color preference of the customer. |






<a name="com-example-BookingStatus"></a>

### BookingStatus
Represents the status of a vehicle booking.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int32](#int32) |  | Unique booking status ID. |
| description | [string](#string) |  | Booking status description. E.g. &#34;Active&#34;. |






<a name="com-example-BookingStatusID"></a>

### BookingStatusID
Represents the booking status ID.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int32](#int32) |  | Unique booking status ID. |






<a name="com-example-EmptyBookingMessage"></a>

### EmptyBookingMessage
An empty message for testing





 

 

 


<a name="com-example-BookingService"></a>

### BookingService
Service for handling vehicle bookings.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| BookVehicle | [Booking](#com-example-Booking) | [BookingStatus](#com-example-BookingStatus) | Used to book a vehicle. Pass in a Booking and a BookingStatus will be returned. |
| BookingUpdates | [BookingStatusID](#com-example-BookingStatusID) | [BookingStatus](#com-example-BookingStatus) stream | Used to subscribe to updates of the BookingStatus. |

 



<a name="Customer-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## Customer.proto
This file has messages for describing a customer.


<a name="com-example-Address"></a>

### Address
Represents a mail address.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| address_line_1 | [string](#string) | required | First address line. |
| address_line_2 | [string](#string) | optional | Second address line. |
| address_line_3 | [string](#string) | optional | Second address line. |
| town | [string](#string) | required | Address town. |
| county | [string](#string) | optional | Address county, if applicable. |
| country | [string](#string) | required | Address country. |






<a name="com-example-Customer"></a>

### Customer
Represents a customer.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int32](#int32) | required | Unique customer ID. |
| first_name | [string](#string) | required | Customer first name. |
| last_name | [string](#string) | required | Customer last name. |
| details | [string](#string) | optional | Customer details. |
| email_address | [string](#string) | optional | Customer e-mail address. |
| phone_number | [string](#string) | repeated | Customer phone numbers, primary first. |
| mail_addresses | [Address](#com-example-Address) | repeated | Customer mail addresses, primary first. |





 

 

 

 



<a name="Vehicle-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## Vehicle.proto
Messages describing manufacturers / vehicles.


<a name="com-example-Manufacturer"></a>

### Manufacturer
Represents a manufacturer of cars.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int32](#int32) | required | The unique manufacturer ID. |
| code | [string](#string) | required | A manufacturer code, e.g. &#34;DKL4P&#34;. |
| details | [string](#string) | optional | Manufacturer details (minimum orders et.c.). |
| category | [Manufacturer.Category](#com-example-Manufacturer-Category) | optional | Manufacturer category. Default: CATEGORY_EXTERNAL |






<a name="com-example-Model"></a>

### Model
Represents a vehicle model.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) | required | The unique model ID. |
| model_code | [string](#string) | required | The car model code, e.g. &#34;PZ003&#34;. |
| model_name | [string](#string) | required | The car model name, e.g. &#34;Z3&#34;. |
| daily_hire_rate_dollars | [sint32](#sint32) | required | Dollars per day. |
| daily_hire_rate_cents | [sint32](#sint32) | required | Cents per day. |






<a name="com-example-Vehicle"></a>

### Vehicle
Represents a vehicle that can be hired.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [int32](#int32) | required | Unique vehicle ID. |
| model | [Model](#com-example-Model) | required | Vehicle model. |
| reg_number | [string](#string) | required | Vehicle registration number. |
| mileage | [sint32](#sint32) | optional | Current vehicle mileage, if known. |
| category | [Vehicle.Category](#com-example-Vehicle-Category) | optional | Vehicle category. |
| daily_hire_rate_dollars | [sint32](#sint32) | optional | Dollars per day. Default: 50 |
| daily_hire_rate_cents | [sint32](#sint32) | optional | Cents per day. |




| Extension | Type | Base | Number | Description |
| --------- | ---- | ---- | ------ | ----------- |
| series | string | Model | 100 | Vehicle model series. |




<a name="com-example-Vehicle-Category"></a>

### Vehicle.Category
Represents a vehicle category. E.g. &#34;Sedan&#34; or &#34;Truck&#34;.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| code | [string](#string) | required | Category code. E.g. &#34;S&#34;. |
| description | [string](#string) | required | Category name. E.g. &#34;Sedan&#34;. |





 


<a name="com-example-Manufacturer-Category"></a>

### Manufacturer.Category
Manufacturer category. A manufacturer may be either inhouse or external.

| Name | Number | Description |
| ---- | ------ | ----------- |
| CATEGORY_INHOUSE | 0 | The manufacturer is inhouse. |
| CATEGORY_EXTERNAL | 1 | The manufacturer is external. |


 


<a name="Vehicle-proto-extensions"></a>

### File-level Extensions
| Extension | Type | Base | Number | Description |
| --------- | ---- | ---- | ------ | ----------- |
| country | string | Manufacturer | 100 | Manufacturer country. Default: `China` |

 

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

