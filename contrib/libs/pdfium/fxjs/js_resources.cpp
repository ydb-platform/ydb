// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "fxjs/js_resources.h"

WideString JSGetStringFromID(JSMessage msg) {
  const char* msg_string = "";
  switch (msg) {
    case JSMessage::kAlert:
      msg_string = "Alert";
      break;
    case JSMessage::kParamError:
      msg_string = "Incorrect number of parameters passed to function.";
      break;
    case JSMessage::kInvalidInputError:
      msg_string = "The input value is invalid.";
      break;
    case JSMessage::kParamTooLongError:
      msg_string = "The input value is too long.";
      break;
    case JSMessage::kParseDateError:
      msg_string =
          "The input value can't be parsed as a valid date/time (%ls).";
      break;
    case JSMessage::kRangeBetweenError:
      msg_string =
          "The input value must be greater than or equal to %ls"
          " and less than or equal to %ls.";
      break;
    case JSMessage::kRangeGreaterError:
      msg_string = "The input value must be greater than or equal to %ls.";
      break;
    case JSMessage::kRangeLessError:
      msg_string = "The input value must be less than or equal to %ls.";
      break;
    case JSMessage::kNotSupportedError:
      msg_string = "Operation not supported.";
      break;
    case JSMessage::kBusyError:
      msg_string = "System is busy.";
      break;
    case JSMessage::kDuplicateEventError:
      msg_string = "Duplicate formfield event found.";
      break;
    case JSMessage::kSecondParamNotDateError:
      msg_string = "The second parameter can't be converted to a Date.";
      break;
    case JSMessage::kSecondParamInvalidDateError:
      msg_string = "The second parameter is an invalid Date.";
      break;
    case JSMessage::kGlobalNotFoundError:
      msg_string = "Global value not found.";
      break;
    case JSMessage::kReadOnlyError:
      msg_string = "Cannot assign to readonly property.";
      break;
    case JSMessage::kTypeError:
      msg_string = "Incorrect parameter type.";
      break;
    case JSMessage::kValueError:
      msg_string = "Incorrect parameter value.";
      break;
    case JSMessage::kPermissionError:
      msg_string = "Permission denied.";
      break;
    case JSMessage::kBadObjectError:
      msg_string = "Object no longer exists.";
      break;
    case JSMessage::kObjectTypeError:
      msg_string = "Object is of the wrong type.";
      break;
    case JSMessage::kUnknownProperty:
      msg_string = "Unknown property.";
      break;
    case JSMessage::kInvalidSetError:
      msg_string = "Set not possible, invalid or unknown.";
      break;
    case JSMessage::kUserGestureRequiredError:
      msg_string = "User gesture required.";
      break;
    case JSMessage::kTooManyOccurrences:
      msg_string = "Too many occurrences.";
      break;
    case JSMessage::kUnknownMethod:
      msg_string = "Unknown method.";
      break;
    case JSMessage::kWouldBeCyclic:
      msg_string = "Operation would create a cycle.";
      break;
  }
  return WideString::FromASCII(msg_string);
}

WideString JSFormatErrorString(const char* class_name,
                               const char* property_name,
                               const WideString& details) {
  WideString result = WideString::FromUTF8(class_name);
  if (property_name) {
    result += L".";
    result += WideString::FromUTF8(property_name);
  }
  result += L": ";
  result += details;
  return result;
}
