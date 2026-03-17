//-----------------------------------------------------------------------------
// Copyright (c) 2018, 2024, Oracle and/or its affiliates.
//
// This software is dual-licensed to you under the Universal Permissive License
// (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
// 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
// either license.
//
// If you elect to accept the software under the Apache License, Version 2.0,
// the following applies:
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//-----------------------------------------------------------------------------

//-----------------------------------------------------------------------------
// dpi.c
//   Include this file in your project in order to embed ODPI-C source without
// having to compile files individually. Only the definitions in the file
// include/dpi.h are intended to be used publicly. Each file can also be
// compiled independently if that is preferable.
//-----------------------------------------------------------------------------

#include "../src/dpiConn.c"
#include "../src/dpiContext.c"
#include "../src/dpiData.c"
#include "../src/dpiDebug.c"
#include "../src/dpiDeqOptions.c"
#include "../src/dpiEnqOptions.c"
#include "../src/dpiEnv.c"
#include "../src/dpiError.c"
#include "../src/dpiGen.c"
#include "../src/dpiGlobal.c"
#include "../src/dpiHandleList.c"
#include "../src/dpiHandlePool.c"
#include "../src/dpiJson.c"
#include "../src/dpiLob.c"
#include "../src/dpiMsgProps.c"
#include "../src/dpiObjectAttr.c"
#include "../src/dpiObject.c"
#include "../src/dpiObjectType.c"
#include "../src/dpiOci.c"
#include "../src/dpiOracleType.c"
#include "../src/dpiPool.c"
#include "../src/dpiQueue.c"
#include "../src/dpiRowid.c"
#include "../src/dpiSodaColl.c"
#include "../src/dpiSodaCollCursor.c"
#include "../src/dpiSodaDb.c"
#include "../src/dpiSodaDoc.c"
#include "../src/dpiSodaDocCursor.c"
#include "../src/dpiStmt.c"
#include "../src/dpiStringList.c"
#include "../src/dpiSubscr.c"
#include "../src/dpiUtils.c"
#include "../src/dpiVar.c"
#include "../src/dpiVector.c"
