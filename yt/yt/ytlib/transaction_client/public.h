#pragma once

#include <yt/yt/client/transaction_client/public.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TTransactionActionData;

class TReqStartTransaction;
class TRspStartTransaction;

class TReqRegisterTransactionActions;
class TRspRegisterTransactionActions;

class TReqReplicateTransactions;
class TRspReplicateTransactions;

class TReqIssueLeases;
class TRspIssueLeases;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TTransactionActionData;
DECLARE_REFCOUNTED_CLASS(TTransaction)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)
DECLARE_REFCOUNTED_STRUCT(IClockManager)

DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)
DECLARE_REFCOUNTED_CLASS(TClockManagerConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicClockManagerConfig)

//! Signatures enable checking tablet transaction integrity.
/*!
 *  When a transaction is created, its signature is #InitialTransactionSignature.
 *  Each batch within a transaction is annotated with a signature; these signatures are
 *  added to the transaction's signature. For a commit to be successful, the final signature must
 *  be equal to #FinalTransactionSignature.
 */
using TTransactionSignature = ui32;
const TTransactionSignature InitialTransactionSignature = 0;
const TTransactionSignature FinalTransactionSignature = 0xffffffffU;

//! Generation provides means for tablet transaction write reties ensuring that each batch is going
//! to be processed at most once.
/*!
 *  Each batch within a transaction is annotated with a generation; whenever a client is unsure whether
 *  some write request was accepted or not (e.g. due to network error), he increments the generation
 *  and re-sends all batches from scratch.
 *
 *  Lowest bytes is reserved for native client retries, while upper bytes may be used for
 *  higher-level API retries (like RPC or HTTP API).
 */
using TTransactionGeneration = ui32;
const TTransactionGeneration InitialTransactionGeneration = 0;
const TTransactionGeneration NativeGenerationMask = 0xffU;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
