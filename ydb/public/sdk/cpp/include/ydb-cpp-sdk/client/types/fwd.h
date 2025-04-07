#pragma once

namespace NYdb::inline Dev {

template<typename TDerived>
struct TRequestSettings;

template<typename TDerived>
struct TSimpleRequestSettings;

template<typename TDerived>
struct TOperationRequestSettings;

template <typename TDerived>
struct TS3Settings;

class TStatus;
class TStreamPartStatus;

class TOperation;

class TYdbException;
class TContractViolation;

class ICredentialsProvider;
class ICredentialsProviderFactory;

class ITokenSource;

namespace NStatusHelpers {
class TYdbErrorException;
}

}  // namespace NYdb
