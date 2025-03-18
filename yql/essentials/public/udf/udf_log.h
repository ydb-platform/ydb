#pragma once
#include "udf_type_size_check.h"
#include "udf_string_ref.h"
#include "udf_ptr.h"
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>
#include <util/generic/maybe.h>
#include <util/stream/output.h>

#include <functional>

namespace NYql {
namespace NUdf {

#define UDF_LOG_LEVEL(XX) \
    XX(Fatal, 0)  \
    XX(Error, 1)  \
    XX(Warn, 2)   \
    XX(Notice, 3) \
    XX(Info, 4) \
    XX(Debug, 5) \
    XX(Trace, 6)

enum class ELogLevel : ui32 {
    UDF_LOG_LEVEL(ENUM_VALUE_GEN)
};

inline bool IsLogLevelAllowed(ELogLevel message, ELogLevel threshold) {
    return static_cast<ui32>(message) <= static_cast<ui32>(threshold);
}

TStringBuf LevelToString(ELogLevel level);
TMaybe<ELogLevel> TryLevelFromString(TStringBuf str);

using TLogComponentId = ui32;

class ILogger : public IRefCounted {
public:
    virtual TLogComponentId RegisterComponent(const TStringRef& component) = 0;
    virtual void SetDefaultLevel(ELogLevel level) = 0;
    virtual void SetComponentLevel(TLogComponentId component, ELogLevel level) = 0;
    virtual bool IsActive(TLogComponentId component, ELogLevel level) const = 0;
    virtual void Log(TLogComponentId component, ELogLevel level, const TStringRef& message) = 0;
};

using TLoggerPtr = TRefCountedPtr<ILogger>;

UDF_ASSERT_TYPE_SIZE(ILogger, 16);

class ILogProvider {
public:
    virtual ~ILogProvider() = default;

    virtual TLoggerPtr MakeLogger() const = 0;
};

UDF_ASSERT_TYPE_SIZE(ILogProvider, 8);

TLoggerPtr MakeNullLogger();
TLoggerPtr MakeSynchronizedLogger(const TLoggerPtr& inner);
using TLogProviderFunc = std::function<void(const TStringRef&, ELogLevel, const TStringRef&)>;
TUniquePtr<ILogProvider> MakeLogProvider(TLogProviderFunc func, TMaybe<ELogLevel> filter = Nothing());

} // namspace NUdf
} // namspace NYql

template<>
inline void Out<NYql::NUdf::ELogLevel>(IOutputStream &o, NYql::NUdf::ELogLevel value) {
    o << NYql::NUdf::LevelToString(value);
}
