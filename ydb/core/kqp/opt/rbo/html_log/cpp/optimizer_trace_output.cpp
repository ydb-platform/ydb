#include "optimizer_trace_output.h"

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/random/random.h>
#include <util/stream/output.h>
#include <util/system/atexit.h>
#include <util/system/fstat.h>
#include <util/system/getpid.h>
#include <util/system/thread.h>
#include <util/system/types.h>

#include <exception>
#include <fstream>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace optimizer_trace {

namespace {

std::string MakeRandomBase64UrlSuffix();

struct TTraceHttpEndpoint {
    std::string Host;
    ui16 Port = 0;
    std::string Url;
};

bool ParsePort(std::string_view value, ui16& port) {
    if (value.empty()) {
        return false;
    }

    ui32 parsed = 0;
    for (const char ch : value) {
        if (ch < '0' || ch > '9') {
            return false;
        }

        parsed = parsed * 10 + static_cast<ui32>(ch - '0');
        if (parsed > 65535) {
            return false;
        }
    }

    if (parsed == 0) {
        return false;
    }

    port = static_cast<ui16>(parsed);
    return true;
}

std::string FormatHttpUrlHost(std::string_view host) {
    if (host.find(':') != std::string_view::npos) {
        std::ostringstream out;
        out << "[" << host << "]";
        return out.str();
    }

    return std::string(host);
}

bool ParseHostPort(std::string_view raw, std::string& host, ui16& port) {
    if (raw.empty()) {
        return false;
    }

    if (raw.front() == '[') {
        const auto bracket = raw.find(']');
        if (bracket == std::string_view::npos || bracket == 1 ||
            bracket + 1 >= raw.size() || raw[bracket + 1] != ':') {
            return false;
        }

        host = std::string(raw.substr(1, bracket - 1));
        return ParsePort(raw.substr(bracket + 2), port);
    }

    const auto colon = raw.rfind(':');
    if (colon == std::string_view::npos || colon == 0 || colon + 1 == raw.size()) {
        return false;
    }

    host = std::string(raw.substr(0, colon));
    return ParsePort(raw.substr(colon + 1), port);
}

std::optional<TTraceHttpEndpoint> TryParseTraceHttpEndpoint(const std::string& value) {
    if (value.find('/') != std::string::npos || value.find('\\') != std::string::npos) {
        return std::nullopt;
    }

    std::string host;
    ui16 port = 0;
    if (!ParseHostPort(value, host, port)) {
        return std::nullopt;
    }

    TTraceHttpEndpoint endpoint;
    endpoint.Host = std::move(host);
    endpoint.Port = port;
    std::ostringstream url;
    url << "http://" << FormatHttpUrlHost(endpoint.Host) << ":" << endpoint.Port << "/";
    endpoint.Url = url.str();
    return endpoint;
}

class TTraceMemorySink final : public ITracePageSink {
public:
    GenerateResult Reset(const std::string& content) override {
        std::lock_guard<std::mutex> lock(Mutex_);
        Content_ = content;
        Ready_ = true;
        return GenerateResult::success();
    }

    GenerateResult Append(const std::string& content) override {
        std::lock_guard<std::mutex> lock(Mutex_);
        Content_ += content;
        Ready_ = true;
        return GenerateResult::success();
    }

    TString Snapshot() const {
        std::lock_guard<std::mutex> lock(Mutex_);
        return TString(Content_.data(), Content_.size());
    }

    bool Ready() const {
        std::lock_guard<std::mutex> lock(Mutex_);
        return Ready_;
    }

    std::size_t Size() const {
        std::lock_guard<std::mutex> lock(Mutex_);
        return Content_.size();
    }

private:
    mutable std::mutex Mutex_;
    std::string Content_;
    bool Ready_ = false;
};

class TTraceHttpCallback final : public THttpServer::ICallBack {
    class TRequest final : public TRequestReplier {
    public:
        TRequest(std::shared_ptr<TTraceMemorySink> sink, std::string title, std::string emptyMessage)
            : Sink_(std::move(sink))
            , Title_(std::move(title))
            , EmptyMessage_(std::move(emptyMessage))
        {
        }

        bool DoReply(const TReplyParams& params) override {
            THttpResponse response;
            response.AddOrReplaceHeader("Cache-Control", "no-store");

            try {
                const TParsedHttpFull parsed(params.Input.FirstLine());
                const TStringBuf path = parsed.Path;

                if (path == "/" || path == "/trace.html") {
                    TString content = Sink_->Snapshot();
                    if (content.empty()) {
                        content = TStringBuilder()
                            << "<!DOCTYPE html><html><head><meta charset=\"UTF-8\">"
                            << "<meta http-equiv=\"refresh\" content=\"1\">"
                            << "<title>" << Title_ << "</title></head><body>"
                            << EmptyMessage_
                            << "</body></html>\n";
                    }
                    response.SetContent(std::move(content), "text/html; charset=utf-8");
                } else if (path == "/status") {
                    TString status = TStringBuilder()
                        << "ready=" << (Sink_->Ready() ? "true" : "false")
                        << "\nbytes=" << Sink_->Size()
                        << "\n";
                    response.SetContent(std::move(status), "text/plain; charset=utf-8");
                } else {
                    response.SetHttpCode(HTTP_NOT_FOUND)
                        .SetContent("not found\n", "text/plain; charset=utf-8");
                }
            } catch (...) {
                response.SetHttpCode(HTTP_BAD_REQUEST)
                    .SetContent("bad request\n", "text/plain; charset=utf-8");
            }

            response.OutTo(params.Output);
            params.Output.Finish();
            return true;
        }

    private:
        std::shared_ptr<TTraceMemorySink> Sink_;
        std::string Title_;
        std::string EmptyMessage_;
    };

public:
    TTraceHttpCallback(std::shared_ptr<TTraceMemorySink> sink, std::string title, std::string emptyMessage)
        : Sink_(std::move(sink))
        , Title_(std::move(title))
        , EmptyMessage_(std::move(emptyMessage))
    {
    }

    TClientRequest* CreateClient() override {
        return new TRequest(Sink_, Title_, EmptyMessage_);
    }

private:
    std::shared_ptr<TTraceMemorySink> Sink_;
    std::string Title_;
    std::string EmptyMessage_;
};

THttpServer::TOptions MakeTraceHttpOptions(const TTraceHttpEndpoint& endpoint, const std::string& threadNamePrefix) {
    THttpServer::TOptions options;
    options.AddBindAddress(TString(endpoint.Host.data(), endpoint.Host.size()), endpoint.Port);
    options.SetThreads(1);
    options.SetMaxConnections(32);
    options.SetMaxQueueSize(32);
    options.EnableKeepAlive(true);

    const TString prefix(threadNamePrefix.data(), threadNamePrefix.size());
    options.SetThreadsName(
        TStringBuilder() << prefix << "-listen",
        TStringBuilder() << prefix << "-http",
        TStringBuilder() << prefix << "-http-fail");
    return options;
}

struct TTraceLogSession {
    TTraceLogSession(std::string path, const TracePageOptions& pageOptions, bool appendToExisting)
        : Path(std::move(path))
        , Page(Path, pageOptions, appendToExisting)
    {
    }

    TTraceLogSession(const TTraceHttpEndpoint& endpoint, const TraceOutputOptions& options)
        : Path(endpoint.Url)
        , HttpMode(true)
        , Url(endpoint.Url)
        , MemorySink(std::make_shared<TTraceMemorySink>())
        , Page(MemorySink, options.pageOptions)
        , HttpCallback(std::make_unique<TTraceHttpCallback>(
            MemorySink,
            options.httpTitle,
            options.emptyHttpMessage))
        , HttpServer(std::make_unique<THttpServer>(
            HttpCallback.get(),
            MakeTraceHttpOptions(endpoint, options.httpThreadNamePrefix)))
    {
        if (!HttpServer->Start()) {
            std::ostringstream error;
            error << "failed to bind " << endpoint.Host << ":" << endpoint.Port
                  << ": " << HttpServer->GetError();
            throw std::runtime_error(error.str());
        }
    }

    std::string Path;
    bool HttpMode = false;
    std::string Url;
    std::mutex Mutex;
    std::shared_ptr<TTraceMemorySink> MemorySink;
    TracePage Page;
    std::unique_ptr<TTraceHttpCallback> HttpCallback;
    std::unique_ptr<THttpServer> HttpServer;
};

std::mutex TraceLogSessionsMutex;
THashMap<std::string, std::shared_ptr<TTraceLogSession>> TraceLogSessions;

void WaitForTraceHttpServer(const std::shared_ptr<TTraceLogSession>& session) {
    if (!session || !session->HttpMode) {
        return;
    }

    Cerr << "Optimizer trace is available at "
         << session->Url << "trace.html"
         << "; press Ctrl+C to stop serving it" << Endl;
    for (;;) {
        Sleep(TDuration::Seconds(3600));
    }
}

void WaitForTraceHttpServerAtExit(void* ctx) {
    std::unique_ptr<std::shared_ptr<TTraceLogSession>> session(
        static_cast<std::shared_ptr<TTraceLogSession>*>(ctx)
    );
    WaitForTraceHttpServer(*session);
}

void RegisterTraceHttpServerAtExit(const std::shared_ptr<TTraceLogSession>& session) {
    AtExit(WaitForTraceHttpServerAtExit, new std::shared_ptr<TTraceLogSession>(session));
}

bool EndsWith(std::string_view value, std::string_view suffix) {
    return value.size() >= suffix.size() &&
        value.substr(value.size() - suffix.size()) == suffix;
}

std::string NormalizeTracePath(const std::string& path) {
    std::string result = path;
    if (!EndsWith(result, ".html") && !EndsWith(result, ".htm")) {
        result += ".html";
    }
    return result;
}

void EnsureParentDir(const std::string& filename) {
    const TFsPath parent = TFsPath(filename).Parent();
    if (parent && !parent.Exists()) {
        parent.MkDirs();
    }
}

bool IsDirectoryOutputPath(std::string_view path) {
    return EndsWith(path, "/") || EndsWith(path, "\\");
}

std::string MakeTraceFileName() {
    const TString timestamp = TInstant::Now().FormatLocalTime("%Y%m%d-%H%M%S");
    std::ostringstream out;
    out << "rbo-trace-"
        << timestamp.c_str()
        << "-" << GetPID()
        << "-" << MakeRandomBase64UrlSuffix()
        << ".html";
    return out.str();
}

std::string MakeTraceFileInDirectory(const TFsPath& directory) {
    for (ui32 attempt = 0; attempt < 16; ++attempt) {
        const TFsPath candidate = directory / MakeTraceFileName();
        if (!candidate.Exists()) {
            return candidate.GetPath();
        }
    }

    return (directory / MakeTraceFileName()).GetPath();
}

bool IsNonEmptyFile(const std::string& filename) {
    TFileStat stat;
    const TFsPath path(filename);
    return path.Stat(stat) && stat.IsFile() && stat.Size > 0;
}

struct TTraceFileOutput {
    std::string Path;
    bool AppendToExisting = false;
};

std::optional<TTraceFileOutput> ResolveTraceFileOutput(const std::string& output, std::string& error) {
    const bool directoryHint = IsDirectoryOutputPath(output);
    const TFsPath rawPath(output);
    if (directoryHint || rawPath.IsDirectory()) {
        if (rawPath.Exists() && !rawPath.IsDirectory()) {
            error = "optimizer trace output path is not a directory: " + output;
            return std::nullopt;
        }
        if (!rawPath.Exists()) {
            try {
                rawPath.MkDirs();
            } catch (const std::exception& e) {
                error = "failed to create optimizer trace output directory " + output + ": " + e.what();
                return std::nullopt;
            } catch (...) {
                error = "failed to create optimizer trace output directory " + output;
                return std::nullopt;
            }
        }
        if (!rawPath.IsDirectory()) {
            error = "optimizer trace output path is not a directory: " + output;
            return std::nullopt;
        }
        return TTraceFileOutput{
            .Path = MakeTraceFileInDirectory(rawPath),
            .AppendToExisting = false
        };
    }

    const std::string tracePath = NormalizeTracePath(output);
    return TTraceFileOutput{
        .Path = tracePath,
        .AppendToExisting = IsNonEmptyFile(tracePath)
    };
}

std::string MakeFallbackSuffix(const std::string& suffix) {
    std::ostringstream out;
    out << GetPID()
        << "." << TThread::CurrentThreadNumericId();
    if (!suffix.empty()) {
        out << "." << suffix;
    } else {
        out << "." << RandomNumber<ui64>();
    }
    return out.str();
}

std::string MakeFallbackTracePath(const std::string& aggregatePath, const std::string& suffix) {
    std::string stem = aggregatePath;
    if (EndsWith(stem, ".html")) {
        stem.resize(stem.size() - std::string_view(".html").size());
    } else if (EndsWith(stem, ".htm")) {
        stem.resize(stem.size() - std::string_view(".htm").size());
    }

    std::ostringstream out;
    out << stem << ".fallback." << MakeFallbackSuffix(suffix) << ".html";
    return out.str();
}

std::shared_ptr<TTraceLogSession> TryPrepareTraceFileLog(
    const TTraceFileOutput& file,
    const TracePageOptions& pageOptions)
{
    EnsureParentDir(file.Path);

    std::lock_guard<std::mutex> lock(TraceLogSessionsMutex);
    if (auto it = TraceLogSessions.find(file.Path); it != TraceLogSessions.end()) {
        return it->second;
    }

    std::ofstream out(file.Path, std::ios::app);
    if (!out.is_open() || !static_cast<bool>(out)) {
        return nullptr;
    }
    out.close();

    auto session = std::make_shared<TTraceLogSession>(file.Path, pageOptions, file.AppendToExisting);
    TraceLogSessions.emplace(file.Path, session);
    return session;
}

std::shared_ptr<TTraceLogSession> TryPrepareTraceHttpLog(
    const TTraceHttpEndpoint& endpoint,
    const TraceOutputOptions& options)
{
    std::lock_guard<std::mutex> lock(TraceLogSessionsMutex);
    if (auto it = TraceLogSessions.find(endpoint.Url); it != TraceLogSessions.end()) {
        return it->second;
    }

    auto session = std::make_shared<TTraceLogSession>(endpoint, options);
    TraceLogSessions.emplace(endpoint.Url, session);
    RegisterTraceHttpServerAtExit(session);
    return session;
}

std::shared_ptr<TTraceLogSession> PrepareTraceLog(const TraceOutputOptions& options, std::string& error) {
    if (auto endpoint = TryParseTraceHttpEndpoint(options.output)) {
        try {
            return TryPrepareTraceHttpLog(*endpoint, options);
        } catch (const std::exception& e) {
            std::ostringstream out;
            out << "failed to start optimizer trace HTTP server at "
                << endpoint->Url << ": " << e.what();
            error = out.str();
        } catch (...) {
            error = "failed to start optimizer trace HTTP server at " + endpoint->Url;
        }

        return nullptr;
    }

    const auto traceFile = ResolveTraceFileOutput(options.output, error);
    if (!traceFile) {
        return nullptr;
    }

    try {
        if (auto result = TryPrepareTraceFileLog(*traceFile, options.pageOptions)) {
            return result;
        }
    } catch (const std::exception& e) {
        std::ostringstream out;
        out << "failed to prepare optimizer trace log at " << traceFile->Path << ": " << e.what();
        error = out.str();
    } catch (...) {
        error = "failed to prepare optimizer trace log at " + traceFile->Path;
    }

    const std::string fallbackPath = MakeFallbackTracePath(traceFile->Path, options.fallbackSuffix);
    try {
        if (auto result = TryPrepareTraceFileLog(
                TTraceFileOutput{.Path = fallbackPath, .AppendToExisting = false},
                options.pageOptions)) {
            return result;
        }
    } catch (const std::exception& e) {
        std::ostringstream out;
        out << "failed to prepare fallback optimizer trace log at "
            << fallbackPath << ": " << e.what();
        error = out.str();
    } catch (...) {
        error = "failed to prepare fallback optimizer trace log at " + fallbackPath;
    }

    if (error.empty()) {
        error = "failed to prepare optimizer trace log at " + traceFile->Path +
            " or fallback path " + fallbackPath;
    }
    return nullptr;
}

std::string MakeRandomBase64UrlSuffix() {
    static constexpr char Alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789-_";

    ui64 value = RandomNumber<ui64>();
    std::string suffix;
    suffix.reserve(8);
    for (ui32 i = 0; i < 8; ++i) {
        suffix.push_back(Alphabet[value & 0x3F]);
        value >>= 6;
    }

    return suffix;
}

} // anonymous namespace

struct TraceOutput::Impl {
    explicit Impl(TraceOutputOptions options) {
        if (options.output.empty()) {
            Error = "optimizer trace output path is empty";
            return;
        }

        Session = PrepareTraceLog(options, Error);
        if (!Session) {
            return;
        }

        SessionLock.emplace(Session->Mutex);
    }

    std::shared_ptr<TTraceLogSession> Session;
    std::optional<std::unique_lock<std::mutex>> SessionLock;
    std::string Error;
};

TraceOutput::TraceOutput(TraceOutputOptions options)
    : Impl_(std::make_unique<Impl>(std::move(options)))
{
}

TraceOutput::~TraceOutput() = default;
TraceOutput::TraceOutput(TraceOutput&&) noexcept = default;
TraceOutput& TraceOutput::operator=(TraceOutput&&) noexcept = default;

bool TraceOutput::isOpen() const {
    return Impl_ && Impl_->Session;
}

bool TraceOutput::httpMode() const {
    return isOpen() && Impl_->Session->HttpMode;
}

const std::string& TraceOutput::output() const {
    static const std::string Empty;
    return isOpen() ? Impl_->Session->Path : Empty;
}

std::string TraceOutput::traceUrl() const {
    if (!httpMode()) {
        return {};
    }
    return Impl_->Session->Url + "trace.html";
}

const std::string& TraceOutput::error() const {
    static const std::string Empty;
    return Impl_ ? Impl_->Error : Empty;
}

Trace& TraceOutput::trace(const std::string& title) {
    if (!isOpen()) {
        throw std::logic_error("TraceOutput::trace() requires an open output");
    }
    return Impl_->Session->Page.trace(title);
}

GenerateResult TraceOutput::submit(Trace::Tile& tile) {
    if (!isOpen()) {
        return GenerateResult::failure("TraceOutput::submit() requires an open output");
    }
    return Impl_->Session->Page.submit(tile);
}

GenerateResult TraceOutput::flush() {
    if (!isOpen()) {
        return GenerateResult::success();
    }
    return Impl_->Session->Page.flush();
}

std::string MakeDefaultTraceTitle() {
    const TString timestamp = TInstant::Now().FormatLocalTime("%Y-%m-%d %H:%M:%S");
    std::ostringstream out;
    out << timestamp.c_str() << " #" << MakeRandomBase64UrlSuffix();
    return out.str();
}

} // namespace optimizer_trace
