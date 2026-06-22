#include "optimizer_trace.h"

#include "optimizer_trace_assets.h"
#include "optimizer_trace_config.h"

#include <library/cpp/json/writer/json.h>

#include <algorithm>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/file.h>
#include <util/system/file_lock.h>
#include <util/system/fstat.h>
#include <util/system/guard.h>
#include <util/system/types.h>

#include <cstdint>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <variant>

namespace optimizer_trace {
namespace {

constexpr std::size_t TraceHtmlSniffBytes = 4 * 1024 * 1024;

char lowerHexDigit(unsigned value) {
    return static_cast<char>(value < 10 ? '0' + value : 'a' + (value - 10));
}

std::string randomHex128() {
    std::random_device random;
    std::string out;
    out.reserve(32);
    for (int i = 0; i < 16; i++) {
        unsigned value = random() & 0xFFu;
        out += lowerHexDigit((value >> 4) & 0xFu);
        out += lowerHexDigit(value & 0xFu);
    }
    return out;
}

std::string base64UrlEncode(const std::string& data) {
    static const char alphabet[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string out;
    out.reserve((data.size() * 4 + 2) / 3);
    std::size_t i = 0;
    while (i + 3 <= data.size()) {
        uint32_t value =
            (static_cast<uint32_t>(static_cast<unsigned char>(data[i])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(data[i + 1])) << 8) |
            static_cast<uint32_t>(static_cast<unsigned char>(data[i + 2]));
        out += alphabet[(value >> 18) & 0x3F];
        out += alphabet[(value >> 12) & 0x3F];
        out += alphabet[(value >> 6) & 0x3F];
        out += alphabet[value & 0x3F];
        i += 3;
    }
    const std::size_t rem = data.size() - i;
    if (rem == 1) {
        uint32_t value = static_cast<uint32_t>(static_cast<unsigned char>(data[i])) << 16;
        out += alphabet[(value >> 18) & 0x3F];
        out += alphabet[(value >> 12) & 0x3F];
    } else if (rem == 2) {
        uint32_t value =
            (static_cast<uint32_t>(static_cast<unsigned char>(data[i])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(data[i + 1])) << 8);
        out += alphabet[(value >> 18) & 0x3F];
        out += alphabet[(value >> 12) & 0x3F];
        out += alphabet[(value >> 6) & 0x3F];
    }
    return out;
}

TStringBuf stringBuf(const std::string& value) {
    return TStringBuf(value.data(), value.size());
}

std::string toStdString(const TString& value) {
    return std::string(value.data(), value.size());
}

bool LooksLikeOptimizerTraceHtml(const std::string& content) {
    return content.find("optimizer-trace-app") != std::string::npos ||
        content.find("optimizer-trace-data") != std::string::npos;
}

template <class TEmit>
std::string buildJson(TEmit&& emit) {
    NJsonWriter::TBuf out(NJsonWriter::HEM_DONT_ESCAPE_HTML);
    emit(out);
    return toStdString(out.Str());
}

void writeString(NJsonWriter::TBuf& out, const std::string& value) {
    out.WriteString(stringBuf(value));
}

void writeKeyString(NJsonWriter::TBuf& out, const char* key, const std::string& value) {
    out.WriteKey(key).WriteString(stringBuf(value));
}

void writeKeyBool(NJsonWriter::TBuf& out, const char* key, bool value) {
    out.WriteKey(key).WriteBool(value);
}

void writeKeySize(NJsonWriter::TBuf& out, const char* key, std::size_t value) {
    out.WriteKey(key).WriteULongLong(static_cast<unsigned long long>(value));
}

void writeKeyDouble(NJsonWriter::TBuf& out, const char* key, double value) {
    out.WriteKey(key).WriteDouble(value);
}

void writeRawJsonValue(NJsonWriter::TBuf& out, const std::string& value) {
    out.UnsafeWriteValue(stringBuf(value));
}

std::string jsonStringLiteral(const std::string& value) {
    return buildJson([&](NJsonWriter::TBuf& out) {
        writeString(out, value);
    });
}

std::string htmlTextEsc(const std::string& input) {
    std::string out;
    out.reserve(input.size());
    for (char ch : input) {
        switch (ch) {
            case '&': out += "&amp;"; break;
            case '<': out += "&lt;"; break;
            case '>': out += "&gt;"; break;
            default: out += ch; break;
        }
    }
    return out;
}

std::string brotliCompress(const std::string& input, int level, bool* ok) {
    if (ok) *ok = false;
#if OPTIMIZER_TRACE_HTML_HAS_LIBBROTLI
    if (level < 0 || level > 11) level = 6;
    size_t encodedSize = BrotliEncoderMaxCompressedSize(input.size());
    if (encodedSize == 0) return std::string();
    std::string out;
    out.resize(encodedSize);
    int rc = BrotliEncoderCompress(
        level,
        BROTLI_DEFAULT_WINDOW,
        BROTLI_MODE_TEXT,
        input.size(),
        reinterpret_cast<const uint8_t*>(input.data()),
        &encodedSize,
        reinterpret_cast<uint8_t*>(&out[0])
    );
    if (rc == BROTLI_FALSE) return std::string();
    out.resize(encodedSize);
    if (ok) *ok = true;
    return out;
#elif OPTIMIZER_TRACE_HTML_HAS_APPLE_BROTLI
    (void)level;
    const compression_algorithm appleBrotli =
        static_cast<compression_algorithm>(0xB02);
    if (input.empty()) {
        unsigned char emptyInput = 0;
        unsigned char buffer[64];
        const size_t written = compression_encode_buffer(
            buffer,
            sizeof(buffer),
            &emptyInput,
            0,
            nullptr,
            appleBrotli
        );
        if (written == 0) return std::string();
        if (ok) *ok = true;
        return std::string(reinterpret_cast<char*>(buffer), written);
    }

    size_t capacity = input.size() + input.size() / 16 + 1024;
    if (capacity < 4096) capacity = 4096;
    for (int attempt = 0; attempt < 8; attempt++) {
        std::vector<uint8_t> out(capacity);
        const size_t written = compression_encode_buffer(
            out.data(),
            out.size(),
            reinterpret_cast<const uint8_t*>(input.data()),
            input.size(),
            nullptr,
            appleBrotli
        );
        if (written != 0) {
            if (ok) *ok = true;
            return std::string(reinterpret_cast<char*>(out.data()), written);
        }
        capacity *= 2;
    }
    return std::string();
#else
    (void)level;
    (void)input;
    return std::string();
#endif
}

}  // namespace

struct Widget::Impl {
    struct Text {
        std::string title;
        std::string text;
        bool wrap = true;
        bool lineNumbers = false;
    };

    struct Table {
        std::string title;
        std::vector<std::pair<std::string, std::string>> rows;
        bool monospaceKey = false;
        bool monospaceValue = false;
    };

    struct List {
        std::string title;
        std::vector<std::pair<std::string, std::string>> items;
        bool ordered = false;
        bool monospaceText = false;
        bool monospaceDetail = false;
    };

    struct Warning {
        std::string title;
        std::string message;
        std::string severity;
        std::string details;
    };

    struct GraphBody {
        std::string title;
        Graph graph;
        bool monospaceNode = false;
        bool monospaceEdge = false;
    };

    struct SwitcherOption {
        std::string id;
        std::string title;
        std::vector<Widget> widgets;
    };

    struct Switcher {
        std::string id;
        std::string title;
        std::vector<SwitcherOption> options;
        std::string defaultOptionId;
    };

    using Body = std::variant<Text, Table, List, Warning, GraphBody, Switcher>;

    explicit Impl(const Body& widgetBody)
        : body(widgetBody) {}

    Body body;
    std::vector<Target> targets;
    bool hasPrimaryTarget = false;
    Target primaryTarget = Target::node("");
};

namespace detail {

struct TileState {
    bool submitted = false;
    bool payloadReleased = false;
};

struct LogRecordContext {
    std::string appendId;
    std::string prevAppendId;
    std::string groupId;
    bool emitTrace = false;
    bool emitStage = false;
    bool emitGroup = false;
};

struct PendingLogRecord {
    std::string record;
    std::string payload;
    std::size_t payloadBytes = 0;
};

}  // namespace detail

GenerateResult::operator bool() const {
    return ok;
}

GenerateResult GenerateResult::success() {
    GenerateResult result;
    result.ok = true;
    return result;
}

GenerateResult GenerateResult::failure(const std::string& message) {
    GenerateResult result;
    result.ok = false;
    result.error = message;
    return result;
}

Target Target::node(const std::string& id) {
    return Target(Type::Node, id);
}

Target Target::subtree(const std::string& id) {
    return Target(Type::Subtree, id);
}

Target::Type Target::type() const {
    return type_;
}

const std::string& Target::nodeId() const {
    return nodeId_;
}

Target::Target(Type targetType, const std::string& targetNodeId)
    : type_(targetType), nodeId_(targetNodeId) {}

GraphNode::GraphNode() = default;

GraphNode::GraphNode(const std::string& id, const std::string& title)
    : id_(id), title_(title) {}

GraphNode& GraphNode::note(const std::string& value) {
    text_ = value;
    return *this;
}

GraphNode& GraphNode::target(const Target& target) {
    targets_.push_back(target);
    return *this;
}

GraphNode& GraphNode::targets(const std::vector<Target>& refs) {
    targets_ = refs;
    return *this;
}

GraphNode& GraphNode::targets(std::initializer_list<Target> refs) {
    targets_.assign(refs.begin(), refs.end());
    return *this;
}

GraphNode& GraphNode::primaryTarget(const Target& target) {
    hasPrimaryTarget_ = true;
    primaryTarget_ = target;
    return *this;
}

GraphEdge::GraphEdge() = default;

GraphEdge::GraphEdge(const std::string& from, const std::string& to)
    : from_(from), to_(to) {}

GraphEdge& GraphEdge::setId(const std::string& value) {
    id_ = value;
    return *this;
}

GraphEdge& GraphEdge::setLabel(const std::string& value) {
    label_ = value;
    return *this;
}

GraphEdge& GraphEdge::note(const std::string& value) {
    text_ = value;
    return *this;
}

GraphEdge& GraphEdge::target(const Target& target) {
    targets_.push_back(target);
    return *this;
}

GraphEdge& GraphEdge::targets(const std::vector<Target>& refs) {
    targets_ = refs;
    return *this;
}

GraphEdge& GraphEdge::targets(std::initializer_list<Target> refs) {
    targets_.assign(refs.begin(), refs.end());
    return *this;
}

GraphEdge& GraphEdge::primaryTarget(const Target& target) {
    hasPrimaryTarget_ = true;
    primaryTarget_ = target;
    return *this;
}

Graph::Graph() = default;

GraphNode& Graph::node(const std::string& id, const std::string& title) {
    nodes_.emplace_back(id, title);
    return nodes_.back();
}

Graph& Graph::addNode(const GraphNode& graphNode) {
    nodes_.push_back(graphNode);
    return *this;
}

GraphEdge& Graph::edge(const std::string& from, const std::string& to) {
    edges_.emplace_back(from, to);
    return edges_.back();
}

Graph& Graph::addEdge(const GraphEdge& graphEdge) {
    edges_.push_back(graphEdge);
    return *this;
}

Graph& Graph::layout(const std::string& direction,
                     double rankSeparation,
                     double nodeSeparation) {
    rankDir_ = direction;
    rankSep_ = rankSeparation;
    nodeSep_ = nodeSeparation;
    return *this;
}

Graph& Graph::directed(bool value) {
    directed_ = value;
    return *this;
}

Graph& Graph::undirected() {
    return directed(false);
}

Widget::Widget()
    : impl_(std::make_shared<Impl>(Impl::Text())) {}

Widget::~Widget() = default;

Widget::Widget(const Widget& other)
    : impl_(other.impl_ ? std::make_shared<Impl>(*other.impl_) : nullptr) {}

Widget& Widget::operator=(const Widget& other) {
    if (this != &other) {
        impl_ = other.impl_ ? std::make_shared<Impl>(*other.impl_) : nullptr;
    }
    return *this;
}

Widget::Widget(Widget&& other) noexcept = default;

Widget& Widget::operator=(Widget&& other) noexcept = default;

Widget Widget::text(const std::string& title, const std::string& body) {
    return Widget(Impl(Impl::Text{title, body, true, false}));
}

Widget Widget::unwrappedText(const std::string& title,
                             const std::string& body,
                             bool lineNumbers) {
    return Widget(Impl(Impl::Text{title, body, false, lineNumbers}));
}

Widget Widget::table(const std::string& title,
                     const std::vector<std::pair<std::string, std::string>>& rows) {
    return Widget(Impl(Impl::Table{title, rows}));
}

Widget Widget::list(const std::string& title,
                    const std::vector<std::string>& items,
                    bool ordered) {
    std::vector<std::pair<std::string, std::string>> normalized;
    normalized.reserve(items.size());
    for (const std::string& item : items) {
        normalized.push_back({item, ""});
    }
    return Widget(Impl(Impl::List{title, normalized, ordered}));
}

Widget Widget::list(const std::string& title,
                    const std::vector<std::pair<std::string, std::string>>& items,
                    bool ordered) {
    return Widget(Impl(Impl::List{title, items, ordered}));
}

Widget Widget::list(const std::string& title,
                    std::initializer_list<std::pair<std::string, std::string>> items,
                    bool ordered) {
    return list(
        title,
        std::vector<std::pair<std::string, std::string>>(items.begin(), items.end()),
        ordered
    );
}

Widget Widget::warning(const std::string& title,
                       const std::string& message,
                       const std::string& severity,
                       const std::string& details) {
    return Widget(Impl(Impl::Warning{title, message, severity, details}));
}

Widget Widget::graph(const std::string& title, const Graph& graph) {
    return Widget(Impl(Impl::GraphBody{title, graph}));
}

Widget Widget::switcher(const std::string& title) {
    return Widget(Impl(Impl::Switcher{"", title, {}, ""}));
}

Widget Widget::switcher(const std::string& id, const std::string& title) {
    return Widget(Impl(Impl::Switcher{id, title, {}, ""}));
}

Widget& Widget::option(const std::string& id,
                       const std::string& title,
                       const std::vector<Widget>& widgets) {
    Impl::Switcher* switcher = std::get_if<Impl::Switcher>(&impl().body);
    if (!switcher) {
        throw std::logic_error("Widget::option() is only valid for switcher widgets");
    }
    switcher->options.push_back(Impl::SwitcherOption{id, title, widgets});
    return *this;
}

Widget& Widget::option(const std::string& id,
                       const std::string& title,
                       std::initializer_list<Widget> widgets) {
    return option(id, title, std::vector<Widget>(widgets.begin(), widgets.end()));
}

Widget& Widget::defaultOption(const std::string& id) {
    Impl::Switcher* switcher = std::get_if<Impl::Switcher>(&impl().body);
    if (!switcher) {
        throw std::logic_error("Widget::defaultOption() is only valid for switcher widgets");
    }
    switcher->defaultOptionId = id;
    return *this;
}

Widget& Widget::target(const Target& target) {
    impl().targets.push_back(target);
    return *this;
}

Widget& Widget::targets(const std::vector<Target>& refs) {
    impl().targets = refs;
    return *this;
}

Widget& Widget::targets(std::initializer_list<Target> refs) {
    impl().targets.assign(refs.begin(), refs.end());
    return *this;
}

Widget& Widget::primaryTarget(const Target& target) {
    impl().hasPrimaryTarget = true;
    impl().primaryTarget = target;
    return *this;
}

Widget& Widget::monospaceKeys(bool value) {
    Impl::Table* table = std::get_if<Impl::Table>(&impl().body);
    if (!table) {
        throw std::logic_error("Widget::monospaceKeys() is only valid for table widgets");
    }
    table->monospaceKey = value;
    return *this;
}

Widget& Widget::monospaceValues(bool value) {
    Impl::Table* table = std::get_if<Impl::Table>(&impl().body);
    if (!table) {
        throw std::logic_error("Widget::monospaceValues() is only valid for table widgets");
    }
    table->monospaceValue = value;
    return *this;
}

Widget& Widget::monospaceTable(bool value) {
    return monospaceKeys(value).monospaceValues(value);
}

Widget& Widget::monospaceListText(bool value) {
    Impl::List* list = std::get_if<Impl::List>(&impl().body);
    if (!list) {
        throw std::logic_error("Widget::monospaceListText() is only valid for list widgets");
    }
    list->monospaceText = value;
    return *this;
}

Widget& Widget::monospaceListDetails(bool value) {
    Impl::List* list = std::get_if<Impl::List>(&impl().body);
    if (!list) {
        throw std::logic_error("Widget::monospaceListDetails() is only valid for list widgets");
    }
    list->monospaceDetail = value;
    return *this;
}

Widget& Widget::monospaceList(bool value) {
    return monospaceListText(value).monospaceListDetails(value);
}

Widget& Widget::monospaceGraphNodes(bool value) {
    Impl::GraphBody* graph = std::get_if<Impl::GraphBody>(&impl().body);
    if (!graph) {
        throw std::logic_error("Widget::monospaceGraphNodes() is only valid for graph widgets");
    }
    graph->monospaceNode = value;
    return *this;
}

Widget& Widget::monospaceGraphEdges(bool value) {
    Impl::GraphBody* graph = std::get_if<Impl::GraphBody>(&impl().body);
    if (!graph) {
        throw std::logic_error("Widget::monospaceGraphEdges() is only valid for graph widgets");
    }
    graph->monospaceEdge = value;
    return *this;
}

Widget& Widget::monospaceGraph(bool value) {
    return monospaceGraphNodes(value).monospaceGraphEdges(value);
}

Widget::Widget(const Impl& impl)
    : impl_(std::make_shared<Impl>(impl)) {}

Widget::Impl& Widget::impl() {
    if (!impl_) throw std::logic_error("Widget is empty after move");
    return *impl_;
}

const Widget::Impl& Widget::impl() const {
    if (!impl_) throw std::logic_error("Widget is empty after move");
    return *impl_;
}

bool Widget::hasContent() const {
    return std::visit([](const auto& body) -> bool {
        using T = std::decay_t<decltype(body)>;
        if constexpr (std::is_same_v<T, Impl::Text>) {
            return !body.text.empty();
        } else if constexpr (std::is_same_v<T, Impl::Table>) {
            return !body.rows.empty();
        } else if constexpr (std::is_same_v<T, Impl::List>) {
            for (const auto& item : body.items) {
                if (!item.first.empty() || !item.second.empty()) return true;
            }
            return false;
        } else if constexpr (std::is_same_v<T, Impl::Warning>) {
            return !body.message.empty() || !body.details.empty();
        } else if constexpr (std::is_same_v<T, Impl::GraphBody>) {
            return !body.graph.nodes_.empty() || !body.graph.edges_.empty();
        } else if constexpr (std::is_same_v<T, Impl::Switcher>) {
            for (const auto& option : body.options) {
                for (const Widget& widget : option.widgets) {
                    if (widget.hasContent()) return true;
                }
            }
            return false;
        }
        return false;
    }, impl().body);
}

Field::Field() = default;

Field::Field(const std::string& key, const std::string& value)
    : key_(key), value_(value) {}

Field& Field::detail(const Widget& widget) {
    detailWidgets_.push_back(widget);
    return *this;
}

Field& Field::details(const std::vector<Widget>& widgets) {
    detailWidgets_ = widgets;
    return *this;
}

Field& Field::details(std::initializer_list<Widget> widgets) {
    detailWidgets_.assign(widgets.begin(), widgets.end());
    return *this;
}

Node::Node(const std::string& id,
           const std::string& operatorName,
           const std::string& fullLabel)
    : id_(id), op_(operatorName), label_(fullLabel) {
    if (id_.empty()) throw std::invalid_argument("Node id must not be empty");
    if (op_.empty()) throw std::invalid_argument("Node operator name must not be empty");
}

Field& Node::field(const std::string& key, const std::string& value) {
    if (key.empty()) throw std::invalid_argument("Node field key must not be empty");
    for (const Field& existing : fields_) {
        if (existing.key_ == key) {
            throw std::invalid_argument("Duplicate Node field key: " + key);
        }
    }
    fields_.emplace_back(key, value);
    return fields_.back();
}

Node& Node::child(const std::string& id,
                  const std::string& operatorName,
                  const std::string& fullLabel) {
    children_.emplace_back(id, operatorName, fullLabel);
    return children_.back();
}

Node& Node::child(const Node& node) {
    children_.push_back(node);
    return children_.back();
}

std::size_t Node::childCount() const {
    return children_.size();
}

Node& Node::childAt(std::size_t index) {
    return children_.at(index);
}

const Node& Node::childAt(std::size_t index) const {
    return children_.at(index);
}

bool Node::hasAnyFields() const {
    if (!fields_.empty()) return true;
    for (const auto& child : children_) {
        if (child.hasAnyFields()) return true;
    }
    return false;
}

bool Node::hasAnyFieldValues() const {
    for (const auto& field : fields_) {
        if (!field.value_.empty()) return true;
    }
    for (const auto& child : children_) {
        if (child.hasAnyFieldValues()) return true;
    }
    return false;
}

bool Node::hasAnyFieldDetails() const {
    for (const auto& field : fields_) {
        if (!field.detailWidgets_.empty()) return true;
    }
    for (const auto& child : children_) {
        if (child.hasAnyFieldDetails()) return true;
    }
    return false;
}

InfoTab::InfoTab() = default;

InfoTab::InfoTab(const std::string& id, const std::string& title)
    : id_(id), title_(title) {
    if (id_.empty()) throw std::invalid_argument("InfoTab id must not be empty");
}

void InfoTab::bindTileState(const std::shared_ptr<detail::TileState>& state) {
    tileState_ = state;
}

void InfoTab::assertMutable() const {
    if (tileState_ && tileState_->submitted) {
        throw std::logic_error("InfoTab cannot be mutated after its tile has been submitted");
    }
}

void InfoTab::releasePayload() {
    std::string().swap(title_);
    std::vector<Widget>().swap(widgets_);
    std::vector<Target>().swap(targets_);
    hasPrimaryTarget_ = false;
    primaryTarget_ = Target::node("");
}

bool InfoTab::hasContent() const {
    for (const Widget& widget : widgets_) {
        if (widget.hasContent()) return true;
    }
    return false;
}

InfoTab& InfoTab::widget(const Widget& widget) {
    assertMutable();
    widgets_.push_back(widget);
    return *this;
}

InfoTab& InfoTab::setWidgets(const std::vector<Widget>& widgets) {
    assertMutable();
    widgets_ = widgets;
    return *this;
}

InfoTab& InfoTab::setWidgets(std::initializer_list<Widget> widgets) {
    assertMutable();
    widgets_.assign(widgets.begin(), widgets.end());
    return *this;
}

InfoTab& InfoTab::target(const Target& target) {
    assertMutable();
    targets_.push_back(target);
    return *this;
}

InfoTab& InfoTab::targets(const std::vector<Target>& refs) {
    assertMutable();
    targets_ = refs;
    return *this;
}

InfoTab& InfoTab::targets(std::initializer_list<Target> refs) {
    assertMutable();
    targets_.assign(refs.begin(), refs.end());
    return *this;
}

InfoTab& InfoTab::primaryTarget(const Target& target) {
    assertMutable();
    hasPrimaryTarget_ = true;
    primaryTarget_ = target;
    return *this;
}

void InfoPanel::bindTileState(const std::shared_ptr<detail::TileState>& state) {
    tileState_ = state;
    for (InfoTab& tab : tabs_) {
        tab.bindTileState(state);
    }
}

void InfoPanel::assertMutable() const {
    if (tileState_ && tileState_->submitted) {
        throw std::logic_error("InfoPanel cannot be mutated after its tile has been submitted");
    }
}

void InfoPanel::releasePayload() {
    for (InfoTab& tab : tabs_) {
        tab.releasePayload();
    }
    payloadReleased_ = true;
}

InfoTab& InfoPanel::tab(const std::string& id, const std::string& title) {
    assertMutable();
    for (InfoTab& tab : tabs_) {
        if (tab.id_ == id) {
            return tab;
        }
    }

    tabs_.emplace_back(id, title);
    tabs_.back().bindTileState(tileState_);
    return tabs_.back();
}

bool InfoPanel::empty() const {
    return payloadReleased_ || !hasContent();
}

bool InfoPanel::hasContent() const {
    for (const InfoTab& tab : tabs_) {
        if (tab.hasContent()) return true;
    }
    return false;
}

Trace::Tile::Tile(const std::string& title, const Node& tree)
    : bodyKind_(BodyKind::Tree),
      id_(randomHex128()),
      title_(title),
      tree_(tree),
      tileState_(std::make_shared<detail::TileState>()) {}

Trace::Tile::Tile(const std::string& title, const std::string& text)
    : bodyKind_(BodyKind::Text),
      id_(randomHex128()),
      title_(title),
      tree_("__text__", "Text", "Text"),
      text_(text),
      tileState_(std::make_shared<detail::TileState>()) {}

Trace::Tile::Tile(const Tile& other)
    : bodyKind_(other.bodyKind_),
      id_(other.id_),
      title_(other.title_),
      tree_(other.tree_),
      text_(other.text_),
      tileState_(std::make_shared<detail::TileState>(
          other.tileState_ ? *other.tileState_ : detail::TileState()
      )),
      infoPanel_(other.infoPanel_) {}

Trace::Tile& Trace::Tile::operator=(const Tile& other) {
    if (this != &other) {
        bodyKind_ = other.bodyKind_;
        id_ = other.id_;
        title_ = other.title_;
        tree_ = other.tree_;
        text_ = other.text_;
        tileState_ = std::make_shared<detail::TileState>(
            other.tileState_ ? *other.tileState_ : detail::TileState()
        );
        infoPanel_ = other.infoPanel_;
    }
    return *this;
}

Trace::Tile::Tile(Tile&& other) noexcept = default;

Trace::Tile& Trace::Tile::operator=(Tile&& other) noexcept = default;

bool Trace::Tile::isText() const {
    return bodyKind_ == BodyKind::Text;
}

Trace::Tile& Trace::Tile::setTree(const Node& root) {
    assertMutable();
    bodyKind_ = BodyKind::Tree;
    tree_ = root;
    std::string().swap(text_);
    return *this;
}

Trace::Tile& Trace::Tile::setText(const std::string& text) {
    assertMutable();
    bodyKind_ = BodyKind::Text;
    tree_ = Node("__text__", "Text", "Text");
    text_ = text;
    return *this;
}

void Trace::Tile::assertMutable() const {
    if (submitted()) {
        throw std::logic_error("Trace::Tile cannot be mutated after submit");
    }
}

bool Trace::Tile::submitted() const {
    return tileState_ && tileState_->submitted;
}

bool Trace::Tile::payloadReleased() const {
    return tileState_ && tileState_->payloadReleased;
}

void Trace::Tile::markSubmittedAndRelease() {
    if (!tileState_) {
        tileState_ = std::make_shared<detail::TileState>();
    }

    tileState_->submitted = true;
    tileState_->payloadReleased = true;

    tree_ = Node("__submitted__", "Submitted", "Submitted");
    std::string().swap(text_);
}

InfoPanel& Trace::Tile::info() {
    assertMutable();
    return infoPanel_;
}

const InfoPanel& Trace::Tile::info() const {
    return infoPanel_;
}

Trace::Stage::Stage(const std::string& name)
    : name_(name), id_(randomHex128()) {}

Trace::Tile& Trace::Stage::addTile(Tile tile) {
    tiles_.push_back(std::move(tile));
    return tiles_.back();
}

Trace::Tile& Trace::Stage::tree(const std::string& title, const Node& root) {
    tiles_.emplace_back(title, root);
    return tiles_.back();
}

Trace::Tile& Trace::Stage::text(const std::string& title, const std::string& body) {
    tiles_.emplace_back(title, body);
    return tiles_.back();
}

Trace::Trace(const std::string& title)
    : title_(title), id_(randomHex128()) {}

Trace::~Trace() = default;
Trace::Trace(const Trace& other) = default;
Trace& Trace::operator=(const Trace& other) = default;
Trace::Trace(Trace&& other) noexcept = default;
Trace& Trace::operator=(Trace&& other) noexcept = default;

void Trace::defineFields(const std::vector<std::pair<std::string, std::string>>& fields) {
    fieldDefinitions_ = fields;
}

void Trace::pinFields(const std::vector<std::string>& keys) {
    pinnedFields_ = keys;
}

void Trace::addPinnedFieldPreset(const std::string& label, const std::vector<std::string>& keys) {
    pinnedFieldPresets_.push_back({label, keys});
}

void Trace::definePinnedFieldPresets(
    const std::vector<std::pair<std::string, std::vector<std::string>>>& presets) {
    pinnedFieldPresets_ = presets;
}

Trace::Stage& Trace::stage(const std::string& name) {
    stages_.emplace_back(name);
    return stages_.back();
}

namespace detail {

class Emitter {
public:
    GenerateResult buildLogShell(const TracePageOptions& options, std::string& content) const {
        (void)options;
        if (!brotliAvailable()) {
            return GenerateResult::failure("Brotli compression is required for streaming trace payloads");
        }

        std::ostringstream out;
        emitDocumentHead(out);
        emitBrotliDecoderRuntime(out);
        emitScriptRuntime(out);
        emitAppMount(out);
        out << "\n";
        if (!out) {
            return GenerateResult::failure("Failed to serialize trace log shell");
        }
        content = out.str();
        return GenerateResult::success();
    }

    std::string traceMetadataSignature(const Trace& trace) const {
        return fieldDefinitionsJson(trace) + "|" +
            pinnedFieldsJson(trace) + "|" +
            nodeColumnPresetsJson(trace);
    }

    std::size_t nodeCount(const Node& node) const {
        std::size_t count = 1;
        for (const Node& child : node.children_) {
            count += nodeCount(child);
        }
        return count;
    }

    std::size_t fieldCount(const Node& node) const {
        std::size_t count = node.fields_.size();
        for (const Node& child : node.children_) {
            count += fieldCount(child);
        }
        return count;
    }

    bool tileHasInfo(const Trace::Tile& tile) const {
        return !tile.isText() && !tile.infoPanel_.empty();
    }

    bool tileHasPinnedValues(const Trace& trace, const Trace::Tile& tile) const {
        return !tile.isText() && !trace.pinnedFields_.empty() && tile.tree_.hasAnyFieldValues();
    }

    void emitTileSummaryData(NJsonWriter::TBuf& out,
                             const Trace& trace,
                             const Trace::Tile& tile,
                             std::size_t payloadIndex,
                             std::size_t payloadBytes) const {
        out.BeginObject();
        writeKeyString(out, "id", tile.id_);
        writeKeyString(out, "title", tile.title_);
        out.WriteKey("type").WriteString(tile.isText() ? "text" : "tree");
        writeKeySize(out, "payloadIndex", payloadIndex);

        out.WriteKey("features").BeginObject();
        writeKeyBool(out, "fields", !tile.isText() && tile.tree_.hasAnyFields());
        writeKeyBool(out, "pinned", tileHasPinnedValues(trace, tile));
        writeKeyBool(out, "info", tileHasInfo(tile));
        out.EndObject();

        out.WriteKey("size").BeginObject();
        writeKeySize(out, "rawBytes", payloadBytes);
        if (!tile.isText()) {
            writeKeySize(out, "nodeCount", nodeCount(tile.tree_));
            writeKeySize(out, "fieldCount", fieldCount(tile.tree_));
        }
        out.EndObject();
        out.EndObject();
    }

    void emitTilePayloadData(NJsonWriter::TBuf& out, const Trace::Tile& tile) const {
        out.BeginObject();
        writeKeyString(out, "tileId", tile.id_);
        if (tile.isText()) {
            writeKeyString(out, "text", tile.text_);
        } else {
            out.WriteKey("tree");
            emitTreeData(out, tile.tree_);
            if (!tile.infoPanel_.empty()) {
                out.WriteKey("infoPanel");
                emitInfoPanelData(out, tile.infoPanel_);
            }
        }
        out.EndObject();
    }

    GenerateResult tileLogRecord(const Trace& trace,
                                 const Trace::Stage& stage,
                                 const Trace::Tile& tile,
                                 const LogRecordContext& context,
                                 std::size_t payloadIndex,
                                 std::string& record,
                                 std::string& payload) const {
        if (tile.payloadReleased()) {
            return GenerateResult::failure(
                "TracePage::submit() cannot serialize tile '" + tile.title_ +
                "' because its payload was already released"
            );
        }

        if (!tile.isText()) {
            std::unordered_set<std::string> nodeIds;
            const std::string context = "trace '" + trace.title_ + "', stage '" +
                stage.name_ + "', tile '" + tile.title_ + "'";
            GenerateResult result = validateTreeNode(tile.tree_, nodeIds, context);
            if (!result) return result;
        }

        payload = buildJson([&](NJsonWriter::TBuf& out) {
            emitTilePayloadData(out, tile);
        });

        NJsonWriter::TBuf out(NJsonWriter::HEM_DONT_ESCAPE_HTML);
        out.BeginObject();
        out.WriteKey("type").WriteString("tile");
        writeKeyString(out, "appendId", context.appendId);
        if (!context.prevAppendId.empty()) {
            writeKeyString(out, "prevAppendId", context.prevAppendId);
        }
        if (context.emitTrace) {
            out.WriteKey("trace").BeginObject();
            writeKeyString(out, "id", trace.id_);
            writeKeyString(out, "name", trace.title_);
            out.WriteKey("fieldDefinitions");
            emitFieldDefinitionsData(out, trace);
            out.WriteKey("pinnedFields");
            emitPinnedFieldsData(out, trace);
            out.WriteKey("nodeColumnPresets");
            emitNodeColumnPresetsData(out, trace);
            out.EndObject();
        }
        if (context.emitStage) {
            out.WriteKey("stage").BeginObject();
            writeKeyString(out, "id", stage.id_);
            writeKeyString(out, "name", stage.name_);
            out.EndObject();
        }
        if (context.emitGroup) {
            out.WriteKey("group").BeginObject();
            writeKeyString(out, "id", context.groupId);
            writeKeyString(out, "name", tile.title_);
            out.EndObject();
        }
        out.WriteKey("tile");
        emitTileSummaryData(out, trace, tile, payloadIndex, payload.size());
        out.EndObject();
        record = toStdString(out.Str());
        return GenerateResult::success();
    }

    GenerateResult buildLogBlock(const TracePageOptions& options,
                                 std::size_t sequence,
                                 const std::string& blockId,
                                 const std::vector<PendingLogRecord>& records,
                                 std::string& content) const {
        if (records.empty()) return GenerateResult::success();

        if (!brotliAvailable()) {
            return GenerateResult::failure("Brotli compression is required for streaming trace payloads");
        }

        const std::string payloadJson = buildJson([&](NJsonWriter::TBuf& out) {
            out.BeginObject();
            out.WriteKey("payloads").BeginList();
            for (const PendingLogRecord& record : records) {
                writeRawJsonValue(out, record.payload);
            }
            out.EndList();
            out.EndObject();
        });

        bool compressed = false;
        std::string compressedPayload = brotliCompress(
            payloadJson,
            options.compressionLevel,
            &compressed
        );
        if (!compressed) {
            return GenerateResult::failure("Brotli compression failed");
        }

        const std::string block = buildJson([&](NJsonWriter::TBuf& out) {
            out.BeginObject();
            out.WriteKey("schemaVersion").WriteInt(3);
            writeKeyString(out, "blockId", blockId);
            out.WriteKey("records").BeginList();
            for (const PendingLogRecord& record : records) {
                writeRawJsonValue(out, record.record);
            }
            out.EndList();
            out.WriteKey("payload").BeginObject();
            out.WriteKey("compression").WriteString("brotli");
            out.WriteKey("encoding").WriteString("base64url");
            writeKeySize(out, "rawBytes", payloadJson.size());
            out.WriteKey("body").WriteString(stringBuf(base64UrlEncode(compressedPayload)));
            out.EndObject();
            out.EndObject();
        });

        std::ostringstream out;
        out << "<script class=\"ot-l\" type=\"application/json\" "
            << "data-seq=\"" << sequence << "\" data-block-id=\"" << blockId
            << "\" data-records=\"" << records.size()
            << "\" data-payload-raw-bytes=\"" << payloadJson.size() << "\">\n"
            << block
            << "\n"
            << "</script>\n";
        if (!out) {
            return GenerateResult::failure("Failed to serialize trace log block");
        }
        content = out.str();
        return GenerateResult::success();
    }

private:
    GenerateResult validateTreeNode(const Node& node,
                                    std::unordered_set<std::string>& nodeIds,
                                    const std::string& context) const {
        if (node.id_.empty()) {
            return GenerateResult::failure("Node id must not be empty in " + context);
        }
        if (node.op_.empty()) {
            return GenerateResult::failure("Node operator name must not be empty for node '" +
                                           node.id_ + "' in " + context);
        }
        if (!nodeIds.insert(node.id_).second) {
            return GenerateResult::failure("Duplicate Node id '" + node.id_ + "' in " + context);
        }

        std::unordered_set<std::string> fieldKeys;
        for (const auto& field : node.fields_) {
            if (field.key_.empty()) {
                return GenerateResult::failure("Node field key must not be empty for node '" +
                                               node.id_ + "' in " + context);
            }
            if (!fieldKeys.insert(field.key_).second) {
                return GenerateResult::failure("Duplicate Node field key '" + field.key_ +
                                               "' for node '" + node.id_ + "' in " + context);
            }
        }

        for (const auto& child : node.children_) {
            GenerateResult result = validateTreeNode(child, nodeIds, context);
            if (!result) return result;
        }
        return GenerateResult::success();
    }

    void emitDocumentHead(std::ostream& out) const {
        out << "<!DOCTYPE html>\n"
            << "<html lang=\"en\"><head>\n"
            << "<meta charset=\"UTF-8\">\n"
            << "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
            << "<!-- Icon paths: Radix Icons v1.3.2, MIT License, Copyright (c) 2022 WorkOS. -->\n"
            << "<title>RBO Trace</title>\n"
            << "<style>\n"
            << optimizer_trace_assets::css() << "\n"
            << "</style>\n";
    }

    void emitFieldDefinitionsData(NJsonWriter::TBuf& out, const Trace& trace) const {
        out.BeginList();
        for (const auto& field : trace.fieldDefinitions_) {
            out.BeginObject();
            writeKeyString(out, "key", field.first);
            writeKeyString(out, "label", field.second);
            out.EndObject();
        }
        out.EndList();
    }

    std::string fieldDefinitionsJson(const Trace& trace) const {
        return buildJson([&](NJsonWriter::TBuf& out) {
            emitFieldDefinitionsData(out, trace);
        });
    }

    void emitPinnedFieldsData(NJsonWriter::TBuf& out, const Trace& trace) const {
        out.BeginList();
        for (const auto& field : trace.pinnedFields_) {
            writeString(out, field);
        }
        out.EndList();
    }

    std::string pinnedFieldsJson(const Trace& trace) const {
        return buildJson([&](NJsonWriter::TBuf& out) {
            emitPinnedFieldsData(out, trace);
        });
    }

    void emitNodeColumnPresetsData(NJsonWriter::TBuf& out, const Trace& trace) const {
        out.BeginList();
        for (const auto& preset : trace.pinnedFieldPresets_) {
            out.BeginObject();
            writeKeyString(out, "label", preset.first);
            out.WriteKey("keys").BeginList();
            for (const auto& key : preset.second) {
                writeString(out, key);
            }
            out.EndList();
            out.EndObject();
        }
        out.EndList();
    }

    std::string nodeColumnPresetsJson(const Trace& trace) const {
        return buildJson([&](NJsonWriter::TBuf& out) {
            emitNodeColumnPresetsData(out, trace);
        });
    }

    bool brotliAvailable() const {
#if OPTIMIZER_TRACE_HTML_HAS_BROTLI
        return true;
#else
        return false;
#endif
    }

    void emitBrotliDecoderRuntime(std::ostream& out) const {
        std::string decoderJs(optimizer_trace_assets::brotliDecoderJs());
        std::string wasm(
            optimizer_trace_assets::brotliDecoderWasm().data(),
            optimizer_trace_assets::brotliDecoderWasm().size()
        );
        std::string wasmBase64 = base64UrlEncode(wasm);
        out << "<script>\n"
            << decoderJs << "\n"
            << "OptimizerTraceBrotliDecoder.workerSource="
            << jsonStringLiteral(decoderJs)
            << ";\n"
            << "OptimizerTraceBrotliDecoder.wasmBase64=\""
            << wasmBase64
            << "\";\n"
            << "OptimizerTraceBrotliDecoder.setWasmBase64(OptimizerTraceBrotliDecoder.wasmBase64);\n"
            << "</script>\n"
            << "<script id=\"optimizer-trace-third-party-notices\" "
            << "type=\"text/plain\" hidden>\n"
            << htmlTextEsc("brotli-dec-wasm v2.3.0\n"
                           "License: MIT OR Apache-2.0\n"
                           "Using MIT license terms.\n\n")
            << htmlTextEsc(std::string(
                optimizer_trace_assets::brotliDecoderLicense().data(),
                optimizer_trace_assets::brotliDecoderLicense().size()
            ))
            << "\n</script>\n";
    }

    void emitScriptRuntime(std::ostream& out) const {
        for (std::string_view jsPart : optimizer_trace_assets::jsParts()) {
            out << "<script>\n";
            out.write(jsPart.data(), static_cast<std::streamsize>(jsPart.size()));
            out << "\n</script>\n";
        }
        out << "</head><body class=\"theme-stone-dark\">\n";
    }

    void emitAppMount(std::ostream& out) const {
        out << "<div id=\"optimizer-trace-app\"></div>\n";
    }

    void emitTargetData(NJsonWriter::TBuf& out, const Target& target) const {
        out.BeginObject();
        switch (target.type()) {
            case Target::Type::Node:
                out.WriteKey("type").WriteString("node");
                writeKeyString(out, "nodeId", target.nodeId());
                break;
            case Target::Type::Subtree:
                out.WriteKey("type").WriteString("subtree");
                writeKeyString(out, "nodeId", target.nodeId());
                break;
        }
        out.EndObject();
    }

    void emitTargetsData(NJsonWriter::TBuf& out, const std::vector<Target>& targets) const {
        out.BeginList();
        for (const Target& target : targets) {
            emitTargetData(out, target);
        }
        out.EndList();
    }

    void emitTargetFields(NJsonWriter::TBuf& out,
                          const std::vector<Target>& targets,
                          bool hasPrimaryTarget,
                          const Target& primaryTarget) const {
        if (!targets.empty()) {
            out.WriteKey("targets");
            emitTargetsData(out, targets);
        }
        if (hasPrimaryTarget) {
            out.WriteKey("primaryTarget");
            emitTargetData(out, primaryTarget);
        }
    }

    void emitWidgetArray(NJsonWriter::TBuf& out, const std::vector<Widget>& widgets) const {
        out.BeginList();
        for (const Widget& widget : widgets) {
            emitWidgetData(out, widget);
        }
        out.EndList();
    }

    void emitTreeData(NJsonWriter::TBuf& out, const Node& node) const {
        out.BeginObject();
        writeKeyString(out, "id", node.id_);
        writeKeyString(out, "op", node.op_);
        writeKeyString(out, "label", node.label_);
        out.WriteKey("fields").BeginList();
        for (const Field& field : node.fields_) {
            out.BeginObject();
            writeKeyString(out, "key", field.key_);
            writeKeyString(out, "value", field.value_);
            if (!field.detailWidgets_.empty()) {
                out.WriteKey("details");
                emitWidgetArray(out, field.detailWidgets_);
            }
            out.EndObject();
        }
        out.EndList();
        out.WriteKey("children").BeginList();
        for (const Node& child : node.children_) {
            emitTreeData(out, child);
        }
        out.EndList();
        out.EndObject();
    }

    void emitGraphNodeData(NJsonWriter::TBuf& out, const GraphNode& node) const {
        out.BeginObject();
        writeKeyString(out, "id", node.id_);
        writeKeyString(out, "title", node.title_);
        if (!node.text_.empty()) {
            writeKeyString(out, "text", node.text_);
        }
        emitTargetFields(out, node.targets_, node.hasPrimaryTarget_, node.primaryTarget_);
        out.EndObject();
    }

    void emitGraphEdgeData(NJsonWriter::TBuf& out, const GraphEdge& edge) const {
        out.BeginObject();
        writeKeyString(out, "from", edge.from_);
        writeKeyString(out, "to", edge.to_);
        if (!edge.id_.empty()) {
            writeKeyString(out, "id", edge.id_);
        }
        if (!edge.label_.empty()) {
            writeKeyString(out, "label", edge.label_);
        }
        if (!edge.text_.empty()) {
            writeKeyString(out, "text", edge.text_);
        }
        emitTargetFields(out, edge.targets_, edge.hasPrimaryTarget_, edge.primaryTarget_);
        out.EndObject();
    }

    void emitGraphData(NJsonWriter::TBuf& out, const Graph& graph) const {
        out.BeginObject();
        out.WriteKey("layout").BeginObject();
        if (!graph.rankDir_.empty()) {
            writeKeyString(out, "rankdir", graph.rankDir_);
        }
        if (graph.rankSep_ > 0.0) {
            writeKeyDouble(out, "ranksep", graph.rankSep_);
        }
        if (graph.nodeSep_ > 0.0) {
            writeKeyDouble(out, "nodesep", graph.nodeSep_);
        }
        out.EndObject();
        out.WriteKey("nodes").BeginList();
        for (const GraphNode& node : graph.nodes_) {
            emitGraphNodeData(out, node);
        }
        out.EndList();
        out.WriteKey("edges").BeginList();
        for (const GraphEdge& edge : graph.edges_) {
            emitGraphEdgeData(out, edge);
        }
        out.EndList();
        if (!graph.directed_) {
            writeKeyBool(out, "directed", false);
        }
        out.EndObject();
    }

    void emitWidgetBodyData(NJsonWriter::TBuf& out, const Widget::Impl::Text& widget) const {
        out.WriteKey("type").WriteString("text");
        writeKeyString(out, "title", widget.title);
        writeKeyString(out, "text", widget.text);
        if (!widget.wrap) {
            writeKeyBool(out, "wrap", false);
        }
        if (widget.lineNumbers) {
            writeKeyBool(out, "lineNumbers", true);
        }
    }

    void emitWidgetBodyData(NJsonWriter::TBuf& out, const Widget::Impl::Table& widget) const {
        out.WriteKey("type").WriteString("table");
        writeKeyString(out, "title", widget.title);
        out.WriteKey("rows").BeginList();
        for (const auto& row : widget.rows) {
            out.BeginList();
            writeString(out, row.first);
            writeString(out, row.second);
            out.EndList();
        }
        out.EndList();
        if (widget.monospaceKey || widget.monospaceValue) {
            out.WriteKey("monospace").BeginObject();
            if (widget.monospaceKey) {
                writeKeyBool(out, "key", true);
            }
            if (widget.monospaceValue) {
                writeKeyBool(out, "value", true);
            }
            out.EndObject();
        }
    }

    void emitWidgetBodyData(NJsonWriter::TBuf& out, const Widget::Impl::List& widget) const {
        out.WriteKey("type").WriteString("list");
        writeKeyString(out, "title", widget.title);
        if (widget.ordered) {
            writeKeyBool(out, "ordered", true);
        }
        out.WriteKey("items").BeginList();
        for (const auto& item : widget.items) {
            out.BeginObject();
            writeKeyString(out, "text", item.first);
            if (!item.second.empty()) {
                writeKeyString(out, "detail", item.second);
            }
            out.EndObject();
        }
        out.EndList();
        if (widget.monospaceText || widget.monospaceDetail) {
            out.WriteKey("monospace").BeginObject();
            if (widget.monospaceText) {
                writeKeyBool(out, "text", true);
            }
            if (widget.monospaceDetail) {
                writeKeyBool(out, "detail", true);
            }
            out.EndObject();
        }
    }

    void emitWidgetBodyData(NJsonWriter::TBuf& out, const Widget::Impl::Warning& widget) const {
        out.WriteKey("type").WriteString("warning");
        writeKeyString(out, "title", widget.title);
        writeKeyString(out, "severity", widget.severity);
        writeKeyString(out, "message", widget.message);
        writeKeyString(out, "details", widget.details);
    }

    void emitWidgetBodyData(NJsonWriter::TBuf& out, const Widget::Impl::GraphBody& widget) const {
        out.WriteKey("type").WriteString("graph");
        writeKeyString(out, "title", widget.title);
        out.WriteKey("graph");
        emitGraphData(out, widget.graph);
        if (widget.monospaceNode || widget.monospaceEdge) {
            out.WriteKey("monospace").BeginObject();
            if (widget.monospaceNode) {
                writeKeyBool(out, "node", true);
            }
            if (widget.monospaceEdge) {
                writeKeyBool(out, "edge", true);
            }
            out.EndObject();
        }
    }

    void emitWidgetBodyData(NJsonWriter::TBuf& out, const Widget::Impl::Switcher& widget) const {
        out.WriteKey("type").WriteString("switcher");
        writeKeyString(out, "title", widget.title);
        if (!widget.id.empty()) {
            writeKeyString(out, "id", widget.id);
        }
        if (!widget.defaultOptionId.empty()) {
            writeKeyString(out, "defaultOptionId", widget.defaultOptionId);
        }
        out.WriteKey("options").BeginList();
        for (const auto& option : widget.options) {
            out.BeginObject();
            writeKeyString(out, "id", option.id);
            writeKeyString(out, "title", option.title);
            out.WriteKey("widgets");
            emitWidgetArray(out, option.widgets);
            out.EndObject();
        }
        out.EndList();
    }

    void emitWidgetData(NJsonWriter::TBuf& out, const Widget& widget) const {
        out.BeginObject();
        std::visit([&](const auto& body) {
            emitWidgetBodyData(out, body);
        }, widget.impl().body);
        emitTargetFields(
            out,
            widget.impl().targets,
            widget.impl().hasPrimaryTarget,
            widget.impl().primaryTarget
        );
        out.EndObject();
    }

    void emitInfoTabData(NJsonWriter::TBuf& out, const InfoTab& tab) const {
        out.BeginObject();
        writeKeyString(out, "id", tab.id_);
        writeKeyString(out, "title", tab.title_);
        out.WriteKey("widgets");
        emitWidgetArray(out, tab.widgets_);
        emitTargetFields(out, tab.targets_, tab.hasPrimaryTarget_, tab.primaryTarget_);
        out.EndObject();
    }

    void emitInfoPanelData(NJsonWriter::TBuf& out, const InfoPanel& panel) const {
        out.BeginObject();
        out.WriteKey("tabs").BeginList();
        for (const InfoTab& tab : panel.tabs_) {
            emitInfoTabData(out, tab);
        }
        out.EndList();
        out.EndObject();
    }
};

}  // namespace detail

class TFileTracePageSink final : public ITracePageSink {
public:
    explicit TFileTracePageSink(std::string filename)
        : Filename_(std::move(filename))
        , LockFilename_(Filename_ + ".lock")
    {
    }

    GenerateResult Reset(const std::string& content) override {
        TFileLock lock{TString(LockFilename_)};
        TGuard<TFileLock> guard(lock);
        return WriteBlockLocked(content, CreateAlways | WrOnly, "write");
    }

    GenerateResult Append(const std::string& content) override {
        TFileLock lock{TString(LockFilename_)};
        TGuard<TFileLock> guard(lock);
        return WriteBlockLocked(content, OpenAlways | WrOnly | ForAppend, "append");
    }

    GenerateResult WriteInitial(const std::string& shell, const std::string& firstBlock) override {
        TFileLock lock{TString(LockFilename_)};
        TGuard<TFileLock> guard(lock);

        const ExistingFileState state = ExistingStateLocked();
        if (state == ExistingFileState::TraceHtml) {
            return WriteBlockLocked(firstBlock, OpenAlways | WrOnly | ForAppend, "append");
        }
        if (state == ExistingFileState::OtherContent) {
            return GenerateResult::failure(
                "Refusing to append optimizer trace output to a non-trace file: " + Filename_
            );
        }

        return WriteBlockLocked(shell + firstBlock, CreateAlways | WrOnly, "write");
    }

private:
    enum class ExistingFileState {
        EmptyOrMissing,
        TraceHtml,
        OtherContent
    };

    ExistingFileState ExistingStateLocked() const {
        const TFileStat stat{TString(Filename_)};
        if (!stat.IsFile() || stat.Size == 0) {
            return ExistingFileState::EmptyOrMissing;
        }

        std::ifstream in(Filename_, std::ios::binary);
        if (!in.is_open()) {
            return ExistingFileState::OtherContent;
        }

        const std::size_t bytesToRead = static_cast<std::size_t>(
            std::min<ui64>(stat.Size, static_cast<ui64>(TraceHtmlSniffBytes))
        );
        std::string prefix(bytesToRead, '\0');
        in.read(prefix.data(), static_cast<std::streamsize>(prefix.size()));
        prefix.resize(static_cast<std::size_t>(in.gcount()));

        return LooksLikeOptimizerTraceHtml(prefix)
            ? ExistingFileState::TraceHtml
            : ExistingFileState::OtherContent;
    }

    GenerateResult WriteBlockLocked(const std::string& content, EOpenMode mode, const char* action) const {
        if (content.size() > std::numeric_limits<ui32>::max()) {
            return GenerateResult::failure(
                std::string("Failed to ") + action + " output file " + Filename_ +
                ": block is too large for a single write"
            );
        }

        TFileHandle out(Filename_, mode);
        if (!out.IsOpen()) {
            return GenerateResult::failure("Failed to open output file: " + Filename_);
        }

        const i32 written = out.Write(content.data(), static_cast<ui32>(content.size()));
        if (written < 0) {
            return GenerateResult::failure(
                std::string("Failed to ") + action + " output file: " + Filename_
            );
        }
        if (static_cast<std::size_t>(written) != content.size()) {
            return GenerateResult::failure(
                std::string("Failed to ") + action + " output file " + Filename_ +
                ": short write"
            );
        }
        return GenerateResult::success();
    }

    std::string Filename_;
    std::string LockFilename_;
};

GenerateResult ITracePageSink::WriteInitial(const std::string& shell, const std::string& firstBlock) {
    return Reset(shell + firstBlock);
}

struct TracePage::StreamState {
    StreamState(
        std::shared_ptr<ITracePageSink> pageSink,
        const TracePageOptions& pageOptions,
        bool pageShellWritten)
        : sink(std::move(pageSink))
        , options(pageOptions)
        , shellWritten(pageShellWritten)
    {
    }

    struct TraceContext {
        std::string lastAppendId;
        std::string traceTitle;
        std::string metadataSignature;
        std::string stageId;
        std::string stageName;
        std::string groupId;
        std::string groupName;
    };

    std::shared_ptr<ITracePageSink> sink;
    TracePageOptions options;
    bool shellWritten = false;
    std::string streamId = randomHex128();
    std::size_t nextSequence = 1;
    std::size_t pendingPayloadBytes = 0;
    std::vector<detail::PendingLogRecord> pendingRecords;
    std::unordered_map<std::string, TraceContext> traceContexts;
};

TracePage::TracePage() = default;

TracePage::TracePage(
    const std::string& filename,
    const TracePageOptions& options,
    bool shellAlreadyWritten)
    : TracePage(std::make_shared<TFileTracePageSink>(filename), options, shellAlreadyWritten)
{
}

TracePage::TracePage(
    std::shared_ptr<ITracePageSink> sink,
    const TracePageOptions& options,
    bool shellAlreadyWritten)
    : stream_(std::make_shared<StreamState>(std::move(sink), options, shellAlreadyWritten))
{
}

TracePage::~TracePage() {
    if (stream_) {
        (void)flush();
    }
}

TracePage::TracePage(const TracePage& other) = default;
TracePage& TracePage::operator=(const TracePage& other) = default;
TracePage::TracePage(TracePage&& other) noexcept = default;
TracePage& TracePage::operator=(TracePage&& other) noexcept = default;

Trace& TracePage::trace(const std::string& title) {
    traces_.emplace_back(title);
    return traces_.back();
}

GenerateResult TracePage::submit(Trace::Tile& tile) {
    if (!stream_) {
        return GenerateResult::failure(
            "TracePage::submit() requires a TracePage constructed with an output sink"
        );
    }

    for (Trace& trace : traces_) {
        for (Trace::Stage& stage : trace.stages_) {
            for (Trace::Tile& candidate : stage.tiles_) {
                if (&candidate != &tile) continue;
                if (candidate.submitted()) {
                    return GenerateResult::failure(
                        "TracePage::submit() tile '" + candidate.title_ +
                        "' has already been submitted"
                    );
                }

                detail::Emitter emitter;
                auto& traceContext = stream_->traceContexts[trace.id_];
                const std::string metadataSignature = emitter.traceMetadataSignature(trace);
                const bool firstTraceRecord = traceContext.lastAppendId.empty();
                const bool emitTrace = firstTraceRecord ||
                    traceContext.traceTitle != trace.title_ ||
                    traceContext.metadataSignature != metadataSignature;
                const bool emitStage = firstTraceRecord ||
                    traceContext.stageId != stage.id_ ||
                    traceContext.stageName != stage.name_;
                const bool emitGroup = firstTraceRecord ||
                    emitStage ||
                    traceContext.groupName != candidate.title_;
                const std::string appendId = randomHex128();
                const std::string groupId = emitGroup ? randomHex128() : traceContext.groupId;
                detail::LogRecordContext logContext;
                logContext.appendId = appendId;
                logContext.prevAppendId = traceContext.lastAppendId;
                logContext.groupId = groupId;
                logContext.emitTrace = emitTrace;
                logContext.emitStage = emitStage;
                logContext.emitGroup = emitGroup;

                detail::PendingLogRecord pending;
                GenerateResult result = emitter.tileLogRecord(
                    trace,
                    stage,
                    candidate,
                    logContext,
                    stream_->pendingRecords.size(),
                    pending.record,
                    pending.payload
                );
                if (!result) return result;
                pending.payloadBytes = pending.payload.size();

                traceContext.lastAppendId = appendId;
                traceContext.traceTitle = trace.title_;
                traceContext.metadataSignature = metadataSignature;
                traceContext.stageId = stage.id_;
                traceContext.stageName = stage.name_;
                traceContext.groupId = groupId;
                traceContext.groupName = candidate.title_;

                stream_->pendingPayloadBytes += pending.payloadBytes;
                stream_->pendingRecords.push_back(std::move(pending));
                candidate.infoPanel_.bindTileState(candidate.tileState_);
                candidate.markSubmittedAndRelease();
                candidate.infoPanel_.releasePayload();

                const bool ruleLimitReached =
                    stream_->options.maxBufferedRules > 0 &&
                    stream_->pendingRecords.size() >= stream_->options.maxBufferedRules;
                const bool byteLimitReached =
                    stream_->options.maxBufferedPayloadBytes > 0 &&
                    stream_->pendingPayloadBytes >= stream_->options.maxBufferedPayloadBytes;
                if (stream_->options.flushOnEverySubmit ||
                    ruleLimitReached ||
                    byteLimitReached) {
                    return flush();
                }
                return GenerateResult::success();
            }
        }
    }

    return GenerateResult::failure("TracePage::submit() tile does not belong to this TracePage");
}

GenerateResult TracePage::flush() {
    if (!stream_) return GenerateResult::success();
    if (stream_->pendingRecords.empty()) return GenerateResult::success();
    if (!stream_->sink) {
        return GenerateResult::failure("TracePage::flush() requires an output sink");
    }

    detail::Emitter emitter;
    std::string shell;
    if (!stream_->shellWritten) {
        GenerateResult result = emitter.buildLogShell(stream_->options, shell);
        if (!result) return result;
    }

    std::string block;
    const std::string blockId = stream_->streamId + ":" + std::to_string(stream_->nextSequence);
    GenerateResult result = emitter.buildLogBlock(
        stream_->options,
        stream_->nextSequence,
        blockId,
        stream_->pendingRecords,
        block
    );
    if (!result) return result;

    if (!stream_->shellWritten) {
        result = stream_->sink->WriteInitial(shell, block);
        if (!result) return result;
        stream_->shellWritten = true;
    } else {
        result = stream_->sink->Append(block);
    }
    if (!result) return result;

    stream_->nextSequence++;
    stream_->pendingPayloadBytes = 0;
    stream_->pendingRecords.clear();
    return GenerateResult::success();
}

}  // namespace optimizer_trace
