#ifndef OPTIMIZER_TRACE_H
#define OPTIMIZER_TRACE_H

#include <cstddef>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace optimizer_trace {

struct TracePageOptions {
    int compressionLevel = 6;
    std::size_t maxBufferedRules = 100;
    std::size_t maxBufferedPayloadBytes = 8 * 1024 * 1024;
    bool flushOnEverySubmit = false;
};

struct GenerateResult {
    bool ok = false;
    std::string error;

    explicit operator bool() const;

    static GenerateResult success();
    static GenerateResult failure(const std::string& message);
};

class Target {
public:
    enum class Type {
        Node,
        Subtree
    };

    static Target node(const std::string& id);
    static Target subtree(const std::string& id);

    Type type() const;
    const std::string& nodeId() const;

private:
    Target(Type targetType, const std::string& targetNodeId);

    Type type_ = Type::Node;
    std::string nodeId_;
};

namespace detail {
class Emitter;
struct TileState;
}  // namespace detail

class GraphNode {
public:
    GraphNode();
    GraphNode(const std::string& id, const std::string& title);

    GraphNode& note(const std::string& value);
    GraphNode& target(const Target& target);
    GraphNode& targets(const std::vector<Target>& refs);
    GraphNode& targets(std::initializer_list<Target> refs);
    GraphNode& primaryTarget(const Target& target);

private:
    friend class detail::Emitter;

    std::string id_;
    std::string title_;
    std::string text_;
    std::vector<Target> targets_;
    bool hasPrimaryTarget_ = false;
    Target primaryTarget_ = Target::node("");
};

class GraphEdge {
public:
    GraphEdge();
    GraphEdge(const std::string& from, const std::string& to);

    GraphEdge& setId(const std::string& value);
    GraphEdge& setLabel(const std::string& value);
    GraphEdge& note(const std::string& value);
    GraphEdge& target(const Target& target);
    GraphEdge& targets(const std::vector<Target>& refs);
    GraphEdge& targets(std::initializer_list<Target> refs);
    GraphEdge& primaryTarget(const Target& target);

private:
    friend class detail::Emitter;

    std::string id_;
    std::string from_;
    std::string to_;
    std::string label_;
    std::string text_;
    std::vector<Target> targets_;
    bool hasPrimaryTarget_ = false;
    Target primaryTarget_ = Target::node("");
};

class Graph {
public:
    Graph();

    GraphNode& node(const std::string& id, const std::string& title);
    Graph& addNode(const GraphNode& graphNode);
    GraphEdge& edge(const std::string& from, const std::string& to);
    Graph& addEdge(const GraphEdge& graphEdge);
    Graph& layout(const std::string& direction, double rankSeparation = 0.0, double nodeSeparation = 0.0);
    Graph& directed(bool value = true);
    Graph& undirected();

private:
    friend class detail::Emitter;
    friend class Widget;

    std::vector<GraphNode> nodes_;
    std::vector<GraphEdge> edges_;
    std::string rankDir_ = "LR";
    double rankSep_ = 0.0;
    double nodeSep_ = 0.0;
    bool directed_ = true;
};

class Widget {
public:
    Widget();
    ~Widget();
    Widget(const Widget& other);
    Widget& operator=(const Widget& other);
    Widget(Widget&& other) noexcept;
    Widget& operator=(Widget&& other) noexcept;

    static Widget text(const std::string& title, const std::string& body);
    static Widget unwrappedText(const std::string& title, const std::string& body, bool lineNumbers = false);
    static Widget table(const std::string& title, const std::vector<std::pair<std::string, std::string>>& rows);
    static Widget list(const std::string& title, const std::vector<std::string>& items, bool ordered = false);
    static Widget list(const std::string& title, const std::vector<std::pair<std::string, std::string>>& items, bool ordered = false);
    static Widget list(const std::string& title, std::initializer_list<std::pair<std::string, std::string>> items, bool ordered = false);
    static Widget warning(const std::string& title, const std::string& message, const std::string& severity = "warning", const std::string& details = "");
    static Widget graph(const std::string& title, const Graph& graph);
    static Widget switcher(const std::string& title);
    static Widget switcher(const std::string& id, const std::string& title);

    Widget& option(const std::string& id, const std::string& title, const std::vector<Widget>& widgets);
    Widget& option(const std::string& id, const std::string& title, std::initializer_list<Widget> widgets);
    Widget& defaultOption(const std::string& id);
    Widget& target(const Target& target);
    Widget& targets(const std::vector<Target>& refs);
    Widget& targets(std::initializer_list<Target> refs);
    Widget& primaryTarget(const Target& target);
    Widget& monospaceKeys(bool value = true);
    Widget& monospaceValues(bool value = true);
    Widget& monospaceTable(bool value = true);
    Widget& monospaceListText(bool value = true);
    Widget& monospaceListDetails(bool value = true);
    Widget& monospaceList(bool value = true);
    Widget& monospaceGraphNodes(bool value = true);
    Widget& monospaceGraphEdges(bool value = true);
    Widget& monospaceGraph(bool value = true);

private:
    friend class detail::Emitter;
    friend class InfoTab;

    struct Impl;

    explicit Widget(const Impl& impl);

    Impl& impl();
    const Impl& impl() const;
    bool hasContent() const;

    std::shared_ptr<Impl> impl_;
};

class Field {
public:
    Field();
    Field(const std::string& key, const std::string& value);

    Field& detail(const Widget& widget);
    Field& details(const std::vector<Widget>& widgets);
    Field& details(std::initializer_list<Widget> widgets);

private:
    friend class Node;
    friend class detail::Emitter;

    std::string key_;
    std::string value_;
    std::vector<Widget> detailWidgets_;
};

class Node {
public:
    Node(const std::string& id,
         const std::string& operatorName,
         const std::string& fullLabel);

    Field& field(const std::string& key, const std::string& value);
    Node& child(const std::string& id, const std::string& operatorName, const std::string& fullLabel);
    Node& child(const Node& node);

    std::size_t childCount() const;
    Node& childAt(std::size_t index);
    const Node& childAt(std::size_t index) const;

private:
    friend class detail::Emitter;

    bool hasAnyFields() const;
    bool hasAnyFieldValues() const;
    bool hasAnyFieldDetails() const;

    std::string id_;
    std::string op_;
    std::string label_;
    std::vector<Field> fields_;
    std::vector<Node> children_;
};

class InfoTab {
public:
    InfoTab();
    InfoTab(const std::string& id, const std::string& title);

    InfoTab& widget(const Widget& widget);
    InfoTab& setWidgets(const std::vector<Widget>& widgets);
    InfoTab& setWidgets(std::initializer_list<Widget> widgets);
    InfoTab& target(const Target& target);
    InfoTab& targets(const std::vector<Target>& refs);
    InfoTab& targets(std::initializer_list<Target> refs);
    InfoTab& primaryTarget(const Target& target);

private:
    friend class InfoPanel;
    friend class detail::Emitter;

    void bindTileState(const std::shared_ptr<detail::TileState>& state);
    void assertMutable() const;
    void releasePayload();
    bool hasContent() const;

    std::shared_ptr<detail::TileState> tileState_;
    std::string id_;
    std::string title_;
    std::vector<Widget> widgets_;
    std::vector<Target> targets_;
    bool hasPrimaryTarget_ = false;
    Target primaryTarget_ = Target::node("");
};

class TracePage;

class InfoPanel {
public:
    InfoTab& tab(const std::string& id, const std::string& title);
    bool empty() const;

private:
    friend class TracePage;
    friend class detail::Emitter;

    void bindTileState(const std::shared_ptr<detail::TileState>& state);
    void assertMutable() const;
    void releasePayload();
    bool hasContent() const;

    std::shared_ptr<detail::TileState> tileState_;
    std::vector<InfoTab> tabs_;
    bool payloadReleased_ = false;
};

class Trace {
public:
    class Tile {
    public:
        enum class BodyKind {
            Tree,
            Text
        };

        Tile(const std::string& title, const Node& tree);
        Tile(const std::string& title, const std::string& text);
        Tile(const Tile& other);
        Tile& operator=(const Tile& other);
        Tile(Tile&& other) noexcept;
        Tile& operator=(Tile&& other) noexcept;

        bool isText() const;
        Tile& setTree(const Node& root);
        Tile& setText(const std::string& text);
        InfoPanel& info();
        const InfoPanel& info() const;

    private:
        friend class Trace;
        friend class TracePage;
        friend class detail::Emitter;

        void assertMutable() const;
        bool submitted() const;
        bool payloadReleased() const;
        void markSubmittedAndRelease();

        BodyKind bodyKind_ = BodyKind::Tree;
        std::string id_;
        std::string title_;
        Node tree_;
        std::string text_;
        std::shared_ptr<detail::TileState> tileState_;
        InfoPanel infoPanel_;
    };

    class Stage {
    public:
        explicit Stage(const std::string& name);

        Tile& addTile(Tile tile);
        Tile& tree(const std::string& title, const Node& root);
        Tile& text(const std::string& title, const std::string& body);

    private:
        friend class Trace;
        friend class TracePage;
        friend class detail::Emitter;

        std::string name_;
        std::string id_;
        std::vector<Tile> tiles_;
    };

    explicit Trace(const std::string& title = "Trace");
    ~Trace();
    Trace(const Trace& other);
    Trace& operator=(const Trace& other);
    Trace(Trace&& other) noexcept;
    Trace& operator=(Trace&& other) noexcept;

    void defineFields(const std::vector<std::pair<std::string, std::string>>& fields);
    void pinFields(const std::vector<std::string>& keys);
    void addPinnedFieldPreset(const std::string& label, const std::vector<std::string>& keys);
    void definePinnedFieldPresets(const std::vector<std::pair<std::string, std::vector<std::string>>>& presets);
    void addDiffFieldPreset(const std::string& label, const std::vector<std::string>& keys);
    void defineDiffFieldPresets(const std::vector<std::pair<std::string, std::vector<std::string>>>& presets);
    Stage& stage(const std::string& name);

private:
    friend class TracePage;
    friend class detail::Emitter;

    std::string title_;
    std::string id_;
    std::vector<Stage> stages_;
    std::vector<std::pair<std::string, std::string>> fieldDefinitions_;
    std::vector<std::string> pinnedFields_;
    std::vector<std::pair<std::string, std::vector<std::string>>> pinnedFieldPresets_;
    std::vector<std::pair<std::string, std::vector<std::string>>> diffFieldPresets_;
};

class ITracePageSink {
public:
    virtual ~ITracePageSink() = default;

    virtual GenerateResult Reset(const std::string& content) = 0;
    virtual GenerateResult Append(const std::string& content) = 0;
    virtual GenerateResult WriteInitial(const std::string& shell, const std::string& firstBlock);
};

class TracePage {
public:
    TracePage();
    explicit TracePage(const std::string& filename, const TracePageOptions& options = TracePageOptions(), bool shellAlreadyWritten = false);
    explicit TracePage(std::shared_ptr<ITracePageSink> sink, const TracePageOptions& options = TracePageOptions(), bool shellAlreadyWritten = false);

    ~TracePage();
    TracePage(const TracePage& other);
    TracePage& operator=(const TracePage& other);
    TracePage(TracePage&& other) noexcept;
    TracePage& operator=(TracePage&& other) noexcept;

    Trace& trace(const std::string& title);
    GenerateResult submit(Trace::Tile& tile);
    GenerateResult flush();

private:
    friend class detail::Emitter;

    struct StreamState;

    std::vector<Trace> traces_;
    std::shared_ptr<StreamState> stream_;
};

}  // namespace optimizer_trace

#endif  // OPTIMIZER_TRACE_H
