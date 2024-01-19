#include "fyamlcpp.h"

#include <contrib/libs/libfyaml/include/libfyaml.h>

#include <util/digest/murmur.h>

namespace NKikimr::NFyaml {

const char* zstr = "";

enum class EErrorType {
    Debug = FYET_DEBUG,
    Info = FYET_INFO,
    Notice = FYET_NOTICE,
    Warning = FYET_WARNING,
    Error = FYET_ERROR,
    Max = FYET_MAX,
};

enum class EErrorModule {
    Unknown = FYEM_UNKNOWN,
    Atom = FYEM_ATOM,
    Scan = FYEM_SCAN,
    Parse = FYEM_PARSE,
    Doc = FYEM_DOC,
    Build = FYEM_BUILD,
    Internal = FYEM_INTERNAL,
    System = FYEM_SYSTEM,
    Max = FYEM_MAX,
};

enum class EParseCfgFlags {
    Quiet = FYPCF_QUIET,
    CollectDiag = FYPCF_COLLECT_DIAG,
    ResolveDocument = FYPCF_RESOLVE_DOCUMENT,
    DisableMmapOpt = FYPCF_DISABLE_MMAP_OPT,
    DisableRecycling = FYPCF_DISABLE_RECYCLING,
    ParseComments = FYPCF_PARSE_COMMENTS,
    DisableDepth_limit = FYPCF_DISABLE_DEPTH_LIMIT,
    DisableAccelerators = FYPCF_DISABLE_ACCELERATORS,
    DisableBuffering = FYPCF_DISABLE_BUFFERING,
    DefaultVersionAuto = FYPCF_DEFAULT_VERSION_AUTO,
    DefaultVersion1_1 = FYPCF_DEFAULT_VERSION_1_1,
    DefaultVersion1_2 = FYPCF_DEFAULT_VERSION_1_2,
    DefaultVersion1_3 = FYPCF_DEFAULT_VERSION_1_3,
    SloppyFlowIndentation = FYPCF_SLOPPY_FLOW_INDENTATION,
    PreferRecursive = FYPCF_PREFER_RECURSIVE,
    JsonAuto = FYPCF_JSON_AUTO,
    JsonNone = FYPCF_JSON_NONE,
    JsonForce = FYPCF_JSON_FORCE,
    YpathAliases = FYPCF_YPATH_ALIASES,
    AllowDuplicateKeys = FYPCF_ALLOW_DUPLICATE_KEYS,
};

enum class EEventType {
    None = FYET_NONE,
    StreamStart = FYET_STREAM_START,
    StreamEnd = FYET_STREAM_END,
    DocumentStart = FYET_DOCUMENT_START,
    DocumentEnd = FYET_DOCUMENT_END,
    MappingStart = FYET_MAPPING_START,
    MappingEnd = FYET_MAPPING_END,
    SequenceStart = FYET_SEQUENCE_START,
    SequenceEnd = FYET_SEQUENCE_END,
    Scalar = FYET_SCALAR,
    Alias = FYET_ALIAS,
};

enum class EScalarStyle {
    Any = FYSS_ANY,
    Plain = FYSS_PLAIN,
    SingleQuoted = FYSS_SINGLE_QUOTED,
    DoubleQuoted = FYSS_DOUBLE_QUOTED,
    Literal = FYSS_LITERAL,
    Folded = FYSS_FOLDED,
    Max = FYSS_MAX,
};

enum class EEmitterWriteType {
    DocumentIndicator = fyewt_document_indicator,
    TagDirective = fyewt_tag_directive,
    VersionDirective = fyewt_version_directive,
    Indent = fyewt_indent,
    Indicator = fyewt_indicator,
    Whitespace = fyewt_whitespace,
    PlainScalar = fyewt_plain_scalar,
    SingleQuotedScalar = fyewt_single_quoted_scalar,
    DoubleQuotedScalar = fyewt_double_quoted_scalar,
    LiteralScalar = fyewt_literal_scalar,
    FoldedScalar = fyewt_folded_scalar,
    Anchor = fyewt_anchor,
    Tag = fyewt_tag,
    Linebreak = fyewt_linebreak,
    Alias = fyewt_alias,
    TerminatingZero = fyewt_terminating_zero,
    PlainScalarKey = fyewt_plain_scalar_key,
    SingleQuotedScalarKey = fyewt_single_quoted_scalar_key,
    DoubleQuotedScalarKey = fyewt_double_quoted_scalar_key,
    Comment = fyewt_comment,
};

enum class ECommentPlacement {
    Top = fycp_top,
    Right = fycp_right,
    Bottom = fycp_bottom,
};

enum EEmitterCfgFlags {
    SortKeys = FYECF_SORT_KEYS,
    OutputComments = FYECF_OUTPUT_COMMENTS,
    StripLabels = FYECF_STRIP_LABELS,
    StripTags = FYECF_STRIP_TAGS,
    StripDoc = FYECF_STRIP_DOC,
    NoEndingNewline = FYECF_NO_ENDING_NEWLINE,
    StripEmptyKv = FYECF_STRIP_EMPTY_KV,
    IndentDefault = FYECF_INDENT_DEFAULT,
    Indent1 = FYECF_INDENT_1,
    Indent2 = FYECF_INDENT_2,
    Indent3 = FYECF_INDENT_3,
    Indent4 = FYECF_INDENT_4,
    Indent5 = FYECF_INDENT_5,
    Indent6 = FYECF_INDENT_6,
    Indent7 = FYECF_INDENT_7,
    Indent8 = FYECF_INDENT_8,
    Indent9 = FYECF_INDENT_9,
    WidthDefault = FYECF_WIDTH_DEFAULT,
    Width80 = FYECF_WIDTH_80,
    Width132 = FYECF_WIDTH_132,
    WidthInf = FYECF_WIDTH_INF,
    ModeOriginal = FYECF_MODE_ORIGINAL,
    ModeBlock = FYECF_MODE_BLOCK,
    ModeFlow = FYECF_MODE_FLOW,
    ModeFlowOneline = FYECF_MODE_FLOW_ONELINE,
    ModeJson = FYECF_MODE_JSON,
    ModeJsonTp = FYECF_MODE_JSON_TP,
    ModeJsonOneline = FYECF_MODE_JSON_ONELINE,
    ModeDejson = FYECF_MODE_DEJSON,
    ModePretty = FYECF_MODE_PRETTY,
    DocStartMarkAuto = FYECF_DOC_START_MARK_AUTO,
    DocStartMarkOff = FYECF_DOC_START_MARK_OFF,
    DocStartMarkOn = FYECF_DOC_START_MARK_ON,
    DocEndMarkAuto = FYECF_DOC_END_MARK_AUTO,
    DocEndMarkOff = FYECF_DOC_END_MARK_OFF,
    DocEndMarkOn = FYECF_DOC_END_MARK_ON,
    VersionDirAuto = FYECF_VERSION_DIR_AUTO,
    VersionDirOff = FYECF_VERSION_DIR_OFF,
    VersionDirOn = FYECF_VERSION_DIR_ON,
    TagDirAuto = FYECF_TAG_DIR_AUTO,
    TagDirOff = FYECF_TAG_DIR_OFF,
    TagDirOn = FYECF_TAG_DIR_ON,

    Default = FYECF_DEFAULT,
};

enum class ENodeWalkFlags {
    DontFollow = FYNWF_DONT_FOLLOW,
    Follow = FYNWF_FOLLOW,
    PtrYaml = FYNWF_PTR_YAML,
    PtrJson = FYNWF_PTR_JSON,
    PtrReljson = FYNWF_PTR_RELJSON,
    PtrYpath = FYNWF_PTR_YPATH,
    UriEncoded = FYNWF_URI_ENCODED,
    MaxdepthDefault = FYNWF_MAXDEPTH_DEFAULT,
    MarkerDefault = FYNWF_MARKER_DEFAULT,
    PtrDefault = FYNWF_PTR_DEFAULT,
};

enum class EPathParseCfgFlags {
    Quiet = FYPPCF_QUIET,
    DisableRecycling = FYPPCF_DISABLE_RECYCLING,
    DisableAccelerators = FYPPCF_DISABLE_ACCELERATORS,
};

enum class EPathExecCfgFlags {
    Quiet = FYPXCF_QUIET,
    DisableRecycling = FYPXCF_DISABLE_RECYCLING,
    DisableAccelerators = FYPXCF_DISABLE_ACCELERATORS,
};

enum class ETokenType {
    /* non-content token types */
    None = FYTT_NONE,
    StreamStart = FYTT_STREAM_START,
    StreamEnd = FYTT_STREAM_END,
    VersionDirective = FYTT_VERSION_DIRECTIVE,
    TagDirective = FYTT_TAG_DIRECTIVE,
    DocumentStart = FYTT_DOCUMENT_START,
    DocumentEnd = FYTT_DOCUMENT_END,
    /* content token types */
    BlockSequenceStart = FYTT_BLOCK_SEQUENCE_START,
    BlockMappingStart = FYTT_BLOCK_MAPPING_START,
    BlockEnd = FYTT_BLOCK_END,
    FlowSequenceStart = FYTT_FLOW_SEQUENCE_START,
    FlowSequenceEnd = FYTT_FLOW_SEQUENCE_END,
    FlowMappingStart = FYTT_FLOW_MAPPING_START,
    FlowMappingEnd = FYTT_FLOW_MAPPING_END,
    BlockEntry = FYTT_BLOCK_ENTRY,
    FlowEntry = FYTT_FLOW_ENTRY,
    Key = FYTT_KEY,
    Value = FYTT_VALUE,
    Alias = FYTT_ALIAS,
    Anchor = FYTT_ANCHOR,
    Tag = FYTT_TAG,
    Scalar = FYTT_SCALAR,

    /* special error reporting */
    Input_marker = FYTT_INPUT_MARKER,

    /* path expression tokens */
    PeSlash = FYTT_PE_SLASH,
    PeRoot = FYTT_PE_ROOT,
    PeThis = FYTT_PE_THIS,
    PeParent = FYTT_PE_PARENT,
    PeMapKey = FYTT_PE_MAP_KEY,
    PeSeqIndex = FYTT_PE_SEQ_INDEX,
    PeSeqSlice = FYTT_PE_SEQ_SLICE,
    PeScalarFilter = FYTT_PE_SCALAR_FILTER,
    PeCollectionFilter = FYTT_PE_COLLECTION_FILTER,
    PeSeqFilter = FYTT_PE_SEQ_FILTER,
    PeMapFilter = FYTT_PE_MAP_FILTER,
    PeUniqueFilter = FYTT_PE_UNIQUE_FILTER,
    PeEveryChild = FYTT_PE_EVERY_CHILD,
    PeEveryChildR = FYTT_PE_EVERY_CHILD_R,
    PeAlias = FYTT_PE_ALIAS,
    PeSibling = FYTT_PE_SIBLING,
    PeComma = FYTT_PE_COMMA,
    PeBarbar = FYTT_PE_BARBAR,
    PeAmpamp = FYTT_PE_AMPAMP,
    PeLparen = FYTT_PE_LPAREN,
    PeRparen = FYTT_PE_RPAREN,

    /* comparison operators */
    PeEqeq = FYTT_PE_EQEQ,
    PeNoteq = FYTT_PE_NOTEQ,
    PeLt = FYTT_PE_LT,
    PeGt = FYTT_PE_GT,
    PeLte = FYTT_PE_LTE,
    PeGte = FYTT_PE_GTE,

    /* scalar expression tokens */
    SePlus = FYTT_SE_PLUS,
    SeMinus = FYTT_SE_MINUS,
    SeMult = FYTT_SE_MULT,
    SeDiv = FYTT_SE_DIV,

    PeMethod = FYTT_PE_METHOD,
    SeMethod = FYTT_SE_METHOD,
};

enum class EComposerReturn {
    OkContinue = FYCR_OK_CONTINUE,
    OkStop = FYCR_OK_STOP,
    OkStartSkip = FYCR_OK_START_SKIP,
    OkStopSkip = FYCR_OK_STOP_SKIP,
    Error = FYCR_ERROR,
};

TDocumentIterator::TDocumentIterator(fy_document_iterator* iterator)
    : Iterator_(iterator, fy_document_iterator_destroy)
{}

TNodeRef::TNodeRef(fy_node* node)
    : Node_(node)
{}

fy_node* TNodeRef::NodeRawPointer() const {
    return Node_;
}

TNode& TNode::operator=(fy_node* node) {
    Node_.reset(node, fy_node_free);
    return *this;
}

TNode::TNode(fy_node* node)
    : Node_(node, fy_node_free)
{}

TNodeRef TNodePairRef::Key() const {
    ENSURE_NODE_NOT_EMPTY(Pair_);
    return TNodeRef(fy_node_pair_key(Pair_));
}

int TNodePairRef::Index(const TNodeRef& node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    ENSURE_NODE_NOT_EMPTY(Pair_);
    return fy_node_mapping_get_pair_index(node.Node_, Pair_);
}

void TNodePairRef::SetKey(const TNodeRef& node) {
    ENSURE_NODE_NOT_EMPTY(Pair_);
    ENSURE_NODE_NOT_EMPTY(node);
    NDetail::RethrowOnError(fy_node_pair_set_key(Pair_, node.Node_), Pair_);
}

TNodeRef TNodePairRef::Value() const {
    ENSURE_NODE_NOT_EMPTY(Pair_);
    return TNodeRef(fy_node_pair_value(Pair_));
}

void TNodePairRef::SetValue(const TNodeRef& node) {
    ENSURE_NODE_NOT_EMPTY(Pair_);
    ENSURE_NODE_NOT_EMPTY(node);
    NDetail::RethrowOnError(fy_node_pair_set_value(Pair_, node.Node_), Pair_);
}

TMappingIterator::TMappingIterator(const TNodeRef& node, bool end)
    : Node_(node)
{
    if (!end) {
        NodePair_ = TNodePairRef(fy_node_mapping_iterate(Node_.Node_, reinterpret_cast<void**>(&NodePair_.Pair_)));
    }
}

TMappingIterator& TMappingIterator::operator++() {
    NodePair_ = TNodePairRef(fy_node_mapping_iterate(Node_.Node_, reinterpret_cast<void**>(&NodePair_.Pair_)));
    return *this;
}

TReverseMappingIterator::TReverseMappingIterator(const TNodeRef& node, bool end)
    : Node_(node)
{
    if (!end) {
        NodePair_ = TNodePairRef(fy_node_mapping_reverse_iterate(Node_.Node_, reinterpret_cast<void**>(&NodePair_.Pair_)));
    }
}

TReverseMappingIterator& TReverseMappingIterator::operator++() {
    NodePair_ = TNodePairRef(fy_node_mapping_reverse_iterate(Node_.Node_, reinterpret_cast<void**>(&NodePair_.Pair_)));
    return *this;
}

size_t TMapping::size() const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    return fy_node_mapping_item_count(Node_);
}

size_t TMapping::empty() const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    return fy_node_mapping_is_empty(Node_);
}

TNodePairRef TMapping::at(int index) const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    auto res = fy_node_mapping_get_by_index(Node_, index);
    Y_ENSURE_EX(res, ({
        TStringStream ss;
        ss << "No such child: " << Path() << "/" << index;
        TFyamlEx(ss.Str());
    }));
    return TNodePairRef(res);
}

TNodePairRef TMapping::operator[](int index) const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    return TNodePairRef(fy_node_mapping_get_by_index(Node_, index));
}

TNodeRef TMapping::at(const TString& index) const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    auto res = fy_node_mapping_lookup_by_string(Node_, index.data(), index.size());
    Y_ENSURE_EX(res, ({
        TStringStream ss;
        ss << "No such child: " << Path() << "/" << index;
        TFyamlEx(ss.Str());
    }));
    return TNodeRef(res);
}

TNodePairRef TMapping::pair_at(const TString& index) const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    auto res = fy_node_mapping_lookup_pair_by_string(Node_, index.data(), index.size());
    Y_ENSURE_EX(res, ({
        TStringStream ss;
        ss << "No such child: " << Path() << "/" << index;
        TFyamlEx(ss.Str());
    }));
    return TNodePairRef(res);
}

TNodePairRef TMapping::pair_at_opt(const TString& index) const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    return TNodePairRef(fy_node_mapping_lookup_pair_by_string(Node_, index.data(), index.size()));
}

TNodeRef TMapping::operator[](const TString& index) const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    return TNodeRef(fy_node_mapping_lookup_by_string(Node_, index.data(), index.size()));
}

TNodeRef TMapping::operator[](const char* str) const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    TString index(str);
    return TNodeRef(fy_node_mapping_lookup_by_string(Node_, index.data(), index.size()));
}

void TMapping::Append(const TNodeRef& key, const TNodeRef& value) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(key);
    ENSURE_NODE_NOT_EMPTY(value);
    NDetail::RethrowOnError(fy_node_mapping_append(Node_, key.Node_, value.Node_), Node_);
}

void TMapping::Prepend(const TNodeRef& key, const TNodeRef& value) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(key);
    ENSURE_NODE_NOT_EMPTY(value);
    NDetail::RethrowOnError(fy_node_mapping_prepend(Node_, key.Node_, value.Node_), Node_);
}

void TMapping::Remove(const TNodePairRef& toRemove) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(toRemove);
    NDetail::RethrowOnError(fy_node_mapping_remove(Node_, toRemove.Pair_), Node_);
    fy_node_free(fy_node_pair_key(toRemove.Pair_));
    fy_node_free(fy_node_pair_value(toRemove.Pair_));
    free(toRemove.Pair_);
}

bool TMapping::Has(TString key) const {
    return fy_node_mapping_lookup_by_string(Node_, key.data(), key.size()) != nullptr;
}

TMappingIterator TMapping::Remove(const TMappingIterator& toRemove) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    Y_DEBUG_ABORT_UNLESS(Node_ == toRemove.Node_);
    TMappingIterator ret = toRemove;
    ++ret;
    fy_node_mapping_remove(Node_, toRemove.NodePair_.Pair_);
    return ret;
}

void TMapping::Remove(const TNodeRef& key) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    fy_node_free(fy_node_mapping_remove_by_key(Node_, key.Node_));
}

TSequenceIterator::TSequenceIterator(const TNodeRef& node, bool end)
    : Node_(node)
{
    if (!end) {
        IterNode_ = TNodeRef(fy_node_sequence_iterate(Node_.Node_, &Iter_));
    }
}

TSequenceIterator& TSequenceIterator::operator++() {
    IterNode_ = TNodeRef(fy_node_sequence_iterate(Node_.Node_, &Iter_));
    return *this;
}

void TSequenceIterator::InsertBefore(const TNodeRef& node) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(IterNode_);
    ENSURE_NODE_NOT_EMPTY(node);
    NDetail::RethrowOnError(fy_node_sequence_insert_before(Node_.Node_, IterNode_.Node_, node.Node_), Node_.Node_);
}

void TSequenceIterator::InsertAfter(const TNodeRef& node) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(IterNode_);
    ENSURE_NODE_NOT_EMPTY(node);
    NDetail::RethrowOnError(fy_node_sequence_insert_after(Node_.Node_, IterNode_.Node_, node.Node_), Node_.Node_);
}

TReverseSequenceIterator::TReverseSequenceIterator(const TNodeRef& node, bool end)
    : Node_(node)
{
    if (!end) {
        IterNode_ = TNodeRef(fy_node_sequence_reverse_iterate(Node_.Node_, &Iter_));
    }
}

TReverseSequenceIterator& TReverseSequenceIterator::operator++() {
    IterNode_ = TNodeRef(fy_node_sequence_reverse_iterate(Node_.Node_, &Iter_));
    return *this;
}

void TReverseSequenceIterator::InsertBefore(const TNodeRef& node) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(IterNode_);
    ENSURE_NODE_NOT_EMPTY(node);
    NDetail::RethrowOnError(fy_node_sequence_insert_after(Node_.Node_, IterNode_.Node_, node.Node_), Node_.Node_);
}

void TReverseSequenceIterator::InsertAfter(const TNodeRef& node) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(IterNode_);
    ENSURE_NODE_NOT_EMPTY(node);
    NDetail::RethrowOnError(fy_node_sequence_insert_before(Node_.Node_, IterNode_.Node_, node.Node_), Node_.Node_);
}

size_t TSequence::size() const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    return fy_node_sequence_item_count(Node_);
}

size_t TSequence::empty() const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    return fy_node_sequence_is_empty(Node_);
}

TNodeRef TSequence::at(int index) const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    auto res = fy_node_sequence_get_by_index(Node_, index);
    Y_ENSURE_EX(res, ({
        TStringStream ss;
        ss << "No such index: " << Path() << "/" << index;
        TFyamlEx(ss.Str());
    }));
    return TNodeRef(res);
}

TNodeRef TSequence::operator[](int index) const {
    ENSURE_NODE_NOT_EMPTY(Node_);
    return TNodeRef(fy_node_sequence_get_by_index(Node_, index));
}

void TSequence::Append(const TNodeRef& node) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(node);
    NDetail::RethrowOnError(fy_node_sequence_append(Node_, node.Node_), Node_);
}

void TSequence::Prepend(const TNodeRef& node) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(node);
    NDetail::RethrowOnError(fy_node_sequence_prepend(Node_, node.Node_), Node_);
}

void TSequence::InsertBefore(const TNodeRef& mark, const TNodeRef& node) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(mark);
    ENSURE_NODE_NOT_EMPTY(node);
    NDetail::RethrowOnError(fy_node_sequence_insert_before(Node_, mark.Node_, node.Node_), Node_);
}

void TSequence::InsertAfter(const TNodeRef& mark, const TNodeRef& node) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(mark);
    ENSURE_NODE_NOT_EMPTY(node);
    NDetail::RethrowOnError(fy_node_sequence_insert_after(Node_, mark.Node_, node.Node_), Node_);
}

TNode TSequence::Remove(const TNodeRef& toRemove) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    ENSURE_NODE_NOT_EMPTY(toRemove.Node_);
    return TNode(fy_node_sequence_remove(Node_, toRemove.Node_));
}

TSequenceIterator TSequence::Remove(const TSequenceIterator& toRemove) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    Y_DEBUG_ABORT_UNLESS(Node_ == toRemove.Node_);
    ENSURE_NODE_NOT_EMPTY(toRemove.IterNode_);
    TSequenceIterator ret = toRemove;
    ++ret;
    fy_node_sequence_remove(Node_, toRemove.IterNode_.Node_);
    fy_node_free(toRemove.IterNode_.Node_); // TODO add extract
    return ret;
}

TReverseSequenceIterator TSequence::Remove(const TReverseSequenceIterator& toRemove) {
    ENSURE_NODE_NOT_EMPTY(Node_);
    Y_DEBUG_ABORT_UNLESS(Node_ == toRemove.Node_);
    ENSURE_NODE_NOT_EMPTY(toRemove.IterNode_);
    TReverseSequenceIterator ret = toRemove;
    ++ret;
    fy_node_sequence_remove(Node_, toRemove.IterNode_.Node_);
    fy_node_free(toRemove.IterNode_.Node_); // TODO add extract
    return ret;
}

TDocumentNodeIterator::TDocumentNodeIterator(TNodeRef&& node)
    : Node_(node)
{
    if (node) {
        Iterator_ = {fy_document_iterator_create(), fy_document_iterator_destroy};
        fy_document_iterator_node_start(Iterator_.get(), node.Node_);
    }
}

TDocumentNodeIterator& TDocumentNodeIterator::operator++() {
    Node_ = fy_document_iterator_node_next(Iterator_.get());
    return *this;
}

TDocument::TDocument(TString str, fy_document* doc, fy_diag* diag)
    : Document_(doc, fy_document_destroy)
    , Diag_(diag, fy_diag_destroy)
{
    auto* userdata = new THashSet<TSimpleSharedPtr<TString>, TStringPtrHashT>({MakeSimpleShared<TString>(std::move(str))});
    fy_document_set_userdata(doc, userdata);
    fy_document_register_on_destroy(doc, &DestroyDocumentStrings);
    RegisterUserDataCleanup();
}

TDocument::TDocument(fy_document* doc, fy_diag* diag)
    : Document_(doc, fy_document_destroy)
    , Diag_(diag, fy_diag_destroy)
{
    RegisterUserDataCleanup();
}

TDocument TDocument::Parse(TString str) {
    const char* cstr = str.empty() ? zstr : str.cbegin();
    fy_diag_cfg dcfg;
    fy_diag_cfg_default(&dcfg);
    std::unique_ptr<fy_diag, void(*)(fy_diag*)> diag(fy_diag_create(&dcfg), fy_diag_destroy);
    fy_diag_set_collect_errors(diag.get(), true);
    fy_parse_cfg cfg{
        "",
        // FYPCF_PARSE_COMMENTS,
        FYPCF_QUIET,
        nullptr,
        diag.get()
    };
    fy_document* doc = fy_document_build_from_string(&cfg, cstr, FY_NT);
    if (!doc) {
        NDetail::ThrowAllExceptionsIfAny(diag.get());
    }
    return TDocument(std::move(str), doc, diag.release());
}

TDocument TDocument::Clone() const {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    fy_document* doc = fy_document_clone(Document_.get());
    fy_document_set_userdata(
        doc,
        new THashSet<TSimpleSharedPtr<TString>, TStringPtrHashT>(
            *reinterpret_cast<THashSet<TSimpleSharedPtr<TString>, TStringPtrHashT>*>(fy_document_get_userdata(Document_.get()))
        )
    );
    fy_document_register_on_destroy(doc, &DestroyDocumentStrings);
    return TDocument(doc, fy_document_get_diag(doc));
}

void TDocument::InsertAt(const char* path, const TNodeRef& node) {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    NDetail::RethrowOnError(fy_document_insert_at(Document_.get(), path, FY_NT, node.Node_), Diag_.get());
}

TNodeRef TDocument::Buildf(const char* content) {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    return TNodeRef(fy_node_build_from_string(Document_.get(), content, strlen(content)));
}

void TDocument::Resolve() {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    if (fy_document_resolve(Document_.get()) != 0) {
        NDetail::ThrowAllExceptionsIfAny(Diag_.get());
    }
}

bool TDocument::HasDirectives() {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    return fy_document_has_directives(Document_.get());
}

bool TDocument::HasExplicitDocumentStart() {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    return fy_document_has_explicit_document_start(Document_.get());
}

bool TDocument::HasExplicitDocumentEnd() {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    return fy_document_has_explicit_document_end(Document_.get());
}

void TDocument::SetParent(const TDocument& doc) {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    ENSURE_DOCUMENT_NOT_EMPTY(doc.Document_);
    NDetail::RethrowOnError(fy_document_set_parent(doc.Document_.get(), Document_.release()), Diag_.get());
}

TNodeRef TDocument::Root() {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    return TNodeRef(fy_document_root(Document_.get()));
}

void TDocument::SetRoot(const TNodeRef& node) {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    ENSURE_NODE_NOT_EMPTY(node.Node_);
    NDetail::RethrowOnError(fy_document_set_root(Document_.get(), node.Node_), Diag_.get());
}

TNodeRef TDocument::CreateAlias(const TString& name) {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    return TNodeRef(fy_node_create_alias_copy(Document_.get(), name.c_str(), name.length()));
}

std::unique_ptr<char, void(*)(char*)> TDocument::EmitToCharArray() const {
    std::unique_ptr<char, void(*)(char*)> res(
        fy_emit_document_to_string(
            Document_.get(),
            (fy_emitter_cfg_flags)(FYECF_DEFAULT | FYECF_MODE_PRETTY | FYECF_OUTPUT_COMMENTS)), &NDetail::FreeChar);
    return res;
}

bool TDocument::RegisterUserDataCleanup() {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    return fy_document_register_meta(Document_.get(), &DestroyUserData, nullptr) == 0;
}

void TDocument::UnregisterUserDataCleanup() {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    fy_document_unregister_meta(Document_.get());
}

TMark TDocument::BeginMark() const {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    auto* fyds = fy_document_get_document_state(Document_.get());
    auto* mark = fy_document_state_start_mark(fyds);
    return TMark{
        mark->input_pos,
        mark->line,
        mark->column,
    };
}

TMark TDocument::EndMark() const {
    ENSURE_DOCUMENT_NOT_EMPTY(Document_);
    auto* fyds = fy_document_get_document_state(Document_.get());
    auto* mark = fy_document_state_end_mark(fyds);
    return TMark{
        mark->input_pos,
        mark->line,
        mark->column,
    };
}

std::unique_ptr<char, void(*)(char*)> TJsonEmitter::EmitToCharArray() const {
    std::unique_ptr<char, void(*)(char*)> res(
        fy_emit_node_to_string(
            Node_.Node_,
            (fy_emitter_cfg_flags)(FYECF_DEFAULT | FYECF_SORT_KEYS | FYECF_MODE_JSON_TP)), &NDetail::FreeChar);
    return res;
}

TParser::TParser(TString rawStream, fy_parser* parser, fy_diag* diag)
    : RawDocumentStream_(std::move(rawStream))
    , Parser_(parser, fy_parser_destroy)
    , Diag_(diag, fy_diag_destroy)
{}

TParser TParser::Create(TString str)
{
    const char* stream = str.empty() ? zstr : str.cbegin();
    fy_diag_cfg dcfg;
    fy_diag_cfg_default(&dcfg);
    std::unique_ptr<fy_diag, void(*)(fy_diag*)> diag(fy_diag_create(&dcfg), fy_diag_destroy);
    fy_diag_set_collect_errors(diag.get(), true);
    fy_parse_cfg cfg{
        "",
        // FYPCF_PARSE_COMMENTS,
        FYPCF_QUIET,
        nullptr,
        diag.get()
    };
    auto* parser = fy_parser_create(&cfg);
    if (!parser) {
        NDetail::ThrowAllExceptionsIfAny(diag.get());
    }

    fy_parser_set_string(parser, stream, -1);

    return TParser(std::move(str), parser, diag.release());
}

std::optional<TDocument> TParser::NextDocument() {
    auto* doc = fy_parse_load_document(Parser_.get());
    if (!doc) {
        return std::nullopt;
    }

    return TDocument(RawDocumentStream_, doc, fy_document_get_diag(doc));
}

namespace NDetail {

fy_node* TNodeOpsBase::CreateReference(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    return fy_node_create_reference(node);
}

fy_node* TNodeOpsBase::Copy(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    return fy_node_copy(fy_node_document(node), node);
}

fy_node* TNodeOpsBase::Copy(fy_node* node, fy_document* to) const {
    ENSURE_NODE_NOT_EMPTY(node);
    auto* fromDoc = fy_node_document(node);
    auto& fromUserdata = *reinterpret_cast<THashSet<TSimpleSharedPtr<TString>, TStringPtrHashT>*>(fy_document_get_userdata(fromDoc));
    auto& toUserdata = *reinterpret_cast<THashSet<TSimpleSharedPtr<TString>, TStringPtrHashT>*>(fy_document_get_userdata(to));
    toUserdata.insert(fromUserdata.begin(), fromUserdata.end());
    return fy_node_copy(to, node);
}

TString TNodeOpsBase::Path(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    char* path = fy_node_get_path(node);

    if (path) {
        TString str(path);
        free(path);
        return str;
    }

    return {};
}

ENodeType TNodeOpsBase::Type(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    return static_cast<ENodeType>(fy_node_get_type(node));
}

bool TNodeOpsBase::IsAlias(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    return fy_node_is_alias(node);
}

fy_node* TNodeOpsBase::ResolveAlias(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    Y_DEBUG_ABORT_UNLESS(IsAlias(node));
    return fy_node_resolve_alias(node);
}

TString TNodeOpsBase::Scalar(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    Y_ENSURE_EX(fy_node_is_scalar(node), TFyamlEx() << "Node is not Scalar: " << Path(node));
    size_t size;
    const char* text = fy_node_get_scalar(node, &size);
    return TString(text, size);
}

TMark TNodeOpsBase::BeginMark(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    std::unique_ptr<fy_document_iterator, void(*)(fy_document_iterator*)> it(
        fy_document_iterator_create(),
        &fy_document_iterator_destroy);
    fy_document_iterator_node_start(it.get(), node);
    auto deleter = [&](fy_event* fye){ fy_document_iterator_event_free(it.get(), fye); };
    std::unique_ptr<fy_event, decltype(deleter)> ev(
        fy_document_iterator_body_next(it.get()),
        deleter);
    auto* mark = fy_event_start_mark(ev.get());

    if (!mark) {
        ythrow yexception() << "can't get begin mark for a node";
    }

    return TMark{
        mark->input_pos,
        mark->line,
        mark->column,
    };
}

namespace {

fy_event_type GetOpenEventType(ENodeType type) {
    switch(type) {
    case ENodeType::Mapping:
        return FYET_MAPPING_START;
    case ENodeType::Sequence:
        return FYET_SEQUENCE_START;
    default:
        Y_ABORT("Not a brackets type");
    }
}

fy_event_type GetCloseEventType(ENodeType type) {
    switch(type) {
    case ENodeType::Mapping:
        return FYET_MAPPING_END;
    case ENodeType::Sequence:
        return FYET_SEQUENCE_END;
    default:
        Y_ABORT("Not a brackets type");
    }
}

} // anonymous namespace

TMark TNodeOpsBase::EndMark(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    std::unique_ptr<fy_document_iterator, void(*)(fy_document_iterator*)> it(
        fy_document_iterator_create(),
        &fy_document_iterator_destroy);
    fy_document_iterator_node_start(it.get(), node);

    auto deleter = [&](fy_event* fye){ fy_document_iterator_event_free(it.get(), fye); };
    std::unique_ptr<fy_event, decltype(deleter)> prevEv(
        nullptr,
        deleter);
    std::unique_ptr<fy_event, decltype(deleter)> ev(
        fy_document_iterator_body_next(it.get()),
        deleter);

    if (IsComplexType(Type(node))) {
        int openBrackets = 0;
        if (ev->type == GetOpenEventType(Type(node))) {
            ++openBrackets;
        }
        if (ev->type == GetCloseEventType(Type(node))) {
            --openBrackets;
        }
        while (ev->type != GetCloseEventType(Type(node)) || openBrackets != 0) {
            std::unique_ptr<fy_event, decltype(deleter)> cur(
                fy_document_iterator_body_next(it.get()),
                deleter);
            if (cur == nullptr) {
                break;
            }
            if (cur->type == GetOpenEventType(Type(node))) {
                ++openBrackets;
            }
            if (cur->type == GetCloseEventType(Type(node))) {
                --openBrackets;
            }
            if (fy_event_get_node_style(cur.get()) != FYNS_BLOCK) {
                prevEv.reset(ev.release());
                ev.reset(cur.release());
            }
        }
    }

    auto* mark = fy_event_end_mark(ev.get());

    if (!mark && prevEv) {
        mark = fy_event_end_mark(prevEv.get());
    }

    if (!mark) {
        ythrow yexception() << "can't get end mark for a node";
    }

    return TMark{
        mark->input_pos,
        mark->line,
        mark->column,
    };
}

fy_node* TNodeOpsBase::Map(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    Y_ENSURE_EX(fy_node_is_mapping(node), TFyamlEx() << "Node is not Mapping: " << Path(node));
    return node;
}

fy_node* TNodeOpsBase::Sequence(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    Y_ENSURE_EX(fy_node_is_sequence(node), TFyamlEx() << "Node is not Sequence: " << Path(node));
    return node;
}

void TNodeOpsBase::Insert(fy_node* thisNode, fy_node* node) {
    ENSURE_NODE_NOT_EMPTY(node);
    RethrowOnError(fy_node_insert(thisNode, node), thisNode);
}

std::optional<TString> TNodeOpsBase::Tag(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    size_t len = 0;
    const char* tag = fy_node_get_tag(node, &len);

    if (tag) {
        return TString(tag, len);
    }

    return std::nullopt;
}

void TNodeOpsBase::SetTag(fy_node* node, const TString& tag) {
    ENSURE_NODE_NOT_EMPTY(node);
    auto* str = new TString(std::move(tag));
    auto* data = new TUserDataHolder(UserData(node), str);
    SetUserData(node, data);
    RethrowOnError(fy_node_set_tag(node, str->c_str(), str->length()), node);
}

bool TNodeOpsBase::RemoveTag(fy_node* node) {
    ENSURE_NODE_NOT_EMPTY(node);
    bool ret = fy_node_remove_tag(node);
    ClearUserData(node);
    return ret;
}

bool TNodeOpsBase::HasAnchor(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    return fy_node_get_anchor(node) != nullptr;
}

void TNodeOpsBase::SetAnchor(fy_node* node, const TString& anchor) {
    auto* str = new TString(anchor);
    auto* data = new TUserDataHolder(UserData(node), str);
    SetUserData(node, data);
    RethrowOnError(fy_node_set_anchor(node, str->c_str(), str->length()), node);
}

bool TNodeOpsBase::DeepEqual(fy_node* thisNode, fy_node* other) {
    ENSURE_NODE_NOT_EMPTY(thisNode);
    ENSURE_NODE_NOT_EMPTY(other);
    return fy_node_compare(thisNode, other);
}

std::unique_ptr<char, void(*)(char*)> TNodeOpsBase::EmitToCharArray(fy_node* node) const {
    std::unique_ptr<char, void(*)(char*)> res(
        fy_emit_node_to_string(
            node,
            (fy_emitter_cfg_flags)(FYECF_DEFAULT)), &FreeChar);
    return res;
}

void TNodeOpsBase::SetStyle(fy_node* node, ENodeStyle style) {
    ENSURE_NODE_NOT_EMPTY(node);
    fy_node_set_style(node, (enum fy_node_style)style);
}

ENodeStyle TNodeOpsBase::Style(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    return (ENodeStyle)fy_node_get_style(node);
}

void TNodeOpsBase::SetUserData(fy_node* node, IBasicUserData* data) {
    ENSURE_NODE_NOT_EMPTY(node);
    fy_node_set_meta(node, data);
}

IBasicUserData* TNodeOpsBase::UserData(fy_node* node) const {
    ENSURE_NODE_NOT_EMPTY(node);
    return reinterpret_cast<IBasicUserData* >(fy_node_get_meta(node));
}

void TNodeOpsBase::ClearUserData(fy_node* node) {
    ENSURE_NODE_NOT_EMPTY(node);
    fy_node_clear_meta(node);
}

void ThrowAllExceptionsIfAny(fy_diag* diag) {
    void* iter = nullptr;
    fy_diag_error* err = fy_diag_errors_iterate(diag, &iter);
    if (err != nullptr) {
        TStringStream ss;
        ss << err->line << ":" << err->column << " " << err->msg;
        TFyamlEx ex(ss.Str());

        while ((err = fy_diag_errors_iterate(diag, &iter)) != nullptr) {
            TStringStream ss;
            ss << err->line << ":" << err->column << " " << err->msg;
            ex.AddError(ss.Str());
        }

        ythrow ex;
    }
}

void RethrowError(fy_diag* diag) {
    void *iter = nullptr;
    fy_diag_error* err;
    TStringStream ss;
    while ((err = fy_diag_errors_iterate(diag, &iter)) != nullptr) {
        ss << err->line << ":" << err->column << " " << err->msg << "\n";
    }
    ythrow TFyamlEx(ss.Str());
}

void RethrowOnError(bool isError, fy_node* node) {
    if (!isError) {
        return;
    }

    std::unique_ptr<fy_diag, void(*)(fy_diag*)> diag(fy_document_get_diag(fy_node_document(node)), fy_diag_unref);
    RethrowError(diag.get());
}

void RethrowOnError(bool isError, fy_node_pair* pair) {
    if (!isError) {
        return;
    }

    std::unique_ptr<fy_diag, void(*)(fy_diag*)> diag(fy_document_get_diag(fy_node_document(fy_node_pair_key(pair))), fy_diag_unref);
    RethrowError(diag.get());
}

void RethrowOnError(bool isError, fy_diag* diag) {
    if (!isError) {
        return;
    }

    RethrowError(diag);
}

void FreeChar(char* mem) {
    free(mem);
}

bool IsComplexType(ENodeType type) {
    return type == ENodeType::Mapping || type == ENodeType::Sequence;
}

} // namespace NDetail

} // namespace NKikimr::NFyaml

template <>
void Out<NKikimr::NFyaml::TDocument>(IOutputStream& out, const NKikimr::NFyaml::TDocument& value) {
    out << value.EmitToCharArray().get();
}

template <>
void Out<NKikimr::NFyaml::TNodeRef>(IOutputStream& out, const NKikimr::NFyaml::TNodeRef& value) {
    out << value.EmitToCharArray().get();
}

template <>
void Out<NKikimr::NFyaml::TJsonEmitter>(IOutputStream& out, const NKikimr::NFyaml::TJsonEmitter& value) {
    out << value.EmitToCharArray().get();
}

bool operator==(const fy_node* node1, const NKikimr::NFyaml::NDetail::TNodeOps<NKikimr::NFyaml::TNodeRef>& node2) {
     return node2.Node() == node1;
}

bool operator==(const fy_node* node1, const NKikimr::NFyaml::NDetail::TNodeOps<NKikimr::NFyaml::TNode>& node2) {
     return node2.Node() == node1;
}
