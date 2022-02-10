#include "http.h"
#include <util/string/builder.h>

namespace NKikimr {
namespace NTracing {
namespace NHttp {

TCollapsedRef::TCollapsedRef(IOutputStream& str, const TString& target, const TString& text)
    : Str(str) {
    Str << "<a class='collapse-ref' data-toggle='collapse' data-target='#" << target << "'>"
        << text << "</a>";
    Str << "<div id='" << target << "' class='collapse'>";
}

TCollapsedRef::~TCollapsedRef() {
    try {
        Str << "</div>";
    }
    catch (...) {
    }
}

TCollapsedRefAjax::TCollapsedRefAjax(
    IOutputStream& str,
    const TString& target,
    const TTimestampInfo& tsInfo,
    const TString& text
)
    : Str(str) {
    Str << "<a class='collapse-ref' data-toggle='collapse' data-target='#" << target
        << "' onclick=\"getCollapsedData(this, '" << target << "', "
        << tsInfo.Mode << ", " << tsInfo.Precision
        << ")\" data-filled=false>" << text << "</a>" << Endl
        << "<div id='" << target << "' class='collapse'>";
}

TCollapsedRefAjax::~TCollapsedRefAjax() {
    try {
        Str << "</div>";
    }
    catch (...) {
    }
}

void OutputStyles(IOutputStream& os) {
    os << R"(<style>
    .collapse-ref {
        cursor: pointer;
    }
    .tab-left {
        margin-left: 20px;
    }
    .first-dropdown-block {
        float: left;
    }
    .dropdown-block {
        margin-left: 20px;
        float: left;
    }
    .panel {
        display: inline-block;
    }
 </style>
)";
}

void OutputScripts(IOutputStream& str) {
    str << R"(
<script>
function getCollapsedData(elem, signalId, tmode, tprecision) {
    if (elem.getAttribute('data-filled') === 'false') {
        $.ajax({
            url: document.URL + '&SignalID=' + signalId + '&TMode=' + tmode + '&TPrecision=' + tprecision,
            success: function(result) {
                var targetElem = document.getElementById(signalId);
                if (targetElem === undefined) {
                    alert('cant find element with id: ' + signalId);
                } else {
                    targetElem.innerHTML = result;
                }
            },
            error: function() { document.getElementById(signalId).innerHTML = 'Error retrieving data'; }
        });
    }
}
</script>
)";
}

void OutputStaticPart(IOutputStream& str) {
    OutputStyles(str);
    OutputScripts(str);
}

class TInfoPanel {
public:
    TInfoPanel(IOutputStream& str, const TString& heading)
        : Str(str) {
        Str << "<div class = \"panel panel-default\">" << Endl
            << "<div class = \"panel-heading\">" << heading << "</div>" << Endl
            << "<div class = \"panel-body\">" << Endl;
    }

    ~TInfoPanel() {
        Str << "</div>" << Endl << "</div>";
    }

private:
    IOutputStream& Str;
};

void OutputTimeDropdown(IOutputStream& str, const TTraceInfo& traceInfo) {
    TInfoPanel panel(str, "Timestamp settings");
    TStringBuilder hrefBase;
    hrefBase << "   <li role = \"presentation\"> <a role = \"menuitem\" tabindex = \"-1\" href = \"tablet?"
        << "NodeID=" << traceInfo.NodeId
        << "&TabletID=" << traceInfo.TabletId
        << "&RandomID=" << traceInfo.TraceId.RandomID
        << "&CreationTime=" << traceInfo.TraceId.CreationTime;

    TStringBuilder hrefPrecision;
    if (traceInfo.TimestampInfo.Precision != TTimestampInfo::PrecisionDefault) {
        hrefPrecision << "&TPrecision=" << traceInfo.TimestampInfo.Precision;
    }

    str << R"(
<div class="first-dropdown-block">
<div class="dropdown">
<button class="btn btn-primary dropdown-toggle" id="timemode" type="button" data-toggle="dropdown">)";
    str << TTimestampInfo::GetTextMode(traceInfo.TimestampInfo.Mode);
    str << R"(<span class="caret"></span></button>
<ul class="dropdown-menu" role="menu" aria-labelledby="timemode">
)";
    str << hrefBase << "&TMode="
        << TTimestampInfo::ModeDisabled << hrefPrecision << "\">disabled</a></li>";
    str << "    <li role=\"presentation\" class=\"divider\"></li>" << Endl;
    str << hrefBase << "&TMode="
        << TTimestampInfo::ModeAbsolute << hrefPrecision << "\">Absolute</a></li>" << Endl;
    str << hrefBase << "&TMode="
        << TTimestampInfo::ModeFirst << hrefPrecision << "\">Relative to the first</a></li>" << Endl;
    str << hrefBase << "&TMode="
        << TTimestampInfo::ModePrevious << hrefPrecision << "\">Relative to the previous</a></li>" << Endl;
    str << R"(</ul>
</div>
</div>
)";

    if (traceInfo.TimestampInfo.Mode == TTimestampInfo::ModeDisabled) {
        return;
    }

    if (traceInfo.TimestampInfo.Mode != TTimestampInfo::ModeDefault) {
        hrefBase << "&TMode=" << traceInfo.TimestampInfo.Mode;
    }

    str << R"(
<div class="dropdown-block">
<div class="dropdown">
<button class="btn btn-primary dropdown-toggle" id="timeprecision" type="button" data-toggle="dropdown">)";
    str << TTimestampInfo::GetTextPrecision(traceInfo.TimestampInfo.Precision);
    str << R"(<span class="caret"></span></button>
<ul class="dropdown-menu" role="menu" aria-labelledby="timeprecision">
)";
    str << hrefBase << "&TPrecision=" << TTimestampInfo::PrecisionSeconds << "\">Seconds</a></li>" << Endl;
    str << hrefBase << "&TPrecision=" << TTimestampInfo::PrecisionMilliseconds << "\">Milliseconds</a></li>" << Endl;
    str << hrefBase << "&TPrecision=" << TTimestampInfo::PrecisionMicroseconds << "\">Microseconds</a></li>" << Endl;
    str << R"(</ul>
</div>
</div>
)";
}

}
}
}
