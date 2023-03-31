#include "backend.h"

#include <library/cpp/unified_agent_client/enum.h>

#include <library/cpp/logger/record.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/serialized_enum.h>

namespace NUnifiedAgent {
    namespace {
        class TDefaultRecordConverter : public IRecordConverter {
        public:
            TDefaultRecordConverter(bool stripTrailingNewLine)
                : StripTrailingNewLine(stripTrailingNewLine)
                , PriorityKey("_priority")
            {
            }

            TClientMessage Convert(const TLogRecord& rec) const override {
                const auto stripTrailingNewLine = StripTrailingNewLine &&
                    rec.Len > 0 && rec.Data[rec.Len - 1] == '\n';

                THashMap<TString, TString> metaFlags{{PriorityKey, NameOf(rec.Priority)}};
                metaFlags.insert(rec.MetaFlags.begin(), rec.MetaFlags.end());

                return {
                    TString(rec.Data, stripTrailingNewLine ? rec.Len - 1 : rec.Len),
                    std::move(metaFlags)
                };
            }

        private:
            const bool StripTrailingNewLine;
            const TString PriorityKey;
        };

        class TClientSessionAdapter: public TLogBackend {
        public:
            explicit TClientSessionAdapter(const TClientSessionPtr& session, THolder<IRecordConverter> recordConverter)
                : Session(session)
                , RecordConverter(std::move(recordConverter))
            {
            }

            void WriteData(const TLogRecord& rec) override {
                Session->Send(RecordConverter->Convert(rec));
            }

            void ReopenLog() override {
            }

        private:
            TClientSessionPtr Session;
            THolder<IRecordConverter> RecordConverter;
        };

        class TSessionHolder {
        protected:
            TSessionHolder(const TClientParameters& parameters, const TSessionParameters& sessionParameters)
                : Client(MakeClient(parameters))
                , Session(Client->CreateSession(sessionParameters))
            {
            }

        protected:
            TClientPtr Client;
            TClientSessionPtr Session;
        };

        class TAgentLogBackend: private TSessionHolder, public TClientSessionAdapter {
        public:
            TAgentLogBackend(const TClientParameters& parameters,
                const TSessionParameters& sessionParameters,
                THolder<IRecordConverter> recordConverter)
                : TSessionHolder(parameters, sessionParameters)
                , TClientSessionAdapter(TSessionHolder::Session, std::move(recordConverter))
            {
            }

            ~TAgentLogBackend() override {
                TSessionHolder::Session->Close();
            }
        };
    }

    THolder<IRecordConverter> MakeDefaultRecordConverter(bool stripTrailingNewLine) {
        return MakeHolder<TDefaultRecordConverter>(stripTrailingNewLine);
    }

    THolder<TLogBackend> AsLogBackend(const TClientSessionPtr& session, bool stripTrailingNewLine) {
        return MakeHolder<TClientSessionAdapter>(session, MakeDefaultRecordConverter(stripTrailingNewLine));
    }

    THolder<TLogBackend> MakeLogBackend(const TClientParameters& parameters,
        const TSessionParameters& sessionParameters,
        THolder<IRecordConverter> recordConverter)
    {
        if (!recordConverter) {
            recordConverter = MakeDefaultRecordConverter();
        }
        return MakeHolder<TAgentLogBackend>(parameters, sessionParameters, std::move(recordConverter));
    }

    THolder<::TLog> MakeLog(const TClientParameters& parameters,
        const TSessionParameters& sessionParameters,
        THolder<IRecordConverter> recordConverter)
    {
        return MakeHolder<::TLog>(MakeLogBackend(parameters, sessionParameters, std::move(recordConverter)));
    }
}
