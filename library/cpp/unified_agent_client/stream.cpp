#include "stream.h"

namespace NUnifiedAgent {
    namespace {
        class TDefaultStreamRecordConverter : public IStreamRecordConverter {
        public:
            TDefaultStreamRecordConverter(bool stripTrailingNewLine)
                : StripTrailingNewLine(stripTrailingNewLine)
            {
            }

            TClientMessage Convert(const void* buf, size_t len) const override {
                TStringBuf str(static_cast<const char*>(buf), len);
                if (StripTrailingNewLine) {
                    str.ChopSuffix("\n");
                }
                return {
                    TString(str),
                    {}
                };
            }

        private:
            const bool StripTrailingNewLine;
        };

        class TClientSessionAdapter: public IOutputStream {
        public:
            explicit TClientSessionAdapter(const TClientSessionPtr& session, THolder<IStreamRecordConverter> recordConverter)
                : Session(session)
                , RecordConverter(std::move(recordConverter))
            {
            }

            void DoWrite(const void* buf, size_t len) override {
                Session->Send(RecordConverter->Convert(buf, len));
            }

            void DoFlush() override {
            }

        private:
            TClientSessionPtr Session;
            THolder<IStreamRecordConverter> RecordConverter;
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

        class TAgentOutputStream: private TSessionHolder, public TClientSessionAdapter {
        public:
            TAgentOutputStream(const TClientParameters& parameters,
                const TSessionParameters& sessionParameters,
                THolder<IStreamRecordConverter> recordConverter)
                : TSessionHolder(parameters, sessionParameters)
                , TClientSessionAdapter(TSessionHolder::Session, std::move(recordConverter))
            {
            }

            ~TAgentOutputStream() override {
                TSessionHolder::Session->Close();
            }
        };
    }

    THolder<IStreamRecordConverter> MakeDefaultStreamRecordConverter(bool stripTrailingNewLine) {
        return MakeHolder<TDefaultStreamRecordConverter>(stripTrailingNewLine);
    }

    THolder<IOutputStream> MakeOutputStream(const TClientParameters& parameters,
        const TSessionParameters& sessionParameters,
        THolder<IStreamRecordConverter> recordConverter)
    {
        if (!recordConverter) {
            recordConverter = MakeDefaultStreamRecordConverter();
        }
        return MakeHolder<TAgentOutputStream>(parameters, sessionParameters, std::move(recordConverter));
    }
}
