#include "client.h"
#include "protocol.h"

#include <library/cpp/clickhouse/client/base/coded.h>
#include <library/cpp/clickhouse/client/base/compressed.h>
#include <library/cpp/clickhouse/client/base/wire_format.h>
#include <library/cpp/clickhouse/client/columns/factory.h>
#include <library/cpp/openssl/io/stream.h>

#include <util/generic/buffer.h>
#include <util/generic/vector.h>
#include <util/network/socket.h>
#include <util/random/random.h>
#include <util/stream/buffered.h>
#include <util/stream/buffer.h>
#include <util/stream/mem.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/unaligned_mem.h>

#include <contrib/libs/lz4/lz4.h>
#include <contrib/restricted/cityhash-1.0.2/city.h>

#define DBMS_NAME "ClickHouse"
#define DBMS_VERSION_MAJOR 1
#define DBMS_VERSION_MINOR 1
#define REVISION 54126

#define DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES 50264
#define DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS 51554
#define DBMS_MIN_REVISION_WITH_BLOCK_INFO 51903
#define DBMS_MIN_REVISION_WITH_CLIENT_INFO 54032
#define DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE 54058
#define DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO 54060

namespace NClickHouse {
    struct TClientInfo {
        ui8 IfaceType = 1; // TCP
        ui8 QueryKind;
        TString InitialUser;
        TString InitialQueryId;
        TString QuotaKey;
        TString OsUser;
        TString ClientHostname;
        TString ClientName;
        TString InitialAddress = "[::ffff:127.0.0.1]:0";
        ui64 ClientVersionMajor = 0;
        ui64 ClientVersionMinor = 0;
        ui32 ClientRevision = 0;
    };

    struct TServerInfo {
        TString Name;
        TString Timezone;
        ui64 VersionMajor;
        ui64 VersionMinor;
        ui64 Revision;
    };

    class TClient::TImpl {
    public:
        TImpl(const TClientOptions& opts);
        ~TImpl();

        void ExecuteQuery(TQuery query);

        void Insert(const TString& table_name, const TBlock& block, const TString& query_id, const TString& deduplication_token);

        void Ping();

        void ResetConnection();

    private:
        bool Handshake();

        bool ReceivePacket(ui64* server_packet = nullptr);

        void SendQuery(const TString& query, const TString& query_id, const TString& deduplication_token = "");

        void SendData(const TBlock& block);

        bool SendHello();

        bool ReadBlock(TBlock* block, TCodedInputStream* input);

        bool ReceiveHello();

        /// Reads data packet form input stream.
        bool ReceiveData();

        /// Reads exception packet form input stream.
        bool ReceiveException(bool rethrow = false);

        void WriteBlock(const TBlock& block, TCodedOutputStream* output);

    private:
        void Disconnect() {
            Socket_ = TSocket();
        }

        /// In case of network errors tries to reconnect to server and
        /// call fuc several times.
        void RetryGuard(std::function<void()> fuc);

    private:
        class EnsureNull {
        public:
            inline EnsureNull(TQueryEvents* ev, TQueryEvents** ptr)
                : ptr_(ptr)
            {
                if (ptr_) {
                    *ptr_ = ev;
                }
            }

            inline ~EnsureNull() {
                if (ptr_) {
                    *ptr_ = nullptr;
                }
            }

        private:
            TQueryEvents** ptr_;
        };

        const TClientOptions Options_;
        TQueryEvents* Events_;
        int Compression_ = CompressionState::Disable;

        TSocket Socket_;

        TSocketInput SocketInput_;
        TSocketOutput SocketOutput_;
        THolder<TBufferedInput> BufferedInput_;
        THolder<TBufferedOutput> BufferedOutput_;
        THolder<TOpenSslClientIO> SslClient_;

        TCodedInputStream Input_;
        TCodedOutputStream Output_;

        TServerInfo ServerInfo_;
    };

    TClient::TImpl::TImpl(const TClientOptions& opts)
        : Options_(opts)
        , Events_(nullptr)
        , Socket_(TNetworkAddress(opts.Host, opts.Port), Options_.ConnectTimeout)
        , SocketInput_(Socket_)
        , SocketOutput_(Socket_)
    {
        if (opts.UseSsl) {
            SslClient_ = MakeHolder<TOpenSslClientIO>(&SocketInput_, &SocketOutput_, opts.SslOptions);
            BufferedInput_ = MakeHolder<TBufferedInput>(SslClient_.Get());
            BufferedOutput_ = MakeHolder<TBufferedOutput>(SslClient_.Get());
        } else {
            BufferedInput_ = MakeHolder<TBufferedInput>(&SocketInput_);
            BufferedOutput_ = MakeHolder<TBufferedOutput>(&SocketOutput_);
        }
        Input_ = TCodedInputStream(BufferedInput_.Get());
        Output_ = TCodedOutputStream(BufferedOutput_.Get());

        if (Options_.RequestTimeout.Seconds()) {
            Socket_.SetSocketTimeout(Options_.RequestTimeout.Seconds());
        }

        if (!Handshake()) {
            ythrow yexception() << "fail to connect to " << Options_.Host;
        }

        if (Options_.CompressionMethod != ECompressionMethod::None) {
            Compression_ = CompressionState::Enable;
        }
    }

    TClient::TImpl::~TImpl() {
        Disconnect();
    }

    void TClient::TImpl::ExecuteQuery(TQuery query) {
        EnsureNull en(static_cast<TQueryEvents*>(&query), &Events_);

        if (Options_.PingBeforeQuery) {
            RetryGuard([this]() { Ping(); });
        }

        SendQuery(query.GetText(), query.GetId());

        ui64 server_packet = 0;
        while (ReceivePacket(&server_packet)) {
            ;
        }
        if (server_packet != ServerCodes::EndOfStream && server_packet != ServerCodes::Exception) {
            ythrow yexception() << "unexpected packet from server while receiving end of query (got: "
                                << (server_packet ? ToString(server_packet) : "nothing") << ")";
        }
    }

    void TClient::TImpl::Insert(const TString& table_name, const TBlock& block, const TString& query_id, const TString& deduplication_token) {
        if (Options_.PingBeforeQuery) {
            RetryGuard([this]() { Ping(); });
        }
        TVector<TString> fields;
        fields.reserve(block.GetColumnCount());

        // Enumerate all fields
        for (TBlock::TIterator bi(block); bi.IsValid(); bi.Next()) {
            fields.push_back(bi.Name());
        }

        TStringBuilder fields_section;
        for (auto elem = fields.begin(); elem != fields.end(); ++elem) {
            if (std::distance(elem, fields.end()) == 1) {
                fields_section << *elem;
            } else {
                fields_section << *elem << ",";
            }
        }

        SendQuery("INSERT INTO " + table_name + " ( " + fields_section + " ) VALUES", query_id, deduplication_token);

        ui64 server_packet(0);
        // Receive data packet.
        while (true) {
            bool ret = ReceivePacket(&server_packet);

            if (!ret) {
                ythrow yexception() << "unable to receive data packet";
            }
            if (server_packet == ServerCodes::Data) {
                break;
            }
            if (server_packet == ServerCodes::Progress) {
                continue;
            }
        }

        // Send data.
        SendData(block);
        // Send empty block as marker of
        // end of data.
        SendData(TBlock());

        // Wait for EOS.
        ui64 eos_packet{0};
        while (ReceivePacket(&eos_packet)) {
            ;
        }

        if (eos_packet != ServerCodes::EndOfStream && eos_packet != ServerCodes::Exception
            && eos_packet != ServerCodes::Log && Options_.RethrowExceptions) {
            ythrow yexception() << "unexpected packet from server while receiving end of query, expected (expected Exception, EndOfStream or Log, got: "
                                << (eos_packet ? ToString(eos_packet) : "nothing") << ")";
        }
    }

    void TClient::TImpl::Ping() {
        TWireFormat::WriteUInt64(&Output_, ClientCodes::Ping);
        Output_.Flush();

        ui64 server_packet;
        const bool ret = ReceivePacket(&server_packet);

        if (!ret || server_packet != ServerCodes::Pong) {
            ythrow yexception() << "fail to ping server";
        }
    }

    void TClient::TImpl::ResetConnection() {
        Socket_ = TSocket(TNetworkAddress(Options_.Host, Options_.Port), Options_.ConnectTimeout);

        if (Options_.UseSsl) {
            SslClient_.Reset(new TOpenSslClientIO(&SocketInput_, &SocketOutput_, Options_.SslOptions));
            BufferedInput_.Reset(new TBufferedInput(SslClient_.Get()));
            BufferedOutput_.Reset(new TBufferedOutput(SslClient_.Get()));
        } else {
            BufferedInput_.Reset(new TBufferedInput(&SocketInput_));
            BufferedOutput_.Reset(new TBufferedOutput(&SocketOutput_));
        }

        SocketInput_ = TSocketInput(Socket_);
        SocketOutput_ = TSocketOutput(Socket_);

        Input_ = TCodedInputStream(BufferedInput_.Get());
        Output_ = TCodedOutputStream(BufferedOutput_.Get());

        if (Options_.RequestTimeout.Seconds()) {
            Socket_.SetSocketTimeout(Options_.RequestTimeout.Seconds());
        }

        if (!Handshake()) {
            ythrow yexception() << "fail to connect to " << Options_.Host;
        }
    }

    bool TClient::TImpl::Handshake() {
        if (!SendHello()) {
            return false;
        }
        if (!ReceiveHello()) {
            return false;
        }
        return true;
    }

    bool TClient::TImpl::ReceivePacket(ui64* server_packet) {
        ui64 packet_type = 0;

        if (!Input_.ReadVarint64(&packet_type)) {
            return false;
        }
        if (server_packet) {
            *server_packet = packet_type;
        }

        switch (packet_type) {
            case ServerCodes::Totals:
            case ServerCodes::Data: {
                if (!ReceiveData()) {
                    ythrow yexception() << "can't read data packet from input stream";
                }
                return true;
            }

            case ServerCodes::Exception: {
                ReceiveException();
                return false;
            }

            case ServerCodes::ProfileInfo: {
                TProfile profile;

                if (!TWireFormat::ReadUInt64(&Input_, &profile.rows)) {
                    return false;
                }
                if (!TWireFormat::ReadUInt64(&Input_, &profile.blocks)) {
                    return false;
                }
                if (!TWireFormat::ReadUInt64(&Input_, &profile.bytes)) {
                    return false;
                }
                if (!TWireFormat::ReadFixed(&Input_, &profile.applied_limit)) {
                    return false;
                }
                if (!TWireFormat::ReadUInt64(&Input_, &profile.rows_before_limit)) {
                    return false;
                }
                if (!TWireFormat::ReadFixed(&Input_, &profile.calculated_rows_before_limit)) {
                    return false;
                }

                if (Events_) {
                    Events_->OnProfile(profile);
                }

                return true;
            }

            case ServerCodes::Progress: {
                TProgress info;

                if (!TWireFormat::ReadUInt64(&Input_, &info.rows)) {
                    return false;
                }
                if (!TWireFormat::ReadUInt64(&Input_, &info.bytes)) {
                    return false;
                }
                if (REVISION >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS) {
                    if (!TWireFormat::ReadUInt64(&Input_, &info.total_rows)) {
                        return false;
                    }
                }

                if (Events_) {
                    Events_->OnProgress(info);
                }

                return true;
            }

            case ServerCodes::Pong: {
                return true;
            }

            case ServerCodes::EndOfStream: {
                if (Events_) {
                    Events_->OnFinish();
                }
                return false;
            }

            default:
                ythrow yexception() << "unimplemented " << (int)packet_type;
                break;
        }

        return false;
    }

    bool TClient::TImpl::ReadBlock(TBlock* block, TCodedInputStream* input) {
        // Additional information about block.
        if (REVISION >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
            ui64 num;
            TBlockInfo info;

            // BlockInfo
            if (!TWireFormat::ReadUInt64(input, &num)) {
                return false;
            }
            if (!TWireFormat::ReadFixed(input, &info.IsOverflows)) {
                return false;
            }
            if (!TWireFormat::ReadUInt64(input, &num)) {
                return false;
            }
            if (!TWireFormat::ReadFixed(input, &info.BucketNum)) {
                return false;
            }
            if (!TWireFormat::ReadUInt64(input, &num)) {
                return false;
            }

            // TODO use data
        }

        ui64 num_columns = 0;
        ui64 num_rows = 0;

        if (!TWireFormat::ReadUInt64(input, &num_columns)) {
            return false;
        }
        if (!TWireFormat::ReadUInt64(input, &num_rows)) {
            return false;
        }

        for (size_t i = 0; i < num_columns; ++i) {
            TString name;
            TString type;

            if (!TWireFormat::ReadString(input, &name)) {
                return false;
            }
            if (!TWireFormat::ReadString(input, &type)) {
                return false;
            }

            if (TColumnRef col = CreateColumnByType(type)) {
                if (num_rows && !col->Load(input, num_rows)) {
                    ythrow yexception() << "can't load";
                }

                block->AppendColumn(name, col);
            } else {
                ythrow yexception() << "unsupported column type: " << type;
            }
        }

        return true;
    }

    bool TClient::TImpl::ReceiveData() {
        TBlock block;

        if (REVISION >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
            TString table_name;

            if (!TWireFormat::ReadString(&Input_, &table_name)) {
                return false;
            }
        }

        if (Compression_ == CompressionState::Enable) {
            TCompressedInput compressed(&Input_);
            TCodedInputStream coded(&compressed);

            if (!ReadBlock(&block, &coded)) {
                return false;
            }
        } else {
            if (!ReadBlock(&block, &Input_)) {
                return false;
            }
        }

        if (Events_) {
            Events_->OnData(block);
        }

        return true;
    }

    bool TClient::TImpl::ReceiveException(bool rethrow) {
        std::unique_ptr<TException> e(new TException);
        TException* current = e.get();

        bool exception_received = true;
        do {
            bool has_nested = false;

            if (!TWireFormat::ReadFixed(&Input_, &current->Code)) {
                exception_received = false;
                break;
            }
            if (!TWireFormat::ReadString(&Input_, &current->Name)) {
                exception_received = false;
                break;
            }
            if (!TWireFormat::ReadString(&Input_, &current->DisplayText)) {
                exception_received = false;
                break;
            }
            if (!TWireFormat::ReadString(&Input_, &current->StackTrace)) {
                exception_received = false;
                break;
            }
            if (!TWireFormat::ReadFixed(&Input_, &has_nested)) {
                exception_received = false;
                break;
            }

            if (has_nested) {
                current->Nested.reset(new TException);
                current = current->Nested.get();
            } else {
                break;
            }
        } while (true);

        if (Events_) {
            Events_->OnServerException(*e);
        }

        if (rethrow || Options_.RethrowExceptions) {
            throw TServerException(std::move(e));
        }

        return exception_received;
    }

    void TClient::TImpl::SendQuery(const TString& query, const TString& query_id, const TString& deduplication_token) {
        TWireFormat::WriteUInt64(&Output_, ClientCodes::Query);
        TWireFormat::WriteString(&Output_, query_id);

        /// Client info.
        if (ServerInfo_.Revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO) {
            TClientInfo info;

            info.QueryKind = 1;
            info.ClientName = "ClickHouse client";
            info.ClientVersionMajor = DBMS_VERSION_MAJOR;
            info.ClientVersionMinor = DBMS_VERSION_MINOR;
            info.ClientRevision = REVISION;

            TWireFormat::WriteFixed(&Output_, info.QueryKind);
            TWireFormat::WriteString(&Output_, info.InitialUser);
            TWireFormat::WriteString(&Output_, info.InitialQueryId);
            TWireFormat::WriteString(&Output_, info.InitialAddress);
            TWireFormat::WriteFixed(&Output_, info.IfaceType);

            TWireFormat::WriteString(&Output_, info.OsUser);
            TWireFormat::WriteString(&Output_, info.ClientHostname);
            TWireFormat::WriteString(&Output_, info.ClientName);
            TWireFormat::WriteUInt64(&Output_, info.ClientVersionMajor);
            TWireFormat::WriteUInt64(&Output_, info.ClientVersionMinor);
            TWireFormat::WriteUInt64(&Output_, info.ClientRevision);

            if (ServerInfo_.Revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO)
                TWireFormat::WriteString(&Output_, info.QuotaKey);
        }

        if (!deduplication_token.empty()) {
            static const TString insert_deduplication_token_setting_name = "insert_deduplication_token";
            TWireFormat::WriteString(&Output_, insert_deduplication_token_setting_name);
            TWireFormat::WriteString(&Output_, deduplication_token);
        }
        TWireFormat::WriteString(&Output_, TString()); // Empty string is a marker of end SETTINGS section

        TWireFormat::WriteUInt64(&Output_, Stages::Complete);
        TWireFormat::WriteUInt64(&Output_, Compression_);
        TWireFormat::WriteString(&Output_, query);
        // Send empty block as marker of
        // end of data
        SendData(TBlock());

        Output_.Flush();
    }

    void TClient::TImpl::WriteBlock(const TBlock& block, TCodedOutputStream* output) {
        /// Дополнительная информация о блоке.
        if (ServerInfo_.Revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO) {
            TWireFormat::WriteUInt64(output, 1);
            TWireFormat::WriteFixed(output, block.Info().IsOverflows);
            TWireFormat::WriteUInt64(output, 2);
            TWireFormat::WriteFixed(output, block.Info().BucketNum);
            TWireFormat::WriteUInt64(output, 0);
        }

        TWireFormat::WriteUInt64(output, block.GetColumnCount());
        TWireFormat::WriteUInt64(output, block.GetRowCount());

        for (TBlock::TIterator bi(block); bi.IsValid(); bi.Next()) {
            TWireFormat::WriteString(output, bi.Name());
            TWireFormat::WriteString(output, bi.Type()->GetName());

            bi.Column()->Save(output);
        }
    }

    void TClient::TImpl::SendData(const TBlock& block) {
        TWireFormat::WriteUInt64(&Output_, ClientCodes::Data);

        if (ServerInfo_.Revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES) {
            TWireFormat::WriteString(&Output_, TString());
        }

        if (Compression_ == CompressionState::Enable) {
            switch (Options_.CompressionMethod) {
                case ECompressionMethod::None: {
                    Y_ABORT_UNLESS(false, "invalid state");
                    break;
                }

                case ECompressionMethod::LZ4: {
                    TBufferOutput tmp;

                    // Serialize block's data
                    {
                        TCodedOutputStream out(&tmp);
                        WriteBlock(block, &out);
                    }
                    // Reserver space for data
                    TBuffer buf;
                    buf.Resize(9 + LZ4_compressBound(tmp.Buffer().Size()));

                    // Compress data
                    int size = LZ4_compress(tmp.Buffer().Data(), buf.Data() + 9, tmp.Buffer().Size());
                    buf.Resize(9 + size);

                    // Fill header
                    ui8* p = (ui8*)buf.Data();
                    // Compression method
                    WriteUnaligned<ui8>(p, (ui8)0x82);
                    p += 1;
                    // Compressed data size with header
                    WriteUnaligned<ui32>(p, (ui32)buf.Size());
                    p += 4;
                    // Original data size
                    WriteUnaligned<ui32>(p, (ui32)tmp.Buffer().Size());

                    TWireFormat::WriteFixed(&Output_, CityHash_v1_0_2::CityHash128(
                                                          buf.Data(), buf.Size()));
                    TWireFormat::WriteBytes(&Output_, buf.Data(), buf.Size());
                    break;
                }
            }
        } else {
            WriteBlock(block, &Output_);
        }

        Output_.Flush();
    }

    bool TClient::TImpl::SendHello() {
        TWireFormat::WriteUInt64(&Output_, ClientCodes::Hello);
        TWireFormat::WriteString(&Output_, TString(DBMS_NAME) + " client");
        TWireFormat::WriteUInt64(&Output_, DBMS_VERSION_MAJOR);
        TWireFormat::WriteUInt64(&Output_, DBMS_VERSION_MINOR);
        TWireFormat::WriteUInt64(&Output_, REVISION);
        TWireFormat::WriteString(&Output_, Options_.DefaultDatabase);
        TWireFormat::WriteString(&Output_, Options_.User);
        TWireFormat::WriteString(&Output_, Options_.Password);

        Output_.Flush();

        return true;
    }

    bool TClient::TImpl::ReceiveHello() {
        ui64 packet_type = 0;

        if (!Input_.ReadVarint64(&packet_type)) {
            return false;
        }

        if (packet_type == ServerCodes::Hello) {
            if (!TWireFormat::ReadString(&Input_, &ServerInfo_.Name)) {
                return false;
            }
            if (!TWireFormat::ReadUInt64(&Input_, &ServerInfo_.VersionMajor)) {
                return false;
            }
            if (!TWireFormat::ReadUInt64(&Input_, &ServerInfo_.VersionMinor)) {
                return false;
            }
            if (!TWireFormat::ReadUInt64(&Input_, &ServerInfo_.Revision)) {
                return false;
            }

            if (ServerInfo_.Revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE) {
                if (!TWireFormat::ReadString(&Input_, &ServerInfo_.Timezone)) {
                    return false;
                }
            }

            return true;
        } else if (packet_type == ServerCodes::Exception) {
            ReceiveException(true);
            return false;
        }

        return false;
    }

    void TClient::TImpl::RetryGuard(std::function<void()> func) {
        for (int i = 0; i <= Options_.SendRetries; ++i) {
            try {
                func();
                return;
            } catch (const yexception&) {
                bool ok = true;

                try {
                    Sleep(Options_.RetryTimeout);
                    ResetConnection();
                } catch (...) {
                    ok = false;
                }

                if (!ok) {
                    throw;
                }
            }
        }
    }

    TClient::TClient(const TClientOptions& opts)
        : Options_(opts)
        , Impl_(new TImpl(opts))
    {
    }

    TClient::~TClient() {
    }

    void TClient::Execute(const TQuery& query) {
        Impl_->ExecuteQuery(query);
    }

    void TClient::Select(const TString& query, TSelectCallback cb, const TString& query_id) {
        Execute(TQuery(query, query_id).OnData(cb));
    }

    void TClient::Select(const TQuery& query) {
        Execute(query);
    }

    void TClient::Insert(const TString& table_name, const TBlock& block, const TString& query_id, const TString& deduplication_token) {
        Impl_->Insert(table_name, block, query_id, deduplication_token);
    }

    void TClient::Ping() {
        Impl_->Ping();
    }

    void TClient::ResetConnection() {
        Impl_->ResetConnection();
    }

}
