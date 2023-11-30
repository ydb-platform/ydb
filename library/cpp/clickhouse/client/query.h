#pragma once

#include "block.h"

#include <util/generic/string.h>

#include <cstdint>
#include <functional>
#include <memory>

namespace NClickHouse {
    /**
 * Settings of individual query.
 */
    struct TQuerySettings {
        /// Максимальное количество потоков выполнения запроса. По-умолчанию - определять автоматически.
        int MaxThreads = 0;
        /// Считать минимумы и максимумы столбцов результата.
        bool Extremes = false;
        /// Тихо пропускать недоступные шарды.
        bool SkipUnavailableShards = false;
        /// Write statistics about read rows, bytes, time elapsed, etc.
        bool OutputFormatWriteStatistics = true;
        /// Use client timezone for interpreting DateTime string values, instead of adopting server timezone.
        bool UseClientTimeZone = false;

        // connect_timeout
        // max_block_size
        // distributed_group_by_no_merge = false
        // strict_insert_defaults = 0
        // network_compression_method = LZ4
        // priority = 0
    };

    struct TException {
        int Code = 0;
        TString Name;
        TString DisplayText;
        TString StackTrace;
        /// Pointer to nested exception.
        std::unique_ptr<TException> Nested;
    };

    struct TProfile {
        ui64 rows = 0;
        ui64 blocks = 0;
        ui64 bytes = 0;
        ui64 rows_before_limit = 0;
        bool applied_limit = false;
        bool calculated_rows_before_limit = false;
    };

    struct TProgress {
        ui64 rows = 0;
        ui64 bytes = 0;
        ui64 total_rows = 0;
    };

    class TQueryEvents {
    public:
        virtual ~TQueryEvents() {
        }

        /// Some data was received.
        virtual void OnData(const TBlock& block) = 0;

        virtual void OnServerException(const TException& e) = 0;

        virtual void OnProfile(const TProfile& profile) = 0;

        virtual void OnProgress(const TProgress& progress) = 0;

        virtual void OnFinish() = 0;
    };

    using TExceptionCallback = std::function<void(const TException& e)>;
    using TProfileCallback = std::function<void(const TProfile& profile)>;
    using TProgressCallback = std::function<void(const TProgress& progress)>;
    using TSelectCallback = std::function<void(const TBlock& block)>;

    class TQuery: public TQueryEvents {
    public:
        TQuery();
        TQuery(const char* query);
        TQuery(const TString& query);
        ~TQuery();

        ///
        inline TString GetText() const {
            return Query_;
        }

        /// Set handler for receiving result data.
        inline TQuery& OnData(TSelectCallback cb) {
            SelectCb_ = cb;
            return *this;
        }

        /// Set handler for receiving server's exception.
        inline TQuery& OnException(TExceptionCallback cb) {
            ExceptionCb_ = cb;
            return *this;
        }

        /// Set handler for receiving a profile of query execution.
        inline TQuery& OnProfile(TProfileCallback pb) {
            ProfileCb_ = pb;
            return *this;
        }

        /// Set handler for receiving a progress of query exceution.
        inline TQuery& OnProgress(TProgressCallback cb) {
            ProgressCb_ = cb;
            return *this;
        }

    private:
        void OnData(const TBlock& block) override {
            if (SelectCb_) {
                SelectCb_(block);
            }
        }

        void OnServerException(const TException& e) override {
            if (ExceptionCb_) {
                ExceptionCb_(e);
            }
        }

        void OnProfile(const TProfile& profile) override {
            if (ProfileCb_) {
                ProfileCb_(profile);
            }
        }

        void OnProgress(const TProgress& progress) override {
            if (ProgressCb_) {
                ProgressCb_(progress);
            }
        }

        void OnFinish() override {
        }

    private:
        TString Query_;
        TExceptionCallback ExceptionCb_;
        TProfileCallback ProfileCb_;
        TProgressCallback ProgressCb_;
        TSelectCallback SelectCb_;
    };

}
