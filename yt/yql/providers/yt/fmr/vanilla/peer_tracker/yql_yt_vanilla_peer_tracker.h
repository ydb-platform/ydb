#pragma once

#include <yql/essentials/utils/runnable.h>

#include <yt/cpp/mapreduce/interface/fwd.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/ysaveload.h>

#include <atomic>

namespace NYql::NFmr {

    ////////////////////////////////////////////////////////////////////////////////

    struct TVanillaPeerTrackerSettings {
        TString Cluster;
        ui64 JobCount = 1;
        TDuration ListJobsInterval = TDuration::Seconds(1);
        TDuration PingClientInterval = TDuration::Seconds(1);
        TDuration PingTimeout = TDuration::Seconds(5);
    };

    class IVanillaExternalPeerTracker : public virtual TThrRefBase {
    public:
        virtual ~IVanillaExternalPeerTracker() = default;

        virtual TString GetOperationId() const = 0;
        virtual ui64 GetPeerCount() const = 0;
        // IP address may be empty
        virtual TString GetPeerAddress(ui64 index) const = 0;
        // some IP addresses may be empty
        virtual TVector<TString> GetPeerAddresses() const = 0;
    };

    using IVanillaExternalPeerTrackerPtr = TIntrusivePtr<IVanillaExternalPeerTracker>;

    class IVanillaPeerTracker : public IVanillaExternalPeerTracker {
    public:
        virtual ui64 GetSelfIndex() const = 0;
        virtual TString GetSelfJobId() const = 0;
        virtual TString GetSelfIpAddress() const = 0;
    };

    using IVanillaPeerTrackerPtr = TIntrusivePtr<IVanillaPeerTracker>;

    class TVanillaPeerTracker : public IVanillaPeerTracker {
    public:
        explicit TVanillaPeerTracker(TVanillaPeerTrackerSettings settings);

        TString GetOperationId() const final;
        ui64 GetSelfIndex() const final;
        TString GetSelfJobId() const final;
        TString GetSelfIpAddress() const final;
        ui64 GetPeerCount() const final;
        // IP address may be empty
        TString GetPeerAddress(ui64 index) const final;
        // some IP addresses may be empty
        TVector<TString> GetPeerAddresses() const final;

        // this method should be called once inside Do method
        // it returns only if this job should exit
        void Run();

        // Lists jobs in the given operation, prints the job with cookie 0.
        // If withPing is true, also pings that job's IP and reports the result.
        static void CheckOperation(
            const TString& cluster,
            const TString& operationId,
            bool withPing = false,
            TDuration pingTimeout = TDuration::Seconds(5));

    private:
        const TVanillaPeerTrackerSettings Settings_;
        const ui64 SelfCookie_;
        const TString SelfJobId_;
        const TString SelfIpAddress_;
        const TString OperationId_;
        mutable TMutex PeersMutex_;
        TVector<TString> PeerIps_;
        std::atomic<bool> Shutdown_{false};
        THolder<TThread> ServerThread_;
        THolder<TThread> ClientThread_;
    };

    struct TStaticVanillaPeerTrackerSettings {
        TString OperationId;
        ui64 SelfIndex = 0;
        TString SelfJobId;
        TVector<TString> PeerIps;

        void Save(IOutputStream* s) const {
            ::SaveMany(s,
                OperationId,
                SelfIndex,
                SelfJobId,
                PeerIps
            );
        }

        void Load(IInputStream* s) {
            ::LoadMany(s,
                OperationId,
                SelfIndex,
                SelfJobId,
                PeerIps
            );
        }
    };

    class TStaticVanillaPeerTracker : public IVanillaPeerTracker {
    public:
        TStaticVanillaPeerTracker(TStaticVanillaPeerTrackerSettings settings)
            : Settings_(std::move(settings))
        {}

        TString GetOperationId() const final {
            return Settings_.OperationId;
        }

        ui64 GetSelfIndex() const final {
            return Settings_.SelfIndex;
        }

        TString GetSelfJobId() const final {
            return Settings_.SelfJobId;
        }

        TString GetSelfIpAddress() const final {
            return Settings_.PeerIps[Settings_.SelfIndex];
        }

        ui64 GetPeerCount() const final {
            return Settings_.PeerIps.size();
        }

        // IP address may be empty
        TString GetPeerAddress(ui64 index) const final {
            Y_ENSURE(index < Settings_.PeerIps.size());
            return Settings_.PeerIps[index];
        }

        // some IP addresses may be empty
        TVector<TString> GetPeerAddresses() const final {
            return Settings_.PeerIps;
        }

    private:
        const TStaticVanillaPeerTrackerSettings Settings_;
    };

    struct TVanillaExternalPeerTrackerSettings {
        TString Cluster;
        TString OperationId;
        TMaybe<TString> Token;
        TDuration ListJobsInterval = TDuration::Seconds(1);
        ui64 MaxFails = 3;
    };

    // Tracks peers of a vanilla operation from the outside (not from inside a job).
    // Start() launches a background thread that first fetches job count from the
    // operation spec (retrying on failure), then periodically refreshes peer IPs
    // via ListJobs. Peer getters block until job count is known.
    class TVanillaExternalPeerTracker
        : public IVanillaExternalPeerTracker
        , public IRunnable
    {
    public:
        explicit TVanillaExternalPeerTracker(TVanillaExternalPeerTrackerSettings settings);
        ~TVanillaExternalPeerTracker();

        // IVanillaExternalPeerTracker
        // Y_ENSURE the tracker is running; block until job count is fetched.
        TString GetOperationId() const override;
        ui64 GetPeerCount() const override;
        TString GetPeerAddress(ui64 index) const override;
        TVector<TString> GetPeerAddresses() const override;

        // IRunnable
        void Start() override;
        void Stop() override;
 private:
        // Must be called with PeersMutex_ held.
        void WaitForJobCount() const;
        ui64 FetchJobCount(const NYT::IClientPtr& client) const;
        void RefreshLoop();
        void Refresh(const NYT::IClientPtr& client);

        const TVanillaExternalPeerTrackerSettings Settings_;
        mutable TMutex PeersMutex_;
        bool Running_ = false;    // protected by PeersMutex_
        ui64 JobCount_ = 0;       // protected by PeersMutex_, written once by RefreshLoop
        mutable TCondVar JobCountReady_;  // signalled when JobCount_ is set or Running_ becomes false
        TVector<TString> PeerIps_; // protected by PeersMutex_
        ui64 Fails_ = 0; // protected by PeersMutex_
        TMaybe<TString> LastError_; // protected by PeersMutex_
        std::atomic<bool> Shutdown_{false};
        THolder<TThread> RefreshThread_;
    };

    ////////////////////////////////////////////////////////////////////////////////

} // namespace NYql::NFmr
