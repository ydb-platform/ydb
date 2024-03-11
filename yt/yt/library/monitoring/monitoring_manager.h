#pragma once

#include "public.h"

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/producer.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NMonitoring {

////////////////////////////////////////////////////////////////////////////////

//! Exposes a tree assembled from results returned by a set of
//! registered NYson::TYsonProducer-s.
/*!
 *  \note
 *  The results are cached and periodically updated.
 */
class TMonitoringManager
    : public TRefCounted
{
public:
    TMonitoringManager();
    ~TMonitoringManager();

    //! Registers a new #producer for a given #path.
    void Register(const NYPath::TYPath& path, NYson::TYsonProducer producer);

    //! Unregisters an existing producer for the specified #path.
    void Unregister(const NYPath::TYPath& path);

    //! Returns the service representing the whole tree.
    /*!
     * \note The service is thread-safe.
     */
    NYTree::IYPathServicePtr GetService();

    //! Starts periodic updates.
    void Start();

    //! Stops periodic updates.
    void Stop();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMonitoring
